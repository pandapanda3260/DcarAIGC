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
from types import TracebackType
from typing import Iterator, Literal


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB = PROJECT_ROOT / "app" / "data" / "dcar_insight.sqlite3"
SCHEMA_VERSION = 16
CURRENT_SCHEMA_MIGRATION_NAME = "remove-manual-review"
SCHEMA_MIGRATION_NAMES = {
    11: "interaction-user-v1-fallback-keys",
    12: "append-only-metric-observations",
    13: "scheduler-run-attempt-history",
    14: "spu-audience-scene-domain",
    15: "spu-llm-assist",
    16: CURRENT_SCHEMA_MIGRATION_NAME,
}
RUNTIME_COMPATIBLE_SCHEMA_VERSIONS = frozenset(SCHEMA_MIGRATION_NAMES)
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


def enable_recursive_triggers(connection: sqlite3.Connection) -> None:
    """Enable the SQLite setting required by append-only DELETE triggers."""

    connection.execute("PRAGMA recursive_triggers = ON")
    if int(connection.execute("PRAGMA recursive_triggers").fetchone()[0]) != 1:
        raise RuntimeError("SQLite recursive triggers could not be enabled")


def configure_connection_safety(connection: sqlite3.Connection) -> None:
    """Enable and verify the connection-local relational safety settings."""

    enable_recursive_triggers(connection)
    connection.execute("PRAGMA foreign_keys = ON")
    if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
        raise RuntimeError("SQLite foreign keys could not be enabled")


def same_database_path(left: Path, right: Path) -> bool:
    """Compare database paths by file identity when both targets exist.

    macOS exposes the same file through both ``/Users`` and
    ``/System/Volumes/Data/Users``.  Those APFS firmlink spellings can retain
    different ``Path.resolve()`` strings, so security decisions must use the
    underlying device/inode identity.  For paths that do not exist yet, retain
    the deterministic canonical-path comparison used by create-time guards.
    """

    left_path = Path(left).expanduser()
    right_path = Path(right).expanduser()
    try:
        left_stat = left_path.stat()
    except (FileNotFoundError, NotADirectoryError):
        left_stat = None
    try:
        right_stat = right_path.stat()
    except (FileNotFoundError, NotADirectoryError):
        right_stat = None
    if left_stat is not None and right_stat is not None:
        return os.path.samestat(left_stat, right_stat)

    # Permission and other I/O failures intentionally propagate instead of
    # turning an unverified alias into a negative security decision.  Only a
    # genuinely missing target may use the deterministic create-time fallback.
    return left_path.resolve(strict=False) == right_path.resolve(strict=False)


def is_formal_database_path(
    path: Path, *, formal_database: Path | None = None
) -> bool:
    """Return whether ``path`` names the formal database, including aliases."""

    return same_database_path(
        path,
        DEFAULT_DB if formal_database is None else formal_database,
    )


class _ClosingSQLiteConnection(sqlite3.Connection):
    """Preserve SQLite transaction semantics and close managed connections.

    ``sqlite3.Connection.__exit__`` commits or rolls back but deliberately
    leaves the file descriptor open.  The application consistently treats
    ``with connect(...)`` as an owned connection scope, so leaving those
    descriptors open eventually exhausts launchd's per-process file limit
    during provider-heavy capture jobs.

    Callers that do not use the connection as a context manager retain the
    normal explicit ``close()`` lifecycle.
    """

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def connect(
    path: Path = DEFAULT_DB, *, read_only: bool | None = None
) -> sqlite3.Connection:
    if (
        os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1"
        and is_formal_database_path(path)
    ):
        raise RuntimeError("test process attempted to open the formal DCar database")
    if read_only is None:
        read_only = os.environ.get("DCAR_READ_ONLY", "0").strip() == "1"
    if read_only:
        if not path.is_file():
            raise RuntimeError(f"read-only SQLite database is missing: {path}")
        connection = sqlite3.connect(
            f"{path.resolve().as_uri()}?mode=ro&immutable=1",
            uri=True,
            timeout=10,
            factory=_ClosingSQLiteConnection,
        )
        connection.row_factory = sqlite3.Row
        try:
            configure_connection_safety(connection)
        except Exception:
            connection.close()
            raise
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(
        path,
        timeout=10,
        factory=_ClosingSQLiteConnection,
    )
    connection.row_factory = sqlite3.Row
    try:
        configure_connection_safety(connection)
    except Exception:
        connection.close()
        raise
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

CREATE INDEX IF NOT EXISTS idx_content_identities_content_primary
ON content_identities(content_id, is_primary DESC, id);

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

CREATE TABLE IF NOT EXISTS content_metric_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    subject_key TEXT NOT NULL,
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
    observation_origin TEXT NOT NULL CHECK(observation_origin IN (
        'provider_capture','legacy_snapshot_baseline','system_correction'
    )),
    legacy_snapshot_id INTEGER UNIQUE,
    observation_sha256 TEXT NOT NULL UNIQUE CHECK(length(observation_sha256)=64),
    recorded_at TEXT NOT NULL,
    CHECK(
        (observation_origin='legacy_snapshot_baseline'
         AND legacy_snapshot_id IS NOT NULL)
        OR
        (observation_origin<>'legacy_snapshot_baseline'
         AND legacy_snapshot_id IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_metric_observations_content_capture
ON content_metric_observations(content_id, captured_at DESC, id DESC);

CREATE TRIGGER IF NOT EXISTS trg_metric_observations_immutable_payload
BEFORE UPDATE OF
    id,subject_key,captured_at,window_key,view_count,comment_count,
    like_count,share_count,collect_count,status,source,raw_response_id,
    metadata_json,observation_origin,legacy_snapshot_id,
    observation_sha256,recorded_at
ON content_metric_observations
BEGIN
    SELECT RAISE(ABORT, 'content metric observation payload is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_metric_observations_no_delete
BEFORE DELETE ON content_metric_observations
BEGIN
    SELECT RAISE(ABORT, 'content metric observations are append-only');
END;

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
    payload_json TEXT NOT NULL,
    evaluated_at TEXT NOT NULL,
    invalidated_at TEXT,
    invalidation_reason TEXT,
    FOREIGN KEY(release_id, rule_version, taxonomy_version, matcher_rule_sha256)
        REFERENCES evaluation_releases(
            id, rule_version, taxonomy_version, matcher_rule_sha256
        ) ON DELETE RESTRICT,
    CHECK(parent_evaluation_id IS NULL OR parent_evaluation_id<>id),
    CHECK(
        (invalidated_at IS NULL AND invalidation_reason IS NULL)
        OR (invalidated_at IS NOT NULL AND invalidation_reason IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_evaluation_automatic_idempotency
ON evaluation_versions(content_id, release_id, evidence_sha256)
WHERE evaluation_source='automatic';
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
    status TEXT NOT NULL
        CHECK(status IN ('running','succeeded','failed','skipped','partial','interrupted')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    details_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(job_id, scheduled_for)
);

CREATE TABLE IF NOT EXISTS scheduler_run_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scheduler_run_id INTEGER NOT NULL
        REFERENCES scheduler_runs(id) ON DELETE RESTRICT,
    attempt_number INTEGER NOT NULL CHECK(attempt_number >= 1),
    invocation_source TEXT NOT NULL CHECK(invocation_source IN (
        'legacy_migration','scheduled','startup_report_catchup','operator_retry'
    )),
    status TEXT NOT NULL
        CHECK(status IN ('running','succeeded','failed','skipped','partial','interrupted')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    details_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(scheduler_run_id, attempt_number),
    CHECK(
        (status='running' AND completed_at IS NULL)
        OR (status!='running' AND completed_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_scheduler_run_attempts_active
ON scheduler_run_attempts(scheduler_run_id)
WHERE status='running';

CREATE TRIGGER IF NOT EXISTS trg_scheduler_run_attempts_terminal_update
BEFORE UPDATE ON scheduler_run_attempts
WHEN NOT (
    OLD.status='running'
    AND OLD.completed_at IS NULL
    AND NEW.status IN ('succeeded','failed','skipped','partial','interrupted')
    AND NEW.completed_at IS NOT NULL
    AND NEW.id IS OLD.id
    AND NEW.scheduler_run_id IS OLD.scheduler_run_id
    AND NEW.attempt_number IS OLD.attempt_number
    AND NEW.invocation_source IS OLD.invocation_source
    AND NEW.started_at IS OLD.started_at
)
BEGIN
    SELECT RAISE(ABORT, 'scheduler attempt permits one running-to-terminal update');
END;

CREATE TRIGGER IF NOT EXISTS trg_scheduler_run_attempts_no_delete
BEFORE DELETE ON scheduler_run_attempts
BEGIN
    SELECT RAISE(ABORT, 'scheduler attempts are append-only');
END;

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

CREATE TABLE IF NOT EXISTS spu_catalog (
    spu_id TEXT PRIMARY KEY,
    brand TEXT NOT NULL,
    series TEXT NOT NULL,
    series_slug TEXT NOT NULL,
    trim_label TEXT,
    is_series_node INTEGER NOT NULL DEFAULT 0 CHECK(is_series_node IN (0,1)),
    model_year INTEGER,
    powertrain TEXT NOT NULL DEFAULT '',
    body_style TEXT NOT NULL DEFAULT '',
    price_low REAL,
    price_high REAL,
    external_ref TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK(
        (is_series_node=1 AND trim_label IS NULL)
        OR (is_series_node=0 AND trim_label IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_spu_catalog_series_node
ON spu_catalog(series_slug) WHERE is_series_node=1;

CREATE TABLE IF NOT EXISTS spu_alias (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alias TEXT NOT NULL,
    alias_type TEXT NOT NULL DEFAULT 'official'
        CHECK(alias_type IN ('official','nickname','slang','model_code')),
    spu_scope TEXT NOT NULL CHECK(spu_scope IN ('series','trim')),
    spu_id TEXT NOT NULL REFERENCES spu_catalog(spu_id) ON DELETE CASCADE,
    ambiguous INTEGER NOT NULL DEFAULT 0 CHECK(ambiguous IN (0,1)),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    UNIQUE(alias, spu_id)
);

CREATE TABLE IF NOT EXISTS audience_dim (
    code TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    definition TEXT NOT NULL DEFAULT '',
    explicit_signals_json TEXT NOT NULL DEFAULT '[]',
    ref_mapping_json TEXT NOT NULL DEFAULT '{}',
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1))
);

CREATE TABLE IF NOT EXISTS scene_dim (
    code TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    definition TEXT NOT NULL DEFAULT '',
    trigger_words_json TEXT NOT NULL DEFAULT '[]',
    negative_words_json TEXT NOT NULL DEFAULT '[]',
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1))
);

CREATE TABLE IF NOT EXISTS spu_audience_map (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope TEXT NOT NULL CHECK(scope IN ('series','trim')),
    scope_key TEXT NOT NULL,
    audience_code TEXT NOT NULL REFERENCES audience_dim(code) ON DELETE RESTRICT,
    role TEXT NOT NULL CHECK(role IN ('primary','secondary')),
    weight REAL NOT NULL DEFAULT 1.0,
    basis TEXT NOT NULL DEFAULT '',
    UNIQUE(scope, scope_key, audience_code)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_spu_audience_primary
ON spu_audience_map(scope, scope_key) WHERE role='primary';

CREATE TABLE IF NOT EXISTS audience_scene_map (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    audience_code TEXT NOT NULL REFERENCES audience_dim(code) ON DELETE RESTRICT,
    scene_code TEXT NOT NULL REFERENCES scene_dim(code) ON DELETE RESTRICT,
    tier TEXT NOT NULL CHECK(tier IN ('core','related')),
    basis TEXT NOT NULL DEFAULT '',
    UNIQUE(audience_code, scene_code)
);

CREATE TABLE IF NOT EXISTS content_spu_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    spu_id TEXT NOT NULL REFERENCES spu_catalog(spu_id) ON DELETE RESTRICT,
    resolved_level TEXT NOT NULL CHECK(resolved_level IN ('series','trim')),
    is_primary INTEGER NOT NULL DEFAULT 0 CHECK(is_primary IN (0,1)),
    status TEXT NOT NULL CHECK(status IN ('confirmed','gray')),
    score INTEGER NOT NULL CHECK(score BETWEEN 0 AND 100),
    evidence_json TEXT NOT NULL DEFAULT '{}',
    rule_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    invalidated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_content_spu_links_content
ON content_spu_links(content_id) WHERE invalidated_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_content_spu_links_spu
ON content_spu_links(spu_id) WHERE invalidated_at IS NULL;

CREATE TABLE IF NOT EXISTS content_scene_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    scene_code TEXT NOT NULL REFERENCES scene_dim(code) ON DELETE RESTRICT,
    score INTEGER NOT NULL CHECK(score BETWEEN 0 AND 100),
    evidence_json TEXT NOT NULL DEFAULT '{}',
    rule_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    invalidated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_content_scene_links_content
ON content_scene_links(content_id) WHERE invalidated_at IS NULL;

CREATE TABLE IF NOT EXISTS content_audience_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    audience_code TEXT NOT NULL REFERENCES audience_dim(code) ON DELETE RESTRICT,
    source TEXT NOT NULL CHECK(source IN ('content_explicit','rule_prior','llm')),
    conflict_flag INTEGER NOT NULL DEFAULT 0 CHECK(conflict_flag IN (0,1)),
    consistency_flag INTEGER NOT NULL DEFAULT 0 CHECK(consistency_flag IN (0,1)),
    evidence_json TEXT NOT NULL DEFAULT '{}',
    rule_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    invalidated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_content_audience_links_content
ON content_audience_links(content_id) WHERE invalidated_at IS NULL;

CREATE TABLE IF NOT EXISTS llm_judgements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    text_sha256 TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('accepted','rejected','error')),
    response_json TEXT NOT NULL DEFAULT '{}',
    verdict_json TEXT NOT NULL DEFAULT '{}',
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_llm_judgements_key
ON llm_judgements(content_id, text_sha256, model, prompt_version);

CREATE TABLE IF NOT EXISTS spu_association_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL CHECK(status IN ('running','succeeded','failed')),
    rule_version TEXT NOT NULL,
    contents_total INTEGER NOT NULL DEFAULT 0,
    spu_linked INTEGER NOT NULL DEFAULT 0,
    trim_resolved INTEGER NOT NULL DEFAULT 0,
    gray_count INTEGER NOT NULL DEFAULT 0,
    scene_linked INTEGER NOT NULL DEFAULT 0,
    audience_linked INTEGER NOT NULL DEFAULT 0,
    insufficient_evidence INTEGER NOT NULL DEFAULT 0,
    summary_json TEXT NOT NULL DEFAULT '{}'
);
"""


class SchemaMigrationError(RuntimeError):
    pass


def schema_compatibility_state(
    connection: sqlite3.Connection,
    *,
    supported_versions: frozenset[int] = RUNTIME_COMPATIBLE_SCHEMA_VERSIONS,
) -> dict[str, object]:
    """Return a fail-closed runtime compatibility verdict for one database.

    ``PRAGMA user_version`` is application-owned, so it is not sufficient on
    its own.  A compatible database must also have exactly one matching
    migration manifest row and no newer migration row hidden behind an older
    pragma value.
    """

    user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    recursive_triggers_enabled = (
        int(connection.execute("PRAGMA recursive_triggers").fetchone()[0]) == 1
    )
    has_manifest = (
        connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='schema_migrations'
            """
        ).fetchone()
        is not None
    )
    expected_name = SCHEMA_MIGRATION_NAMES.get(user_version)
    actual_name: str | None = None
    max_migration_version: int | None = None
    manifest_row_count = 0
    if has_manifest:
        rows = connection.execute(
            "SELECT name FROM schema_migrations WHERE version=?", (user_version,)
        ).fetchall()
        manifest_row_count = len(rows)
        if len(rows) == 1:
            actual_name = str(rows[0]["name"])
        maximum = connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0]
        if maximum is not None:
            max_migration_version = int(maximum)
    compatible = bool(
        recursive_triggers_enabled
        and user_version in supported_versions
        and expected_name is not None
        and has_manifest
        and manifest_row_count == 1
        and actual_name == expected_name
        and max_migration_version == user_version
    )
    if compatible and user_version in {11, 12, 13, 14, 15, 16}:
        try:
            _validate_v9_structure(connection)
            _validate_v10_structure(connection)
            _validate_v11(connection)
            if user_version >= 12:
                _validate_v12_structure(connection)
            if user_version >= 13:
                _validate_v13_structure(connection)
            if user_version >= 14:
                _validate_v14_structure(connection)
            if user_version >= 15:
                _validate_v15_structure(connection)
            if user_version >= 16:
                _validate_v16_structure(connection)
        except SchemaMigrationError:
            compatible = False
    return {
        "compatible": compatible,
        "user_version": user_version,
        "supported_versions": sorted(supported_versions),
        "expected_migration_name": expected_name,
        "actual_migration_name": actual_name,
        "max_migration_version": max_migration_version,
        "recursive_triggers_enabled": recursive_triggers_enabled,
    }


def require_schema_compatibility(
    connection: sqlite3.Connection,
    *,
    supported_versions: frozenset[int] = RUNTIME_COMPATIBLE_SCHEMA_VERSIONS,
) -> int:
    state = schema_compatibility_state(
        connection, supported_versions=supported_versions
    )
    if not bool(state["compatible"]):
        raise SchemaMigrationError(
            "incompatible or incomplete schema: "
            f"user_version={state['user_version']}, "
            f"migration={state['actual_migration_name']!r}, "
            f"max_migration_version={state['max_migration_version']!r}, "
            f"recursive_triggers={state['recursive_triggers_enabled']!r}, "
            f"supported={state['supported_versions']}"
        )
    user_version = state["user_version"]
    if not isinstance(user_version, int):
        raise SchemaMigrationError("schema compatibility returned a non-integer version")
    return user_version


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

#: v16 起人工复核域（review_queue/evaluation_reviews/review_reopen_events/
#: manual_evidence）从 SCHEMA_SQL 模板中移除。v8→v9 迁移阶梯与 v15 及更早
#: 版本的结构校验仍需要这些表当时的逐字定义，冻结在下面的常量里；
#: v15→v16 迁移最终删除这四张表并重建 evaluation_versions。
_LEGACY_REVIEW_TABLES = (
    "review_queue",
    "evaluation_reviews",
    "review_reopen_events",
    "manual_evidence",
)

_LEGACY_V15_TABLE_SQL: dict[str, str] = {
    "evaluation_versions": """CREATE TABLE IF NOT EXISTS evaluation_versions (
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
)""",
    "review_queue": """CREATE TABLE IF NOT EXISTS review_queue (
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
)""",
    "evaluation_reviews": """CREATE TABLE IF NOT EXISTS evaluation_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER REFERENCES review_queue(id) ON DELETE RESTRICT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,
    previous_evaluation_id INTEGER REFERENCES evaluation_versions(id) ON DELETE RESTRICT,
    resulting_evaluation_id INTEGER REFERENCES evaluation_versions(id) ON DELETE RESTRICT,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    reviewer TEXT NOT NULL,
    created_at TEXT NOT NULL
)""",
    "review_reopen_events": """CREATE TABLE IF NOT EXISTS review_reopen_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES review_queue(id) ON DELETE RESTRICT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,
    previous_review_id INTEGER REFERENCES evaluation_reviews(id) ON DELETE RESTRICT,
    base_evaluation_id INTEGER REFERENCES evaluation_versions(id) ON DELETE RESTRICT,
    reopened_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
)""",
    "manual_evidence": """CREATE TABLE IF NOT EXISTS manual_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id INTEGER NOT NULL REFERENCES evaluation_reviews(id) ON DELETE RESTRICT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,
    evidence_type TEXT NOT NULL,
    text_value TEXT,
    local_path TEXT,
    sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL
)""",
}

_LEGACY_V15_INDEX_SQL = (
    """CREATE UNIQUE INDEX IF NOT EXISTS uq_evaluation_manual_idempotency
ON evaluation_versions(release_id, review_id)
WHERE evaluation_source='manual_review' AND review_id IS NOT NULL""",
    """CREATE INDEX IF NOT EXISTS idx_evaluation_review
ON evaluation_versions(review_id) WHERE review_id IS NOT NULL""",
    """CREATE INDEX IF NOT EXISTS idx_review_queue_status_priority
ON review_queue(status, priority DESC, updated_at, id)""",
    """CREATE INDEX IF NOT EXISTS idx_review_queue_evaluation
ON review_queue(evaluation_id) WHERE evaluation_id IS NOT NULL""",
    """CREATE INDEX IF NOT EXISTS idx_evaluation_reviews_queue
ON evaluation_reviews(queue_id, created_at, id)""",
    """CREATE UNIQUE INDEX IF NOT EXISTS uq_evaluation_review_result
ON evaluation_reviews(resulting_evaluation_id)
WHERE resulting_evaluation_id IS NOT NULL""",
    """CREATE INDEX IF NOT EXISTS idx_review_reopen_queue
ON review_reopen_events(queue_id, created_at, id)""",
    """CREATE INDEX IF NOT EXISTS idx_manual_evidence_review
ON manual_evidence(review_id, id)""",
)


def _legacy_v15_table_statement(table: str, replacements: dict[str, str]) -> str:
    try:
        statement = _LEGACY_V15_TABLE_SQL[table]
    except KeyError as exc:
        raise SchemaMigrationError(f"missing legacy schema template for {table}") from exc
    statement = statement.replace("CREATE TABLE IF NOT EXISTS", "CREATE TABLE", 1)
    for source in sorted(replacements, key=len, reverse=True):
        statement = re.sub(rf"\b{re.escape(source)}\b", replacements[source], statement)
    return statement


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
            if table in _LEGACY_V15_TABLE_SQL:
                connection.execute(_legacy_v15_table_statement(table, replacements))
            else:
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
        for statement in _LEGACY_V15_INDEX_SQL:
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
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (12,'append-only-metric-observations',?)
            """,
            (captured_at,),
        )
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (13,'scheduler-run-attempt-history',?)
            """,
            (captured_at,),
        )
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (14,'spu-audience-scene-domain',?)
            """,
            (captured_at,),
        )
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (15,'spu-llm-assist',?)
            """,
            (captured_at,),
        )
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (16,'remove-manual-review',?)
            """,
            (captured_at,),
        )
        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _validate_v9_structure(connection: sqlite3.Connection) -> None:
    """校验 v9 引入的评估版本结构。

    v16 起人工复核域被移除：以 ``review_queue`` 表是否存在判定当前库处于
    哪个制度。旧制度（≤v15）要求 review 列/索引齐全；新制度（≥v16）要求
    它们已彻底消失。
    """

    columns = set(_table_columns(connection, "evaluation_versions"))
    legacy_review_domain = "review_queue" in _table_names(connection)
    required = {
        "release_id",
        "parent_evaluation_id",
        "matcher_rule_sha256",
    }
    if legacy_review_domain:
        required = required | {"review_id"}
    if required - columns:
        raise SchemaMigrationError(
            f"schema v9 is missing evaluation columns: {sorted(required - columns)}"
        )
    if not legacy_review_domain and ({"review_id", "pending_review"} & columns):
        raise SchemaMigrationError(
            "schema v16 evaluation_versions still carries review columns"
        )
    indexes = {
        str(row["name"])
        for row in connection.execute("PRAGMA index_list(evaluation_versions)")
    }
    required_indexes = {
        "uq_evaluation_automatic_idempotency",
        "uq_evaluation_migrated_parent_idempotency",
    }
    if legacy_review_domain:
        required_indexes = required_indexes | {"uq_evaluation_manual_idempotency"}
    elif "uq_evaluation_manual_idempotency" in indexes:
        raise SchemaMigrationError(
            "schema v16 still carries the manual review idempotency index"
        )
    if required_indexes - indexes or "uq_evaluation_idempotency" in indexes:
        raise SchemaMigrationError("schema v9 evaluation indexes are inconsistent")


def _validate_v9(connection: sqlite3.Connection) -> None:
    _validate_v9_structure(connection)
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


def _validate_v10_structure(connection: sqlite3.Connection) -> None:
    tables = _table_names(connection)
    missing = set(_NEW_V10_TABLES) - tables
    if missing:
        raise SchemaMigrationError(f"schema v10 is missing tables: {sorted(missing)}")
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
        str(row["name"]) for row in connection.execute("PRAGMA index_list(comments)")
    }
    if "uq_comments_identity_per_evidence" not in indexes:
        raise SchemaMigrationError(
            "schema v10 is missing the comment identity uniqueness index"
        )


def _validate_v10(connection: sqlite3.Connection) -> None:
    _validate_v10_structure(connection)
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


def metric_observation_sha256(
    *,
    observation_origin: str,
    legacy_snapshot_id: int | None,
    subject_key: str,
    captured_at: str,
    window_key: str,
    view_count: int | None,
    comment_count: int | None,
    like_count: int | None,
    share_count: int | None,
    collect_count: int | None,
    status: str,
    source: str,
    raw_response_id: int | None,
    metadata_json: str,
) -> str:
    payload = {
        "schema": "content-metric-observation-v1",
        "observation_origin": observation_origin,
        "legacy_snapshot_id": legacy_snapshot_id,
        "subject_key": subject_key,
        "captured_at": captured_at,
        "window_key": window_key,
        "view_count": view_count,
        "comment_count": comment_count,
        "like_count": like_count,
        "share_count": share_count,
        "collect_count": collect_count,
        "status": status,
        "source": source,
        "raw_response_id": raw_response_id,
        "metadata_json": metadata_json,
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _migrate_v11_to_v12(connection: sqlite3.Connection) -> None:
    """Create and backfill immutable metric observations without changing snapshots."""

    if connection.in_transaction:
        raise SchemaMigrationError("v12 migration requires no active transaction")
    if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
        raise SchemaMigrationError("v12 migration requires PRAGMA foreign_keys=ON")
    state = schema_compatibility_state(
        connection, supported_versions=frozenset({11})
    )
    if not bool(state["compatible"]):
        raise SchemaMigrationError("v12 migration requires complete schema v11")
    required_tables = {
        "content_items",
        "content_identities",
        "provider_raw_responses",
        "content_metric_snapshots",
    }
    missing_tables = required_tables - _table_names(connection)
    if missing_tables:
        raise SchemaMigrationError(
            "v12 migration is missing metric source tables: "
            f"{sorted(missing_tables)}"
        )
    required_columns = {
        "content_items": {"id", "link_id"},
        "content_identities": {
            "id",
            "content_id",
            "platform_identity_key",
            "is_primary",
        },
        "provider_raw_responses": {"id"},
        "content_metric_snapshots": {
            "id",
            "content_id",
            "captured_at",
            "window_key",
            "view_count",
            "comment_count",
            "like_count",
            "share_count",
            "collect_count",
            "status",
            "source",
            "raw_response_id",
            "metadata_json",
        },
    }
    for table, required in required_columns.items():
        missing_columns = required - set(_table_columns(connection, table))
        if missing_columns:
            raise SchemaMigrationError(
                f"v12 migration source table {table} is incomplete: "
                f"{sorted(missing_columns)}"
            )
    existing_objects = {
        str(row["name"])
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE name IN (
                'idx_content_identities_content_primary',
                'content_metric_observations',
                'idx_metric_observations_content_capture',
                'trg_metric_observations_immutable_payload',
                'trg_metric_observations_no_delete'
            )
            """
        ).fetchall()
    }
    if existing_objects:
        raise SchemaMigrationError(
            f"v12 metric objects already exist before migration: {sorted(existing_objects)}"
        )

    snapshot_columns = _table_columns(connection, "content_metric_snapshots")
    snapshot_count = int(
        connection.execute("SELECT COUNT(*) FROM content_metric_snapshots").fetchone()[0]
    )
    snapshot_hash = _table_projection_sha256(
        connection, "content_metric_snapshots", snapshot_columns
    )
    statements = _schema_statements()
    metric_statements = [
        statement
        for statement in statements
        if statement.startswith("CREATE TABLE IF NOT EXISTS content_metric_observations ")
        or statement.startswith(
            "CREATE INDEX IF NOT EXISTS idx_content_identities_content_primary"
        )
        or statement.startswith(
            "CREATE INDEX IF NOT EXISTS idx_metric_observations_content_capture"
        )
        or statement.startswith(
            "CREATE TRIGGER IF NOT EXISTS trg_metric_observations_immutable_payload"
        )
        or statement.startswith(
            "CREATE TRIGGER IF NOT EXISTS trg_metric_observations_no_delete"
        )
    ]
    if len(metric_statements) != 5:
        raise SchemaMigrationError("v12 metric schema template is incomplete")

    applied_at = now_utc()
    try:
        connection.execute("BEGIN IMMEDIATE")
        for statement in metric_statements:
            connection.execute(statement)
        _migration_checkpoint("v12_metric_table_created")

        rows = connection.execute(
            """
            SELECT s.*,
                   COALESCE(
                       (
                           SELECT ci.platform_identity_key
                           FROM content_identities ci
                           WHERE ci.content_id=s.content_id
                           ORDER BY ci.is_primary DESC,ci.id
                           LIMIT 1
                       ),
                       'link:' || c.link_id
                   ) subject_key
            FROM content_metric_snapshots s
            JOIN content_items c ON c.id=s.content_id
            ORDER BY s.id
            """
        ).fetchall()
        for row in rows:
            values = {
                "observation_origin": "legacy_snapshot_baseline",
                "legacy_snapshot_id": int(row["id"]),
                "subject_key": str(row["subject_key"]),
                "captured_at": str(row["captured_at"]),
                "window_key": str(row["window_key"]),
                "view_count": row["view_count"],
                "comment_count": row["comment_count"],
                "like_count": row["like_count"],
                "share_count": row["share_count"],
                "collect_count": row["collect_count"],
                "status": str(row["status"]),
                "source": str(row["source"]),
                "raw_response_id": row["raw_response_id"],
                "metadata_json": str(row["metadata_json"]),
            }
            digest = metric_observation_sha256(**values)
            connection.execute(
                """
                INSERT INTO content_metric_observations(
                    content_id,subject_key,captured_at,window_key,
                    view_count,comment_count,like_count,share_count,collect_count,
                    status,source,raw_response_id,metadata_json,observation_origin,
                    legacy_snapshot_id,observation_sha256,recorded_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["content_id"],
                    values["subject_key"],
                    values["captured_at"],
                    values["window_key"],
                    values["view_count"],
                    values["comment_count"],
                    values["like_count"],
                    values["share_count"],
                    values["collect_count"],
                    values["status"],
                    values["source"],
                    values["raw_response_id"],
                    values["metadata_json"],
                    values["observation_origin"],
                    values["legacy_snapshot_id"],
                    digest,
                    applied_at,
                ),
            )
        _migration_checkpoint("v12_metric_observations_backfilled")

        baseline_count = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM content_metric_observations
                WHERE observation_origin='legacy_snapshot_baseline'
                """
            ).fetchone()[0]
        )
        if baseline_count != snapshot_count:
            raise SchemaMigrationError(
                "v12 metric baseline count changed: "
                f"snapshots={snapshot_count},observations={baseline_count}"
            )
        mismatches = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM content_metric_snapshots s
                JOIN content_metric_observations o ON o.legacy_snapshot_id=s.id
                WHERE o.content_id IS NOT s.content_id
                   OR o.captured_at IS NOT s.captured_at
                   OR o.window_key IS NOT s.window_key
                   OR o.view_count IS NOT s.view_count
                   OR o.comment_count IS NOT s.comment_count
                   OR o.like_count IS NOT s.like_count
                   OR o.share_count IS NOT s.share_count
                   OR o.collect_count IS NOT s.collect_count
                   OR o.status IS NOT s.status
                   OR o.source IS NOT s.source
                   OR o.raw_response_id IS NOT s.raw_response_id
                   OR o.metadata_json IS NOT s.metadata_json
                """
            ).fetchone()[0]
        )
        if mismatches:
            raise SchemaMigrationError(
                f"v12 metric baseline differs from {mismatches} snapshots"
            )
        if (
            _table_projection_sha256(
                connection, "content_metric_snapshots", snapshot_columns
            )
            != snapshot_hash
        ):
            raise SchemaMigrationError("v12 migration changed metric snapshots")
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise SchemaMigrationError(
                f"v12 foreign-key violations before commit: {len(violations)}"
            )
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (12,'append-only-metric-observations',?)
            """,
            (applied_at,),
        )
        connection.execute("PRAGMA user_version=12")
        _migration_checkpoint("v12_metric_schema_stamped")
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise SchemaMigrationError("v12 foreign-key check failed after commit")
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity != "ok":
        raise SchemaMigrationError(f"v12 integrity check failed: {integrity}")


def _normalized_schema_sql(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def _validate_v12_structure(connection: sqlite3.Connection) -> None:
    required = {
        "id",
        "content_id",
        "subject_key",
        "captured_at",
        "window_key",
        "view_count",
        "comment_count",
        "like_count",
        "share_count",
        "collect_count",
        "status",
        "source",
        "raw_response_id",
        "metadata_json",
        "observation_origin",
        "legacy_snapshot_id",
        "observation_sha256",
        "recorded_at",
    }
    columns = set(_table_columns(connection, "content_metric_observations"))
    if required - columns:
        raise SchemaMigrationError(
            "schema v12 is missing metric observation columns: "
            f"{sorted(required - columns)}"
        )
    indexes = {
        str(row["name"])
        for row in connection.execute(
            "PRAGMA index_list(content_metric_observations)"
        ).fetchall()
    }
    if "idx_metric_observations_content_capture" not in indexes:
        raise SchemaMigrationError("schema v12 is missing metric capture index")
    trigger = connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type='trigger' AND name='trg_metric_observations_immutable_payload'
        """
    ).fetchone()
    if trigger is None:
        raise SchemaMigrationError("schema v12 is missing metric immutability trigger")
    no_delete_trigger = connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type='trigger' AND name='trg_metric_observations_no_delete'
        """
    ).fetchone()
    if no_delete_trigger is None:
        raise SchemaMigrationError("schema v12 is missing metric no-delete trigger")

    object_prefixes = {
        ("index", "idx_content_identities_content_primary"):
            "CREATE INDEX IF NOT EXISTS idx_content_identities_content_primary",
        ("table", "content_metric_observations"):
            "CREATE TABLE IF NOT EXISTS content_metric_observations ",
        ("index", "idx_metric_observations_content_capture"):
            "CREATE INDEX IF NOT EXISTS idx_metric_observations_content_capture",
        ("trigger", "trg_metric_observations_immutable_payload"):
            "CREATE TRIGGER IF NOT EXISTS trg_metric_observations_immutable_payload",
        ("trigger", "trg_metric_observations_no_delete"):
            "CREATE TRIGGER IF NOT EXISTS trg_metric_observations_no_delete",
    }
    statements = _schema_statements()
    for (object_type, name), prefix in object_prefixes.items():
        expected_candidates = [
            statement for statement in statements if statement.startswith(prefix)
        ]
        if len(expected_candidates) != 1:
            raise SchemaMigrationError(f"schema v12 template is missing {name}")
        expected = expected_candidates[0].replace(" IF NOT EXISTS", "", 1)
        actual_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type=? AND name=?",
            (object_type, name),
        ).fetchone()
        if actual_row is None or actual_row[0] is None:
            raise SchemaMigrationError(f"schema v12 is missing {name}")
        if _normalized_schema_sql(str(actual_row[0])) != _normalized_schema_sql(
            expected
        ):
            raise SchemaMigrationError(f"schema v12 object definition drifted: {name}")


def _validate_v12(connection: sqlite3.Connection) -> None:
    _validate_v12_structure(connection)
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise SchemaMigrationError("schema v12 has foreign-key violations")


_V12_SCHEDULER_RUNS_SQL = """
CREATE TABLE scheduler_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    scheduled_for TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running','succeeded','failed','skipped')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    details_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(job_id, scheduled_for)
)
"""


def _schema_object_sql(*, object_type: str, name: str, prefix: str) -> str:
    candidates = [
        statement for statement in _schema_statements() if statement.startswith(prefix)
    ]
    if len(candidates) != 1:
        raise SchemaMigrationError(f"schema template is missing {name}")
    expected = candidates[0].replace(" IF NOT EXISTS", "", 1)
    if object_type not in {"table", "index", "trigger"}:
        raise SchemaMigrationError(f"unsupported schema object type: {object_type}")
    return expected


def _require_exact_schema_object(
    connection: sqlite3.Connection,
    *,
    object_type: str,
    name: str,
    prefix: str,
) -> None:
    expected = _schema_object_sql(
        object_type=object_type,
        name=name,
        prefix=prefix,
    )
    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type=? AND name=?",
        (object_type, name),
    ).fetchone()
    if row is None or row[0] is None:
        raise SchemaMigrationError(f"schema v13 is missing {name}")
    if _normalized_schema_sql(str(row[0])) != _normalized_schema_sql(expected):
        raise SchemaMigrationError(f"schema v13 object definition drifted: {name}")


def _migrate_v12_to_v13(connection: sqlite3.Connection) -> None:
    """Add immutable per-occurrence scheduler attempt history."""

    if connection.in_transaction:
        raise SchemaMigrationError("v13 migration requires no active transaction")
    if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
        raise SchemaMigrationError("v13 migration requires PRAGMA foreign_keys=ON")
    state = schema_compatibility_state(
        connection, supported_versions=frozenset({12})
    )
    if not bool(state["compatible"]):
        raise SchemaMigrationError("v13 migration requires complete schema v12")

    scheduler_sql = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='scheduler_runs'"
    ).fetchone()
    if scheduler_sql is None or scheduler_sql[0] is None:
        raise SchemaMigrationError("v13 migration is missing scheduler_runs")
    if _normalized_schema_sql(str(scheduler_sql[0])) != _normalized_schema_sql(
        _V12_SCHEDULER_RUNS_SQL
    ):
        raise SchemaMigrationError("v13 migration source scheduler_runs drifted")

    residue = {
        str(row["name"])
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE name IN (
                'scheduler_runs_v12_legacy',
                'scheduler_run_attempts',
                'uq_scheduler_run_attempts_active',
                'trg_scheduler_run_attempts_terminal_update',
                'trg_scheduler_run_attempts_no_delete'
            )
            """
        ).fetchall()
    }
    if residue:
        raise SchemaMigrationError(
            f"v13 scheduler objects already exist before migration: {sorted(residue)}"
        )

    run_columns = _table_columns(connection, "scheduler_runs")
    expected_run_columns = [
        "id",
        "job_id",
        "scheduled_for",
        "status",
        "started_at",
        "completed_at",
        "details_json",
    ]
    if run_columns != expected_run_columns:
        raise SchemaMigrationError(
            f"v13 scheduler source columns drifted: {run_columns}"
        )
    run_count = int(connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0])
    run_hash = _table_projection_sha256(
        connection, "scheduler_runs", run_columns
    )

    statements = _schema_statements()
    scheduler_run_statement = [
        statement
        for statement in statements
        if statement.startswith("CREATE TABLE IF NOT EXISTS scheduler_runs ")
    ]
    attempt_statements = [
        statement
        for statement in statements
        if statement.startswith("CREATE TABLE IF NOT EXISTS scheduler_run_attempts ")
        or statement.startswith(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_scheduler_run_attempts_active"
        )
        or statement.startswith(
            "CREATE TRIGGER IF NOT EXISTS trg_scheduler_run_attempts_terminal_update"
        )
        or statement.startswith(
            "CREATE TRIGGER IF NOT EXISTS trg_scheduler_run_attempts_no_delete"
        )
    ]
    if len(scheduler_run_statement) != 1 or len(attempt_statements) != 4:
        raise SchemaMigrationError("v13 scheduler schema template is incomplete")

    applied_at = now_utc()
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "ALTER TABLE scheduler_runs RENAME TO scheduler_runs_v12_legacy"
        )
        connection.execute(scheduler_run_statement[0])
        columns_sql = ",".join(f'"{column}"' for column in run_columns)
        connection.execute(
            f"INSERT INTO scheduler_runs({columns_sql}) "
            f"SELECT {columns_sql} FROM scheduler_runs_v12_legacy"
        )
        connection.execute("DROP TABLE scheduler_runs_v12_legacy")
        _migration_checkpoint("v13_scheduler_runs_rebuilt")

        for statement in attempt_statements:
            connection.execute(statement)
        connection.execute(
            """
            INSERT INTO scheduler_run_attempts(
                scheduler_run_id,attempt_number,invocation_source,status,
                started_at,completed_at,details_json
            )
            SELECT id,1,'legacy_migration',status,started_at,completed_at,details_json
            FROM scheduler_runs
            ORDER BY id
            """
        )
        _migration_checkpoint("v13_scheduler_attempts_backfilled")

        if int(connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0]) != run_count:
            raise SchemaMigrationError("v13 migration changed scheduler run count")
        if (
            _table_projection_sha256(connection, "scheduler_runs", run_columns)
            != run_hash
        ):
            raise SchemaMigrationError("v13 migration changed scheduler run projection")
        attempt_count = int(
            connection.execute("SELECT COUNT(*) FROM scheduler_run_attempts").fetchone()[0]
        )
        if attempt_count != run_count:
            raise SchemaMigrationError(
                "v13 scheduler attempt baseline count changed: "
                f"runs={run_count},attempts={attempt_count}"
            )
        mismatches = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM scheduler_runs r
                JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id
                WHERE a.attempt_number!=1
                   OR a.invocation_source!='legacy_migration'
                   OR a.status IS NOT r.status
                   OR a.started_at IS NOT r.started_at
                   OR a.completed_at IS NOT r.completed_at
                   OR a.details_json IS NOT r.details_json
                """
            ).fetchone()[0]
        )
        if mismatches:
            raise SchemaMigrationError(
                f"v13 scheduler attempt baseline differs from {mismatches} runs"
            )
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise SchemaMigrationError(
                f"v13 foreign-key violations before commit: {len(violations)}"
            )
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (13,'scheduler-run-attempt-history',?)
            """,
            (applied_at,),
        )
        connection.execute("PRAGMA user_version=13")
        _migration_checkpoint("v13_scheduler_schema_stamped")
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise SchemaMigrationError("v13 foreign-key check failed after commit")
    quick = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    if quick != "ok":
        raise SchemaMigrationError(f"v13 quick check failed: {quick}")
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity != "ok":
        raise SchemaMigrationError(f"v13 integrity check failed: {integrity}")


def _validate_v13_structure(connection: sqlite3.Connection) -> None:
    expected_run_columns = [
        "id",
        "job_id",
        "scheduled_for",
        "status",
        "started_at",
        "completed_at",
        "details_json",
    ]
    if _table_columns(connection, "scheduler_runs") != expected_run_columns:
        raise SchemaMigrationError("schema v13 scheduler_runs columns drifted")
    expected_attempt_columns = [
        "id",
        "scheduler_run_id",
        "attempt_number",
        "invocation_source",
        "status",
        "started_at",
        "completed_at",
        "details_json",
    ]
    if _table_columns(connection, "scheduler_run_attempts") != expected_attempt_columns:
        raise SchemaMigrationError("schema v13 scheduler attempt columns drifted")

    object_specs = (
        (
            "table",
            "scheduler_runs",
            "CREATE TABLE IF NOT EXISTS scheduler_runs ",
        ),
        (
            "table",
            "scheduler_run_attempts",
            "CREATE TABLE IF NOT EXISTS scheduler_run_attempts ",
        ),
        (
            "index",
            "uq_scheduler_run_attempts_active",
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_scheduler_run_attempts_active",
        ),
        (
            "trigger",
            "trg_scheduler_run_attempts_terminal_update",
            "CREATE TRIGGER IF NOT EXISTS trg_scheduler_run_attempts_terminal_update",
        ),
        (
            "trigger",
            "trg_scheduler_run_attempts_no_delete",
            "CREATE TRIGGER IF NOT EXISTS trg_scheduler_run_attempts_no_delete",
        ),
    )
    for object_type, name, prefix in object_specs:
        _require_exact_schema_object(
            connection,
            object_type=object_type,
            name=name,
            prefix=prefix,
        )

    run_unique_indexes = [
        str(row["name"])
        for row in connection.execute("PRAGMA index_list(scheduler_runs)").fetchall()
        if int(row["unique"]) == 1
        and [
            str(column["name"])
            for column in connection.execute(
                f"PRAGMA index_info('{str(row['name'])}')"
            ).fetchall()
        ]
        == ["job_id", "scheduled_for"]
    ]
    if len(run_unique_indexes) != 1:
        raise SchemaMigrationError("schema v13 occurrence uniqueness drifted")
    attempt_unique_indexes = [
        str(row["name"])
        for row in connection.execute(
            "PRAGMA index_list(scheduler_run_attempts)"
        ).fetchall()
        if int(row["unique"]) == 1
        and [
            str(column["name"])
            for column in connection.execute(
                f"PRAGMA index_info('{str(row['name'])}')"
            ).fetchall()
        ]
        == ["scheduler_run_id", "attempt_number"]
    ]
    if len(attempt_unique_indexes) != 1:
        raise SchemaMigrationError("schema v13 attempt uniqueness drifted")
    foreign_keys = connection.execute(
        "PRAGMA foreign_key_list(scheduler_run_attempts)"
    ).fetchall()
    if len(foreign_keys) != 1:
        raise SchemaMigrationError("schema v13 scheduler attempt FK drifted")
    foreign_key = foreign_keys[0]
    if (
        str(foreign_key["table"]) != "scheduler_runs"
        or str(foreign_key["from"]) != "scheduler_run_id"
        or str(foreign_key["to"]) != "id"
        or str(foreign_key["on_delete"]).upper() != "RESTRICT"
    ):
        raise SchemaMigrationError("schema v13 scheduler attempt FK drifted")


def _validate_v13(connection: sqlite3.Connection) -> None:
    _validate_v13_structure(connection)
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise SchemaMigrationError("schema v13 has foreign-key violations")


_V14_NEW_TABLES = (
    "spu_catalog",
    "spu_alias",
    "audience_dim",
    "scene_dim",
    "spu_audience_map",
    "audience_scene_map",
    "content_spu_links",
    "content_scene_links",
    "content_audience_links",
    "spu_association_runs",
)

_V14_NEW_INDEXES = (
    ("uq_spu_catalog_series_node", "CREATE UNIQUE INDEX IF NOT EXISTS uq_spu_catalog_series_node"),
    ("uq_spu_audience_primary", "CREATE UNIQUE INDEX IF NOT EXISTS uq_spu_audience_primary"),
    ("idx_content_spu_links_content", "CREATE INDEX IF NOT EXISTS idx_content_spu_links_content"),
    ("idx_content_spu_links_spu", "CREATE INDEX IF NOT EXISTS idx_content_spu_links_spu"),
    ("idx_content_scene_links_content", "CREATE INDEX IF NOT EXISTS idx_content_scene_links_content"),
    ("idx_content_audience_links_content", "CREATE INDEX IF NOT EXISTS idx_content_audience_links_content"),
)


def _migrate_v13_to_v14(connection: sqlite3.Connection) -> None:
    """新增 SPU×人群×用车场景 关联域（纯增量建表，不改动既有表）。

    设计基线见 `docs/SPU人群场景关联与统计方案_v0.1.md`。维度种子由
    `spu_audience.ensure_assets` 幂等补齐，迁移本身只负责结构。
    """

    if connection.in_transaction:
        raise SchemaMigrationError("v14 migration requires no active transaction")
    if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
        raise SchemaMigrationError("v14 migration requires PRAGMA foreign_keys=ON")
    state = schema_compatibility_state(
        connection, supported_versions=frozenset({13})
    )
    if not bool(state["compatible"]):
        raise SchemaMigrationError("v14 migration requires complete schema v13")
    residue = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','index')"
        )
    } & set(_V14_NEW_TABLES)
    if residue:
        raise SchemaMigrationError(
            f"v14 migration found leftover objects: {sorted(residue)}"
        )
    captured_at = now_utc()
    connection.execute("BEGIN IMMEDIATE")
    try:
        for table in _V14_NEW_TABLES:
            statement = _schema_table_statement(table, {})
            connection.execute(statement)
        for name, prefix in _V14_NEW_INDEXES:
            connection.execute(
                _schema_object_sql(object_type="index", name=name, prefix=prefix)
            )
        from .spu_audience import ensure_assets  # 函数级导入避免循环依赖

        ensure_assets(connection)
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (14,'spu-audience-scene-domain',?)
            """,
            (captured_at,),
        )
        connection.execute("PRAGMA user_version=14")
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise SchemaMigrationError(
                f"v14 foreign-key violations before commit: {len(violations)}"
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity != "ok":
        raise SchemaMigrationError(f"v14 integrity check failed: {integrity}")


#: v14 时代的 content_audience_links 定义（v15 重建该表扩充 source 取值并加
#: evidence_json）。v14 库在升级前用这份冻结定义校验，升级后按 SCHEMA_SQL 现行
#: 模板校验，两者其一匹配即可——其余对象仍然严格对齐现行模板。
_V14_CONTENT_AUDIENCE_LINKS_SQL = """
CREATE TABLE content_audience_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    audience_code TEXT NOT NULL REFERENCES audience_dim(code) ON DELETE RESTRICT,
    source TEXT NOT NULL CHECK(source IN ('content_explicit','rule_prior')),
    conflict_flag INTEGER NOT NULL DEFAULT 0 CHECK(conflict_flag IN (0,1)),
    consistency_flag INTEGER NOT NULL DEFAULT 0 CHECK(consistency_flag IN (0,1)),
    rule_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    invalidated_at TEXT
)
""".strip()


def _validate_v14_structure(connection: sqlite3.Connection) -> None:
    for table in _V14_NEW_TABLES:
        expected = _schema_object_sql(
            object_type="table",
            name=table,
            prefix=f"CREATE TABLE IF NOT EXISTS {table} ",
        )
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if row is None or row[0] is None:
            raise SchemaMigrationError(f"schema v14 is missing {table}")
        actual = _normalized_schema_sql(str(row[0]))
        allowed = {_normalized_schema_sql(expected)}
        if table == "content_audience_links":
            allowed.add(_normalized_schema_sql(_V14_CONTENT_AUDIENCE_LINKS_SQL))
        if actual not in allowed:
            raise SchemaMigrationError(f"schema v14 object definition drifted: {table}")
    for name, prefix in _V14_NEW_INDEXES:
        expected = _schema_object_sql(object_type="index", name=name, prefix=prefix)
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
            (name,),
        ).fetchone()
        if row is None or row[0] is None:
            raise SchemaMigrationError(f"schema v14 is missing {name}")
        if _normalized_schema_sql(str(row[0])) != _normalized_schema_sql(expected):
            raise SchemaMigrationError(f"schema v14 object definition drifted: {name}")


def _validate_v14(connection: sqlite3.Connection) -> None:
    _validate_v14_structure(connection)
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise SchemaMigrationError("schema v14 has foreign-key violations")


_V15_NEW_TABLES = ("llm_judgements",)

_V15_NEW_INDEXES = (
    (
        "uq_llm_judgements_key",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_llm_judgements_key",
    ),
)

_V15_AUDIENCE_COPY_COLUMNS = (
    "id",
    "content_id",
    "audience_code",
    "source",
    "conflict_flag",
    "consistency_flag",
    "rule_version",
    "created_at",
    "invalidated_at",
)


def _migrate_v14_to_v15(connection: sqlite3.Connection) -> None:
    """SPU 关联域接入 LLM 辅助（B 链，无人工复核版）。

    - 重建 ``content_audience_links``：source 允许 'llm'，新增 evidence_json
      （既有行取默认 '{}'，数据逐列拷贝并做 count+hash 对账）；
    - 新增 ``llm_judgements`` 判定缓存表：content+文本哈希幂等，规则重算后
      重放已验收判定，不重复调用大模型付费。
    """

    if connection.in_transaction:
        raise SchemaMigrationError("v15 migration requires no active transaction")
    if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
        raise SchemaMigrationError("v15 migration requires PRAGMA foreign_keys=ON")
    state = schema_compatibility_state(
        connection, supported_versions=frozenset({14})
    )
    if not bool(state["compatible"]):
        raise SchemaMigrationError("v15 migration requires complete schema v14")
    residue = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','index')"
        )
    } & (set(_V15_NEW_TABLES) | {"content_audience_links_v14_old"})
    if residue:
        raise SchemaMigrationError(
            f"v15 migration found leftover objects: {sorted(residue)}"
        )
    copy_columns = list(_V15_AUDIENCE_COPY_COLUMNS)
    old_count = int(
        connection.execute("SELECT COUNT(*) FROM content_audience_links").fetchone()[0]
    )
    old_hash = _table_projection_sha256(
        connection, "content_audience_links", copy_columns
    )
    captured_at = now_utc()
    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        connection.execute("BEGIN IMMEDIATE")
        # 顺序保证最终表/索引的 sqlite_master DDL 与 SCHEMA_SQL 模板逐字一致
        # （若反过来"建新表再 RENAME"，SQLite 会把表名改写成带引号形式，
        # 过不了 _validate_v15_structure 的逐字校验——真库拷贝实测踩过）。
        connection.execute(
            "ALTER TABLE content_audience_links RENAME TO content_audience_links_v14_old"
        )
        connection.execute(_schema_table_statement("content_audience_links", {}))
        names = ",".join(copy_columns)
        connection.execute(
            f"INSERT INTO content_audience_links({names}) "
            f"SELECT {names} FROM content_audience_links_v14_old"
        )
        connection.execute("DROP TABLE content_audience_links_v14_old")
        connection.execute(
            _schema_object_sql(
                object_type="index",
                name="idx_content_audience_links_content",
                prefix="CREATE INDEX IF NOT EXISTS idx_content_audience_links_content",
            )
        )
        for table in _V15_NEW_TABLES:
            connection.execute(_schema_table_statement(table, {}))
        for name, prefix in _V15_NEW_INDEXES:
            connection.execute(
                _schema_object_sql(object_type="index", name=name, prefix=prefix)
            )
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (15,'spu-llm-assist',?)
            """,
            (captured_at,),
        )
        connection.execute("PRAGMA user_version=15")
        new_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM content_audience_links"
            ).fetchone()[0]
        )
        new_hash = _table_projection_sha256(
            connection, "content_audience_links", copy_columns
        )
        if new_count != old_count or new_hash != old_hash:
            raise SchemaMigrationError(
                "v15 content_audience_links rebuild lost or altered rows: "
                f"count {old_count}->{new_count}"
            )
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise SchemaMigrationError(
                f"v15 foreign-key violations before commit: {len(violations)}"
            )
        connection.commit()
    except Exception:
        connection.rollback()
        connection.execute("PRAGMA foreign_keys=ON")
        raise
    connection.execute("PRAGMA foreign_keys=ON")
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity != "ok":
        raise SchemaMigrationError(f"v15 integrity check failed: {integrity}")


def _validate_v15_structure(connection: sqlite3.Connection) -> None:
    for table in ("content_audience_links",) + _V15_NEW_TABLES:
        expected = _schema_object_sql(
            object_type="table",
            name=table,
            prefix=f"CREATE TABLE IF NOT EXISTS {table} ",
        )
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if row is None or row[0] is None:
            raise SchemaMigrationError(f"schema v15 is missing {table}")
        if _normalized_schema_sql(str(row[0])) != _normalized_schema_sql(expected):
            raise SchemaMigrationError(f"schema v15 object definition drifted: {table}")
    for name, prefix in _V15_NEW_INDEXES:
        expected = _schema_object_sql(object_type="index", name=name, prefix=prefix)
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
            (name,),
        ).fetchone()
        if row is None or row[0] is None:
            raise SchemaMigrationError(f"schema v15 is missing {name}")
        if _normalized_schema_sql(str(row[0])) != _normalized_schema_sql(expected):
            raise SchemaMigrationError(f"schema v15 object definition drifted: {name}")


def _validate_v15(connection: sqlite3.Connection) -> None:
    _validate_v15_structure(connection)
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise SchemaMigrationError("schema v15 has foreign-key violations")


_V16_EVALUATION_COPY_COLUMNS = (
    "id",
    "content_id",
    "evidence_envelope_id",
    "release_id",
    "parent_evaluation_id",
    "rule_version",
    "taxonomy_version",
    "matcher_rule_sha256",
    "evidence_sha256",
    "evaluation_source",
    "evaluation_status",
    "evidence_level",
    "primary_selling_point_code",
    "selling_point_score",
    "selling_point_included",
    "content_direction",
    "content_automotive_score",
    "audience_automotive_score",
    "acquisition_potential_score",
    "payload_json",
    "evaluated_at",
    "invalidated_at",
    "invalidation_reason",
)

#: 删除顺序按外键依赖从子到父。
_V16_DROPPED_TABLES = (
    "manual_evidence",
    "review_reopen_events",
    "evaluation_reviews",
    "review_queue",
)

_V16_REMOVED_INDEXES = (
    "uq_evaluation_manual_idempotency",
    "idx_evaluation_review",
    "idx_review_queue_status_priority",
    "idx_review_queue_evaluation",
    "idx_evaluation_reviews_queue",
    "uq_evaluation_review_result",
    "idx_review_reopen_queue",
    "idx_manual_evidence_review",
)

_V16_EVALUATION_INDEXES = (
    (
        "uq_evaluation_automatic_idempotency",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_evaluation_automatic_idempotency",
    ),
    (
        "uq_evaluation_migrated_parent_idempotency",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_evaluation_migrated_parent_idempotency",
    ),
    (
        "idx_evaluation_content_audit",
        "CREATE INDEX IF NOT EXISTS idx_evaluation_content_audit",
    ),
    (
        "idx_evaluation_release_current",
        "CREATE INDEX IF NOT EXISTS idx_evaluation_release_current",
    ),
    (
        "idx_evaluation_parent",
        "CREATE INDEX IF NOT EXISTS idx_evaluation_parent",
    ),
)


def _migrate_v15_to_v16(connection: sqlite3.Connection) -> None:
    """消灭人工复核域（Mark 2026-08-17 口径定稿）。

    - 删除 ``review_queue``/``evaluation_reviews``/``review_reopen_events``/
      ``manual_evidence`` 四张表（历史 1270 条队列已全部 resolved、10 条复核
      记录随之删除，Mark 知情确认审计断链）；
    - 重建 ``evaluation_versions``：去掉 ``review_id``、``pending_review``
      两列与 manual_review↔review_id 约束；``evaluation_source`` 仍允许
      'manual_review'——既有 18 个人工结论版本原样保留为当前结论（自然沉底，
      下次证据/规则变化重评时由自动评估接管）；
    - 灰区语义不再落库：60–74 弱匹配与 V0/V1 可由 selling_point_score /
      evidence_level 推导，无需 pending_review 派生列。

    重建遵循 v15 教训：旧表改名→按 SCHEMA_SQL 模板原文建新表→逐列拷贝
    （显式 id）→删旧表→按模板重建索引；期间开启 legacy_alter_table，
    避免 RENAME 把 evaluation_matches 等存量表 DDL 里的外键目标改写掉。
    """

    if connection.in_transaction:
        raise SchemaMigrationError("v16 migration requires no active transaction")
    if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
        raise SchemaMigrationError("v16 migration requires PRAGMA foreign_keys=ON")
    state = schema_compatibility_state(
        connection, supported_versions=frozenset({15})
    )
    if not bool(state["compatible"]):
        raise SchemaMigrationError("v16 migration requires complete schema v15")
    residue = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','index')"
        )
    } & {"evaluation_versions_v15_old"}
    if residue:
        raise SchemaMigrationError(
            f"v16 migration found leftover objects: {sorted(residue)}"
        )
    copy_columns = list(_V16_EVALUATION_COPY_COLUMNS)
    old_count = int(
        connection.execute("SELECT COUNT(*) FROM evaluation_versions").fetchone()[0]
    )
    old_hash = _table_projection_sha256(
        connection, "evaluation_versions", copy_columns
    )
    manual_count = int(
        connection.execute(
            "SELECT COUNT(*) FROM evaluation_versions WHERE evaluation_source='manual_review'"
        ).fetchone()[0]
    )
    sequences = {
        str(row["name"]): int(row["seq"])
        for row in connection.execute("SELECT name,seq FROM sqlite_sequence")
    }
    captured_at = now_utc()
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute("PRAGMA legacy_alter_table=ON")
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "ALTER TABLE evaluation_versions RENAME TO evaluation_versions_v15_old"
        )
        connection.execute(_schema_table_statement("evaluation_versions", {}))
        names = ",".join(copy_columns)
        connection.execute(
            f"INSERT INTO evaluation_versions({names}) "
            f"SELECT {names} FROM evaluation_versions_v15_old"
        )
        connection.execute("DROP TABLE evaluation_versions_v15_old")
        for table in _V16_DROPPED_TABLES:
            connection.execute(f'DROP TABLE "{table}"')
        for name, prefix in _V16_EVALUATION_INDEXES:
            connection.execute(
                _schema_object_sql(object_type="index", name=name, prefix=prefix)
            )
        _restore_sequences(connection, sequences, tables=("evaluation_versions",))
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (16,'remove-manual-review',?)
            """,
            (captured_at,),
        )
        connection.execute("PRAGMA user_version=16")
        new_count = int(
            connection.execute("SELECT COUNT(*) FROM evaluation_versions").fetchone()[0]
        )
        new_hash = _table_projection_sha256(
            connection, "evaluation_versions", copy_columns
        )
        if new_count != old_count or new_hash != old_hash:
            raise SchemaMigrationError(
                "v16 evaluation_versions rebuild lost or altered rows: "
                f"count {old_count}->{new_count}"
            )
        new_manual = int(
            connection.execute(
                "SELECT COUNT(*) FROM evaluation_versions WHERE evaluation_source='manual_review'"
            ).fetchone()[0]
        )
        if new_manual != manual_count:
            raise SchemaMigrationError(
                "v16 manual_review evaluation rows changed during rebuild: "
                f"{manual_count}->{new_manual}"
            )
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise SchemaMigrationError(
                f"v16 foreign-key violations before commit: {len(violations)}"
            )
        connection.commit()
    except Exception:
        connection.rollback()
        connection.execute("PRAGMA legacy_alter_table=OFF")
        connection.execute("PRAGMA foreign_keys=ON")
        raise
    connection.execute("PRAGMA legacy_alter_table=OFF")
    connection.execute("PRAGMA foreign_keys=ON")
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity != "ok":
        raise SchemaMigrationError(f"v16 integrity check failed: {integrity}")


def _validate_v16_structure(connection: sqlite3.Connection) -> None:
    expected = _schema_object_sql(
        object_type="table",
        name="evaluation_versions",
        prefix="CREATE TABLE IF NOT EXISTS evaluation_versions ",
    )
    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='evaluation_versions'"
    ).fetchone()
    if row is None or row[0] is None:
        raise SchemaMigrationError("schema v16 is missing evaluation_versions")
    if _normalized_schema_sql(str(row[0])) != _normalized_schema_sql(expected):
        raise SchemaMigrationError(
            "schema v16 object definition drifted: evaluation_versions"
        )
    for name, prefix in _V16_EVALUATION_INDEXES:
        expected = _schema_object_sql(object_type="index", name=name, prefix=prefix)
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
            (name,),
        ).fetchone()
        if row is None or row[0] is None:
            raise SchemaMigrationError(f"schema v16 is missing {name}")
        if _normalized_schema_sql(str(row[0])) != _normalized_schema_sql(expected):
            raise SchemaMigrationError(f"schema v16 object definition drifted: {name}")
    leftovers = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','index')"
        )
    } & (set(_V16_DROPPED_TABLES) | set(_V16_REMOVED_INDEXES))
    if leftovers:
        raise SchemaMigrationError(
            f"schema v16 still contains manual review objects: {sorted(leftovers)}"
        )


def _validate_v16(connection: sqlite3.Connection) -> None:
    _validate_v16_structure(connection)
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise SchemaMigrationError("schema v16 has foreign-key violations")


def initialize_database(connection: sqlite3.Connection) -> None:
    if int(connection.execute("PRAGMA recursive_triggers").fetchone()[0]) != 1:
        raise SchemaMigrationError(
            "database initialization requires PRAGMA recursive_triggers=ON"
        )
    tables = _table_names(connection)
    if not tables:
        _create_fresh_schema(connection)
        _validate_v9(connection)
        _validate_v10(connection)
        _validate_v11(connection)
        _validate_v12(connection)
        _validate_v13(connection)
        _validate_v14(connection)
        _validate_v15(connection)
        _validate_v16(connection)
        require_schema_compatibility(
            connection, supported_versions=frozenset({SCHEMA_VERSION})
        )
        return
    if "schema_migrations" not in tables:
        raise SchemaMigrationError("database has tables but no schema_migrations")
    row = connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    version = int(row[0]) if row is not None and row[0] is not None else 0
    pragma_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if pragma_version != version:
        raise SchemaMigrationError(
            "schema manifest and PRAGMA user_version disagree: "
            f"manifest={version},pragma={pragma_version}"
        )
    if version == 8:
        _migrate_v8_to_v9(connection)
        version = 9
    if version == 9:
        _migrate_v9_to_v10(connection)
        version = 10
    if version == 10:
        _migrate_v10_to_v11(connection)
        version = 11
    if version == 11:
        _migrate_v11_to_v12(connection)
        version = 12
    if version == 12:
        _migrate_v12_to_v13(connection)
        version = 13
    if version == 13:
        _migrate_v13_to_v14(connection)
        version = 14
    if version == 14:
        _migrate_v14_to_v15(connection)
        version = 15
    if version == 15:
        _migrate_v15_to_v16(connection)
        version = 16
    if version != SCHEMA_VERSION:
        raise SchemaMigrationError(f"unsupported schema version: {version}")
    _validate_v9(connection)
    _validate_v10(connection)
    _validate_v11(connection)
    _validate_v12(connection)
    _validate_v13(connection)
    _validate_v14(connection)
    _validate_v15(connection)
    _validate_v16(connection)
    require_schema_compatibility(
        connection, supported_versions=frozenset({SCHEMA_VERSION})
    )
