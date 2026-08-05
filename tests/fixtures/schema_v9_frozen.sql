
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
