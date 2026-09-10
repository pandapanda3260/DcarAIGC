import type { AccountGroup, BusinessDirection } from "./accountClassification";

export type Section = "overview" | "tasks" | "accounts" | "contents" | "selling-points" | "spu-audience" | "users";
export type UserRole = "superadmin" | "admin" | "operator" | "new_user";
export type UserStatus = "active" | "disabled";
export type AuthSession = { authenticated: true; username: string; role?: UserRole };
export type ManagedUser = {
  username: string; phone: string | null; role: UserRole; status: UserStatus;
  created_at: string; password_updated_at: string;
};
export type ManagedUsersResult = { actor: { username: string; role: UserRole }; items: ManagedUser[] };
export type WindowKey = "yesterday" | "this_week" | "last_week";
export type OverviewChannelKey = "douyin" | "xiaohongshu";
export type BusinessSceneKey = "used_car" | "new_car" | "media";
export type ConclusionMetricKey =
  | "selling_point_count_share"
  | "core_selling_point_count_share"
  | "selling_point_exposure_share"
  | "core_selling_point_exposure_share"
  | "content_verticality"
  | "automotive_user_rate"
  | "acquisition_potential";

export type AudienceQuality = {
  captured_comment_count: number;
  declared_comment_count: number;
  comment_collection_coverage_percentage: number | null;
  identity_coverage_percentage: number | null;
  candidate_user_count: number;
  classified_user_count: number;
  classification_coverage_percentage: number | null;
  capped_content_count: number;
  audience_definition_version: string;
  classifier_version: string;
  user_key_version: string;
  evidence_window_start: string;
  evidence_window_end: string;
  report_cutoff_at: string;
  warm_up: boolean;
};

export type MetricStatus =
  | "available"
  | "below_threshold"
  | "sample_only"
  | "not_applicable"
  | "not_calculable"
  | "missing"
  | "stale";

export type Metric = {
  kind: "quantity" | "ratio" | "score";
  value?: number | null;
  numerator?: number | null;
  denominator?: number;
  percentage?: number | null;
  unit: string;
  status: MetricStatus;
  eligible_count?: number | null;
  scale?: number;
  scorable_items?: number;
  total_items?: number;
  coverage_percentage?: number | null;
  reason: string;
};

export type ConclusionGroup = {
  label: string;
  publication_count: number;
  audience_quality?: AudienceQuality | null;
  metrics: Record<ConclusionMetricKey, Metric>;
};

export type OverviewChannel = {
  platform: OverviewChannelKey;
  label: string;
  publication_count: number;
  evidence_coverage_percentage: number | null;
  valid_exposure_items: number;
  exposure_coverage_percentage: number | null;
  summary: ConclusionGroup;
  scenes: Record<BusinessSceneKey, ConclusionGroup>;
};

export type OverviewWindow = {
  period_start: string;
  period_end: string;
  eligible_count: number;
  unassociated_content_count: number;
  metrics: Record<string, Metric>;
  channels: Record<OverviewChannelKey, OverviewChannel>;
};

export type DataFreshness = {
  status: "current" | "stale" | "unknown";
  latest_published_at: string | null;
  last_successful_capture_at: string | null;
  latest_capture_run: {
    scheduled_for: string;
    status: "running" | "succeeded" | "failed" | "partial" | "interrupted" | "skipped";
    completed_at: string | null;
  } | null;
};

export type Overview = {
  status: string;
  report_version: string;
  generated_at: string;
  timezone: string;
  windows: Record<WindowKey, OverviewWindow>;
  data_freshness: DataFreshness;
  data_quality: {
    missing_published_at: number;
    duplicate_fingerprint_coverage: number;
    duplicate_calibration_ready: boolean;
    confirmed_duplicate_count: number;
  };
};

export type TaskRevision = {
  revision: number;
  created_at: string;
  invalidated_at?: string | null;
  invalidation_reason?: string | null;
  revision_state: "current" | "stale" | "historical";
  files: Array<{ file_kind: string; byte_size: number; status: string }>;
};

export type Task = {
  id: string;
  name: string;
  task_type: string;
  period_start: string;
  period_end: string;
  task_status: string;
  progress: number;
  content_count: number;
  revision_count: number;
  historical_revision_count: number;
  current_valid_revision: TaskRevision | null;
  stale_display_revision: TaskRevision | null;
  display_effective_revision: TaskRevision | null;
  message?: string;
};

export type TaskDetail = Task & {
  events: Array<{ id: number; event_type: string; message: string; created_at: string }>;
  revisions: TaskRevision[];
  content_counts: Record<string, number>;
};

export type MetricsFreshnessDetail = {
  status: "available" | "below_threshold" | "not_applicable";
  fresh_count: number;
  as_of_snapshot_count: number;
  eligible_count: number;
  percentage: number | null;
  reason: string;
};

export type DiscoveryCoverageDetail = {
  status: "available" | "below_threshold" | "not_applicable";
  covered_identity_occurrence_count: number;
  eligible_identity_occurrence_count: number;
  observed_occurrence_count: number;
  expected_occurrence_count: number;
  percentage: number | null;
  reason: string;
};

export type ReportView = {
  task: { task_status: string; name: string };
  metadata?: {
    collection_cutoff_at?: string | null;
    account_classification_version?: string;
  };
  data_quality: Record<string, unknown>;
  data_quality_details?: {
    discovery_coverage?: DiscoveryCoverageDetail | null;
    metrics_freshness?: MetricsFreshnessDetail | null;
  } | null;
  summary_metrics: Record<string, Metric>;
  channels?: Record<OverviewChannelKey, OverviewChannel> | null;
  platform_dimensions: Array<Record<string, string | number | null>>;
  account_group_dimensions?: Array<Record<string, string | number | null>>;
  business_direction_dimensions?: Array<Record<string, string | number | null>>;
  /** Frozen legacy reports only; never relabel these as current account groups. */
  account_type_dimensions?: Array<Record<string, string | number | null>>;
  content_direction_dimensions: Array<Record<string, string | number | null>>;
  content_details: Array<Record<string, string | number | boolean | null>>;
  capture_summary: Array<Record<string, string | number>>;
  provider_costs: Array<Record<string, string | number>>;
};

export type PlatformIdentity = {
  id: number;
  platform: string;
  uid: string | null;
  nickname: string;
  real_name_status: string;
  avatar_url: string | null;
  unique_id: string | null;
  matrix_account_id: string | null;
  profile_ref: string | null;
  monitoring_status: "monitored" | "not_monitored" | "unknown";
  authorization_status: "authorized" | "unauthorized" | "unknown";
  follower_count: number | null;
  platform_work_count: number | null;
  content_count: number;
  data_date: string | null;
  data_status: string;
};

export type AccountStatus = "daily" | "weekly" | "paused" | "unmarked";

export type Account = {
  directory_row_id?: number;
  directory_identity_status?: "existing_verified" | "uid_unverified" | "identity_missing";
  id: number;
  phone: string;
  operator_name: string;
  account_group: AccountGroup;
  business_direction: BusinessDirection;
  account_status: AccountStatus;
  update_frequency: "daily" | "weekly" | null;
  enabled: boolean;
  automatic_capture?: {
    eligible: boolean;
    reason_code: string;
    reason_label: string;
  };
  platforms: PlatformIdentity[];
};

export type AccountRosterStatus = {
  ready: boolean;
  active_profile_id: "matrix_hybrid_v1" | "tikhub_managed_v1" | "integrated_route_v1" | null;
  activation_id: number | null;
  source_family: "matrix" | "system";
  snapshot_id: number | null;
  pending_snapshot_id: number | null;
  source_type: string | null;
  source_captured_at: string | null;
  accepted_at: string | null;
  current_count: number;
  unresolved_count: number;
  pending_removal_count: number;
  sync_mode: string;
  message: string;
  diff?: Record<string, unknown>;
};

export type DouyinAuthorizationState = "active" | "unbound" | "pending_match";

export type DouyinAuthorization = {
  id: string;
  bound_username: string;
  account_id: number | null;
  platform_uid: string | null;
  access_expires_at: number | null;
  refresh_expires_at: number | null;
  renew_count: number;
  scopes: string[];
  version: number;
  needs_reauthorization: boolean;
  status: DouyinAuthorizationState;
  match_reason: string | null;
  updated_at: number;
};

export type DouyinAuthorizationStatus = {
  id: string;
  account_id: number | null;
  platform_uid: string | null;
  status: DouyinAuthorizationState;
  match_reason: string | null;
  refresh_expires_at: number | null;
  needs_reauthorization: boolean;
  updated_at: number;
  authorized: boolean;
  scopes: string[];
};

export type ContentTagSpu = {
  spu_id: string;
  series: string;
  brand: string;
  trim_label: string | null;
  resolved_level: "series" | "trim";
  score: number;
  matched_aliases: string[];
};

export type ContentTagAudience = {
  code: string;
  label: string;
  source: "content_explicit" | "rule_prior" | "llm";
};

export type ContentTagScene = { code: string; label: string };

export type ContentItem = {
  id: number;
  link_id: string;
  platform: string;
  canonical_url: string;
  platform_content_id: string | null;
  published_at: string | null;
  title: string;
  body: string;
  content_type: string;
  raw_account_uid: string;
  raw_account_name: string;
  account_group: AccountGroup;
  business_direction: BusinessDirection;
  content_direction: string;
  primary_selling_point_code: string | null;
  evidence_level: string | null;
  content_automotive_score: number | null;
  display_evaluation_id: number | null;
  evaluation_release_id: string | null;
  evaluation_freshness: "current" | "stale" | "missing";
  evaluation_is_stale: boolean;
  view_count: number | null;
  comment_count: number | null;
  like_count: number | null;
  metrics_captured_at: string | null;
  duplicate_original_link_id: string | null;
  spu: ContentTagSpu | null;
  spu_secondary_count: number;
  spu_gray_count: number;
  audience: ContentTagAudience | null;
  scenes: ContentTagScene[];
  // 列表接口的非数据库字段：writer 按证据台账投影，只读副本仅投影有效保留预览；只决定媒体框样式与去向
  local_media_available: boolean;
};

export type EvidenceMedia = {
  artifact_id: number; index: number; kind: "video" | "image"; name: string; url: string;
  bundle_id?: string; member_id?: string; original_index?: number;
  sha256?: string; byte_size?: number; available?: boolean;
};

export type MediaLifecycle = {
  bundle_id: string; state: string; operation_state: string;
  reason: string; http_status: number; read_only: boolean;
  can_restore: boolean; can_reprocess: boolean; can_reacquire: boolean;
  archive_verified_at: string | null; delete_due_at: string | null; deleted_at: string | null;
  registered_at: string; original_artifact_id: number; original_member_count: number; original_bytes: number;
  evidence_cutoff: unknown; protections: Record<string, unknown>; last_error: string | null;
  restore_request: { status: string; run_id?: number; requested_at?: string } | null;
  completion_gate_aged: { reason: string; first_listed_at: string } | null;
};

export type MediaLifecycleSummary = {
  read_only: boolean; snapshot_only: boolean; as_of: string;
  snapshot_captured_at?: string | null; snapshot_lag_seconds?: number | null;
  counts: Record<string, number>; totals: Record<string, number>;
  manual_count: number | null; manual_bytes: number | null; manual_longest_age_seconds: number | null;
  earliest_delete_due_at: string | null; archive_root_health: string;
  latest_jobs: Array<{ id: number; job_id: string; status: string; completed_at: string | null; reason?: string | null }>;
  manual_todos: Array<{ bundle_id: string; content_id: number; link_id: string;
    registered_at?: string | null; registered_bytes: number | null; member_count: number | null;
    age_seconds: number | null; protected: boolean; evidence_ready?: boolean;
    account_id?: number | null; platform?: string | null; account_uid?: string | null; account_name?: string | null;
    latest_processing?: { processor_type: string; status: string; attempt_count: number; updated_at: string } | null;
    protections?: Record<string, unknown>;
    first_listed_at: string; last_error: string | null; blockers: string[]; resolution: string | null }>;
  blocker_groups?: Array<{ category: string; count: number; registered_bytes: number | null }>;
  expiry_debt?: Array<{ bundle_id: string; content_id: number; link_id: string;
    account_id?: number | null; platform?: string | null; registered_bytes: number | null;
    delete_due_at: string; overdue_seconds: number | null; operation_state: string;
    delay_reason: string; protected: boolean; last_error: string | null }>;
  expiry_debt_bytes?: number | null; expiry_debt_longest_overdue_seconds?: number | null;
};

export type EvidenceBundle = {
  content: Pick<ContentItem, "id" | "link_id" | "platform" | "canonical_url" | "title" | "body" | "content_type" | "published_at" | "raw_account_uid" | "raw_account_name">;
  display_evaluation_id: number | null;
  evaluation_freshness: "current" | "stale" | "missing";
  evaluation_is_stale: boolean;
  evaluation: Record<string, unknown> | null;
  media: EvidenceMedia[];
  media_availability: { status: "available" | "omitted" | "missing" | "unavailable"; reason: string; code?: string };
  previews?: EvidenceMedia[];
  media_lifecycle?: MediaLifecycle | null;
  read_only?: boolean;
  asr: { status: string; model: string | null; text: string };
  ocr: { status: string; observation_count: number; text: string };
  comments: {
    status: string;
    captured_at: string | null;
    declared_count: number | null;
    stored_count: number;
    top_items: Array<{ body: string; like_count: number | null; published_at: string | null }>;
  };
  processing_slots: Array<{
    id: number;
    processor_type: string;
    processor_version: string;
    status: string;
    attempt_count: number;
    error_message: string | null;
    updated_at: string;
  }>;
};

export type SellingPointChannelWindowHits = {
  primary_hits: number;
  primary_views: number;
};

export type SellingPointWindowSceneHits = {
  primary_hits: number;
  total_hits: number;
  channels: Partial<Record<OverviewChannelKey, SellingPointChannelWindowHits>>;
};

export type SellingPointWindowMeta = {
  period_start: string;
  period_end: string;
  scene_denominators: Partial<Record<BusinessSceneKey, Partial<Record<OverviewChannelKey, {
    publication_count: number;
    valid_exposure_views: number;
  }>>>>;
};

export type SellingPoint = {
  code: string;
  tier: string;
  label: string;
  definition: string;
  matcher_rule: Record<string, unknown> | null;
  readonly scenes: ReadonlyArray<BusinessSceneKey>;
  readonly positive_evidence: ReadonlyArray<string>;
  readonly negative_evidence: ReadonlyArray<string>;
  readonly boundary_rules: ReadonlyArray<string>;
  enabled?: boolean;
  primary_hits?: number;
  total_hits?: number;
  readonly scene_hits?: Partial<Record<BusinessSceneKey, { primary_hits: number; total_hits: number }>>;
  readonly window_hits?: Partial<Record<WindowKey, Partial<Record<BusinessSceneKey, SellingPointWindowSceneHits>>>>;
};

export type SellingPointResponse = {
  taxonomy: { version: string; status: string } | null;
  windows?: Partial<Record<WindowKey, SellingPointWindowMeta>>;
  items: SellingPoint[];
};

export type SpuAliasEntry = { alias: string; alias_type: string; ambiguous: boolean };

export type SpuAssetRow = {
  spu_id: string;
  brand: string;
  series: string;
  series_slug: string;
  trim_label: string | null;
  is_series_node: boolean;
  model_year: number | null;
  powertrain: string;
  body_style: string;
  price_low: number | null;
  price_high: number | null;
  audience_primary: string | null;
  audience_secondary: string | null;
  aliases: SpuAliasEntry[];
};

export type SpuAudienceDim = { code: string; label: string; definition: string; signals: string[] };

export type SpuSceneDim = {
  code: string;
  label: string;
  definition: string;
  triggers: string[];
  negatives: string[];
};

export type SpuLlmSummary = {
  enabled?: boolean;
  targets?: number;
  called?: number;
  cache_hits?: number;
  accepted?: number;
  rejected?: number;
  errors?: number;
  spu_filled?: number;
  gray_upgraded?: number;
  gray_overridden?: number;
  trim_refined?: number;
  scene_filled?: number;
  audience_filled?: number;
  out_of_catalog?: number;
  aborted?: string | null;
  error?: string;
  note?: string;
};

export type SpuAssociationRun = {
  id: number;
  started_at: string;
  finished_at: string | null;
  status: "running" | "succeeded" | "failed";
  rule_version: string;
  contents_total: number;
  spu_linked: number;
  trim_resolved: number;
  gray_count: number;
  scene_linked: number;
  audience_linked: number;
  insufficient_evidence: number;
  summary?: { processed?: number; eligible?: number; published_total?: number; mode?: string; note?: string; phase?: string; llm_processed?: number; llm_total?: number; llm?: SpuLlmSummary | null } | null;
};

export type SpuAudienceAssets = {
  ready: boolean;
  rule_version?: string;
  seed_version?: string;
  spu: SpuAssetRow[];
  audiences: SpuAudienceDim[];
  scenes: SpuSceneDim[];
  audience_scene_map: Array<{ audience_code: string; core: string[]; related: string[] }>;
  last_run: SpuAssociationRun | null;
  stale_content_count?: number;
};

export type SpuStatsKey = { code: string; label: string };

export type SpuStatsDetailRow = {
  spu: { spu_id: string; label: string; series: string | null; trim_label: string | null };
  audience: SpuStatsKey;
  scene: SpuStatsKey;
  posts: number;
  views: number | null;
  low_sample: boolean;
};

export type SpuChannelShare = {
  posts: number;
  views: number | null;
  post_share: number | null;
  view_share: number | null;
  post_denominator: number;
  view_denominator: number;
};

export type SpuRollupRow = {
  key: string;
  label: string;
  posts: number;
  views: number | null;
  channels?: Record<string, SpuChannelShare>;
};

export type SpuAudienceStats = {
  ready: boolean;
  window?: string;
  platform?: string;
  rule_version?: string;
  totals?: { posts: number; valid_exposure_views: number };
  coverage?: {
    spu_percentage: number | null;
    audience_percentage: number | null;
    scene_percentage: number | null;
    trim_percentage: number | null;
  };
  exposure_gate?: { classified_share: number | null; threshold: number; status: string };
  channel_totals?: Record<string, { posts: number; valid_views: number; classified_share: number | null; views_published: boolean }>;
  detail?: SpuStatsDetailRow[];
  spu_rollup?: SpuRollupRow[];
  audience_rollup?: SpuRollupRow[];
  scene_rollup?: SpuRollupRow[];
  gaps?: {
    missing: Array<{ series: string; series_posts: number; audience: SpuStatsKey; scene: SpuStatsKey }>;
    overflow: Array<{ audience: SpuStatsKey; scene: SpuStatsKey; posts: number }>;
  };
  footnotes?: string[];
};
