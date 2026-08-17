export type Section = "overview" | "tasks" | "accounts" | "contents" | "selling-points" | "spu-audience";
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
    status: "running" | "succeeded" | "failed" | "skipped";
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
    pending_reviews: number;
    terminal_reviews: number;
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
  };
  data_quality: Record<string, unknown>;
  data_quality_details?: {
    discovery_coverage?: DiscoveryCoverageDetail | null;
    metrics_freshness?: MetricsFreshnessDetail | null;
  } | null;
  summary_metrics: Record<string, Metric>;
  channels?: Record<OverviewChannelKey, OverviewChannel> | null;
  platform_dimensions: Array<Record<string, string | number | null>>;
  account_type_dimensions: Array<Record<string, string | number | null>>;
  content_direction_dimensions: Array<Record<string, string | number | null>>;
  content_details: Array<Record<string, string | number | boolean | null>>;
  review_summary: Array<Record<string, string | number>>;
  capture_summary: Array<Record<string, string | number>>;
  provider_costs: Array<Record<string, string | number>>;
};

export type PlatformIdentity = {
  platform: string;
  uid: string;
  nickname: string;
  real_name_status: string;
  follower_count: number | null;
  content_count: number;
};

export type PendingPlatformIdentity = {
  platform: string;
  uid: string;
  nickname: string;
  content_count: number;
  first_published_at: string | null;
  last_published_at: string | null;
};

export type Account = {
  id: number;
  phone: string;
  operator_name: string;
  account_type: string;
  content_direction: string;
  enabled: boolean;
  platforms: PlatformIdentity[];
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
  account_type: string;
  content_direction: string;
  primary_selling_point_code: string | null;
  evidence_level: string | null;
  content_automotive_score: number | null;
  display_evaluation_id: number | null;
  evaluation_release_id: string | null;
  evaluation_freshness: "current" | "stale" | "missing";
  evaluation_is_stale: boolean;
  review_queue_id: number | null;
  review_status: string | null;
  pending_review_count: number;
  terminal_review_count: number;
  view_count: number | null;
  comment_count: number | null;
  metrics_captured_at: string | null;
  duplicate_original_link_id: string | null;
  spu: ContentTagSpu | null;
  spu_secondary_count: number;
  spu_gray_count: number;
  audience: ContentTagAudience | null;
  scenes: ContentTagScene[];
};

export type EvidenceBundle = {
  content: Pick<ContentItem, "id" | "link_id" | "platform" | "canonical_url" | "title" | "body" | "content_type" | "published_at" | "raw_account_uid" | "raw_account_name">;
  base_evaluation_id: number | null;
  display_evaluation_id: number | null;
  evaluation_freshness: "current" | "stale" | "missing";
  evaluation_is_stale: boolean;
  evaluation: Record<string, unknown> | null;
  media: Array<{ artifact_id: number; index: number; kind: "video" | "image"; name: string; url: string }>;
  media_availability: { status: "available" | "omitted" | "missing"; reason: string };
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
  review: { id: number; reason_code: string; status: string } | null;
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
