export type Section = "overview" | "tasks" | "accounts" | "contents" | "selling-points";
export type WindowKey = "yesterday" | "this_week" | "last_week";

export type Metric = {
  kind: "quantity" | "ratio" | "score";
  value?: number | null;
  percentage?: number | null;
  unit: string;
  status: string;
  coverage_percentage?: number | null;
  reason: string;
};

export type OverviewWindow = {
  period_start: string;
  period_end: string;
  eligible_count: number;
  unassociated_content_count: number;
  metrics: Record<string, Metric>;
  empty_explanation: string;
};

export type Overview = {
  status: string;
  report_version: string;
  generated_at: string;
  timezone: string;
  windows: Record<WindowKey, OverviewWindow>;
  data_quality: {
    missing_published_at: number;
    pending_reviews: number;
    terminal_reviews: number;
  };
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
  message?: string;
};

export type TaskDetail = Task & {
  events: Array<{ id: number; event_type: string; message: string; created_at: string }>;
  revisions: Array<{
    revision: number;
    created_at: string;
    invalidated_at?: string | null;
    files: Array<{ file_kind: string; byte_size: number; status: string }>;
  }>;
  content_counts: Record<string, number>;
};

export type ReportView = {
  task: { task_status: string; name: string };
  data_quality: Record<string, number>;
  summary_metrics: Record<string, Metric>;
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
  review_queue_id: number | null;
  review_status: string | null;
  pending_review_count: number;
  terminal_review_count: number;
  view_count: number | null;
  comment_count: number | null;
  metrics_captured_at: string | null;
  duplicate_original_link_id: string | null;
};

export type EvidenceBundle = {
  content: Pick<ContentItem, "id" | "link_id" | "platform" | "canonical_url" | "title" | "body" | "content_type" | "published_at" | "raw_account_uid" | "raw_account_name">;
  base_evaluation_id: number | null;
  evaluation: Record<string, unknown> | null;
  media: Array<{ artifact_id: number; index: number; kind: "video" | "image"; name: string; url: string }>;
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

export type SellingPoint = {
  code: string;
  tier: string;
  label: string;
  definition: string;
  scenes: string[];
  positive_evidence?: string[];
  negative_evidence?: string[];
  boundary_rules?: string[];
  enabled?: boolean;
  primary_hits?: number;
  total_hits?: number;
};

export type SellingPointResponse = {
  taxonomy: { version: string; status: string } | null;
  items: SellingPoint[];
};
