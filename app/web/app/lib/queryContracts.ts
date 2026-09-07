import type { AccountStatus, SpuAssociationRun, SpuAudienceAssets } from "./types";

export type ContentSearchFilters = {
  query: string;
  platform: string;
  accountType: string;
  direction: string;
  sellingPoint: string;
  spuSeries: string;
  audience: string;
  scene: string;
  publishedFrom?: string;
  publishedTo?: string;
};

export type ContentSearchRequest = {
  page: number;
  page_size: number;
  query: string;
  platform: string | null;
  account_type: string | null;
  content_direction: string | null;
  selling_point: string | null;
  spu_series: string | null;
  audience: string | null;
  scene: string | null;
  published_from?: string;
  published_to?: string;
};

export type AccountSearchFilters = {
  query: string;
  platform: string;
  accountType: string;
  direction: string;
  accountStatus?: AccountStatus | "";
};

export type AccountSearchRequest = {
  page: number;
  page_size: number;
  query: string;
  platform: string | null;
  account_type: string | null;
  content_direction: string | null;
  account_status: AccountStatus | null;
};

function positiveInteger(value: number) {
  return Math.max(1, Math.trunc(value));
}

export function buildContentSearchRequest(
  filters: ContentSearchFilters,
  page: number,
  pageSize: number,
): ContentSearchRequest {
  return {
    page: positiveInteger(page),
    page_size: positiveInteger(pageSize),
    query: filters.query,
    platform: filters.platform || null,
    account_type: filters.accountType || null,
    content_direction: filters.direction || null,
    selling_point: filters.sellingPoint || null,
    spu_series: filters.spuSeries || null,
    audience: filters.audience || null,
    scene: filters.scene || null,
    ...(filters.publishedFrom ? { published_from: filters.publishedFrom } : {}),
    ...(filters.publishedTo ? { published_to: filters.publishedTo } : {}),
  };
}

export function buildAccountSearchRequest(
  filters: AccountSearchFilters,
  page: number,
  pageSize: number,
): AccountSearchRequest {
  return {
    page: positiveInteger(page),
    page_size: positiveInteger(pageSize),
    query: filters.query,
    platform: filters.platform || null,
    account_type: filters.accountType || null,
    content_direction: filters.direction || null,
    account_status: filters.accountStatus || null,
  };
}

export function lastPageFor(total: number, pageSize: number) {
  return Math.max(1, Math.ceil(Math.max(0, total) / positiveInteger(pageSize)));
}

const generatingTaskStatuses = new Set(["queued", "running", "cancel_requested"]);

export function isGeneratingTaskStatus(status: string | null | undefined) {
  return Boolean(status && generatingTaskStatuses.has(status));
}

export function shouldAutoAssociateSpu(assets: SpuAudienceAssets) {
  return assets.ready
    && (assets.stale_content_count ?? 0) > 0
    && assets.last_run?.status !== "running";
}

export function didSpuRunReachTerminal(
  previous: SpuAssociationRun["status"] | null,
  current: SpuAssociationRun["status"] | null,
) {
  return previous === "running" && current !== null && current !== "running";
}
