import type { ContentSearchRequest } from "../lib/queryContracts";

export const CONTENT_EXPORT_PATH = "/workbench-api/content-exports";
export type ContentExportJob = {
  id: string;
  status: "queued" | "running" | "succeeded" | "failed";
  filters: ContentSearchRequest;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  total: number | null;
  completed_rows: number;
  filename: string | null;
  error: string | null;
  request_id: string;
};
export type PendingContentExport = { request_id: string; filters: ContentSearchRequest };

const filterFields = ["query", "platform", "account_group", "business_direction", "content_direction", "selling_point", "spu_series", "audience", "scene", "published_from", "published_to"] as const;
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Pagination is deliberately excluded from export identity and scope. */
export function exportFilters(filters: ContentSearchRequest): ContentSearchRequest {
  return {
    page: 1, page_size: 100, query: filters.query ?? "", platform: filters.platform || null,
    account_group: filters.account_group || null, business_direction: filters.business_direction || null,
    content_direction: filters.content_direction || null, selling_point: filters.selling_point || null,
    spu_series: filters.spu_series || null, audience: filters.audience || null, scene: filters.scene || null,
    ...(filters.published_from ? { published_from: filters.published_from } : {}),
    ...(filters.published_to ? { published_to: filters.published_to } : {}),
  };
}

export function exportFilterKey(filters: ContentSearchRequest): string {
  return JSON.stringify(exportFilters(filters));
}

export function hasExportFilters(filters: ContentSearchRequest): boolean {
  return filterFields.some((field) => typeof filters[field] === "string" && filters[field]!.trim() !== "");
}

function validFilters(value: unknown): value is ContentSearchRequest {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const fields = value as Record<string, unknown>;
  return filterFields.every((key) => fields[key] == null || typeof fields[key] === "string");
}

export function isContentExportJob(value: unknown): value is ContentExportJob {
  if (!value || typeof value !== "object") return false;
  const job = value as ContentExportJob;
  return typeof job.id === "string" && /^[a-zA-Z0-9_-]{1,100}$/.test(job.id)
    && ["queued", "running", "succeeded", "failed"].includes(job.status)
    && validFilters(job.filters) && typeof job.created_at === "string"
    && typeof job.request_id === "string" && uuid.test(job.request_id)
    && (job.total === null || Number.isSafeInteger(job.total) && job.total >= 0)
    && Number.isSafeInteger(job.completed_rows) && job.completed_rows >= 0
    && (job.filename == null || typeof job.filename === "string")
    && (job.error == null || typeof job.error === "string");
}

export function isActiveExport(job: ContentExportJob): boolean {
  return job.status === "queued" || job.status === "running";
}

export function matchingActiveExport(jobs: ContentExportJob[], filters: ContentSearchRequest): ContentExportJob | undefined {
  const key = exportFilterKey(filters);
  return jobs.find((job) => isActiveExport(job) && exportFilterKey(job.filters) === key);
}

export function readPendingExport(value: string | null): PendingContentExport | null {
  try {
    const pending = JSON.parse(value ?? "null") as PendingContentExport | null;
    if (!pending || typeof pending.request_id !== "string" || !uuid.test(pending.request_id)
        || !validFilters(pending.filters) || !hasExportFilters(pending.filters)) return null;
    return { request_id: pending.request_id, filters: exportFilters(pending.filters) };
  } catch { return null; }
}

export function exportGenerationBlock(input: {
  filters: ContentSearchRequest; total: number | null; queryPending: boolean; queryError: boolean; unappliedQuery: boolean;
}): string {
  if (input.unappliedQuery) return "搜索词尚未应用，请先搜索，再导出筛选结果。";
  if (!hasExportFilters(input.filters)) return "请先选择发布时间范围，再生成导出文件。";
  if (input.queryError) return "当前筛选结果读取失败，请重新搜索后再导出。";
  if (input.queryPending || input.total === null || !Number.isSafeInteger(input.total) || input.total < 0) return "正在确认当前筛选结果，请稍候。";
  if (input.total === 0) return "当前筛选没有内容，请调整条件后再导出。";
  return "";
}

export function exportStatusText(job: ContentExportJob): string {
  if (job.status === "queued") return "等待生成";
  if (job.status === "succeeded") return `已完成 · ${(job.total ?? job.completed_rows).toLocaleString("zh-CN")} 条`;
  if (job.status === "failed") return "生成失败";
  return job.total == null ? "正在生成" : `正在生成 · ${job.completed_rows.toLocaleString("zh-CN")} / ${job.total.toLocaleString("zh-CN")} 条`;
}
