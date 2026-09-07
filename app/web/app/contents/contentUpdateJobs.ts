import { ApiRequestError, jsonRequest, readQueryJson } from "../lib/api";
import type { ContentUpdateResult } from "./contentUpdate";
import type { ServiceHealth } from "../lib/serviceStatus";
export const CONTENT_UPDATE_API_BASE = "/workbench-api";

export function contentUpdateAvailability(health: ServiceHealth | undefined, failed: boolean) {
  if (failed) return "unavailable";
  if (!health) return "checking";
  if (health.status !== "ok" || typeof health.read_only !== "boolean") return "unavailable";
  if (health.read_only || health.automation?.scheduler_state === "read_only") return "read_only";
  return "available";
}

export type ContentUpdateJob = {
  id: number;
  request_id?: string;
  request_ids?: string[];
  content_id: number;
  title: string;
  status: "queued" | "running" | "succeeded" | "failed";
  stage: string;
  stage_label: string;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
  result: ContentUpdateResult | null;
  error: string | null;
  error_code?: string | null;
};
export type PendingContentUpdate = {
  contentId: number;
  title: string;
  requestId: string;
  createdAt: string;
  status: "submitting" | "uncertain" | "rejected";
  error: string;
};

export function isActiveContentUpdateJob(job: ContentUpdateJob) {
  return job.status === "queued" || job.status === "running";
}
export function contentUpdateJobBlocksWrites(job: ContentUpdateJob) {
  return isActiveContentUpdateJob(job) || job.error_code === "result_uncertain";
}

export function isContentUpdateJob(value: unknown): value is ContentUpdateJob {
  if (!value || typeof value !== "object") return false;
  const job = value as ContentUpdateJob;
  return Number.isSafeInteger(job.id) && job.id > 0 && Number.isSafeInteger(job.content_id) && job.content_id > 0
    && ["queued", "running", "succeeded", "failed"].includes(job.status)
    && typeof job.title === "string" && typeof job.stage_label === "string" && typeof job.created_at === "string";
}

export function formatJobElapsed(job: Pick<ContentUpdateJob, "created_at" | "completed_at">, now: number) {
  const start = Date.parse(job.created_at);
  const end = job.completed_at ? Date.parse(job.completed_at) : now;
  if (!Number.isFinite(start) || !Number.isFinite(end)) return "用时待确认";
  const seconds = Math.max(0, Math.floor((end - start) / 1000));
  if (seconds < 60) return `已用时 ${seconds} 秒`;
  const minutes = Math.floor(seconds / 60);
  return minutes < 60 ? `已用时 ${minutes} 分 ${seconds % 60} 秒` : `已用时 ${Math.floor(minutes / 60)} 小时 ${minutes % 60} 分`;
}

// Only explicit submission calls this function. Polling and read retries never do.
export async function submitContentUpdateJob(contentId: number, requestId: string, title: string): Promise<ContentUpdateJob> {
  const response = await readQueryJson<{ job: ContentUpdateJob }>(
    `${CONTENT_UPDATE_API_BASE}/contents/${contentId}/update-jobs`, jsonRequest({ request_id: requestId, title: title.slice(0, 500) }), 15_000,
  );
  if (!isContentUpdateJob(response.job) || response.job.content_id !== contentId) {
    throw new ApiRequestError("提交回执未能确认，请重新读取状态或确认原请求。", { status: 202, retryable: false });
  }
  return response.job;
}

export function submissionFailureStatus(reason: unknown): PendingContentUpdate["status"] {
  // A timeout/network/5xx/malformed success may still have queued paid work.
  return reason instanceof ApiRequestError && reason.status != null && reason.status >= 400 && reason.status < 500
    ? "rejected" : "uncertain";
}

export function readPendingContentUpdates(raw: string | null): PendingContentUpdate[] {
  try {
    const values: unknown = JSON.parse(raw ?? "[]");
    if (!Array.isArray(values)) return [];
    return values.filter((value): value is PendingContentUpdate => value && Number.isSafeInteger(value.contentId)
      && value.contentId > 0 && typeof value.requestId === "string" && /^[0-9a-f-]{36}$/i.test(value.requestId)
      && typeof value.title === "string" && typeof value.createdAt === "string"
      && ["submitting", "uncertain", "rejected"].includes(value.status))
      .map((value) => ({ ...value, status: value.status === "submitting" ? "uncertain" : value.status,
        error: value.status === "submitting" ? "上次提交的结果尚未确认，请先查询状态。" : value.error || "" }));
  } catch { return []; }
}

export function reconcilePendingContentUpdates(pending: PendingContentUpdate[], jobs: ContentUpdateJob[]) {
  // Match the actual idempotency key, never guess ownership from a content ID/time.
  return pending.filter((request) => !jobs.some((job) => (job.request_id === request.requestId || job.request_ids?.includes(request.requestId)) && job.content_id === request.contentId));
}

export function mergeContentUpdateJobs(existing: ContentUpdateJob[], incoming: ContentUpdateJob[]) {
  const jobs = new Map(existing.map((job) => [job.id, job]));
  for (const job of incoming) {
    const previous = jobs.get(job.id);
    // A slow list response must not rewind a terminal receipt or a later stage.
    if (previous && ((!isActiveContentUpdateJob(previous) && isActiveContentUpdateJob(job))
      || Date.parse(previous.updated_at) > Date.parse(job.updated_at))) continue;
    jobs.set(job.id, job);
  }
  return [...jobs.values()].sort((a, b) => Number(isActiveContentUpdateJob(b)) - Number(isActiveContentUpdateJob(a)) || b.id - a.id).slice(0, 100);
}
