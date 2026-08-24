import { keepPreviousData, queryOptions } from "@tanstack/react-query";
import { jsonRequest, readJson } from "./api";
import { isGeneratingTaskStatus } from "./queryContracts";
import type { AccountSearchRequest, ContentSearchRequest } from "./queryContracts";
import type {
  Account,
  ContentItem,
  DouyinAuthorization,
  DouyinAuthorizationStatus,
  Overview,
  PendingPlatformIdentity,
  ReportView,
  SellingPointResponse,
  SpuAudienceAssets,
  SpuAudienceStats,
  Task,
  TaskDetail,
} from "./types";

export type ContentSearchResult = { items: ContentItem[]; total: number };
export type AccountSearchResult = {
  items: Account[];
  total: number;
  legacy_unassociated_content_count: number;
  pending_platform_identity_count: number;
  pending_platform_identities: PendingPlatformIdentity[];
};
export type DouyinAuthorizationsResult = { items: DouyinAuthorization[] };
export type DouyinAuthorizationStatusesResult = { items: DouyinAuthorizationStatus[] };

function readDouyinAuthorizations() {
  return readJson<DouyinAuthorizationsResult>("/api/douyin/authorizations");
}

export const queryKeys = {
  overview: ["overview"] as const,
  contents: ["contents"] as const,
  contentSearch: (request: ContentSearchRequest) => ["contents", "search", request] as const,
  accounts: ["accounts"] as const,
  accountSearch: (request: AccountSearchRequest) => ["accounts", "search", request] as const,
  douyinAuthorizations: ["douyin", "authorizations"] as const,
  authorizationStatuses: ["douyin", "authorization-statuses"] as const,
  sellingPoints: ["selling-points"] as const,
  activeSellingPoints: ["selling-points", "active"] as const,
  draftSellingPoints: ["selling-points", "draft"] as const,
  spu: ["spu"] as const,
  spuAssets: ["spu", "assets"] as const,
  spuStatsPrefix: ["spu", "stats"] as const,
  spuStats: (window: string, platform: string) => ["spu", "stats", window, platform] as const,
  tasks: ["tasks"] as const,
  tasksList: ["tasks", "list"] as const,
  taskDetail: (taskId: string) => ["tasks", "detail", taskId] as const,
  taskReport: (taskId: string, revision: number | null | undefined) => ["tasks", "report", taskId, revision] as const,
};

export function overviewQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.overview,
    queryFn: () => readJson<Overview>("/api/v8/overview"),
  });
}

export function contentSearchQueryOptions(request: ContentSearchRequest) {
  return queryOptions({
    queryKey: queryKeys.contentSearch(request),
    queryFn: () => readJson<ContentSearchResult>("/api/v8/contents/search", jsonRequest(request)),
    placeholderData: keepPreviousData,
  });
}

export function accountSearchQueryOptions(request: AccountSearchRequest) {
  return queryOptions({
    queryKey: queryKeys.accountSearch(request),
    queryFn: () => readJson<AccountSearchResult>("/api/v8/accounts/search", jsonRequest(request)),
    placeholderData: keepPreviousData,
  });
}

export function douyinAuthorizationsQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.douyinAuthorizations,
    queryFn: readDouyinAuthorizations,
    staleTime: 0,
    refetchOnMount: "always",
  });
}

export function douyinAuthorizationStatusesQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.authorizationStatuses,
    queryFn: () => readJson<DouyinAuthorizationStatusesResult>("/api/douyin/authorization-statuses"),
    staleTime: 0,
    refetchOnMount: "always",
    refetchOnWindowFocus: "always",
  });
}

export function activeSellingPointsQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.activeSellingPoints,
    queryFn: () => readJson<SellingPointResponse>("/api/v8/selling-points"),
    staleTime: 60_000,
    refetchOnWindowFocus: true,
  });
}

export function draftSellingPointsQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.draftSellingPoints,
    queryFn: () => readJson<SellingPointResponse>("/api/v8/selling-points/draft"),
  });
}

export function spuAssetsQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.spuAssets,
    queryFn: () => readJson<SpuAudienceAssets>("/api/v8/spu-audience/assets"),
    refetchInterval: (query) => query.state.data?.last_run?.status === "running" ? 5_000 : false,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: (query) => query.state.data?.last_run?.status === "running" ? "always" : false,
  });
}

export function spuStatsQueryOptions(window: string, platform: string) {
  const search = new URLSearchParams({ window });
  if (platform) search.set("platform", platform);
  return queryOptions({
    queryKey: queryKeys.spuStats(window, platform),
    queryFn: () => readJson<SpuAudienceStats>(`/api/v8/spu-audience/stats?${search.toString()}`),
    placeholderData: keepPreviousData,
  });
}

export function tasksListQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.tasksList,
    queryFn: () => readJson<{ items: Task[] }>("/api/v8/tasks"),
    refetchInterval: (query) => query.state.data?.items.some((task) => isGeneratingTaskStatus(task.task_status)) ? 1_500 : false,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: (query) => query.state.data?.items.some((task) => isGeneratingTaskStatus(task.task_status)) ? "always" : false,
  });
}

export function taskDetailQueryOptions(taskId: string) {
  return queryOptions({
    queryKey: queryKeys.taskDetail(taskId),
    queryFn: () => readJson<TaskDetail>(`/api/v8/tasks/${encodeURIComponent(taskId)}`),
    refetchInterval: (query) => isGeneratingTaskStatus(query.state.data?.task_status) ? 1_500 : false,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: (query) => isGeneratingTaskStatus(query.state.data?.task_status) ? "always" : false,
  });
}

export function taskReportQueryOptions(taskId: string, revision: number | null | undefined) {
  return queryOptions({
    queryKey: queryKeys.taskReport(taskId, revision),
    queryFn: () => {
      if (!revision) throw new Error("缺少报告版本。");
      return readJson<ReportView>(`/api/v8/tasks/${encodeURIComponent(taskId)}/revisions/${revision}/report`);
    },
    enabled: Boolean(revision),
    staleTime: Infinity,
  });
}
