import { keepPreviousData, queryOptions } from "@tanstack/react-query";
import { CONTENT_SEARCH_PATH } from "./features";
import { ApiRequestError, jsonRequest, readQueryJson, requireApprovedSession } from "./api";
import {
  buildAccountSearchRequest,
  buildContentSearchRequest,
  isGeneratingTaskStatus,
} from "./queryContracts";
import type { AccountSearchRequest, ContentSearchRequest } from "./queryContracts";
import type {
  Account,
  AccountRosterStatus,
  AuthSession,
  ContentItem,
  DouyinAuthorization,
  DouyinAuthorizationStatus,
  ManagedUsersResult,
  Overview,
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
  account_management_version?: number;
  roster: AccountRosterStatus;
};
export type DouyinAuthorizationsResult = { items: DouyinAuthorization[] };
export type DouyinAuthorizationStatusesResult = { items: DouyinAuthorizationStatus[]; unavailable?: boolean };

export const defaultContentSearchRequest = buildContentSearchRequest({
  query: "", platform: "", accountType: "", direction: "", sellingPoint: "",
  spuSeries: "", audience: "", scene: "",
}, 1, 50);

export const defaultAccountSearchRequest = buildAccountSearchRequest({
  query: "", platform: "", accountType: "", direction: "", accountStatus: "",
}, 1, 50);

function readDouyinAuthorizations() {
  return readQueryJson<DouyinAuthorizationsResult>("/api/douyin/authorizations");
}

async function readDouyinAuthorizationStatuses(): Promise<DouyinAuthorizationStatusesResult> {
  try {
    return await readQueryJson<DouyinAuthorizationStatusesResult>("/api/douyin/authorization-statuses");
  } catch (reason) {
    // 未部署抖音授权服务的环境（本地网关）对 /api/douyin/* 固定返回 403/404：
    // 这不是运行故障，页面以"—"静默展示；真实故障（超时、5xx）继续抛出。
    if (reason instanceof ApiRequestError && reason.code !== "approval_required" && (reason.status === 403 || reason.status === 404)) {
      return { items: [], unavailable: true };
    }
    throw reason;
  }
}

export const queryKeys = {
  session: ["auth", "session"] as const,
  users: ["auth", "users"] as const,
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

// 当前登录身份（用户名 + 角色）：侧栏"用户管理&质检"分组与抖音授权页共用；bypass 模式没有 role。
export function sessionQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.session,
    queryFn: () => readQueryJson<AuthSession>("/auth/session").then(requireApprovedSession),
    staleTime: 30_000,
    refetchInterval: 30_000,
    refetchOnWindowFocus: "always",
    refetchOnMount: "always",
  });
}

// 用户权限页列表：每次进入页面都重新读取（用户数是两位数以内，成本可忽略）。
export function usersQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.users,
    queryFn: () => readQueryJson<ManagedUsersResult>("/auth/users"),
    staleTime: 0,
  });
}

export function overviewQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.overview,
    queryFn: () => readQueryJson<Overview>("/api/v8/overview"),
  });
}

export function contentSearchQueryOptions(request: ContentSearchRequest) {
  return queryOptions({
    queryKey: queryKeys.contentSearch(request),
    queryFn: () => readQueryJson<ContentSearchResult>(CONTENT_SEARCH_PATH, jsonRequest(request)),
    placeholderData: keepPreviousData,
  });
}

// Compatibility belongs to the HTTP adapter, never the visible account filters.
// Older services default to the active roster unless explicitly asked for all.
let accountManagementVersion = 1;
async function readAccountSearch(request: AccountSearchRequest): Promise<AccountSearchResult> {
  const legacy = accountManagementVersion < 2;
  let result = await readQueryJson<AccountSearchResult>("/api/v8/accounts/search", jsonRequest(
    legacy ? { ...request, scope: "all" } : request,
  ));
  accountManagementVersion = result.account_management_version ?? 1;
  if (!legacy && accountManagementVersion < 2) {
    // A service rollback must not silently hide saved accounts on this read.
    result = await readQueryJson<AccountSearchResult>("/api/v8/accounts/search", jsonRequest({ ...request, scope: "all" }));
    accountManagementVersion = result.account_management_version ?? 1;
  }
  return result;
}

export function accountSearchQueryOptions(request: AccountSearchRequest) {
  return queryOptions({
    queryKey: queryKeys.accountSearch(request),
    queryFn: () => readAccountSearch(request),
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
    queryFn: readDouyinAuthorizationStatuses,
    staleTime: 0,
    refetchOnMount: "always",
    refetchOnWindowFocus: "always",
  });
}

export function activeSellingPointsQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.activeSellingPoints,
    queryFn: () => readQueryJson<SellingPointResponse>("/api/v8/selling-points"),
    staleTime: 60_000,
    refetchOnWindowFocus: true,
  });
}

export function draftSellingPointsQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.draftSellingPoints,
    queryFn: () => readQueryJson<SellingPointResponse>("/api/v8/selling-points/draft"),
  });
}

export function spuAssetsQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.spuAssets,
    queryFn: () => readQueryJson<SpuAudienceAssets>("/api/v8/spu-audience/assets"),
    refetchInterval: (query) => !query.state.error && query.state.data?.last_run?.status === "running" ? 5_000 : false,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: (query) => !query.state.error && query.state.data?.last_run?.status === "running" ? "always" : false,
  });
}

export function spuStatsQueryOptions(window: string, platform: string) {
  const search = new URLSearchParams({ window });
  if (platform) search.set("platform", platform);
  return queryOptions({
    queryKey: queryKeys.spuStats(window, platform),
    queryFn: () => readQueryJson<SpuAudienceStats>(`/api/v8/spu-audience/stats?${search.toString()}`),
    placeholderData: keepPreviousData,
  });
}

export function tasksListQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.tasksList,
    queryFn: () => readQueryJson<{ items: Task[] }>("/api/v8/tasks"),
    refetchInterval: (query) => !query.state.error && query.state.data?.items.some((task) => isGeneratingTaskStatus(task.task_status)) ? 1_500 : false,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: (query) => !query.state.error && query.state.data?.items.some((task) => isGeneratingTaskStatus(task.task_status)) ? "always" : false,
  });
}

export function taskDetailQueryOptions(taskId: string) {
  return queryOptions({
    queryKey: queryKeys.taskDetail(taskId),
    queryFn: () => readQueryJson<TaskDetail>(`/api/v8/tasks/${encodeURIComponent(taskId)}`),
    refetchInterval: (query) => !query.state.error && isGeneratingTaskStatus(query.state.data?.task_status) ? 1_500 : false,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: (query) => !query.state.error && isGeneratingTaskStatus(query.state.data?.task_status) ? "always" : false,
  });
}

export function taskReportQueryOptions(taskId: string, revision: number | null | undefined) {
  return queryOptions({
    queryKey: queryKeys.taskReport(taskId, revision),
    queryFn: () => {
      if (!revision) throw new Error("缺少报告版本。");
      return readQueryJson<ReportView>(`/api/v8/tasks/${encodeURIComponent(taskId)}/revisions/${revision}/report`);
    },
    enabled: Boolean(revision),
    staleTime: Infinity,
  });
}
