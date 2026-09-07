export const API_BASE = process.env.NEXT_PUBLIC_DCAR_API_BASE ?? "";
// 与 lib/paths.ts 的 WEB_BASE_PATH 同源；这里不 import（测试用 node 直接加载本文件，相对导入需带扩展名）。
const LOGIN_PATH = `${process.env.NEXT_PUBLIC_DCAR_BASE_PATH ?? ""}/login`;
const WEB_BASE_PATH = process.env.NEXT_PUBLIC_DCAR_BASE_PATH ?? "";
let approvalRedirecting = false;
let clearSessionData: (() => void) | undefined;

// 由浏览器 QueryClient 注册，避免 API 层反向依赖查询模块（及 SSR 共享客户端）。
export function setSessionDataClearer(clear: () => void) {
  clearSessionData = clear;
}

export function redirectToApprovalShell() {
  if (typeof window === "undefined") return false;
  if (!approvalRedirecting) {
    approvalRedirecting = true;
    clearSessionData?.();
    // Reload the current document: the gateway now renders its navigation and
    // permission empty state, without loading business components or data.
    const pathname = window.location.pathname;
    const section = pathname.slice(WEB_BASE_PATH.length).replace(/\/$/, "");
    const isWorkbenchPage = pathname.startsWith(WEB_BASE_PATH + "/")
      && ["/overview", "/contents", "/accounts", "/selling-points", "/spu-audience", "/tasks"].includes(section);
    const query = new URLSearchParams(window.location.search);
    query.delete("_rsc");
    query.delete("_data");
    const search = query.toString();
    window.location.replace(isWorkbenchPage
      ? pathname + (search ? `?${search}` : "")
      : `${WEB_BASE_PATH}/overview`);
  }
  return true;
}

export function handleApprovalRequired(status: number, code: unknown) {
  if (status !== 403 || code !== "approval_required") return false;
  return redirectToApprovalShell();
}

// 整页跳转期间不再把错误或已在途的业务响应交还页面，避免旧数据重新进入缓存或弹出操作失败提示。
function waitForApprovalNavigation<T>(): Promise<T> {
  return new Promise<T>(() => {});
}

export function requireApprovedSession<T extends { role?: string }>(session: T): T | Promise<T> {
  if (session.role === "new_user" && redirectToApprovalShell()) return waitForApprovalNavigation<T>();
  return session;
}

// 会话失效（网关 401）时统一整页跳转到登录页：整页跳转即清空内存里的查询缓存与页面状态，
// 登录后由网关按 return_to 回跳。服务端渲染阶段不做跳转。
export function redirectToLogin() {
  if (typeof window === "undefined") return;
  const returnTo = window.location.pathname + window.location.search;
  window.location.replace(LOGIN_PATH + "?return_to=" + encodeURIComponent(returnTo));
}

export class ApiRequestError extends Error {
  readonly status: number | null;
  readonly retryable: boolean;
  readonly code: string | null;

  constructor(message: string, options: { status?: number | null; retryable?: boolean; code?: string | null } = {}) {
    super(message);
    this.name = "ApiRequestError";
    this.status = options.status ?? null;
    this.retryable = options.retryable ?? false;
    this.code = options.code ?? null;
  }
}

export function isAbortError(reason: unknown) {
  return reason instanceof Error && reason.name === "AbortError";
}

export function shouldRetryQuery(failureCount: number, error: unknown) {
  if (isAbortError(error) || !(error instanceof ApiRequestError)) return false;
  return error.retryable && failureCount < 1;
}

export function apiUrl(path: string) {
  if (path === "/workbench-api" || path.startsWith("/workbench-api/")) return `${WEB_BASE_PATH}${path}`;
  return `${API_BASE}${path}`;
}

export function apiErrorMessage(detail: unknown, status: number, code?: unknown) {
  const mediaMessages: Record<string, string> = {
    original_archived: "原件已归档，请在本地申请恢复；不会付费重抓。",
    original_restoring: "原件正在等待恢复，请稍后查看。",
    original_expiry_pending: "已到删除时间，等待安全删除；不能再恢复原件。",
    original_purge_in_progress: "原件正在安全删除，不能再恢复。",
    original_expired: "原件已到期删除；预览和已完成结论仍保留。",
    replica_original_omitted: "线上只读副本不包含原件，请在本地查看。",
    original_integrity_error: "原件完整性校验失败，请联系管理员；不会自动重抓。",
    managed_source_pending: "新来源尚未完成实例登记，不会回退到旧来源。",
    explicit_reacquire_contract_not_bound: "尚未建立独立任务及授权，不能付费重新获取媒体。",
  };
  if (typeof code === "string" && mediaMessages[code]) return mediaMessages[code];
  if (status >= 500) return "服务暂时不可用，请稍后重试。";
  if (status === 422) return "提交的信息有误，请检查后重试。";
  const text = typeof detail === "string" ? detail.trim() : "";
  // 后端异常里可能带接口字段名、内部状态或英文堆栈；这些内容只留在日志中。
  if (text && !/[A-Za-z_]{3,}/.test(text)) return text;
  if (status === 401) return "登录已过期，请重新登录。";
  if (status === 403) return "当前操作不可用，请刷新页面后重试。";
  if (status === 404) return "没有找到需要的数据，请刷新页面后重试。";
  if (status === 409) return "数据已经发生变化，请刷新页面后重试。";
  if (status === 429) return "操作太频繁，请稍后再试。";
  return "操作没有完成，请稍后重试。";
}

export async function readJson<T>(path: string, init?: RequestInit): Promise<T> {
  if (approvalRedirecting) return waitForApprovalNavigation<T>();
  let response: Response;
  try {
    response = await fetch(apiUrl(path), init);
  } catch (reason) {
    if (approvalRedirecting) return waitForApprovalNavigation<T>();
    if (isAbortError(reason)) throw reason;
    throw new ApiRequestError("无法连接数据服务，请检查网络或稍后重试。", { retryable: true });
  }
  if (approvalRedirecting) return waitForApprovalNavigation<T>();
  if (!response.ok) {
    if (response.status === 401) redirectToLogin();
    const body = (await response.json().catch(() => null)) as { detail?: unknown; code?: string } | null;
    if (handleApprovalRequired(response.status, body?.code)) return waitForApprovalNavigation<T>();
    if (approvalRedirecting) return waitForApprovalNavigation<T>();
    throw new ApiRequestError(apiErrorMessage(body?.detail, response.status, body?.code), {
      status: response.status,
      retryable: response.status >= 500 && !body?.code,
      code: body?.code ?? null,
    });
  }
  const result = (await response.json()) as T;
  return approvalRedirecting ? waitForApprovalNavigation<T>() : result;
}

export const QUERY_READ_TIMEOUT_MS = 15_000;

export async function readQueryJson<T>(
  path: string,
  init?: RequestInit,
  timeoutMs = QUERY_READ_TIMEOUT_MS,
): Promise<T> {
  // Callers that already own cancellation keep their exact AbortSignal contract.
  if (init?.signal) return readJson<T>(path, init);
  const controller = new AbortController();
  const timeoutError = new DOMException("读取超时，请重新加载。", "AbortError");
  const timer = setTimeout(() => controller.abort(timeoutError), timeoutMs);
  try {
    return await readJson<T>(path, { ...init, signal: controller.signal });
  } catch (reason) {
    if (controller.signal.aborted) throw timeoutError;
    throw reason;
  } finally {
    clearTimeout(timer);
  }
}


export type MediaRestorePurpose = "evidence" | "reprocess";
export type MediaRestoreReceipt = {
  run_id: number; bundle_id: string; purpose: MediaRestorePurpose;
  status: string; http_status: 202; provider_cost: number; requested_at?: string;
};

export async function requestMediaRestore(
  contentId: number, bundleId: string, purpose: MediaRestorePurpose,
): Promise<MediaRestoreReceipt> {
  if (!Number.isSafeInteger(contentId) || contentId <= 0
      || !/^[0-9a-f]{32}$/.test(bundleId) || !["evidence", "reprocess"].includes(purpose)) {
    throw new ApiRequestError("恢复目标或用途无效，请刷新页面后重试。", { retryable: false });
  }
  // Explicit user action only. Never retry this POST from a polling query.
  const result = await readJson<MediaRestoreReceipt>(
    "/api/v8/contents/" + contentId + "/media/restore", jsonRequest({ bundle_id: bundleId, purpose }),
  );
  if (!Number.isSafeInteger(result.run_id) || result.run_id <= 0 || result.bundle_id !== bundleId
      || result.purpose !== purpose || result.http_status !== 202) {
    throw new ApiRequestError("恢复作业回执不完整，请重新读取状态；不要重复提交。",
      { status: 202, code: "restore_receipt_invalid", retryable: false });
  }
  return result;
}

type DownloadFile = { blob: Blob; filename: string };

function attachmentFilename(disposition: string | null, fallback: string) {
  const encoded = disposition?.match(/filename\*\s*=\s*UTF-8''([^;]+)/i)?.[1];
  let filename = "";
  if (encoded) {
    try { filename = decodeURIComponent(encoded.trim()); } catch { /* Use the plain filename or fallback. */ }
  }
  if (!filename) {
    const plain = disposition?.match(/(?:^|;)\s*filename\s*=\s*(?:"([^"]+)"|([^;]+))/i);
    filename = plain?.[1] || plain?.[2]?.trim() || fallback;
  }
  // A download name must stay a filename, even if a proxy returns a bad header.
  return Array.from(filename, (character) => (
    character === "/" || character === "\\" || character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127 ? "_" : character
  )).join("").trim() || fallback;
}

export async function readDownload(
  path: string,
  options: { contentTypes: readonly string[]; fallbackFilename: string | ((contentType: string) => string) },
): Promise<DownloadFile> {
  if (approvalRedirecting) return waitForApprovalNavigation<DownloadFile>();
  let response: Response;
  try {
    response = await fetch(apiUrl(path));
  } catch {
    if (approvalRedirecting) return waitForApprovalNavigation<DownloadFile>();
    throw new Error("无法连接数据服务，请检查网络或稍后重试。");
  }
  if (approvalRedirecting) return waitForApprovalNavigation<DownloadFile>();
  if (!response.ok) {
    if (response.status === 401) redirectToLogin();
    const body = (await response.json().catch(() => null)) as { detail?: unknown; code?: string } | null;
    if (handleApprovalRequired(response.status, body?.code)) return waitForApprovalNavigation<DownloadFile>();
    if (approvalRedirecting) return waitForApprovalNavigation<DownloadFile>();
    throw new Error(apiErrorMessage(body?.detail, response.status, body?.code));
  }
  const contentType = (response.headers.get("Content-Type") ?? "").split(";")[0].trim().toLowerCase();
  if (!options.contentTypes.includes(contentType)) {
    throw new Error("下载服务返回的文件格式不正确，请刷新后重试。");
  }
  const blob = await response.blob();
  if (approvalRedirecting) return waitForApprovalNavigation<DownloadFile>();
  if (blob.size === 0) throw new Error("下载文件为空，请稍后重试。");
  const fallback = typeof options.fallbackFilename === "function" ? options.fallbackFilename(contentType) : options.fallbackFilename;
  return {
    blob,
    filename: attachmentFilename(response.headers.get("Content-Disposition"), fallback),
  };
}

export function saveDownload(file: DownloadFile) {
  const url = URL.createObjectURL(file.blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = file.filename;
  document.body.appendChild(anchor);
  try { anchor.click(); }
  finally {
    anchor.remove();
    // Let the browser consume both downloads before releasing their object URLs.
    setTimeout(() => URL.revokeObjectURL(url), 60_000);
  }
}

export function jsonRequest(body: unknown, method = "POST"): RequestInit {
  return {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

export function markedJsonRequest(body: unknown, marker: string): RequestInit {
  return {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Dcar-Request": marker,
    },
    body: JSON.stringify(body),
  };
}
