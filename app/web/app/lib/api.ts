export const API_BASE = process.env.NEXT_PUBLIC_DCAR_API_BASE ?? "";

export class ApiRequestError extends Error {
  readonly status: number | null;
  readonly retryable: boolean;

  constructor(message: string, options: { status?: number | null; retryable?: boolean } = {}) {
    super(message);
    this.name = "ApiRequestError";
    this.status = options.status ?? null;
    this.retryable = options.retryable ?? false;
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
  return `${API_BASE}${path}`;
}

export function apiErrorMessage(detail: unknown, status: number) {
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
  let response: Response;
  try {
    response = await fetch(apiUrl(path), init);
  } catch (reason) {
    if (isAbortError(reason)) throw reason;
    throw new ApiRequestError("无法连接数据服务，请检查网络或稍后重试。", { retryable: true });
  }
  if (!response.ok) {
    const body = (await response.json().catch(() => null)) as { detail?: unknown } | null;
    throw new ApiRequestError(apiErrorMessage(body?.detail, response.status), {
      status: response.status,
      retryable: response.status >= 500,
    });
  }
  return (await response.json()) as T;
}

export function jsonRequest(body: unknown, method = "POST"): RequestInit {
  return {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

export function parseCsv(text: string): Array<Record<string, string>> {
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let quoted = false;
  const source = text.replace(/^\uFEFF/, "");
  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    if (quoted && character === '"' && source[index + 1] === '"') {
      cell += '"';
      index += 1;
    } else if (character === '"') {
      quoted = !quoted;
    } else if (character === "," && !quoted) {
      row.push(cell);
      cell = "";
    } else if ((character === "\n" || character === "\r") && !quoted) {
      if (character === "\r" && source[index + 1] === "\n") index += 1;
      row.push(cell);
      if (row.some((value) => value !== "")) rows.push(row);
      row = [];
      cell = "";
    } else {
      cell += character;
    }
  }
  row.push(cell);
  if (row.some((value) => value !== "")) rows.push(row);
  const headers = rows.shift()?.map((value) => value.trim()) ?? [];
  return rows.map((values) => Object.fromEntries(
    headers.map((header, index) => [header, values[index] ?? ""]),
  ));
}
