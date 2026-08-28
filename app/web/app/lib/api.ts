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
  let response: Response;
  try {
    response = await fetch(apiUrl(path));
  } catch {
    throw new Error("无法连接数据服务，请检查网络或稍后重试。");
  }
  if (!response.ok) {
    const body = (await response.json().catch(() => null)) as { detail?: unknown } | null;
    throw new Error(apiErrorMessage(body?.detail, response.status));
  }
  const contentType = (response.headers.get("Content-Type") ?? "").split(";")[0].trim().toLowerCase();
  if (!options.contentTypes.includes(contentType)) {
    throw new Error("下载服务返回的文件格式不正确，请刷新后重试。");
  }
  const blob = await response.blob();
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
