import { execFile } from "node:child_process";

type SearchResult = { items: unknown[]; total: number; page?: number; page_size?: number };
type Reader = (payload: Record<string, unknown>) => Promise<SearchResult>;
const MAX_BODY = 8192;
let activeReads = 0;

class SearchError extends Error {
  status: number;
  constructor(status: number, message: string) { super(message); this.status = status; }
}

// Use the existing authenticated page service for read-only queries. The helper
// reuses the API query with live WAL visibility; it never starts a Writer.
export async function readContentSearch(payload: Record<string, unknown>): Promise<SearchResult> {
  const { DCAR_CONTENT_SEARCH_PYTHON: python, DCAR_CONTENT_SEARCH_HELPER: helper,
    DCAR_CONTENT_SEARCH_DB: db, DCAR_CONTENT_SEARCH_BACKEND: backend,
    DCAR_CONTENT_SEARCH_PROJECT_ROOT: project } = process.env;
  if (!python || !helper || !db || !backend || !project) throw new SearchError(503, "内容查询暂不可用，请稍后重试。");
  if (activeReads >= 4) throw new SearchError(429, "查询较多，请稍后重试。");
  activeReads += 1;
  try {
    return await new Promise((resolve, reject) => {
      const child = execFile(python, ["-B", "-I", helper, "--db", db,
        "--backend-root", backend, "--project-root", project], {
        timeout: 12_000, maxBuffer: 8 * 1024 * 1024, windowsHide: true,
        env: { ...process.env, DCAR_READ_ONLY: "1", DCAR_SCHEDULER_ENABLED: "0", DCAR_LLM_DISABLED: "1" },
      }, (error, stdout) => {
        try {
          const result = JSON.parse(stdout);
          if (error && result?.error?.status === 422) {
            reject(new SearchError(422, "日期或筛选条件无效，请检查后重试。")); return;
          }
          if (error || !Array.isArray(result?.items) || !Number.isSafeInteger(result.total)
              || result.total < 0 || result.items.length > 100) throw new Error("Invalid content query result");
          resolve(result);
        } catch { reject(new SearchError(503, "内容查询暂不可用，请稍后重试。")); }
      });
      child.stdin?.on("error", () => {});
      child.stdin?.end(JSON.stringify(payload));
    });
  } finally { activeReads -= 1; }
}

export async function contentSearchResponse(request: Request, reader: Reader = readContentSearch): Promise<Response> {
  const headers = { "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff" };
  if (!request.headers.get("x-dcar-authenticated-user")) {
    return Response.json({ detail: "登录已过期，请重新登录。" }, { status: 401, headers });
  }
  if (request.method !== "POST") return new Response(null, { status: 405, headers });
  if (!request.headers.get("content-type")?.startsWith("application/json")) {
    return Response.json({ detail: "筛选条件无效。" }, { status: 422, headers });
  }
  try {
    // Bound streamed bodies too, rather than trusting a client length header.
    const stream = request.body?.getReader();
    if (!stream) throw new SearchError(422, "筛选条件无效。");
    const chunks: Uint8Array[] = [];
    let length = 0;
    for (;;) {
      const { done, value } = await stream.read();
      if (done) break;
      length += value.byteLength;
      if (length > MAX_BODY) { await stream.cancel(); throw new SearchError(413, "筛选条件过长。"); }
      chunks.push(value);
    }
    let payload: unknown;
    try { payload = JSON.parse(Buffer.concat(chunks).toString("utf8")); }
    catch { throw new SearchError(422, "筛选条件无效。"); }
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw new SearchError(422, "筛选条件无效。");
    return Response.json(await reader(payload as Record<string, unknown>), { headers });
  } catch (error) {
    const status = error instanceof SearchError ? error.status : 503;
    return Response.json({ detail: error instanceof SearchError ? error.message : "内容查询暂不可用，请稍后重试。" }, {
      status, headers: { ...headers, ...(status === 429 ? { "Retry-After": "2" } : {}) },
    });
  }
}
