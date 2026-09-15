import { execFile, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { open, realpath } from "node:fs/promises";
import { isAbsolute, relative, sep } from "node:path";

type Result = { job?: Record<string, unknown>; jobs?: Record<string, unknown>[]; reused?: boolean; file_path?: string; worker_active?: boolean };
type Runner = (command: Record<string, unknown>) => Promise<Result>;
type Dependencies = { run?: Runner; wake?: () => void; jobsRoot?: () => string };
const HEADERS = { "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff" };
const XLSX_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
const JOB_FIELDS = ["id", "status", "filters", "created_at", "started_at", "completed_at", "total", "completed_rows", "filename", "error", "request_id"];
const ID = /^(?:[a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})$/;
let activeCommands = 0;
let workerLaunching = false;

class ExportError extends Error {
  status: number;
  constructor(status: number, message: string) { super(message); this.status = status; }
}

function configuration() {
  const { DCAR_CONTENT_SEARCH_PYTHON: python, DCAR_CONTENT_EXPORT_HELPER: helper,
    DCAR_CONTENT_SEARCH_DB: db, DCAR_CONTENT_SEARCH_BACKEND: backend,
    DCAR_CONTENT_SEARCH_PROJECT_ROOT: project, DCAR_CONTENT_EXPORT_ROOT: jobsRoot } = process.env;
  if (![python, helper, db, backend, project, jobsRoot].every((value) => value && isAbsolute(value))) {
    throw new ExportError(503, "导出服务暂不可用，请稍后重试。");
  }
  const args = ["-B", "-I", helper!, "--db", db!, "--backend-root", backend!, "--project-root", project!, "--jobs-root", jobsRoot!];
  // Export workers receive no provider, authentication or scheduler credentials.
  const env: NodeJS.ProcessEnv = { NODE_ENV: "production", PATH: process.env.PATH, LANG: "en_US.UTF-8",
    TMPDIR: process.env.TMPDIR, PYTHONDONTWRITEBYTECODE: "1", DCAR_READ_ONLY: "1",
    DCAR_SCHEDULER_ENABLED: "0", DCAR_LLM_DISABLED: "1",
    DCAR_CONTENT_DATA_MODE: process.env.DCAR_CONTENT_DATA_MODE,
    DCAR_ACTIVE_SNAPSHOT: process.env.DCAR_ACTIVE_SNAPSHOT };
  return { python: python!, args, env, jobsRoot: jobsRoot! };
}

export async function runExportCommand(command: Record<string, unknown>): Promise<Result> {
  const config = configuration();
  if (activeCommands >= 4) throw new ExportError(429, "导出请求较多，请稍后重试。");
  activeCommands += 1;
  try {
    return await new Promise((accept, reject) => {
      const child = execFile(config.python, config.args, {
        timeout: 10_000, maxBuffer: 256 * 1024, windowsHide: true, env: config.env,
      }, (error, stdout) => {
        try {
          const result = JSON.parse(stdout);
          if (result?.error) {
            const status = [404, 409, 413, 422, 429, 503].includes(result.error.status) ? result.error.status : 503;
            const detail = typeof result.error.detail === "string" && result.error.detail.length < 300
              ? result.error.detail : "导出服务暂不可用，请稍后重试。";
            reject(new ExportError(status, detail)); return;
          }
          if (error || !result || typeof result !== "object" || Array.isArray(result)) throw new Error("Invalid export response");
          accept(result);
        } catch { reject(new ExportError(503, "暂时无法确认导出状态，请重新读取导出记录。")); }
      });
      child.stdin?.on("error", () => {});
      child.stdin?.end(JSON.stringify(command));
    });
  } finally { activeCommands -= 1; }
}

function wakeExportWorker() {
  if (workerLaunching) return;
  const config = configuration();
  workerLaunching = true;
  const child = spawn(config.python, [...config.args, "--worker"], {
    detached: true, stdio: "ignore", windowsHide: true, env: config.env,
  });
  child.once("error", () => { workerLaunching = false; });
  child.once("exit", () => { workerLaunching = false; });
  child.unref();
}

async function boundedJson(request: Request) {
  if (!request.headers.get("content-type")?.startsWith("application/json")) throw new ExportError(422, "提交的信息有误。");
  const reader = request.body?.getReader();
  if (!reader) throw new ExportError(422, "请选择导出条件。");
  let length = 0;
  const chunks: Uint8Array[] = [];
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    length += value.byteLength;
    if (length > 8192) { await reader.cancel(); throw new ExportError(413, "导出条件过长。"); }
    chunks.push(value);
  }
  try { return JSON.parse(Buffer.concat(chunks).toString("utf8")); }
  catch { throw new ExportError(422, "导出条件格式无效。"); }
}

function publicJob(job: Record<string, unknown>) {
  return Object.fromEntries(JOB_FIELDS.filter((key) => key in job).map((key) => [key, job[key]]));
}

async function download(result: Result, jobsRoot: string) {
  if (result.job?.status !== "succeeded" || !result.file_path) throw new ExportError(409, "文件尚未生成完成，请稍后再下载。");
  const root = await realpath(jobsRoot);
  const file = await realpath(result.file_path).catch(() => { throw new ExportError(404, "导出文件暂不可用，请重新生成。"); });
  const childPath = relative(root, file);
  if (childPath === "" || childPath.startsWith(`..${sep}`) || childPath === ".." || isAbsolute(childPath) || !file.endsWith(".xlsx")) {
    throw new ExportError(404, "导出文件暂不可用，请重新生成。");
  }
  const handle = await open(file, "r");
  const stat = await handle.stat();
  if (!stat.isFile() || stat.size === 0) { await handle.close(); throw new ExportError(404, "导出文件暂不可用，请重新生成。"); }
  let closed = false;
  const close = async () => { if (!closed) { closed = true; await handle.close(); } };
  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const buffer = Buffer.allocUnsafe(64 * 1024);
        const { bytesRead } = await handle.read(buffer, 0, buffer.length, null);
        if (!bytesRead) { await close(); controller.close(); }
        else controller.enqueue(new Uint8Array(buffer.subarray(0, bytesRead)));
      } catch (error) { await close(); controller.error(error); }
    },
    async cancel() { await close(); },
  });
  const filename = typeof result.job.filename === "string" ? result.job.filename.replace(/[\r\n/\\]/g, "_") : "内容明细.xlsx";
  return new Response(stream, { headers: { ...HEADERS, "Content-Type": XLSX_TYPE, "Content-Length": String(stat.size),
    "Content-Disposition": `attachment; filename="content-export.xlsx"; filename*=UTF-8''${encodeURIComponent(filename)}` } });
}

// Gateway authentication/approval runs first and replaces the identity header.
// User-scoped records are never addressed by a browser-supplied owner or path.
export async function contentExportsResponse(request: Request, id?: string, action?: "download", dependencies: Dependencies = {}) {
  const username = request.headers.get("x-dcar-authenticated-user");
  if (!username) return Response.json({ detail: "登录已过期，请重新登录。" }, { status: 401, headers: HEADERS });
  const run = dependencies.run ?? runExportCommand;
  const wake = dependencies.wake ?? wakeExportWorker;
  const owner = createHash("sha256").update(username).digest("hex");
  try {
    if (id && !ID.test(id)) throw new ExportError(404, "没有找到该导出记录。");
    if ((id && request.method !== "GET") || (!id && !["GET", "POST"].includes(request.method))) {
      return new Response(null, { status: 405, headers: HEADERS });
    }
    let result: Result;
    if (request.method === "POST") {
      const origin = request.headers.get("origin"), host = request.headers.get("x-forwarded-host"), protocol = request.headers.get("x-forwarded-proto");
      if (!origin || !host || !protocol || origin !== `${protocol}://${host}`) throw new ExportError(403, "页面来源无效，请刷新后重试。");
      const body = await boundedJson(request);
      if (!body || typeof body !== "object" || Array.isArray(body) || Object.keys(body).some((key) => !["request_id", "filters"].includes(key))
        || typeof body.request_id !== "string" || !ID.test(body.request_id) || !body.filters || typeof body.filters !== "object" || Array.isArray(body.filters)) {
        throw new ExportError(422, "导出条件格式无效。");
      }
      result = await run({ action: "create", owner, request_id: body.request_id, filters: body.filters });
    } else {
      result = await run({ action: id ? "get" : "list", owner, ...(id ? { id } : {}) });
    }
    if (action === "download") return await download(result, (dependencies.jobsRoot ?? (() => configuration().jobsRoot))());
    const jobs = result.jobs ?? (result.job ? [result.job] : []);
    if (!result.worker_active && jobs.some((job) => job.status === "queued" || job.status === "running")) {
      // Resume only tasks that were already explicitly requested. Durable
      // acknowledgement stays valid even if a worker cannot start yet.
      try { wake(); } catch { /* The next status read retries the worker wake. */ }
    }
    if (id || request.method === "POST") {
      if (!result.job) throw new ExportError(503, "暂时无法确认导出状态，请重新读取导出记录。");
      return Response.json({ job: publicJob(result.job), ...(request.method === "POST" ? { reused: Boolean(result.reused) } : {}) }, {
        status: request.method === "POST" ? 202 : 200, headers: HEADERS,
      });
    }
    return Response.json({ jobs: jobs.map(publicJob) }, { headers: HEADERS });
  } catch (error) {
    return Response.json({ detail: error instanceof ExportError ? error.message : "导出服务暂不可用，请稍后重试。" }, {
      status: error instanceof ExportError ? error.status : 503, headers: HEADERS,
    });
  }
}
