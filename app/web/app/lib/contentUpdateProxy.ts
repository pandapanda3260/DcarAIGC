import { readFileSync } from "node:fs";

// Server-only adapter. The auth gateway replaces this identity header before
// forwarding to the loopback web server; session cookies never enter the queue.
export async function proxyContentUpdates(request: Request, path: string[]): Promise<Response> {
  const endpoint = path.join("/");
  const isSubmit = /^contents\/[1-9]\d*\/update-jobs$/.test(endpoint);
  const isRead = /^content-update-jobs(?:\/[1-9]\d*)?$/.test(endpoint);
  if ((!isSubmit && !isRead) || (isSubmit ? request.method !== "POST" : request.method !== "GET")) {
    return Response.json({ detail: "没有找到需要的数据。" }, { status: 404 });
  }
  if (!request.headers.get("x-dcar-authenticated-user")) {
    return Response.json({ detail: "登录已过期，请重新登录。" }, { status: 401 });
  }
  if (isSubmit) {
    const origin = request.headers.get("origin");
    const host = request.headers.get("x-forwarded-host");
    const protocol = request.headers.get("x-forwarded-proto");
    // Require the original browser origin injected by the trusted gateway.
    if (!origin || !host || !protocol || origin !== `${protocol}://${host}`) {
      return Response.json({ detail: "页面来源无效，请刷新后重试。" }, { status: 403 });
    }
    const length = Number(request.headers.get("content-length"));
    if (length > 8192 || !request.headers.get("content-type")?.startsWith("application/json")) {
      return Response.json({ detail: "提交的信息有误。" }, { status: 422 });
    }
  }
  const tokenFile = process.env.DCAR_UPDATE_COORDINATOR_TOKEN_FILE;
  if (!tokenFile) {
    return Response.json({ detail: "更新服务暂时不可用，请稍后重试。" }, { status: 503 });
  }
  let body: string | undefined;
  if (isSubmit) {
    body = await request.text();
    if (Buffer.byteLength(body) > 8192) {
      return Response.json({ detail: "提交的信息过长。" }, { status: 413 });
    }
  }
  try {
    const token = readFileSync(tokenFile, "utf8").trim();
    const response = await fetch(`http://127.0.0.1:8767/api/v8/${endpoint}`, {
      method: request.method,
      headers: { "Content-Type": "application/json", "X-Dcar-Update-Key": token },
      body,
      // This is the durable acknowledgement/status read, never the long update.
      signal: AbortSignal.timeout(10_000),
      redirect: "error",
      cache: "no-store",
    });
    const content = await response.text();
    return new Response(content, { status: response.status, headers: {
      "Content-Type": "application/json", "Cache-Control": "no-store",
    } });
  } catch {
    // A lost POST response is ambiguous; the client retains the same request_id.
    return Response.json({ detail: "暂时无法确认任务状态，请查询更新记录。" }, { status: 503 });
  }
}
