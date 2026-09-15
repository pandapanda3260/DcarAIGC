import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { contentExportsResponse } from "../app/lib/contentExportServer.ts";

const id = "c56776c4-7e47-4284-ae28-2f495f2aa775";
const filters = { page: 9, page_size: 50, platform: "douyin", published_from: "2026-09-01", published_to: "2026-09-12" };
function request(method = "GET", user = "operator", body = { request_id: id, filters }, extra = {}) {
  return new Request("http://localhost:4174/workbench-api/content-exports", { method,
    headers: { ...(user ? { "x-dcar-authenticated-user": user } : {}), "content-type": "application/json",
      origin: "http://localhost:4173", "x-forwarded-host": "localhost:4173", "x-forwarded-proto": "http", ...extra },
    ...(method === "POST" ? { body: JSON.stringify(body) } : {}),
  });
}

test("export requests require identity, same-origin submission, bounded input and fixed schema", async () => {
  const deps = { run: () => assert.fail("must reject before invoking the helper") };
  assert.equal((await contentExportsResponse(request("GET", ""), undefined, undefined, deps)).status, 401);
  assert.equal((await contentExportsResponse(request("POST", "operator", undefined, { origin: "https://other.invalid" }), undefined, undefined, deps)).status, 403);
  for (const body of [{ request_id: id, filters, owner: "another-user" }, { request_id: "../../etc/passwd", filters }, { request_id: id, filters: [] }]) {
    assert.equal((await contentExportsResponse(request("POST", "operator", body), undefined, undefined, deps)).status, 422);
  }
  assert.equal((await contentExportsResponse(request("POST", "operator", { request_id: id, filters: { query: "x".repeat(9000) } }), undefined, undefined, deps)).status, 413);
  assert.equal((await contentExportsResponse(request(), "../../etc/passwd", "download", deps)).status, 404);
});

test("durable acknowledgement preserves filters and idempotency; only trusted identity determines owner", async () => {
  let calls = 0, wakes = 0;
  const response = await contentExportsResponse(request("POST", "operator"), undefined, undefined, {
    async run(command) {
      calls++;
      assert.deepEqual(command, { action: "create", owner: createHash("sha256").update("operator").digest("hex"), request_id: id, filters });
      return { job: { id, status: "queued", filters, filename: "内容明细.xlsx", owner: "private", file_path: "/private/secret" }, reused: true };
    },
    wake() { wakes++; throw new Error("worker could not start yet"); },
  });
  assert.equal(response.status, 202);
  const body = await response.json();
  assert.equal(body.reused, true);
  assert.equal(body.job.owner, undefined);
  assert.equal(body.job.file_path, undefined);
  assert.equal(wakes, 1); assert.equal(calls, 1);
});

test("history queries are owner-scoped and do not create jobs; active worker is not duplicated", async () => {
  const owners = [];
  const deps = { run: async (command) => {
    assert.equal(command.action, "list"); owners.push(command.owner);
    return { jobs: [{ id, status: "running", filters }], worker_active: true };
  }, wake() { assert.fail("active worker must not restart"); } };
  for (const user of ["user-a", "user-b"]) assert.equal((await contentExportsResponse(request("GET", user), undefined, undefined, deps)).status, 200);
  assert.notEqual(owners[0], owners[1]);
});

test("completed files stream as authenticated XLSX attachments and missing/unfinished files return JSON", async () => {
  const root = await mkdtemp(join(tmpdir(), "dcar-export-bff-"));
  const outside = await mkdtemp(join(tmpdir(), "dcar-export-outside-"));
  try {
    const path = join(root, "result.xlsx");
    const bytes = Buffer.from("PK\x03\x04fixture-xlsx-content");
    await writeFile(path, bytes);
    const run = async (command) => {
      assert.equal(command.action, "get"); assert.equal(command.id, id);
      return { job: { id, status: "succeeded", filename: "内容明细.xlsx" }, file_path: path };
    };
    const response = await contentExportsResponse(request(), id, "download", { run, jobsRoot: () => root });
    assert.equal(response.status, 200);
    assert.match(response.headers.get("Content-Disposition"), /filename\*=UTF-8''/);
    assert.equal(response.headers.get("Cache-Control"), "private, no-store");
    assert.deepEqual(Buffer.from(await response.arrayBuffer()), bytes);
    const pending = await contentExportsResponse(request(), id, "download", { run: async () => ({ job: { status: "running" } }), jobsRoot: () => root });
    assert.equal(pending.status, 409);
    const forbidden = join(outside, "secret.xlsx"); await writeFile(forbidden, bytes);
    const wrongPath = await contentExportsResponse(request(), id, "download", { run: async () => ({ job: { status: "succeeded" }, file_path: forbidden }), jobsRoot: () => root });
    assert.equal(wrongPath.status, 404);
    const missing = await contentExportsResponse(request(), id, "download", { run: async () => ({ job: { status: "succeeded" }, file_path: join(root, "missing.xlsx") }), jobsRoot: () => root });
    assert.equal(missing.status, 404);
  } finally { await rm(root, { recursive: true }); await rm(outside, { recursive: true }); }
});

test("an uncertain create is never repeated by the adapter and internal errors do not leak", async () => {
  let calls = 0;
  const result = await contentExportsResponse(request("POST"), undefined, undefined, { run: async () => {
    calls++; throw new Error("private key and filesystem details");
  } });
  assert.equal(calls, 1); assert.equal(result.status, 503);
  assert.doesNotMatch(await result.text(), /private key|filesystem/);
});
