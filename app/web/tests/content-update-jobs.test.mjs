import assert from "node:assert/strict";
import test from "node:test";
import { build } from "esbuild";

const bundle = await build({ entryPoints: [new URL("../app/contents/contentUpdateJobs.ts", import.meta.url).pathname], bundle: true, write: false, platform: "node", format: "esm" });
const { submitContentUpdateJob, readPendingContentUpdates, reconcilePendingContentUpdates, mergeContentUpdateJobs, formatJobElapsed, submissionFailureStatus, contentUpdateJobBlocksWrites, contentUpdateAvailability } = await import(`data:text/javascript;base64,${Buffer.from(bundle.outputFiles[0].text).toString("base64")}`);
const requestId = "12345678-1234-4234-8234-123456789abc";
const job = (overrides = {}) => ({ id: 1, content_id: 12, title: "测试内容", status: "running", stage: "updating", stage_label: "正在获取平台数据", created_at: "2026-09-06T08:00:00Z", updated_at: "2026-09-06T08:01:00Z", completed_at: null, result: null, error: null, ...overrides });
const pending = { contentId: 12, title: "测试内容", requestId, createdAt: "2026-09-06T08:00:00Z", status: "submitting", error: "" };

test("update queues require a confirmed writable service and never poll a read-only snapshot", () => {
  const writer = { status: "ok", read_only: false };
  assert.equal(contentUpdateAvailability(writer, false), "available");
  assert.equal(contentUpdateAvailability({ ...writer, automation: { scheduler_state: "paused" } }, false), "available");
  assert.equal(contentUpdateAvailability({ ...writer, read_only: true }, false), "read_only");
  assert.equal(contentUpdateAvailability({ ...writer, automation: { scheduler_state: "read_only" } }, false), "read_only");
  assert.equal(contentUpdateAvailability(undefined, false), "checking");
  for (const health of [writer, { ...writer, read_only: true }, undefined]) {
    assert.equal(contentUpdateAvailability(health, true), "unavailable");
  }
  for (const health of [{ status: "ok" }, { ...writer, status: "unavailable" }]) {
    assert.equal(contentUpdateAvailability(health, false), "unavailable");
  }
});

test("reload preserves uncertain submission identity, and GET only reconciles exact aliases", () => {
  const restored = readPendingContentUpdates(JSON.stringify([pending]));
  assert.equal(restored[0].requestId, requestId);
  assert.equal(restored[0].status, "uncertain");
  assert.equal(reconcilePendingContentUpdates(restored, [job()]).length, 1);
  assert.equal(reconcilePendingContentUpdates(restored, [job({ request_ids: [requestId] })]).length, 0);
  assert.equal(reconcilePendingContentUpdates(restored, [job({ content_id: 13, request_id: requestId })]).length, 1);
});

test("an explicit retry reuses its request ID, submission targets same-origin BFF, titles are bounded", async (t) => {
  const requests = [];
  t.mock.method(globalThis, "fetch", async (url, init) => {
    requests.push({ url, body: JSON.parse(init.body), method: init.method });
    return Response.json({ job: job({ request_id: requestId }) }, { status: 202 });
  });
  await submitContentUpdateJob(12, requestId, "长".repeat(600));
  await submitContentUpdateJob(12, requestId, "长".repeat(600));
  assert.equal(requests.length, 2);
  for (const request of requests) {
    assert.equal(request.url, "/workbench-api/contents/12/update-jobs");
    assert.equal(request.body.request_id, requestId);
    assert.equal(request.body.title.length, 500);
  }
});

test("uncertain transport or malformed receipts are never accepted or automatically resubmitted", async (t) => {
  let calls = 0;
  t.mock.method(globalThis, "fetch", async () => { calls++; return Response.json({ job: job({ content_id: 999 }) }, { status: 202 }); });
  try { await submitContentUpdateJob(12, requestId, "测试"); assert.fail("invalid receipt accepted"); }
  catch (reason) { assert.equal(submissionFailureStatus(reason), "uncertain"); }
  assert.equal(calls, 1);
  t.mock.method(globalThis, "fetch", async () => { calls++; throw new TypeError("network"); });
  try { await submitContentUpdateJob(12, requestId, "测试"); assert.fail("network accepted"); }
  catch (reason) { assert.equal(submissionFailureStatus(reason), "uncertain"); }
  assert.equal(calls, 2);
});

test("delayed reads cannot rewind a finished job or a newer real stage", () => {
  const completed = job({ status: "succeeded", completed_at: "2026-09-06T08:02:00Z", updated_at: "2026-09-06T08:02:00Z" });
  assert.equal(mergeContentUpdateJobs([completed], [job()])[0].status, "succeeded");
  assert.equal(mergeContentUpdateJobs([job({ stage: "refreshing", updated_at: "2026-09-06T08:03:00Z" })], [job()])[0].stage, "refreshing");
  assert.equal(formatJobElapsed(completed, Date.parse("2026-09-06T09:00:00Z")), "已用时 2 分 0 秒");
});

test("an uncertain terminal result continues to block a new paid request and conflicting writes", () => {
  assert.equal(contentUpdateJobBlocksWrites(job({ status: "failed", error_code: "result_uncertain" })), true);
  assert.equal(contentUpdateJobBlocksWrites(job({ status: "failed", error_code: "validation_failed" })), false);
  assert.equal(contentUpdateJobBlocksWrites(job({ status: "succeeded" })), false);
  assert.equal(contentUpdateJobBlocksWrites(job({ status: "queued" })), true);
});
