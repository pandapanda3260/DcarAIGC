import assert from "node:assert/strict";
import test from "node:test";
import { build } from "esbuild";

const bundle = await build({ entryPoints: [new URL("../app/contents/contentUpdatePresentation.ts", import.meta.url).pathname], bundle: true, write: false, platform: "node", format: "esm" });
const { contentUpdateRowState } = await import(`data:text/javascript;base64,${Buffer.from(bundle.outputFiles[0].text).toString("base64")}`);
const job = (overrides = {}) => ({ id: 1, content_id: 12, title: "测试内容", status: "running", stage: "updating", stage_label: "正在更新数据", created_at: "2026-09-06T08:00:00Z", updated_at: "2026-09-06T08:01:00Z", completed_at: null, result: null, error: null, ...overrides });
const pending = (overrides = {}) => ({ contentId: 12, title: "测试内容", requestId: "12345678-1234-4234-8234-123456789abc", createdAt: "2026-09-06T08:00:00Z", status: "submitting", error: "", ...overrides });
const completed = (overrides = {}) => job({ status: "succeeded", stage: "completed", stage_label: "更新完成", result: { status: "succeeded", provider_cost: 0, metrics: { status: "succeeded" }, media: { status: "evidence_ready" } }, ...overrides });

test("rows without their own update history have no status", () => {
  assert.equal(contentUpdateRowState(12, [], []), null);
  assert.equal(contentUpdateRowState(12, [job({ content_id: 99 })], [pending({ contentId: 99 })]), null);
});

test("a current submission takes precedence over previous receipts and preserves uncertainty", () => {
  const history = [completed()];
  assert.equal(contentUpdateRowState(12, history, [pending()]).label, "正在提交");
  assert.equal(contentUpdateRowState(12, history, [pending()]).tone, "active");
  assert.deepEqual(contentUpdateRowState(12, history, [pending({ status: "uncertain", error: "请求超时，结果尚未确认" })]), {
    label: "提交待确认", tone: "warning", description: "请求超时，结果尚未确认",
  });
  assert.deepEqual(contentUpdateRowState(12, history, [pending({ status: "rejected", error: "队列已满" })]), {
    label: "提交未成功", tone: "warning", description: "队列已满",
  });
});

test("the latest pending attempt is selected without changing caller arrays", () => {
  const requests = [pending({ status: "rejected" }), pending({ status: "uncertain", createdAt: "2026-09-06T09:00:00Z" })];
  assert.equal(contentUpdateRowState(12, [], requests).label, "提交待确认");
  assert.equal(requests[0].status, "rejected");
});

test("queued and running work use the reported real stage and outrank terminal history", () => {
  assert.deepEqual(contentUpdateRowState(12, [completed({ id: 10 }), job({ status: "queued", stage_label: "等待更新" })], []), {
    label: "排队中", tone: "active", description: "等待更新",
  });
  assert.deepEqual(contentUpdateRowState(12, [completed({ id: 10 }), job({ stage_label: "正在获取平台数据" })], []), {
    label: "更新中", tone: "active", description: "正在获取平台数据",
  });
});

test("an uncertain result remains visible while it blocks further writes", () => {
  const uncertain = job({ status: "failed", error_code: "result_uncertain", error: "连接中断，请人工核实结果" });
  assert.deepEqual(contentUpdateRowState(12, [completed({ id: 20 }), uncertain], []), {
    label: "结果待确认", tone: "warning", description: "连接中断，请人工核实结果",
  });
});

test("the newest terminal result wins, so an old failure cannot obscure a new success", () => {
  const failed = job({ status: "failed", error: "平台数据暂不可用" });
  const history = [failed, completed({ id: 8 })];
  assert.equal(contentUpdateRowState(12, history, []).label, "已更新");
  assert.equal(contentUpdateRowState(12, history, []).tone, "success");
  assert.equal(history[0].id, 1);
  assert.deepEqual(contentUpdateRowState(12, [completed(), { ...failed, id: 9 }], []), {
    label: "更新未完成", tone: "warning", description: "平台数据暂不可用",
  });
});

test("a completed job reports missing data as partial instead of claiming full success", () => {
  const state = contentUpdateRowState(12, [completed({ result: { status: "partial", provider_cost: 0, metrics: { status: "partial", missing_fields: ["view_count"] } } })], []);
  assert.equal(state.label, "部分更新");
  assert.equal(state.tone, "warning");
  assert.match(state.description, /阅读数尚无新数据/);
  const incomplete = contentUpdateRowState(12, [completed({ result: { status: "succeeded", stages: [{ stage: "comments", status: "failed" }] } })], []);
  assert.equal(incomplete.label, "部分更新");
  assert.match(incomplete.description, /评论资料更新未完成/);
});

test("missing, failed or unfamiliar business receipts never display a success badge", () => {
  for (const result of [null, { status: "failed" }, { status: "queued" }, { status: "succeeded", metrics: { status: "running" } }]) {
    const state = contentUpdateRowState(12, [completed({ result })], []);
    assert.equal(state.label, "结果待确认");
    assert.equal(state.tone, "warning");
    assert.ok(state.description);
  }
});
