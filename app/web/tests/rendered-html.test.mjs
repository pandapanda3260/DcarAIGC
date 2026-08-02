import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import test from "node:test";

async function render() {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);
  return worker.fetch(
    new Request("http://localhost/", { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
}

test("server-renders the DCar product shell", async () => {
  const response = await render();
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") ?? "", /^text\/html\b/i);
  const html = await response.text();
  assert.match(html, /<html lang="zh-CN">/i);
  assert.match(html, /<title>DCar Insight · 内容运营工作台<\/title>/i);
  assert.match(html, /正在读取 v8 运营数据/);
  assert.doesNotMatch(html, /codex-preview|Your site is taking shape|Starter Project/i);
});

test("contains the five-page v8 workflow and keeps private search values out of URLs", async () => {
  const [dashboard, layout, packageJson] = await Promise.all([
    readFile(new URL("../app/InsightDashboard.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/layout.tsx", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);
  for (const label of ["概览", "任务", "账号", "内容", "卖点"]) {
    assert.match(dashboard, new RegExp(label));
  }
  assert.match(dashboard, /\/api\/v8\/overview/);
  assert.match(dashboard, /\/api\/v8\/accounts\/search/);
  assert.match(dashboard, /method: "POST"/);
  assert.doesNotMatch(dashboard, /\/api\/runs\/full|\?phone=/);
  assert.match(dashboard, /待复核/);
  assert.match(dashboard, /\/api\/v8\/reviews\/\$\{item\.review_queue_id\}\/start/);
  assert.match(dashboard, /\/api\/v8\/reviews\/\$\{reviewItem\.review_queue_id\}\/resolve/);
  assert.match(dashboard, /人工证据/);
  assert.match(dashboard, /媒体|补证/);
  assert.match(dashboard, /\/api\/v8\/selling-points\/draft/);
  assert.match(dashboard, /\/api\/v8\/selling-points\/publish/);
  assert.match(dashboard, /发布标准/);
  assert.match(dashboard, /\/api\/v8\/tasks\/\$\{taskId\}\/revisions/);
  assert.match(dashboard, /新建自定义报告/);
  assert.match(dashboard, /报告概览/);
  assert.match(dashboard, /文件与日志/);
  assert.match(dashboard, /\/api\/v8\/accounts\/export/);
  assert.match(dashboard, /\/api\/v8\/accounts\/import/);
  assert.match(dashboard, /\/api\/v8\/contents\/validate/);
  assert.match(dashboard, /\/api\/v8\/contents\/import/);
  assert.match(dashboard, /\/api\/v8\/contents\/\$\{item\.id\}\/update-data/);
  assert.match(dashboard, /新增账号/);
  assert.match(dashboard, /新增内容/);
  assert.match(dashboard, /暂不可计算/);
  assert.match(layout, /DCar Insight · 内容运营工作台/);
  assert.doesNotMatch(packageJson, /react-loading-skeleton/);
  await assert.rejects(access(new URL("../app/_sites-preview", import.meta.url)));
});

test("removes the old static frontend while retaining the read-only v7 history API", async () => {
  const [dashboard, apiSource] = await Promise.all([
    readFile(new URL("../app/InsightDashboard.tsx", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/api.py", import.meta.url), "utf8"),
  ]);
  await assert.rejects(access(new URL("../app/EvaluationDashboard.tsx", import.meta.url)));
  await assert.rejects(access(new URL("../public/data/latest-report.json", import.meta.url)));
  for (const oldName of ["运行总览", "新建评估", "结果报告", "数据资产"]) {
    assert.doesNotMatch(dashboard, new RegExp(oldName));
  }
  assert.doesNotMatch(dashboard, /latest-report\.json|channel-structured-conclusions-v7\.0/);
  assert.match(apiSource, /LEGACY_REPORT_VERSION = "channel-structured-conclusions-v7\.0"/);
  assert.match(apiSource, /\/api\/v7\/history\/reports/);
});
