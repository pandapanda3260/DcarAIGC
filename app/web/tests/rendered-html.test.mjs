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
  assert.match(html, /<title>DCar Insight · 内容评估工作台<\/title>/i);
  assert.match(html, /正在读取本地评估结果/);
  assert.doesNotMatch(html, /codex-preview|Your site is taking shape|Starter Project/i);
});

test("contains the requested workflow surfaces and no starter dependency", async () => {
  const [dashboard, layout, packageJson] = await Promise.all([
    readFile(new URL("../app/EvaluationDashboard.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/layout.tsx", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);
  for (const label of ["运行总览", "新建评估", "结果报告", "内容明细", "数据资产"]) {
    assert.match(dashboard, new RegExp(label));
  }
  assert.match(dashboard, /\/api\/runs\/cache-regression/);
  assert.match(dashboard, /付费 API 刷新关闭/);
  assert.match(layout, /DCar Insight · 内容评估工作台/);
  assert.doesNotMatch(packageJson, /react-loading-skeleton/);
  await assert.rejects(access(new URL("../app/_sites-preview", import.meta.url)));
});

test("bundles the frozen v6.2 dual-channel report", async () => {
  const report = JSON.parse(await readFile(new URL("../public/data/latest-report.json", import.meta.url), "utf8"));
  assert.equal(report.report_version, "channel-structured-conclusions-v6.2-tikhub");
  assert.equal(report.channels.douyin.denominator, 438);
  assert.equal(report.channels.xiaohongshu.denominator, 338);
  assert.equal(report.channels.douyin.summary.core_selling_point_count_share.value, 42.24);
  assert.equal(report.channels.douyin.summary.acquisition_effect_estimate.value, 34);
  assert.deepEqual(Object.keys(report.channels.douyin.scenes), ["二手车", "新车", "媒体-AI小懂"]);
});
