import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import test from "node:test";

async function render(path = "/overview") {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}-${path}`);
  const { default: worker } = await import(workerUrl.href);
  return worker.fetch(
    new Request(`http://localhost${path}`, { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
}

test("server-renders every real v8 product route", async () => {
  const expectations = [
    ["/overview", "内容运营概览"], ["/tasks", "数据报告任务"],
    ["/tasks/D8-TEST", "数据报告任务"], ["/accounts", "运营账号"],
    ["/contents", "内容数据"], ["/selling-points", "卖点标准"],
  ];
  for (const [path, title] of expectations) {
    const response = await render(path);
    assert.equal(response.status, 200, `${path} should render`);
    assert.match(response.headers.get("content-type") ?? "", /^text\/html\b/i);
    const html = await response.text();
    assert.match(html, /<html lang="zh-CN">/i);
    assert.match(html, new RegExp(title));
    assert.match(html, /href="\/overview"/);
    assert.match(html, /href="\/tasks"/);
    assert.match(html, /href="\/accounts"/);
    assert.match(html, /href="\/contents"/);
    assert.match(html, /href="\/selling-points"/);
    assert.doesNotMatch(html, /codex-preview|Your site is taking shape|Starter Project/i);
  }
});

test("root redirects to overview instead of keeping hidden client-side view state", async () => {
  const response = await render("/");
  assert.ok(response.status >= 300 && response.status < 400, `unexpected status ${response.status}`);
  assert.match(response.headers.get("location") ?? "", /\/overview$/);
});

test("overview keeps time windows outside the fixed channel conclusion structure", async () => {
  const source = await readFile(new URL("../app/overview/OverviewPage.tsx", import.meta.url), "utf8");
  assert.match(source, /yesterday:\s*"昨天"/);
  assert.match(source, /this_week:\s*"本周"/);
  assert.match(source, /last_week:\s*"上周"/);
  assert.match(source, /channelOrder[^=]*=\s*\["douyin",\s*"xiaohongshu"\]/);
  assert.match(source, /sceneOrder[^=]*=\s*\["used_car",\s*"new_car",\s*"media"\]/);
  const labels = [
    "卖点条数占比", "核心卖点条数占比", "卖点曝光占比", "核心卖点曝光占比",
    "内容垂直度", "互动用户垂直度", "内容拉新效果预估",
  ];
  let previous = -1;
  for (const label of labels) {
    const position = source.indexOf(label);
    assert.ok(position > previous, `${label} should exist in the fixed metric order`);
    previous = position;
  }
  assert.match(source, /<h3>【\{channel\.label\}渠道】<\/h3>/);
  assert.match(source, /<h4>汇总<\/h4>/);
  assert.match(source, /<h4>三个业务场景<\/h4>/);
  assert.match(source, /activeWindow\.channels\[key\]/);
  assert.match(source, /以下仍保留完整渠道与场景结构/);
  assert.match(source, /运营补充指标（不属于上述七项结论）/);
});

test("routes preserve operations and expose evidence-backed review controls", async () => {
  const [shell, accounts, contents, evidence, tasks, taskDetail, sellingPoints, apiSource, layout, packageJson] = await Promise.all([
    readFile(new URL("../app/components/AppShell.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/accounts/AccountsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/ContentsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/EvidenceModal.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/tasks/TasksPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/tasks/[id]/TaskDetailPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/selling-points/SellingPointsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/api.py", import.meta.url), "utf8"),
    readFile(new URL("../app/layout.tsx", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);
  for (const href of ["/overview", "/tasks", "/accounts", "/contents", "/selling-points"]) assert.match(shell, new RegExp(`href: "${href}"`));
  assert.match(accounts, /pending_platform_identities/);
  assert.match(accounts, /手机号完整显示/);
  assert.match(accounts, /\/api\/v8\/accounts\/import/);
  assert.match(accounts, /\/api\/v8\/accounts\/export/);
  assert.match(contents, /\/api\/v8\/contents\/validate/);
  assert.match(contents, /\/api\/v8\/contents\/import/);
  assert.match(contents, /\/update-data/);
  assert.match(contents, /查看证据/);
  assert.match(evidence, /base_evaluation_id/);
  assert.match(evidence, /ASR 全文/);
  assert.match(evidence, /OCR 全文/);
  assert.match(evidence, /评论摘要/);
  assert.match(evidence, /媒体处理槽/);
  assert.match(evidence, /allow_paid_refresh/);
  assert.match(evidence, /\/evidence/);
  assert.match(evidence, /\/media\/retry/);
  assert.match(tasks, /新建自定义报告/);
  assert.match(taskDetail, /\/cancel/);
  assert.match(taskDetail, /\/resume/);
  assert.match(taskDetail, /文件与日志/);
  assert.match(sellingPoints, /\/api\/v8\/selling-points\/draft/);
  assert.match(sellingPoints, /\/api\/v8\/selling-points\/publish/);
  assert.match(apiSource, /\/api\/v8\/media-processing\/search/);
  assert.match(apiSource, /\/api\/v8\/contents\/\{content_id\}\/evidence/);
  assert.match(apiSource, /LEGACY_REPORT_VERSION = "channel-structured-conclusions-v7\.0"/);
  assert.match(layout, /DCar Insight · 内容运营工作台/);
  assert.doesNotMatch(packageJson, /react-loading-skeleton/);
  await assert.rejects(access(new URL("../app/InsightDashboard.tsx", import.meta.url)));
  await assert.rejects(access(new URL("../app/_sites-preview", import.meta.url)));
});

test("private account searches stay in POST bodies and obsolete static assets remain absent", async () => {
  const [accounts, contents, apiSource] = await Promise.all([
    readFile(new URL("../app/accounts/AccountsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/ContentsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/api.py", import.meta.url), "utf8"),
  ]);
  assert.match(accounts, /jsonRequest\(\{ page: 1, page_size: 50/);
  assert.doesNotMatch(accounts, /\?phone=|URLSearchParams/);
  assert.match(contents, /jsonRequest\(\{ page: 1, page_size: 50/);
  assert.doesNotMatch(contents, /latest-report\.json|channel-structured-conclusions-v7\.0/);
  assert.match(apiSource, /\/api\/v7\/history\/reports/);
  await assert.rejects(access(new URL("../app/EvaluationDashboard.tsx", import.meta.url)));
  await assert.rejects(access(new URL("../public/data/latest-report.json", import.meta.url)));
});
