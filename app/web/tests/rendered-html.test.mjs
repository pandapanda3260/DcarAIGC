import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import test from "node:test";
import {
  buildContentPatch,
  buildContentRequest,
  buildContentSaveOperation,
  fromShanghaiDateTimeLocal,
  toShanghaiDateTimeLocal,
} from "../app/contents/contentForm.ts";
import { jsonRequest } from "../app/lib/api.ts";

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
    ["/overview", "数据概览"], ["/tasks", "数据报告任务"],
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

test("sidebar uses the bundled Dongchedi app mark and brand colors", async () => {
  const [shell, styles, icon] = await Promise.all([
    readFile(new URL("../app/components/AppShell.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../public/dongchedi-app-icon.svg", import.meta.url), "utf8"),
  ]);
  assert.match(shell, /src="\/dongchedi-app-icon\.svg"/);
  assert.match(shell, /alt="懂车帝 App"/);
  assert.match(shell, /<strong>Dcar Sentinel<\/strong>/);
  assert.match(shell, /内容运营工作台 · V1\.0/);
  assert.doesNotMatch(shell, /内容运营工作台 · v8/);
  assert.doesNotMatch(shell, /v8\.2 合同|本地优先|topbar-statuses|safe-chip/);
  assert.doesNotMatch(shell, /<strong>DCar Insight<\/strong>/);
  assert.match(styles, /--dcd-brand:\s*#ffcd32/i);
  assert.match(styles, /--dcd-on-brand:\s*#1f2129/i);
  assert.match(styles, /\.sidebar nav a\.active\s*\{\s*background:\s*var\(--dcd-brand\);\s*color:\s*var\(--dcd-on-brand\);\s*\}/);
  assert.match(icon, /fill="#FFCD32"/);
  assert.match(icon, /fill="#1F2129"/);
});

test("sidebar navigation uses semantic graphical icons instead of character marks", async () => {
  const [shell, styles] = await Promise.all([
    readFile(new URL("../app/components/AppShell.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);
  assert.doesNotMatch(shell, /mark:\s*"[概任账内卖]"/);
  assert.match(shell, /className="nav-icon"/);
  assert.match(shell, /stroke="currentColor"/);
  assert.match(shell, /aria-hidden="true"/);
  assert.match(shell, /focusable="false"/);
  assert.match(shell, /data-nav-icon=\{section\}/);
  for (const section of ["overview", "tasks", "accounts", "contents", "selling-points"]) {
    const key = section === "selling-points" ? `"${section}"` : section;
    assert.match(shell, new RegExp(`${key}:\\s*<>`));
  }
  assert.match(styles, /\.sidebar nav a \.nav-icon\s*\{[^}]*width:\s*20px;[^}]*height:\s*20px;/);
});

test("overview keeps time windows outside the fixed channel conclusion structure", async () => {
  const [source, shell, styles, automotiveLines, douyinBrand, xiaohongshuBrand] = await Promise.all([
    readFile(new URL("../app/overview/OverviewPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/components/AppShell.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../public/overview-automotive-lines.webp", import.meta.url)),
    readFile(new URL("../public/brand-douyin-tiktok.svg", import.meta.url), "utf8"),
    readFile(new URL("../public/brand-xiaohongshu.svg", import.meta.url), "utf8"),
  ]);
  assert.match(source, /yesterday:\s*"昨天"/);
  assert.match(source, /this_week:\s*"本周"/);
  assert.match(source, /last_week:\s*"上周"/);
  assert.match(source, /channelOrder[^=]*=\s*\["douyin",\s*"xiaohongshu"\]/);
  assert.match(source, /sceneOrder[^=]*=\s*\["used_car",\s*"new_car",\s*"media"\]/);
  const labels = [
    "卖点条数占比", "核心卖点条数占比", "卖点曝光占比", "核心卖点曝光占比",
    "内容垂直度", "互动用户汽车兴趣占比", "内容拉新效果预估",
  ];
  let previous = -1;
  for (const label of labels) {
    const position = source.indexOf(label);
    assert.ok(position > previous, `${label} should exist in the fixed metric order`);
    previous = position;
  }
  assert.match(source, /<h3>\{channel\.label\}渠道<\/h3>/);
  assert.match(source, /<h4>汇总<\/h4>/);
  assert.match(source, /<h4>三个业务场景<\/h4>/);
  assert.match(source, /activeWindow\.channels\[key\]/);
  assert.match(source, /className="channel-switch" role="group" aria-label="统计窗口"/);
  assert.match(source, /type="button" aria-pressed=\{windowKey === key\}/);
  assert.match(source, /<AppShell active="overview" actions=\{windowSwitch\}>/);
  assert.match(source, /brand-douyin-tiktok\.svg/);
  assert.match(source, /brand-xiaohongshu\.svg/);
  assert.match(source, /sceneIcons[\s\S]*used_car:\s*CarIcon/);
  assert.match(source, /className="overview-support-grid"/);
  assert.match(source, />\{windowLabels\[windowKey\]\}统计边界</);
  assert.match(source, />数据质量状态</);
  assert.doesNotMatch(source, /运营补充说明|operationalMetrics|operational-details/);
  assert.match(shell, /多渠道内容运营核心指标总览与场景分析/);
  assert.match(shell, /data-section=\{active\}/);
  assert.doesNotMatch(source, /CHANNEL CONCLUSIONS BY WINDOW|时间窗口筛选，渠道结构保持不变|每个窗口固定输出|以下仍保留完整渠道与场景结构/);
  assert.doesNotMatch(source, /activeWindow\?\.empty_explanation|window-empty-explanation/);
  assert.match(source, /className="page-stack overview-dashboard"/);
  assert.match(source, /data-channel=\{channel\.platform\}/);
  assert.match(source, /data-scene=\{sceneKey\}/);
  assert.match(source, /metricIcons[\s\S]*satisfies Record<ConclusionMetricKey/);
  assert.match(styles, /overview-automotive-lines\.webp/);
  assert.match(styles, /\.overview-dashboard \.conclusion-summary-grid\s*\{[^}]*grid-template-columns:\s*repeat\(12,/);
  assert.match(styles, /\.overview-dashboard \.conclusion-metric-card:nth-child\(n\+5\)\s*\{[^}]*grid-column:\s*span 4/);
  assert.match(styles, /data-scene="new_car"/);
  assert.match(styles, /data-scene="media"/);
  assert.match(styles, /\.overview-support-grid\s*\{[^}]*grid-template-columns:\s*minmax\(280px, 2fr\) minmax\(0, 3fr\)/);
  assert.match(styles, /@container \(max-width:\s*719px\)/);
  assert.match(styles, /@container \(max-width:\s*1079px\)[\s\S]*\.overview-dashboard \.scene-conclusion-grid\s*\{\s*grid-template-columns:\s*1fr;/);
  assert.match(styles, /\.overview-dashboard \.scene-conclusion-card\s*\{[^}]*--scene-accent:\s*var\(--channel-accent\);/);
  assert.doesNotMatch(styles, /\.operational-details|\.support-placeholder-lines|\.support-extra-list/);
  assert.ok(automotiveLines.byteLength > 1000, "automotive header artwork should be a real raster asset");
  assert.match(douyinBrand, /viewBox="0 0 256 290"/);
  assert.match(douyinBrand, /fill="#ff004f"/i);
  assert.match(douyinBrand, /fill="#00f2ea"/i);
  assert.match(xiaohongshuBrand, /viewBox="0 0 24 24"/);
});

test("selling point standards use E/X/M scenes, scene-local hits, and matcher-only editing", async () => {
  const [source, types, shell, styles, heroArtwork] = await Promise.all([
    readFile(new URL("../app/selling-points/SellingPointsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/types.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/components/AppShell.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../public/selling-points-hero-bg.png", import.meta.url)),
  ]);
  for (const [code, title, scene] of [["E", "二手车", "used_car"], ["X", "新车", "new_car"], ["M", "媒体", "media"]]) {
    assert.match(source, new RegExp(`code: "${code}", title: "${title}"[^\n]*scene: "${scene}"`));
  }
  assert.match(source, /type StandardFamilyCode = "E" \| "X" \| "M";/);
  assert.doesNotMatch(source, /code: "C"|"OTHER"|otherFamily|displayFamilies|nativeFamilyCodes/);
  assert.match(source, /\.filter\(\(point\) => point\.scenes\.includes\(scene\)\)/);
  assert.match(source, /pointsForFamily\(data\.items, family\.scene\)/);
  assert.doesNotMatch(source, /point\.code\.startsWith/);
  assert.match(source, /className="selling-point-summary-grid"/);
  assert.match(source, /className="selling-point-table"/);
  assert.match(source, /<caption className="visually-hidden">\{family\.code\} \{family\.title\}卖点标准<\/caption>/);
  assert.match(source, /<th scope="col">一级类目<\/th>/);
  assert.match(source, /<th scope="col">卖点标准<\/th>/);
  assert.match(source, /<th scope="col">层级与适用范围<\/th>/);
  assert.match(source, /point\.scenes\.map\(\(pointScene\)/);
  assert.match(source, /return point\.scene_hits\?\.\[scene\]/);
  assert.match(source, /sceneHits\(point, family\.scene\)/);
  assert.match(source, /pointSceneHits\.primary_hits/);
  assert.match(source, /pointSceneHits\.total_hits/);
  assert.doesNotMatch(source, /point\.primary_hits|point\.total_hits/);
  assert.match(types, /matcher_rule: Record<string, unknown> \| null;/);
  for (const projection of ["scenes", "positive_evidence", "negative_evidence", "boundary_rules"]) {
    assert.match(types, new RegExp(`readonly ${projection}:`));
  }
  assert.match(source, /matcherRuleJson: JSON\.stringify\(point\.matcher_rule \?\? \{\}, null, 2\)/);
  assert.match(source, /JSON\.parse\(form\.matcherRuleJson\)/);
  assert.match(source, /匹配规则 JSON 格式无效/);
  assert.match(source, /匹配规则必须是一个 JSON 对象/);
  assert.match(source, /matcher_rule: matcherRule/);
  assert.doesNotMatch(source, /positiveEvidence|negativeEvidence|boundaryRules|businessSceneOptions|type="checkbox"/);
  assert.match(source, /className="selling-point-projection-preview" aria-label="规则只读投影"/);
  assert.match(source, /projectionPoint\.positive_evidence/);
  assert.match(source, /projectionPoint\.negative_evidence/);
  assert.match(source, /projectionPoint\.boundary_rules/);
  assert.doesNotMatch(source, /\/api\/v8\/selling-points\/publish|发布标准/);
  assert.match(source, /草稿须完成评估回填与验收后，由发布流程原子激活/);
  assert.match(source, /<AppShell active="selling-points" actions=\{shellActions\}>/);
  assert.match(source, /PencilSimpleIcon/);
  assert.match(source, /role="region" aria-label=\{`\$\{family\.code\} \$\{family\.title\}卖点标准表格`\} tabIndex=\{0\}/);
  assert.match(shell, /围绕 E、X、M 三个业务场景/);
  assert.doesNotMatch(shell, /E、X、M、C|四类标准系列/);
  assert.match(styles, /selling-points-hero-bg\.png/);
  assert.match(styles, /\.selling-point-summary-grid\s*\{[^}]*grid-template-columns:\s*repeat\(3,/);
  assert.doesNotMatch(styles, /data-family="C"|data-family="OTHER"/);
  assert.match(styles, /\.selling-point-table\s*\{[^}]*min-width:\s*940px;/);
  assert.match(styles, /@media \(max-width:\s*480px\)/);
  const response = await render("/selling-points");
  const html = await response.text();
  assert.match(html, /围绕 E、X、M 三个业务场景/);
  assert.doesNotMatch(html, /E、X、M、C|生态场景|其他标准/);
  assert.ok(heroArtwork.byteLength > 1000, "selling point header artwork should be a real raster asset");
});

test("routes preserve operations and expose evidence-backed review controls", async () => {
  const [shell, accounts, contents, evidence, tasks, taskDetail, sellingPoints, apiSource, formatSource, layout, packageJson] = await Promise.all([
    readFile(new URL("../app/components/AppShell.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/accounts/AccountsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/ContentsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/EvidenceModal.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/tasks/TasksPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/tasks/[id]/TaskDetailPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/selling-points/SellingPointsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/api.py", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/format.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/layout.tsx", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);
  for (const href of ["/overview", "/tasks", "/accounts", "/contents", "/selling-points"]) assert.match(shell, new RegExp(`href: "${href}"`));
  assert.match(accounts, /pending_platform_identities/);
  assert.match(accounts, /className="pending-identity-table"/);
  assert.match(accounts, /<caption className="visually-hidden">待归属平台身份列表<\/caption>/);
  assert.match(accounts, /<th>平台<\/th><th>平台 UID<\/th><th>昵称<\/th><th>关联内容<\/th>/);
  assert.doesNotMatch(accounts, /pending-identity-grid/);
  assert.match(accounts, /一个手机号对应一行账号主数据/);
  assert.match(accounts, /className="account-master-table"/);
  assert.match(accounts, /以手机号为锚点的账号主数据列表/);
  assert.match(accounts, /className="account-base-group" colSpan=\{4\} scope="colgroup">账号基础信息/);
  assert.match(accounts, /platformKeys\.map\(\(key\) => <th className="account-platform-group"/);
  assert.match(accounts, /className="account-management-group" colSpan=\{2\} scope="colgroup">账号管理/);
  assert.match(accounts, /<PlatformHeaderMark platformKey=\{key\} \/>/);
  assert.match(accounts, /data-platform-columns=\{key\}/);
  assert.match(formatSource, /platformKeys = \["douyin", "xiaohongshu", "wechat_channels", "kuaishou"\]/);
  assert.match(accounts, /<th className="account-platform-start" scope="col">UID<\/th><th scope="col">是否实名<\/th><th scope="col">昵称<\/th><th scope="col">粉丝量<\/th><th scope="col">关联内容量<\/th>/);
  assert.match(accounts, /<th className="account-sticky account-phone" scope="row"><strong>\{item\.phone\}<\/strong><\/th>/);
  assert.match(accounts, /<AccountPlatformCells account=\{item\} \/>/);
  assert.match(apiSource, /NULL follower_count/);
  assert.match(apiSource, /COUNT\(c\.id\) content_count/);
  assert.match(accounts, /\/api\/v8\/accounts\/import/);
  assert.match(accounts, /\/api\/v8\/accounts\/export/);
  assert.match(contents, /\/api\/v8\/contents\/validate/);
  assert.match(contents, /\/api\/v8\/contents\/import/);
  assert.match(contents, /\/update-data/);
  assert.match(contents, /查看证据/);
  assert.match(contents, /再次复核/);
  assert.match(contents, /旧规则评估/);
  assert.match(taskDetail, /display_effective_revision/);
  assert.match(taskDetail, /历史规则报告，已过时/);
  assert.match(taskDetail, /revision_state === "current"/);
  assert.match(taskDetail, /revision_state === "stale"/);
  assert.doesNotMatch(taskDetail, /revisions\.find\(\(item\) => !item\.invalidated_at\)/);
  assert.match(tasks, /current_valid_revision/);
  assert.match(tasks, /stale_display_revision/);
  assert.match(tasks, /historical_revision_count/);
  assert.match(evidence, /base_evaluation_id/);
  assert.match(evidence, /display_evaluation_id/);
  assert.match(evidence, /旧规则，已过时/);
  assert.match(evidence, /当前 release 尚无评估/);
  assert.match(evidence, /当前评估摘要/);
  assert.match(evidence, /ASR 全文/);
  assert.match(evidence, /OCR 全文/);
  assert.match(evidence, /评论摘要/);
  assert.match(evidence, /媒体处理槽/);
  assert.match(evidence, /allow_paid_refresh/);
  assert.match(evidence, /\/evidence/);
  assert.match(evidence, /\/media\/retry/);
  assert.match(evidence, /\/api\/v8\/reviews\/\$\{item\.review_queue_id\}\/reopen/);
  assert.match(evidence, /不会改写当前结论，也不会生成新评估/);
  assert.match(evidence, /提交人工复核表单，才会追加新的评估版本/);
  assert.match(evidence, /再次复核发起失败/);
  assert.match(tasks, /新建自定义报告/);
  assert.match(taskDetail, /\/cancel/);
  assert.match(taskDetail, /\/resume/);
  assert.match(taskDetail, /文件与日志/);
  assert.match(sellingPoints, /\/api\/v8\/selling-points\/draft/);
  assert.doesNotMatch(sellingPoints, /\/api\/v8\/selling-points\/publish/);
  assert.match(sellingPoints, /\/api\/v8\/selling-points\/items\/\$\{editingCode\}/);
  assert.match(sellingPoints, /method: "DELETE"/);
  assert.match(sellingPoints, /jsonRequest\(\{/);
  assert.match(sellingPoints, /matcher_rule: matcherRule/);
  assert.doesNotMatch(sellingPoints, /scenes: form\.scenes|positive_evidence:|negative_evidence:|boundary_rules:/);
  assert.match(apiSource, /\/api\/v8\/media-processing\/search/);
  assert.match(apiSource, /\/api\/v8\/contents\/\{content_id\}\/evidence/);
  assert.match(apiSource, /LEGACY_REPORT_VERSION = "channel-structured-conclusions-v7\.0"/);
  assert.match(layout, /DCar Insight · 内容运营工作台/);
  assert.doesNotMatch(packageJson, /react-loading-skeleton/);
  await assert.rejects(access(new URL("../app/InsightDashboard.tsx", import.meta.url)));
  await assert.rejects(access(new URL("../app/_sites-preview", import.meta.url)));
});

test("task detail renders channel conclusions in the platforms tab without fabricating rates", async () => {
  const [taskDetail, types] = await Promise.all([
    readFile(new URL("../app/tasks/[id]/TaskDetailPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/types.ts", import.meta.url), "utf8"),
  ]);
  assert.match(types, /channels\?: Record<OverviewChannelKey, OverviewChannel> \| null;/);
  assert.match(taskDetail, /channelOrder[^=]*=\s*\["douyin",\s*"xiaohongshu"\]/);
  assert.match(taskDetail, /sceneOrder[^=]*=\s*\["used_car",\s*"new_car",\s*"media"\]/);
  const labels = [
    "卖点条数占比", "核心卖点条数占比", "卖点曝光占比", "核心卖点曝光占比",
    "内容垂直度", "互动用户汽车兴趣占比", "内容拉新效果预估",
  ];
  let previous = -1;
  for (const label of labels) {
    const position = taskDetail.indexOf(label);
    assert.ok(position > previous, `${label} should exist in the fixed metric order`);
    previous = position;
  }
  assert.match(taskDetail, /report\?\.channels \? </);
  assert.match(taskDetail, /channel-conclusion-table/);
  assert.match(taskDetail, /data-channel=\{channelKey\}/);
  assert.match(taskDetail, /below_threshold:\s*"覆盖不足"/);
  assert.match(taskDetail, /missing:\s*"有效样本不足"/);
  assert.match(taskDetail, /not_applicable:\s*"无适用内容"/);
  assert.match(taskDetail, /（仅样本）/);
  assert.match(taskDetail, /用户级去重口径/);
  assert.match(taskDetail, /channel_conclusions\.csv/);
  assert.match(taskDetail, /没有渠道\/场景结论/);
  assert.match(taskDetail, /平台发布分布/);
  assert.match(taskDetail, /platform_dimensions/);
  assert.doesNotMatch(taskDetail, /audience_verticality|互动用户垂直度/);
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

test("content editing sends only changed fields and preserves Shanghai timestamps", async () => {
  const original = {
    id: 7,
    platform: "douyin",
    platformContentId: "123456789",
    canonicalUrl: "https://www.douyin.com/video/123456789",
    publishedAt: "2026-08-03T16:00",
    title: "原始标题",
    body: "原始正文",
    contentType: "video",
    accountUid: "account-uid",
    accountName: "账号昵称",
    accountType: "original",
    contentDirection: "new_car",
  };
  assert.equal(toShanghaiDateTimeLocal("2026-08-03T08:00:00Z"), "2026-08-03T16:00");
  assert.equal(fromShanghaiDateTimeLocal("2026-08-03T16:00"), "2026-08-03T08:00:00.000Z");
  assert.throws(() => fromShanghaiDateTimeLocal("2026-02-30T12:00"), /发布日期格式无效/);
  assert.deepEqual(buildContentPatch(original, { ...original, title: "新标题" }), {
    title: "新标题",
  });
  assert.deepEqual(
    buildContentPatch(original, {
      ...original,
      title: "新标题",
      contentDirection: "media",
    }),
    { title: "新标题", content_direction: "media" },
  );
  assert.deepEqual(buildContentPatch(original, original), {});
  assert.equal(Object.keys(buildContentRequest(original)).length, 11);
  const saveOperation = buildContentSaveOperation(
    { ...original, title: "真实保存标题" },
    original,
  );
  const saveRequest = new Request(
    `http://localhost${saveOperation.path}`,
    jsonRequest(saveOperation.body, saveOperation.method),
  );
  assert.equal(saveRequest.method, "PATCH");
  assert.equal(new URL(saveRequest.url).pathname, "/api/v8/contents/7");
  assert.deepEqual(await saveRequest.json(), { title: "真实保存标题" });
  assert.equal(saveOperation.unchanged, false);
  assert.equal(buildContentSaveOperation(original, original).unchanged, true);

  const source = await readFile(
    new URL("../app/contents/ContentsPage.tsx", import.meta.url),
    "utf8",
  );
  assert.match(source, /buildContentSaveOperation\(form, originalForm\)/);
  assert.match(source, /toShanghaiDateTimeLocal\(item\.published_at\)/);
  assert.doesNotMatch(source, /new Date\(form\.publishedAt\)\.toISOString\(\)/);
});
