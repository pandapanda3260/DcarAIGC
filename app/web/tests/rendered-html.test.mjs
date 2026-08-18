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
import {
  metricCompactValue,
  metricEvidence,
  metricPublishesValue,
  metricStatus,
  metricUnavailableLabel,
  metricValue,
} from "../app/lib/format.ts";

function ratioMetric(status, reason, percentage = 62.05) {
  return {
    kind: "ratio",
    numerator: 62,
    denominator: 100,
    percentage,
    unit: "percent",
    status,
    eligible_count: 100,
    coverage_percentage: 62,
    reason,
  };
}

test("metric presentation uses causes and never publishes a gated number", () => {
  const belowCases = [
    ["用户身份覆盖率 37.48%，低于 95% 门槛", "身份数据待补齐"],
    ["用户分类覆盖率 0.0%，低于 100% 门槛", "用户分类未完成"],
    ["去重有效用户 6 人，低于 30 人门槛", "互动用户少于30人"],
    ["分类器定标未通过，暂不发布比例", "分类器未通过定标"],
    ["可归类有效曝光 10/1000，覆盖 1%：低于 90% 发布门槛", "曝光归类待补齐"],
    ["用户级汽车兴趣占比尚未接入用户聚合，暂不发布", "用户聚合未接入"],
    ["", "暂不发布"],
  ];
  for (const [reason, expected] of belowCases) {
    const metric = ratioMetric("below_threshold", reason);
    assert.equal(metricPublishesValue(metric), false);
    assert.equal(metricUnavailableLabel(metric), expected);
    assert.equal(metricValue(metric), expected);
    assert.equal(metricCompactValue(metric), expected);
    assert.equal(metricStatus(metric), "暂不发布");
    assert.doesNotMatch(`${metricValue(metric)} ${metricStatus(metric)}`, /覆盖不足/);
  }

  const uncalibrated = ratioMetric(
    "not_calculable",
    "重复内容感知指纹尚未完成定标，重复率暂不可计算",
    null,
  );
  assert.equal(metricPublishesValue(uncalibrated), false);
  assert.equal(metricUnavailableLabel(uncalibrated), "重复识别待补齐");
  assert.equal(metricValue(uncalibrated), "重复识别待补齐");

  const available = ratioMetric("available", "", 62.05);
  assert.equal(metricPublishesValue(available), true);
  assert.equal(metricValue(available), "62.05%");
  assert.equal(metricCompactValue(available), "62.05%");
  assert.equal(metricEvidence(available), "62/100");

  const sample = ratioMetric("sample_only", "分类器未经金标核对，数值仅供参考", 62.05);
  assert.equal(metricPublishesValue(sample), true);
  assert.equal(metricCompactValue(sample), "62.05%（仅样本）");

  const missingWithValue = { kind: "quantity", value: 1234, unit: "view", status: "missing", coverage_percentage: 0, reason: "" };
  assert.equal(metricPublishesValue(missingWithValue), false);
  assert.equal(metricValue(missingWithValue), "无数据");

  const availableQuantity = { ...missingWithValue, status: "available" };
  assert.equal(metricValue(availableQuantity), "1,234");
});

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
    ["/spu-audience", "SPU人群"],
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
    assert.match(html, /href="\/spu-audience"/);
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
  assert.match(shell, /src=\{publicAssetPath\("\/dongchedi-app-icon\.svg"\)\}/);
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

test("logout posts to the session gateway and follows its login redirect", async () => {
  const [component, shell] = await Promise.all([
    readFile(new URL("../app/components/LogoutButton.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/components/AppShell.tsx", import.meta.url), "utf8"),
  ]);
  assert.match(shell, /<LogoutButton \/>/);
  assert.match(component, /fetch\(publicAssetPath\("\/auth\/logout"\)/);
  assert.match(component, /method:\s*"POST"/);
  assert.match(component, /"X-Dcar-Request":\s*"logout"/);
  assert.match(component, /credentials:\s*"same-origin"/);
  assert.match(component, /cache:\s*"no-store"/);
  assert.match(component, /await response\.json\(\)/);
  assert.match(component, /payload\.redirect_to/);
  assert.match(component, /window\.location\.replace/);
  assert.match(component, /publicAssetPath\("\/login"\)/);
  assert.doesNotMatch(component, /XMLHttpRequest|\bxhr\b|BASIC_AUTH|NEXT_PUBLIC_DCAR_AUTH_MODE|__logout__|api\/v8\/health|location\.reload|本地环境未启用登录/);

  const response = await render("/accounts");
  const html = await response.text();
  assert.match(html, /aria-label="退出登录"/);
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
  for (const section of ["overview", "tasks", "accounts", "contents", "selling-points", "spu-audience"]) {
    const key = section.includes("-") ? `"${section}"` : section;
    assert.match(shell, new RegExp(`${key}:\\s*<>`));
  }
  assert.match(styles, /\.sidebar nav a \.nav-icon\s*\{[^}]*width:\s*20px;[^}]*height:\s*20px;/);
});

test("overview keeps time windows outside the fixed channel conclusion structure", async () => {
  const [source, formatSource, shell, styles, automotiveLines, douyinBrand, xiaohongshuBrand] = await Promise.all([
    readFile(new URL("../app/overview/OverviewPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/format.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/components/AppShell.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../public/overview-automotive-lines.webp", import.meta.url)),
    readFile(new URL("../public/brand-douyin-tiktok.svg", import.meta.url), "utf8"),
    readFile(new URL("../public/brand-xiaohongshu.svg", import.meta.url), "utf8"),
  ]);
  assert.match(source, /yesterday:\s*"昨天"/);
  assert.match(source, /this_week:\s*"本周"/);
  assert.match(source, /last_week:\s*"上周"/);
  assert.match(source, /useState<WindowKey>\("last_week"\)/);
  assert.match(source, /const activeWindow = overview\?\.windows\[windowKey\]/);
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
  assert.match(source, /可归类曝光覆盖 \{channel\.exposure_coverage_percentage/);
  assert.doesNotMatch(source, /曝光交叉覆盖/);
  assert.match(source, /曝光分母仅含 view_count&gt;0 的有效曝光，未取得有效曝光不入分母/);
  // 未发布数值由统一 presenter 给短语，完整原因同时提供给辅助技术。
  assert.doesNotMatch(source, /coverage-limitation-reason/);
  assert.doesNotMatch(source, /whiteSpace: "normal"/);
  assert.match(source, /title=\{evidence\}/);
  assert.match(source, /aria-describedby=\{publishesValue \? undefined : reasonId\}/);
  assert.match(source, /publishesValue \? <p>\{evidence\}<\/p> : <span id=\{reasonId\} className="visually-hidden">\{evidence\}<\/span>/);
  assert.match(source, /!publishesValue && <span id=\{reasonId\} className="visually-hidden">\{evidence\}<\/span>/);
  assert.doesNotMatch(source, /unavailableReasonLabels|unavailableStatusLabels|function conclusionValue|function metricEvidence/);
  assert.match(formatSource, /const unavailableReasonLabels/);
  assert.match(formatSource, /below_threshold:\s*"暂不发布"/);
  assert.doesNotMatch(`${source}\n${formatSource}`, /有效样本不足|"覆盖不足"/);
  assert.match(source, /activeWindow\.channels\[key\]/);
  assert.match(source, /className="channel-switch" role="group" aria-label="统计窗口"/);
  assert.match(source, /type="button" aria-pressed=\{windowKey === key\}/);
  assert.match(source, /onClick=\{\(\) => setWindowKey\(key\)\}/);
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
  assert.doesNotMatch(source, /data_freshness|数据已停止更新|自动更新异常|latestCaptureFailed|最近一次账号抓取成功|最新内容发布|最近一次日抓取失败/);
  assert.doesNotMatch(source, /activeWindowIsEmpty|empty_explanation|window-empty-explanation|所选窗口暂无已发布内容/);
  assert.doesNotMatch(source, /并非抓取故障/);
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

test("selling point statistics default to last week", async () => {
  const source = await readFile(
    new URL("../app/selling-points/SellingPointsPage.tsx", import.meta.url),
    "utf8",
  );
  assert.match(source, /useState<WindowKey>\("last_week"\)/);
});

test("data freshness types accept every scheduler run status returned by the API", async () => {
  const types = await readFile(new URL("../app/lib/types.ts", import.meta.url), "utf8");
  const freshnessStart = types.indexOf("export type DataFreshness");
  const freshness = types.slice(
    freshnessStart,
    types.indexOf("export type Overview =", freshnessStart),
  );
  assert.match(
    freshness,
    /status: "running" \| "succeeded" \| "failed" \| "partial" \| "interrupted" \| "skipped";/,
  );
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
  assert.match(source, /PlusIcon/);
  assert.match(source, /role="region" aria-label=\{`\$\{family\.code\} \$\{family\.title\}卖点标准表格`\} tabIndex=\{0\}/);
  assert.match(shell, /围绕 E、X、M 三个业务场景/);
  assert.doesNotMatch(shell, /E、X、M、C|四类标准系列/);
  assert.match(styles, /selling-points-hero-bg\.png/);
  assert.match(styles, /\.selling-point-summary-grid\s*\{[^}]*grid-template-columns:\s*repeat\(3,/);
  assert.doesNotMatch(styles, /data-family="C"|data-family="OTHER"/);
  assert.match(styles, /\.selling-point-table\s*\{[^}]*min-width:\s*1262px;/);
  assert.match(styles, /@media \(max-width:\s*480px\)/);
  const response = await render("/selling-points");
  const html = await response.text();
  assert.match(html, /围绕 E、X、M 三个业务场景/);
  assert.doesNotMatch(html, /E、X、M、C|生态场景|其他标准/);
  assert.ok(heroArtwork.byteLength > 1000, "selling point header artwork should be a real raster asset");
});

test("spu audience page keeps rule assets, association and 3D stats together", async () => {
  const [page, wrapper, contents, shell, types, formatSource, styles, apiSource, storageSource, spuModule] = await Promise.all([
    readFile(new URL("../app/spu-audience/SpuAudiencePage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/spu-audience/page.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/ContentsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/components/AppShell.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/types.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/format.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/api.py", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/storage.py", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/spu_audience.py", import.meta.url), "utf8"),
  ]);
  // 导航与页面骨架
  assert.match(shell, /id: "spu-audience", label: "SPU人群", href: "\/spu-audience"/);
  assert.match(wrapper, /<SpuAudiencePage \/>/);
  assert.match(page, /<AppShell active="spu-audience" actions=\{shellActions\}>/);
  assert.match(page, /刷新数据/);
  assert.match(page, /const shellActions = \([\s\S]*刷新数据[\s\S]*className="secondary spu-shell-action"[\s\S]*新增车型[\s\S]*\);\s*\n\s*return \(/);
  assert.doesNotMatch(page, /className="secondary spu-block-action"/);
  assert.match(styles, /\.spu-shell-action\s*\{[^}]*display:\s*inline-flex;[^}]*align-items:\s*center;[^}]*gap:\s*6px;/);
  assert.doesNotMatch(page, /运行关联/);
  // 页面信息架构（2026-08-16 Mark 定稿）：删掉刷新记录面板，规则链在上、规则校准在下
  // 版式对齐卖点页后（Mark 要求"没必要的文字删一删"），两个纯文字的分节包装标题
  //（"车型 → 人群 → 场景 规则链""规则预期 vs 内容实际"）连同长说明段一并删除，
  // 顺序改由各区块自己的 <h2> 断言；覆盖率总览上提到页首。
  {
    const sectionOrder = ["<h2 id=\"spu-summary-title\">识别覆盖</h2>", "<h2>车系与款型库</h2>", "<h2>目标人群</h2>", "<h2>用车场景</h2>", "<h2>人群 × 场景映射</h2>", "<h2>覆盖缺口</h2>", "<h2>规则外溢</h2>"];
    let previous = -1;
    for (const sectionLabel of sectionOrder) {
      const position = page.indexOf(sectionLabel);
      assert.ok(position > previous, `${sectionLabel} 应按 规则链在上、规则校准在下 的顺序出现`);
      previous = position;
    }
  }
  assert.doesNotMatch(page, /最近一次数据刷新|spu-run-panel/);
  // 统计窗口：卖点页同款控件；只有 昨天/本周/上周，默认上周
  assert.match(page, /className="selling-point-window-control spu-window-control"/);
  assert.match(page, /统计窗口/);
  assert.match(page, /useState<string>\("last_week"\)/);
  assert.doesNotMatch(page, /label: "全部" \}/);
  assert.match(page, /stats\?window=last_week/);
  // 车型库并入卖点页式渠道统计列；三维明细表与独立榜单删除
  assert.match(page, /<th>命中统计<\/th><th>抖音条数占比<\/th><th>抖音曝光占比<\/th><th>小红书条数占比<\/th><th>小红书曝光占比<\/th><th>操作<\/th>/);
  assert.match(page, /<th>品牌<\/th><th>车系<\/th><th>款型<\/th><th>目标人群<\/th>/);
  assert.doesNotMatch(page, /<th>识别别名<\/th>/);
  assert.match(page, /识别别名（顿号或逗号分隔）/);
  assert.match(page, /selling-point-hit-value/);
  assert.match(page, /selling-point-share-value/);
  assert.match(page, /条发布/);
  assert.match(page, /次有效曝光/);
  // 车型库使用独立紧凑列宽，不能再被全局 1480px 宽表兜底覆盖
  assert.match(styles, /table:not\(\.selling-point-table\):not\(\.spu-catalog-table\):has\(th:nth-child\(7\)\)\s*\{[^}]*min-width:\s*1480px;/);
  assert.doesNotMatch(styles, /table:not\(\.selling-point-table\):has\(th:nth-child\(7\)\)\s*\{[^}]*min-width:\s*1480px;/);
  assert.match(styles, /\.spu-catalog-table\s*\{[^}]*width:\s*100%;[^}]*min-width:\s*1152px;/);
  assert.match(styles, /\.spu-catalog-table thead th,\s*\.spu-catalog-table tbody td\s*\{[^}]*padding-inline:\s*8px;/);
  assert.match(styles, /\.spu-catalog-table th:nth-child\(2\)\s*\{[^}]*width:\s*116px;/);
  assert.match(styles, /\.spu-catalog-table th:nth-child\(11\)\s*\{[^}]*width:\s*58px;/);
  assert.match(styles, /\.spu-catalog-table :is\(th, td\):nth-child\(n\+6\):nth-child\(-n\+10\)\s*\{[^}]*text-align:\s*center;/);
  assert.match(styles, /\.spu-catalog-table \.selling-point-hit-value\s*\{[^}]*justify-items:\s*center;/);
  assert.match(styles, /\.spu-catalog-table \.selling-point-share-value,[\s\S]*\.spu-catalog-table \.selling-point-hit-empty\s*\{[^}]*text-align:\s*center;/);
  assert.doesNotMatch(page, /车型 × 人群 × 场景 数据表现|spu-detail-table|车型榜|人群榜|场景榜|spu-rollup/);
  assert.match(page, /<th>内容显式信号<\/th><th>发布条数<\/th><th>曝光量<\/th>/);
  assert.match(page, /<th>负向词<\/th><th>发布条数<\/th><th>曝光量<\/th>/);
  // 人群与场景是两个全宽纵向区块，不在宽屏下压成左右两列
  assert.match(styles, /\.spu-dim-grid\s*\{[^}]*grid-template-columns:\s*minmax\(0,\s*1fr\);/);
  // 自动保鲜：打开页面增量补算、保存规则后自动全量重算；资产与统计请求互不拖累
  assert.match(page, /associate\?mode=incremental/);
  assert.match(page, /stale_content_count/);
  assert.match(page, /规则资产读取失败/);
  assert.match(page, /统计读取失败/);
  // 车型库翻页：复用全站 Pagination 组件、放在表格上方（账号页同款），前端切片+末页夹紧
  assert.match(page, /import \{ Pagination \} from "\.\.\/components\/Pagination"/);
  assert.match(page, /import \{\s*filterVehicleSeriesGroups,\s*matchVehicleSeriesGroup,\s*sortVehicleCatalogRows,\s*\} from "\.\/vehicleCatalogSort"/);
  assert.match(page, /filteredSeriesTotal > 0 && <Pagination page=\{catalogSafePage\} pageSize=\{catalogPageSize\} total=\{filteredSeriesTotal\} busy=\{saving\} ariaLabel="车型库分页" unitLabel="个车系" placement="top"/);
  assert.match(page, /const catalogRows = useMemo\(\(\) => sortVehicleCatalogRows\(assets\?\.spu \?\? \[\]\), \[assets\]\)/);
  assert.match(page, /热门品牌优先/);
  assert.match(page, /Math\.min\(catalogPage, catalogLastPage\)/);
  // 大车系库先搜索再分页；改查询回到第1页，空结果给出明确反馈且不显示伪造的第1/1页
  assert.match(page, /filterVehicleSeriesGroups\(seriesGroups, catalogQuery\)/);
  assert.match(page, /total=\{filteredSeriesTotal\}/);
  assert.match(page, /aria-label="搜索品牌、车系、款型、别名或拼音"/);
  assert.match(page, /placeholder="搜索品牌、车系、款型、别名或拼音"/);
  assert.match(page, /spellCheck=\{false\}/);
  assert.match(page, /setCatalogQuery\(event\.target\.value\); setCatalogPage\(1\)/);
  assert.match(page, /未找到匹配“\$\{catalogQuery\.trim\(\)\}”的车型/);
  assert.match(page, /matchVehicleSeriesGroup\(group, catalogQuery\)/);
  assert.match(page, /匹配款型：\$\{catalogMatch\.matchedTrimLabel\}/);
  assert.match(styles, /\.spu-catalog-search:focus-within\s*\{[^}]*border-color:\s*var\(--teal\);/);
  assert.doesNotMatch(page, /function pageWindow/);
  // 车型库带"经人群推导"的核心场景列，回应"车型库看不到场景"的反馈
  assert.match(page, /<th>核心场景（经人群推导）<\/th>/);
  assert.match(page, /coreScenesByAudience/);
  // 页面读写走 v8 API，统计为 GET（只读副本可用），关联为 POST（副本被写保护拦截）
  assert.match(page, /\/api\/v8\/spu-audience\/assets/);
  assert.match(page, /\/api\/v8\/spu-audience\/stats\?\$\{search\.toString\(\)\}/);
  assert.match(page, /"\/api\/v8\/spu-audience\/associate", \{ method: "POST" \}/);
  assert.match(page, /\/api\/v8\/spu-audience\/spu/);
  // 规则校准区 + 口径脚注（脚注挂在车型库面板底部）
  assert.match(page, /覆盖缺口/);
  assert.match(page, /规则外溢/);
  assert.match(page, /className="spu-footnotes"/);
  // 车系聚合视图：品牌/车系拆列，车系名提升为主信息；款型点开才出现，残量行改叫「仅识别到车系」
  assert.match(page, /catalogPageGroups\.map/);
  assert.match(page, /expandedSeries\.has\(group\.slug\)/);
  assert.match(page, /seriesAggregate\(group\)/);
  assert.match(page, /className="spu-series-name">\{seriesNode\.series\}<\/strong>/);
  assert.match(page, /<td colSpan=\{11\}>/);
  assert.match(page, /仅识别到车系/);
  assert.doesNotMatch(page, /车系合计（含仅识别到车系的内容）/);
  assert.match(page, /formatVehicleMeta\(seriesNode\)/);
  assert.match(page, /powertrainLabels\[row\.powertrain\] \?\? row\.powertrain/);
  assert.match(page, /className="spu-audience-primary"/);
  assert.match(styles, /\.spu-audience-primary\s*\{[^}]*background:\s*#e8efff;[^}]*color:\s*#245fcf;[^}]*font-weight:\s*750;/);
  assert.doesNotMatch(page, /车系兜底（未细化）/);
  // 款型展开入口是弱化后的次级按钮，同时保留展开态、键盘焦点和车系级读屏说明
  assert.match(page, /className="spu-series-toggle"[\s\S]*aria-expanded=\{expanded\}[\s\S]*aria-label=\{`\$\{expanded \? "收起" : "展开"\}\$\{seriesNode\.series\}的 \$\{group\.trims\.length\} 个款型`\}/);
  assert.match(styles, /\.spu-series-toggle\s*\{[^}]*color:\s*#74838a;[^}]*font-size:\s*9px;[^}]*font-weight:\s*600;/);
  assert.match(styles, /\.spu-series-toggle:focus-visible\s*\{[^}]*outline:\s*2px solid var\(--teal\);[^}]*outline-offset:\s*2px;/);
  // 主表隐藏完整别名，编辑链路仍可回填普通/歧义别名并随保存请求提交
  assert.match(page, /歧义别名（需语境确认才计入）/);
  assert.match(page, /row\.aliases\.filter\(\(item\) => !item\.ambiguous\)/);
  assert.match(page, /row\.aliases\.filter\(\(item\) => item\.ambiguous\)/);
  assert.match(page, /audience_secondary: form\.audienceSecondary \|\| null,\s*aliases,/);
  // 内容列表新增 SPU/人群/场景 三列与筛选，且顺序在卖点列之前；证据等级从卖点列拆出为独立列
  assert.match(contents, /<th>内容方向<\/th><th>车型<\/th><th>人群<\/th><th>场景<\/th><th>卖点<\/th><th>证据等级<\/th><th>垂直度<\/th>/);
  // 界面不暴露独立的英文 "SPU" 文本节点（会被浏览器翻译插件误译成"空间物理单元"）
  assert.doesNotMatch(contents, /<th>SPU<\/th>/);
  assert.doesNotMatch(page, /<th>SPU<\/th>/);
  // 车型单元格两行制：第一行 品牌+型号（去重），第二行 款型/命中词/另提及车系
  assert.match(contents, /function spuDisplayName/);
  assert.match(contents, /spu\.series\.startsWith\(spu\.brand\) \? spu\.series : `\$\{spu\.brand\} \$\{spu\.series\}`/);
  assert.match(contents, /命中「\$\{alias\}」/);
  assert.match(contents, /另提及 \$\{item\.spu_secondary_count\} 车系/);
  assert.match(contents, /spu-tag-subline/);
  assert.match(types, /matched_aliases: string\[\];/);
  assert.match(types, /spu_secondary_count: number;/);
  assert.match(contents, /EVIDENCE_LEVEL_HINTS/);
  assert.match(contents, /className="evidence-level-tag" title=\{EVIDENCE_LEVEL_HINTS\[item\.evidence_level\]\}/);
  // 证据等级展示用「V码-中文短标签」（2026-08-15 Mark 定稿），未知等级回退裸 V 码
  assert.match(contents, /V3: "V3-信息完整",\s*V2: "V2-媒体存在",\s*V1: "V1-只有文字",\s*V0: "V0-不可用",/);
  assert.match(contents, /\{EVIDENCE_LEVEL_LABELS\[item\.evidence_level\] \?\? item\.evidence_level\}/);
  // 卖点列多行完整显示：不再用单行省略号，V 值也不再挤在卖点下方的 cell-subline 里
  assert.match(styles, /\.content-table \.selling-point-name \{ display: block; white-space: normal; overflow-wrap: anywhere; \}/);
  assert.doesNotMatch(styles, /\.content-table \.selling-point-name \{[^}]*text-overflow/);
  assert.doesNotMatch(contents, /<span className="cell-subline">\{item\.evidence_level/);
  assert.match(contents, /spu_series: filters\.spuSeries \|\| null, audience: filters\.audience \|\| null, scene: filters\.scene \|\| null/);
  assert.match(contents, /灰区/);
  // v16 起系统无人工复核：内容页不再有复核状态筛选、待复核/再次复核入口
  assert.doesNotMatch(contents, /复核/);
  assert.doesNotMatch(contents, /review_status|review_queue_id|pending_review_count/);
  assert.match(contents, /未细化/);
  assert.match(contents, /content-scene-cell/);
  assert.match(contents, /\/api\/v8\/spu-audience\/assets/);
  assert.match(types, /spu: ContentTagSpu \| null;/);
  assert.match(types, /audience: ContentTagAudience \| null;/);
  assert.match(types, /scenes: ContentTagScene\[\];/);
  assert.match(formatSource, /content_explicit: "内容显式", rule_prior: "规则推断"/);
  // 列宽口径：证据等级列拆出后内容表总宽 2126
  assert.match(styles, /\.content-table \{ min-width: 2126px !important; \}/);
  assert.match(styles, /\.main-area\[data-section="spu-audience"\]/);
  // 后端：v15 关联域（v14 + LLM 辅助）+ 端点 + 内容检索标签
  assert.match(storageSource, /SCHEMA_VERSION = 16/);
  assert.match(storageSource, /CURRENT_SCHEMA_MIGRATION_NAME = "remove-manual-review"/);
  assert.match(storageSource, /spu-audience-scene-domain/);
  assert.match(storageSource, /spu-llm-assist/);
  assert.match(storageSource, /CREATE TABLE IF NOT EXISTS spu_catalog/);
  assert.match(storageSource, /CREATE TABLE IF NOT EXISTS content_spu_links/);
  assert.match(storageSource, /CREATE TABLE IF NOT EXISTS llm_judgements/);
  assert.match(storageSource, /def _migrate_v13_to_v14/);
  assert.match(storageSource, /def _migrate_v14_to_v15/);
  assert.match(apiSource, /\/api\/v8\/spu-audience\/assets/);
  assert.match(apiSource, /\/api\/v8\/spu-audience\/stats/);
  assert.match(apiSource, /\/api\/v8\/spu-audience\/associate/);
  assert.match(apiSource, /spu_content_labels\(connection/);
  assert.match(apiSource, /spu_series: Optional\[str\]/);
  // 关联模块：规则链版本、款型细化、场景基础分修正、人群显式信号门槛
  assert.match(spuModule, /ASSOCIATION_RULE_VERSION = "spu-association-v2"/);
  // 执行通道（系统能力）：V2/V3 SQL 预过滤 + 批量预取 + 分批提交 + 后台任务 + CLI
  assert.match(spuModule, /def _eligible_v23_contents/);
  assert.match(spuModule, /evidence_level IN \('V2','V3'\)/);
  assert.match(spuModule, /def _artifact_paths_by_content/);
  assert.match(spuModule, /def start_association_run/);
  assert.match(spuModule, /def recover_orphan_association_runs/);
  assert.match(spuModule, /def dry_run_summary/);
  assert.match(spuModule, /SQL_ID_CHUNK = 800/);
  assert.match(apiSource, /start_association_run\(db_path=config\.db_path\)/);
  assert.match(apiSource, /_run_spu_association_job, config\.db_path, run_id, since, scope_window/);
  assert.match(apiSource, /recover_orphan_association_runs\(/);
  assert.doesNotMatch(page, /数据刷新已在后台启动|数据刷新完成|runParticipatedCount|runLlmFilledCount|const \[message, setMessage\]/);
  assert.match(page, /window\.setInterval/);
  assert.match(page, /证据完整（V2\/V3）/);
  // 车型库渠道占比的数据源：build_stats 返回分平台桶与发布门槛
  assert.match(spuModule, /"channels": channels,/);
  assert.match(spuModule, /"post_share": post_share,/);
  assert.match(spuModule, /"view_share": view_share,/);
  assert.match(spuModule, /views_published/);
  // 增量模式与单条补算是系统能力：页面打开/保存规则/内容更新数据都会自动触发
  assert.match(spuModule, /def resolve_incremental_since/);
  assert.match(spuModule, /def associate_single_content/);
  // LLM 双轨（B 链，无人工复核版）：规则链后补空，降级不阻塞
  assert.match(spuModule, /llm_hook/);
  assert.match(spuModule, /def default_llm_hook/);
  assert.match(apiSource, /llm_hook=default_spu_llm_hook\(\)/);
  assert.match(formatSource, /llm: "大模型补充"/);
  // 「刷新数据」弹窗四选一：先选范围再确认，范围按发布时间、与统计窗口同口径（_window_bounds 同源）
  for (const label of ["昨天", "本周", "上周", "全部内容"]) assert.match(page, new RegExp(`label: "${label}"`));
  assert.match(page, /选择刷新范围/);
  assert.match(page, /推荐/);
  assert.match(page, /耗时较长/);
  assert.match(page, /spu-refresh-modal/);
  assert.match(page, /spu-refresh-option/);
  assert.match(page, /selectedRefreshScope/);
  assert.match(page, /refreshRequestRef\.current/);
  assert.match(page, /onChange=\{\(\) => setSelectedRefreshScope\(item\.key\)\}/);
  assert.match(page, /refreshData\(selectedRefreshScope\)/);
  assert.doesNotMatch(page, /refreshData\(item\.key\)/);
  assert.match(page, /aria-labelledby="spu-refresh-title"/);
  assert.match(page, /event\.key === "Escape"/);
  assert.match(page, /开始刷新/);
  assert.match(page, /associate\?mode=\$\{scope\}/);
  assert.match(page, /setRefreshPicker\(true\)/);
  assert.match(styles, /\.spu-refresh-options/);
  assert.match(styles, /\.modal-panel\.spu-refresh-modal/);
  assert.match(styles, /grid-template-columns: repeat\(2, minmax\(0, 1fr\)\)/);
  assert.match(styles, /\.spu-refresh-option\[data-selected="true"\]/);
  assert.match(styles, /@media \(max-width: 600px\)/);
  assert.match(spuModule, /scope_window/);
  assert.match(spuModule, /window=scope_window/);
  assert.match(apiSource, /"full", "incremental", "yesterday", "this_week", "last_week"/);
  assert.match(apiSource, /mode: str = "full"/);
  assert.match(apiSource, /resolve_incremental_since\(connection\)/);
  assert.match(apiSource, /associate_single_content\(content_id, db_path=db_path\)/);
  const cli = await readFile(new URL("../../../scripts/run_spu_association.py", import.meta.url), "utf8");
  assert.match(cli, /--apply/);
  assert.match(cli, /只处理 V2\/V3/);
  assert.match(cli, /dry_run_summary/);
  assert.match(cli, /"--window", choices=\("yesterday", "this_week", "last_week"\)/);
  assert.match(spuModule, /SCENE_BASE_SCORE = 36/);
  assert.match(spuModule, /EXPLICIT_SIGNAL_MIN_HITS = 2/);
  assert.match(spuModule, /def resolve_trim/);
  assert.match(spuModule, /车系兜底节点/);
});

test("routes preserve operations and expose read-only evidence workbench", async () => {
  const [shell, accounts, pagination, contents, evidence, tasks, taskDetail, sellingPoints, apiSource, formatSource, layout, packageJson] = await Promise.all([
    readFile(new URL("../app/components/AppShell.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/accounts/AccountsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/components/Pagination.tsx", import.meta.url), "utf8"),
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
  for (const href of ["/overview", "/tasks", "/accounts", "/contents", "/selling-points", "/spu-audience"]) assert.match(shell, new RegExp(`href: "${href}"`));
  assert.match(accounts, /pending_platform_identities/);
  assert.match(accounts, /className="pending-identity-table"/);
  assert.match(accounts, /<caption className="visually-hidden">待归属平台身份列表<\/caption>/);
  assert.match(accounts, /<th>平台<\/th><th>平台 UID<\/th><th>昵称<\/th><th>关联内容<\/th>/);
  assert.doesNotMatch(accounts, /pending-identity-grid/);
  assert.match(accounts, /一个手机号对应一行账号主数据/);
  assert.match(accounts, /className="account-master-table"/);
  assert.match(accounts, /account-master-panel">\s*\n\s*<Pagination /);
  assert.match(accounts, /<Pagination page=\{page\} pageSize=\{pageSize\} total=\{total\} busy=\{fetching \|\| saving\} ariaLabel="账号分页" unitLabel="个账号" placement="top"/);
  assert.match(accounts, /page_size: size/);
  assert.match(accounts, /reload\(\{ page: 1 \}\)/);
  assert.match(pagination, /aria-label=\{ariaLabel\}/);
  assert.match(pagination, /首页.*上一页/s);
  assert.match(pagination, /下一页.*末页/s);
  assert.match(pagination, /pagination-\$\{placement\}/);
  assert.match(contents, /<Pagination page=\{page\} pageSize=\{pageSize\} total=\{total\}/);
  assert.doesNotMatch(contents, /function pageWindow/);
  assert.match(accounts, /以手机号为锚点的账号主数据列表/);
  assert.match(accounts, /className="account-base-group" colSpan=\{4\} scope="colgroup">账号基础信息/);
  assert.match(accounts, /platformKeys\.map\(\(key\) => <th className="account-platform-group"/);
  assert.match(accounts, /className="account-management-group" colSpan=\{2\} scope="colgroup">账号管理/);
  assert.match(accounts, /<PlatformHeaderMark platformKey=\{key\} \/>/);
  assert.match(accounts, /data-platform-columns=\{key\}/);
  assert.match(formatSource, /platformKeys = \["douyin", "xiaohongshu", "wechat_channels", "kuaishou"\]/);
  assert.match(accounts, /<th className="account-platform-start" scope="col">UID<\/th><th scope="col">是否实名<\/th><th scope="col">昵称<\/th><th className="account-number-cell" scope="col">粉丝量<\/th><th className="account-number-cell" scope="col">关联内容量<\/th>/);
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
  assert.match(contents, /旧规则评估/);
  assert.match(taskDetail, /display_effective_revision/);
  assert.match(taskDetail, /历史规则报告，已过时/);
  assert.match(taskDetail, /revision_state === "current"/);
  assert.match(taskDetail, /revision_state === "stale"/);
  assert.doesNotMatch(taskDetail, /revisions\.find\(\(item\) => !item\.invalidated_at\)/);
  assert.match(tasks, /current_valid_revision/);
  assert.match(tasks, /stale_display_revision/);
  assert.match(tasks, /historical_revision_count/);
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
  // v16 起证据弹窗是纯只读面板：无复核表单、无 reviews 接口调用
  assert.doesNotMatch(evidence, /复核/);
  assert.doesNotMatch(evidence, /\/api\/v8\/reviews/);
  assert.match(evidence, /证据或规则变化后系统会自动重新评估并追加新版本/);
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

test("task list shows background generation progress on cards instead of jumping away", async () => {
  const [tasks, taskDetail, styles, apiSource, reportsSource] = await Promise.all([
    readFile(new URL("../app/tasks/TasksPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/tasks/[id]/TaskDetailPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/api.py", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/reports.py", import.meta.url), "utf8"),
  ]);
  // 点“生成报告”后留在列表：任务以卡片形式出现，右侧是查看详情，卡片自己显示进度
  assert.doesNotMatch(tasks, /useRouter|router\.push/);
  assert.match(tasks, /className="task-card-list"/);
  assert.match(tasks, /查看详情/);
  assert.match(tasks, /role="progressbar"/);
  assert.match(tasks, /GENERATING_STATUSES = new Set\(\["queued", "running", "cancel_requested"\]\)/);
  assert.match(tasks, /setInterval\(/);
  assert.doesNotMatch(tasks, /<table>|<thead>/);
  assert.match(taskDetail, /task-progress-panel/);
  assert.match(taskDetail, /setInterval\(/);
  assert.match(styles, /\.task-card \{/);
  assert.match(styles, /\.task-card \.progress-track i \{ background: var\(--teal\); transition: width \.4s ease; \}/);
  assert.doesNotMatch(styles, /\.main-area\[data-section="tasks"\] \.table-panel/);
  // 生成跑在请求之外，创建接口只返回排队中的任务
  assert.match(apiSource, /background\.add_task\(/);
  assert.match(apiSource, /_run_task_in_background/);
  assert.doesNotMatch(apiSource, /create_and_run_task/);
  assert.match(reportsSource, /def advance_task_progress\(/);
  for (const stage of [35, 65, 85]) assert.match(reportsSource, new RegExp(`progress=${stage},`));
  // 任务消息与事件全程中文：后端新事件直接写中文，前端对历史存量做展示层翻译
  assert.match(reportsSource, /第 \{revision\} 版报告已生成/);
  assert.doesNotMatch(reportsSource, /已请求生成新 revision|等待生成新 revision|revision \{revision\} 已生成/);
  assert.match(tasks, /humanizeTaskMessage/);
  assert.match(taskDetail, /humanizeTaskMessage/);
});

test("task detail renders channel conclusions in the platforms tab without fabricating rates", async () => {
  const [taskDetail, types, formatSource] = await Promise.all([
    readFile(new URL("../app/tasks/[id]/TaskDetailPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/types.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/format.ts", import.meta.url), "utf8"),
  ]);
  assert.match(types, /channels\?: Record<OverviewChannelKey, OverviewChannel> \| null;/);
  assert.match(types, /export type MetricStatus =[\s\S]*"available"[\s\S]*"below_threshold"[\s\S]*"sample_only"[\s\S]*"stale";/);
  assert.match(types, /status: MetricStatus;/);
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
  assert.match(taskDetail, /metricCompactValue\(metric\)/);
  assert.match(taskDetail, /!metricPublishesValue\(metric\) && <span className="visually-hidden">。完整原因：\{metricEvidence\(metric\)\}<\/span>/);
  assert.doesNotMatch(taskDetail, /unavailableReasonLabels|unavailableConclusionLabels|function conclusionCell/);
  assert.doesNotMatch(`${taskDetail}\n${formatSource}`, /有效样本不足|"覆盖不足"/);
  assert.match(formatSource, /身份数据待补齐/);
  assert.match(formatSource, /用户分类未完成/);
  assert.match(formatSource, /互动用户少于30人/);
  assert.match(formatSource, /metric\.status === "sample_only" \? `\$\{value\}（仅样本）` : value/);
  assert.match(taskDetail, /用户级去重口径/);
  assert.match(taskDetail, /channel_conclusions\.csv/);
  assert.match(taskDetail, /没有渠道\/场景结论/);
  assert.match(taskDetail, /平台发布分布/);
  assert.match(taskDetail, /platform_dimensions/);
  assert.doesNotMatch(taskDetail, /audience_verticality|互动用户垂直度/);
});

test("task detail renders v8.5 and v8.6 quality details without leaking raw values", async () => {
  const source = await readFile(
    new URL("../app/tasks/[id]/TaskDetailPage.tsx", import.meta.url),
    "utf8",
  );
  const types = await readFile(new URL("../app/lib/types.ts", import.meta.url), "utf8");
  const discoveryFormatter = source.slice(
    source.indexOf("function discoveryCoverageRow"),
    source.indexOf("function metricsFreshnessRow"),
  );

  assert.match(types, /data_quality_details\?:[\s\S]*metrics_freshness\?: MetricsFreshnessDetail \| null/);
  assert.match(types, /discovery_coverage\?: DiscoveryCoverageDetail \| null/);
  assert.match(types, /collection_cutoff_at\?: string \| null/);
  assert.match(source, /report\.data_quality_details\?\.discovery_coverage/);
  assert.match(source, /report\.data_quality\.discovery_coverage/);
  assert.match(source, /`计划内的账号采集执行 \$\{eligible\} 次，实际覆盖 \$\{covered\} 次`/);
  assert.match(source, /detail\.status === "not_applicable" \? "无适用内容"/);
  assert.match(source, /typeof detail\.reason === "string" && detail\.reason\.trim\(\)/);
  assert.match(source, /report\.data_quality_details\?\.metrics_freshness/);
  assert.match(source, /report\.data_quality\.metrics_freshness/);
  assert.match(source, /`\$\{freshCount\}\/\$\{eligibleCount\} 条内容在截止前有新数据`/);
  assert.match(source, /"无适用内容"/);
  assert.match(source, /`采集截止 \$\{formatDateTime\(cutoff\)\}`/);
  assert.match(source, /value \? "已通过" : "未通过"/);
  assert.doesNotMatch(source, /<dd>\{value\}<\/dd>/);
  assert.doesNotMatch(source, /String\(value\)|`\$\{value\}%`/);
  assert.doesNotMatch(discoveryFormatter, /\b90\b/);

  // 质量检查在界面上只出现中文名与白话说明，接口字段名不落进 DOM（防止 revision 式英文再回来）
  assert.match(source, /qualityCheckCopy/);
  for (const name of ["发现覆盖", "详情覆盖", "指标新鲜度", "正式评估覆盖", "核心产物覆盖", "媒体处理终态覆盖", "重复指纹覆盖", "重复识别定标", "周评论证据覆盖"]) {
    assert.match(source, new RegExp(name));
  }
  assert.doesNotMatch(source, /<dt>\{key\}<\/dt>|<dt>discovery_coverage<\/dt>|definition-list/);
  // 周评论检查只对周报有实际意义，其他任务不显示这行恒为 100% 的占位
  assert.match(source, /key !== "weekly_comment_coverage" \|\| detail\?\.task_type === "weekly"/);
  // 文件与日志：产物名称、大小、事件类型全部转中文，raw 值只留在 title/接口里
  assert.match(source, /fileKindLabels/);
  assert.match(source, /渠道结论表 CSV/);
  assert.match(source, /formatBytes\(file\.byte_size\)/);
  assert.doesNotMatch(source, /\{file\.file_kind\} · /);
  assert.match(source, /taskEventLabels/);
  assert.match(source, /请求重新生成/);
  assert.doesNotMatch(source, /<strong>\{event\.event_type\}<\/strong>/);
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
