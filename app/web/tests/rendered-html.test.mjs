import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import test from "node:test";
import {
  buildContentPatch,
  buildContentSaveOperation,
  fromShanghaiDateTimeLocal,
  toShanghaiDateTimeLocal,
} from "../app/contents/contentForm.ts";
import { apiErrorMessage, jsonRequest, readDownload, saveDownload } from "../app/lib/api.ts";
import {
  metricCompactValue,
  metricEvidence,
  metricPublishesValue,
  metricStatus,
  metricUnavailableLabel,
  metricValue,
  humanizeTaskMessage,
  humanizeTaskStatus,
  plainMetricReason,
  taskWasSuperseded,
} from "../app/lib/format.ts";

async function readWorkbenchShell() {
  return (await Promise.all(["AppShell", "WorkbenchChrome"].map((name) =>
    readFile(new URL(`../app/components/${name}.tsx`, import.meta.url), "utf8"),
  ))).join("\n");
}

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
    ["用户身份覆盖率 37.48%，低于 95% 门槛", "部分用户身份信息不足"],
    ["用户分类覆盖率 0.0%，低于 100% 门槛", "用户分类未完成"],
    ["去重有效用户 6 人，低于 30 人门槛", "互动用户少于 30 人"],
    ["分类器定标未通过，暂不发布比例", "用户分类结果还没完成校验"],
    ["可归类有效曝光 10/1000，覆盖 1%：低于 90% 发布门槛", "部分曝光还没完成分类"],
    ["用户级汽车兴趣占比尚未接入用户聚合，暂不发布", "暂时无法按用户汇总"],
    ["", "暂不显示"],
  ];
  for (const [reason, expected] of belowCases) {
    const metric = ratioMetric("below_threshold", reason);
    assert.equal(metricPublishesValue(metric), false);
    assert.equal(metricUnavailableLabel(metric), expected);
    assert.equal(metricValue(metric), expected);
    assert.equal(metricCompactValue(metric), expected);
    assert.equal(metricStatus(metric), "暂不显示");
    assert.doesNotMatch(`${metricValue(metric)} ${metricStatus(metric)}`, /覆盖不足/);
  }

  const uncalibrated = ratioMetric(
    "not_calculable",
    "重复内容感知指纹尚未完成定标，重复率暂不可计算",
    null,
  );
  assert.equal(metricPublishesValue(uncalibrated), false);
  assert.equal(metricUnavailableLabel(uncalibrated), "重复内容识别规则还没完成校验");
  assert.equal(metricValue(uncalibrated), "重复内容识别规则还没完成校验");

  const available = ratioMetric("available", "", 62.05);
  assert.equal(metricPublishesValue(available), true);
  assert.equal(metricValue(available), "62.05%");
  assert.equal(metricCompactValue(available), "62.05%");
  assert.equal(metricEvidence(available), "62/100");

  const sample = ratioMetric("sample_only", "分类器未经金标核对，数值仅供参考", 62.05);
  assert.equal(metricPublishesValue(sample), true);
  assert.equal(metricCompactValue(sample), "62.05%（仅供参考）");

  const missingWithValue = { kind: "quantity", value: 1234, unit: "view", status: "missing", coverage_percentage: 0, reason: "" };
  assert.equal(metricPublishesValue(missingWithValue), false);
  assert.equal(metricValue(missingWithValue), "暂无数据");

  const availableQuantity = { ...missingWithValue, status: "available" };
  assert.equal(metricValue(availableQuantity), "1,234");
});

test("user-facing errors and historical task messages stay in plain Chinese", () => {
  assert.equal(apiErrorMessage([{ loc: ["body", "name"], msg: "invalid" }], 422), "提交的信息有误，请检查后重试。");
  assert.equal(apiErrorMessage("UNIQUE constraint failed", 409), "数据已经发生变化，请刷新页面后重试。");
  assert.equal(apiErrorMessage("internal traceback", 500), "服务暂时不可用，请稍后重试。");
  assert.equal(apiErrorMessage("当前处于只读保护模式：可以查看数据，但暂时不能修改。", 403), "当前处于只读保护模式：可以查看数据，但暂时不能修改。");
  assert.equal(plainMetricReason("曝光量快照覆盖率为 50.00%，低于 90% 发布阈值"), "有曝光量的数据占 50.00%，低于至少 90% 的要求");
  assert.equal(plainMetricReason("报告窗口发现覆盖率为 80.00%，低于 90% 发布门槛"), "计划采集完成率为 80.00%，低于至少 90% 的要求");
  assert.equal(humanizeTaskMessage("revision 1 invalidated from freeze manifest"), "第 1 版报告已作废。");
  assert.equal(humanizeTaskMessage("已由发现补跑后的新任务替代；revision 1 仅供审计"), "这份旧任务已被更新后的报告替代；第 1 版仅用于保留记录。");
  assert.equal(taskWasSuperseded("已由发现补跑后的新任务替代；revision 1 仅供审计"), true);
  assert.equal(humanizeTaskStatus("failed", "已由发现补跑后的新任务替代；revision 1 仅供审计"), "已被替代");
});

test("report downloads preserve task-linked UTF-8 names and the original file bytes", async (t) => {
  const task = "D8-D-20260826-20260826";
  for (const [format, contentType, extension, label, bytes] of [
    ["image", "image/png", "png", "图片报告", new Uint8Array([137, 80, 78, 71])],
    ["image", "image/svg+xml; charset=utf-8", "svg", "图片报告", "<svg/>"],
    ["xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "xlsx", "数据明细", new Uint8Array([80, 75, 3, 4])],
  ]) {
    const path = `/api/v8/tasks/${task}/revisions/2/download?format=${format}`;
    const filename = `2026-08-26 日报_${task}_v2_${label}.${extension}`;
    const payload = new Blob([bytes]);
    t.mock.method(globalThis, "fetch", async (url) => {
      assert.equal(url, path);
      return new Response(payload, { headers: {
        "Content-Type": contentType,
        "Content-Disposition": `attachment; filename="report.${extension}"; filename*=UTF-8''${encodeURIComponent(filename)}`,
      } });
    });
    const file = await readDownload(path, {
      contentTypes: [contentType.split(";")[0]],
      fallbackFilename: `${task}_v2.${extension}`,
    });
    assert.equal(file.filename, filename);
    assert.deepEqual(await file.blob.arrayBuffer(), await payload.arrayBuffer());
  }
});

test("download errors, old ZIP responses and empty files never become local reports", async (t) => {
  const options = { contentTypes: ["image/png"], fallbackFilename: "D8-TEST_v1_图片报告.png" };
  for (const [response, message] of [
    [Response.json({ detail: "找不到这版报告。" }, { status: 404 }), /找不到这版报告/],
    [Response.json({ detail: "expired" }, { status: 401 }), /登录已过期/],
    [new Response("old archive", { headers: { "Content-Type": "application/zip" } }), /文件格式不正确/],
    [new Response("<html>login</html>", { headers: { "Content-Type": "text/html" } }), /文件格式不正确/],
    [new Response("", { headers: { "Content-Type": "image/png" } }), /文件为空/],
  ]) {
    t.mock.method(globalThis, "fetch", async () => response);
    await assert.rejects(readDownload("/report", options), message);
  }
  t.mock.method(globalThis, "fetch", async () => { throw new TypeError("network"); });
  await assert.rejects(readDownload("/report", options), /无法连接数据服务/);
});

test("download names fall back safely if an attachment header is absent or malformed", async (t) => {
  const options = { contentTypes: ["image/png"], fallbackFilename: "D8-TEST_v2_图片报告.png" };
  for (const [disposition, expected] of [
    [null, options.fallbackFilename],
    ["attachment; filename*=UTF-8''%broken", options.fallbackFilename],
    ['attachment; filename="D8-TEST_v2.png"; filename*=UTF-8\'\'%broken', "D8-TEST_v2.png"],
    ["attachment; filename*=UTF-8''..%2Freport%5Cname%00.png", ".._report_name_.png"],
  ]) {
    t.mock.method(globalThis, "fetch", async () => new Response("png", { headers: {
      "Content-Type": "image/png",
      ...(disposition ? { "Content-Disposition": disposition } : {}),
    } }));
    assert.equal((await readDownload("/report", options)).filename, expected);
  }
  t.mock.method(globalThis, "fetch", async () => new Response("<svg/>", { headers: { "Content-Type": "image/svg+xml" } }));
  const svg = await readDownload("/report", {
    contentTypes: ["image/png", "image/svg+xml"],
    fallbackFilename: (contentType) => `D8-TEST_v2_图片报告.${contentType === "image/svg+xml" ? "svg" : "png"}`,
  });
  assert.equal(svg.filename, "D8-TEST_v2_图片报告.svg");
  assert.equal(svg.blob.type, "image/svg+xml");
});

test("saving two reports starts two named downloads and releases blobs after the browser consumes them", (t) => {
  const originalDocument = Object.getOwnPropertyDescriptor(globalThis, "document");
  const clicks = [];
  const removed = [];
  const revoked = [];
  const callbacks = [];
  const attached = new Set();
  let sequence = 0;
  t.mock.method(URL, "createObjectURL", () => `blob:report-${++sequence}`);
  t.mock.method(URL, "revokeObjectURL", (url) => revoked.push(url));
  t.mock.method(globalThis, "setTimeout", (callback, delay) => {
    assert.equal(delay, 60_000);
    callbacks.push(callback);
  });
  Object.defineProperty(globalThis, "document", { configurable: true, value: {
    createElement(tag) {
      assert.equal(tag, "a");
      return {
        click() { assert.ok(attached.has(this)); clicks.push([this.href, this.download]); },
        remove() { attached.delete(this); removed.push(this.href); },
      };
    },
    body: { appendChild(anchor) { attached.add(anchor); } },
  } });
  try {
    for (const filename of ["日报_D8-TEST_v1_图片报告.png", "日报_D8-TEST_v1_数据明细.xlsx"]) {
      saveDownload({ blob: new Blob([filename]), filename });
    }
    assert.deepEqual(clicks, [
      ["blob:report-1", "日报_D8-TEST_v1_图片报告.png"],
      ["blob:report-2", "日报_D8-TEST_v1_数据明细.xlsx"],
    ]);
    assert.equal(attached.size, 0);
    assert.deepEqual(removed, ["blob:report-1", "blob:report-2"]);
    assert.deepEqual(revoked, []);
    callbacks.forEach((callback) => callback());
    assert.deepEqual(revoked, removed);
  } finally {
    if (originalDocument) Object.defineProperty(globalThis, "document", originalDocument);
    else delete globalThis.document;
  }
});

test("task detail has one download button that always downloads both displayed-revision files", async () => {
  const source = await readFile(new URL("../app/tasks/[id]/TaskDetailPage.tsx", import.meta.url), "utf8");
  assert.match(source, /encodeURIComponent\(detail\.id\)\}\/revisions\/\$\{displayRevision\}\/download/);
  assert.match(source, /await Promise\.all\([\s\S]*readDownload\(`\$\{path\}\?format=\$\{kind\}`[\s\S]*files\.forEach\(saveDownload\)/);
  assert.match(source, /async function downloadReport\(\)/);
  assert.match(source, /const formats = \["image", "xlsx"\] as const/);
  assert.equal((source.match(/className="primary report-download-button"/g) ?? []).length, 1);
  assert.doesNotMatch(source, /report-download-options|report-download-menu|report-download-controls|单独下载|<details|<summary/);
  assert.match(source, /disabled=\{downloading\}/);
  assert.match(source, /允许浏览器下载多个文件/);
  assert.doesNotMatch(source, /<a[^>]+report-download-button/);
});

test("task detail places a direct parent-page link beside the task title", async () => {
  const source = await readFile(new URL("../app/tasks/[id]/TaskDetailPage.tsx", import.meta.url), "utf8");
  const heading = source.match(/<div className="task-detail-heading">([\s\S]*?)<div className="task-detail-heading-copy">/)?.[1];
  assert.ok(heading, "the back control belongs to the visible task heading");
  assert.match(heading, /<Link href="\/tasks" className="task-back-button" aria-label="返回任务列表" title="返回任务列表">/);
  assert.doesNotMatch(source, /<AppShell active="tasks" actions=|window\.history\.back|router\.back/);
});

async function render(path = "/overview") {
  const workerUrl = process.env.DCAR_TEST_WEB_DIST
    ? pathToFileURL(`${process.env.DCAR_TEST_WEB_DIST}/server/index.js`)
    : new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}-${path}`);
  const { default: worker } = await import(workerUrl.href);
  return worker.fetch(
    new Request(`http://localhost${process.env.DCAR_TEST_WEB_BASE_PATH ?? ""}${path}`, { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
}

test("server-renders every real v8 product route", async () => {
  const expectations = [
    ["/overview", "数据概览"], ["/tasks", "数据报告任务"],
    ["/tasks/D8-TEST", "数据报告任务"], ["/accounts", "运营账号"],
    ["/accounts/douyin-authorization", "运营账号"],
    ["/contents", "内容数据"], ["/selling-points", "卖点标准"],
    ["/spu-audience", "SPU人群（未生效）"],
    ["/users", "用户权限"],
  ];
  for (const [path, title] of expectations) {
    let response;
    await assert.doesNotReject(async () => {
      response = await render(path);
    }, `${path} SSR render must resolve`);
    assert.equal(response.status, 200, `${path} should render`);
    assert.match(response.headers.get("content-type") ?? "", /^text\/html\b/i);
    const html = await response.text();
    assert.match(html, /<html lang="zh-CN">/i);
    assert.match(html, new RegExp(title));
    const base = process.env.DCAR_TEST_WEB_BASE_PATH ?? "";
    for (const route of ["overview", "tasks", "contents", "selling-points", "spu-audience"]) {
      assert.ok(html.includes(`href="${base}/${route}"`), `${route} link includes its deployment base path`);
    }
    assert.ok(!html.includes(`href="${base}/accounts"`), "SSR does not grant an account role before reading session");
    assert.match(html, /class="loading-screen"/);
    assert.doesNotMatch(html, /codex-preview|Your site is taking shape|Starter Project/i);
  }
});

test("douyin authorization management locks every scan to the selected business account", async () => {
  const [source, queries, api] = await Promise.all([
    readFile(new URL("../app/accounts/douyin-authorization/DouyinAuthorizationPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/queries.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/api.ts", import.meta.url), "utf8"),
  ]);

  assert.match(source, /<AppShell active="accounts" header=\{/);
  assert.match(source, /href="\/accounts">返回账号页<\/Link>/);
  assert.match(source, /const sessionQuery = useQuery\(sessionQueryOptions\(\)\)/);
  assert.match(source, /sessionQuery\.data\?\.username === "temporary-bypass"/);
  assert.match(source, /enabled: canUseControl/);
  assert.match(source, /PRODUCTION_AUTHORIZATION_URL = "https:\/\/origin\.tj\.cn\/dcar\/accounts\/douyin-authorization"/);
  assert.match(source, /isBypassMode \? <article[\s\S]*请在正式 HTTPS 工作台完成扫码[\s\S]*href=\{productionUrl\}/);
  assert.match(source, /authorizationDataReady && !targetWasRequested && <>[\s\S]*aria-label="抖音授权统计"/);
  assert.match(source, /const authorizationReadPending = !queryError && \(sessionQuery\.isPending \|\| \(canUseControl && !authorizationDataReady\)/);
  assert.match(source, /const rawAccountId = params\.get\("account_id"\) \?\? ""/);
  assert.match(source, /const platformUid = params\.get\("platform_uid"\) \?\? ""/);
  assert.match(source, /const targetIsValid = Number\.isSafeInteger\(accountId\) && accountId > 0 && \/\^\\d\{6,24\}\$\/\.test\(platformUid\)/);
  assert.match(source, /buildAccountSearchRequest\(\{ query: platformUid, accountType: "", direction: "", platform: "douyin" \}, 1, 100\)/);
  assert.match(source, /accountSearchQueryOptions\(targetRequest\), enabled: canUseControl && targetIsValid/);
  assert.match(source, /targetAccountQuery\.data\?\.items\.find\(\(item\) => item\.id === accountId\)/);
  assert.match(source, /identity\.platform === "douyin" && identity\.uid === uid/);
  assert.match(queries, /readAccountSearch[\s\S]*readQueryJson<AccountSearchResult>\("\/api\/v8\/accounts\/search"/);
  assert.match(source, /targetWasRequested && !targetIsValid[\s\S]*授权目标参数无效/);
  assert.match(source, /targetIsValid && targetAccountQuery\.isSuccess && !targetMatches[\s\S]*目标账号不存在或抖音账号编号已变化/);
  assert.match(source, /targetMatches && !targetAccount\?\.enabled[\s\S]*已有授权仍可解绑/);
  assert.match(source, /authorizationDataReady && targetIsValid && targetMatches && <article className="panel douyin-authorization-target">/);
  assert.match(source, /disabled=\{Boolean\(busyAction\) \|\| !actionsAvailable \|\| !targetCanAuthorize\}/);
  assert.match(source, /markedJsonRequest\(\{ account_id: accountId, platform_uid: platformUid \}, "douyin-oauth-start"\)/);
  assert.match(source, /window\.location\.assign\(result\.authorize_url\)/);
  assert.match(source, /authorizationDataReady && !targetWasRequested && <>[\s\S]*本页不提供统一扫码入口[\s\S]*返回账号列表/);
  assert.doesNotMatch(source, /markedJsonRequest\(\{\}, "douyin-oauth-start"\)|选择业务账号[^\n]*开始扫码授权/);
  assert.doesNotMatch(source, /\/api\/douyin\/accounts\/search|douyin-accounts-search/);
  assert.doesNotMatch(source, /\/api\/douyin\/authorizations\/match|douyin-authorization-match|人工匹配业务账号/);
  assert.match(source, /"\/api\/douyin\/authorizations\/reauthorize"[\s\S]*"douyin-authorization-reauthorize"/);
  assert.match(source, /"\/api\/douyin\/authorizations\/unbind"[\s\S]*"douyin-authorization-unbind"/);
  assert.match(source, /activeTargetAuthorization\.needs_reauthorization \? "需重新授权" : "已授权"/);
  assert.match(source, /statusesQuery\.data\?\.unavailable \? "抖音授权服务在当前环境未启用。"/);
  assert.match(source, /onClick=\{\(\) => void reauthorize\(activeTargetAuthorization\)\}[\s\S]*重新扫码授权/);
  assert.match(source, /onClick=\{\(\) => void unbind\(activeTargetAuthorization\)\}>解绑/);
  assert.match(source, /item\.status === "pending_match" && <button[\s\S]*onClick=\{\(\) => void unbind\(item\)\}>作废<\/button>/);
  assert.match(source, /item\.status === "pending_match" \? "历史待处理授权已作废。" : "抖音开放平台授权已解绑。"/);
  assert.match(source, /旧版遗留的待匹配记录只允许作废，不再人工匹配/);
  assert.match(source, /new BroadcastChannel\(CALLBACK_CHANNEL\)/);
  assert.match(source, /window\.opener\.postMessage\(CALLBACK_MESSAGE, window\.location\.origin\)/);
  assert.doesNotMatch(source, /postMessage\([^\n]+,\s*["']\*["']/);
  assert.match(source, /const noticeCopy: Record/);
  assert.match(source, /"oauth-target-unavailable"[\s\S]*当前业务账号已停用、抖音账号编号已变化或账号目录暂时不可用/);
  assert.match(source, /"oauth-conflict"[\s\S]*扫码账号与当前业务账号原有授权不一致/);
  assert.match(source, /const callbackNotice = noticeCopy\[notice\] \?\? null/);
  assert.match(source, /const queryError = sessionQuery\.isError[\s\S]*authorizationsQuery\.isError[\s\S]*statusesQuery\.isError/);
  assert.match(source, /\{queryError && <Notice tone="error">\{queryError\}<\/Notice>\}/);
  assert.doesNotMatch(source, /authorizationsQuery\.isError && <Notice|statusesQuery\.isError && <Notice/);
  assert.doesNotMatch(source, /avatar|<img|<Image/);
  assert.match(queries, /douyinAuthorizationsQueryOptions[\s\S]*readDouyinAuthorizations/);
  assert.match(queries, /douyinAuthorizationStatusesQueryOptions[\s\S]*queryFn: readDouyinAuthorizationStatuses[\s\S]*staleTime: 0[\s\S]*refetchOnMount: "always"[\s\S]*refetchOnWindowFocus: "always"/);
  assert.match(queries, /reason instanceof ApiRequestError && reason\.code !== "approval_required" && \(reason\.status === 403 \|\| reason\.status === 404\)[\s\S]*\{ items: \[\], unavailable: true \}/);
  assert.match(api, /"X-Dcar-Request": marker/);
});

test("root redirects to overview instead of keeping hidden client-side view state", async () => {
  const response = await render("/");
  assert.ok(response.status >= 300 && response.status < 400, `unexpected status ${response.status}`);
  assert.match(response.headers.get("location") ?? "", /\/overview$/);
});

test("sidebar uses the bundled Dongchedi app mark and brand colors", async () => {
  const [shell, styles, icon] = await Promise.all([
    readWorkbenchShell(),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../public/dongchedi-app-icon.svg", import.meta.url), "utf8"),
  ]);
  assert.match(shell, /src=\{publicAssetPath\("\/dongchedi-app-icon\.svg"\)\}/);
  assert.match(shell, /alt="懂车帝 App"/);
  assert.match(shell, /<strong>Dcar AIGC<\/strong>/);
  assert.match(shell, /开心瓦瓦·运营工作台/);
  assert.doesNotMatch(shell, /Dcar Sentinel|内容运营工作台 · V1\.0|开心瓦瓦·内容运营工作台/);
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
    readWorkbenchShell(),
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
    readWorkbenchShell(),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);
  assert.doesNotMatch(shell, /mark:\s*"[概任账内卖]"/);
  assert.match(shell, /className="nav-icon"/);
  assert.match(shell, /stroke="currentColor"/);
  assert.match(shell, /aria-hidden="true"/);
  assert.match(shell, /focusable="false"/);
  assert.match(shell, /data-nav-icon=\{section\}/);
  for (const section of ["overview", "tasks", "accounts", "contents", "selling-points", "spu-audience", "users"]) {
    const key = section.includes("-") ? `"${section}"` : section;
    assert.match(shell, new RegExp(`${key}:\\s*<>`));
  }
  assert.match(styles, /\.sidebar nav a \.nav-icon\s*\{[^}]*width:\s*20px;[^}]*height:\s*20px;/);
});

// Overview content and availability are exercised by overview-report.test.mjs;
// request/retry/window behavior is covered by overview-recovery.test.mjs.

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

test("selling point standards retain E/X/M scenes and scene-local hits", async () => {
  const [source, types, shell, styles] = await Promise.all([
    readFile(new URL("../app/selling-points/SellingPointsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/types.ts", import.meta.url), "utf8"),
    readWorkbenchShell(),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);
  for (const [code, title, scene] of [["E", "二手车", "used_car"], ["X", "新车", "new_car"], ["M", "媒体", "media"]]) {
    assert.match(source, new RegExp(`code: "${code}", title: "${title}"[^\n]*scene: "${scene}"`));
  }
  assert.match(source, /type StandardFamilyCode = "E" \| "X" \| "M";/);
  assert.doesNotMatch(source, /code: "C"|"OTHER"|otherFamily|displayFamilies|nativeFamilyCodes/);
  assert.match(source, /\.filter\(\(point\) => point\.scenes\.includes\(scene\)\)/);
  assert.match(source, /pointsForFamily\(data\.items, family\.scene\)/);
  assert.match(source, /useQuery\(activeSellingPointsQueryOptions\(\)\)/);
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
  assert.match(source, /<AppShell active="selling-points">/);
  assert.match(source, /role="region" aria-label=\{`\$\{family\.code\} \$\{family\.title\}卖点标准表格`\} tabIndex=\{0\}/);
  assert.match(shell, /围绕 E、X、M 三个业务场景/);
  assert.doesNotMatch(shell, /E、X、M、C|四类标准系列/);
  assert.doesNotMatch(styles, /selling-points-hero-bg\.png/);
  assert.match(styles, /\.selling-point-summary-grid\s*\{[^}]*grid-template-columns:\s*repeat\(3,/);
  assert.doesNotMatch(styles, /data-family="C"|data-family="OTHER"/);
  assert.match(styles, /\.selling-point-table\s*\{[^}]*min-width:\s*1174px;/);
  assert.match(styles, /@media \(max-width:\s*480px\)/);
  const response = await render("/selling-points");
  const html = await response.text();
  assert.match(html, /围绕 E、X、M 三个业务场景/);
  assert.doesNotMatch(html, /E、X、M、C|生态场景|其他标准/);
});

test("spu audience page keeps rule assets, association and 3D stats together", async () => {
  const [page, wrapper, contents, shell, types, formatSource, styles, apiSource, storageSource, spuModule, queryContracts, queries] = await Promise.all([
    readFile(new URL("../app/spu-audience/SpuAudiencePage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/spu-audience/page.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/ContentsPage.tsx", import.meta.url), "utf8"),
    readWorkbenchShell(),
    readFile(new URL("../app/lib/types.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/format.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/api.py", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/storage.py", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/spu_audience.py", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/queryContracts.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/queries.ts", import.meta.url), "utf8"),
  ]);
  // 导航与页面骨架
  assert.match(shell, /id: "spu-audience", label: "SPU人群（未生效）", href: "\/spu-audience"/);
  assert.match(wrapper, /<SpuAudiencePage \/>/);
  assert.match(page, /<AppShell active="spu-audience" actions=\{shellActions\}>/);
  assert.match(page, /刷新数据/);
  assert.match(page, /const shellActions = \([\s\S]*重新识别内容[\s\S]*className="secondary spu-shell-action"[\s\S]*新增车型[\s\S]*\);\s*\n\s*return \(/);
  assert.doesNotMatch(page, /className="secondary spu-block-action"/);
  assert.match(styles, /\.spu-shell-action\s*\{[^}]*display:\s*inline-flex;[^}]*align-items:\s*center;[^}]*gap:\s*6px;/);
  assert.doesNotMatch(page, /运行关联/);
  // 页面信息架构（2026-08-16 Mark 定稿）：删掉刷新记录面板，规则链在上、规则校准在下
  // 版式对齐卖点页后（Mark 要求"没必要的文字删一删"），两个纯文字的分节包装标题
  //（"车型 → 人群 → 场景 规则链""规则预期 vs 内容实际"）连同长说明段一并删除，
  // 顺序改由各区块自己的 <h2> 断言；覆盖率总览上提到页首。
  {
    const sectionOrder = ["<h2 id=\"spu-summary-title\">识别完成情况</h2>", "<h2>车系与款型库</h2>", "<h2>目标人群</h2>", "<h2>用车场景</h2>", "<h2>人群对应的用车场景</h2>", "<h2>已配置但没有内容</h2>", "<h2>内容已有但规则未配置</h2>"];
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
  assert.match(page, /useQuery\(spuStatsQueryOptions\(statWindow, statPlatform\)\)/);
  // 车型库并入卖点页式渠道统计列；三维明细表与独立榜单删除
  assert.match(page, /<th>识别结果<\/th><th>抖音条数占比<\/th><th>抖音曝光占比<\/th><th>小红书条数占比<\/th><th>小红书曝光占比<\/th><th>操作<\/th>/);
  assert.match(page, /<th>品牌<\/th><th>车系<\/th><th>款型<\/th><th>目标人群<\/th>/);
  assert.doesNotMatch(page, /<th>识别别名<\/th>/);
  assert.match(page, /识别别名（顿号或逗号分隔）/);
  assert.match(page, /selling-point-hit-value/);
  assert.match(page, /selling-point-share-value/);
  assert.match(page, /条发布/);
  assert.match(page, /次曝光/);
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
  assert.match(page, /<th>内容中出现的特征<\/th><th>发布条数<\/th><th>曝光量<\/th>/);
  assert.match(page, /<th>负向词<\/th><th>发布条数<\/th><th>曝光量<\/th>/);
  // 人群与场景是两个全宽纵向区块，不在宽屏下压成左右两列
  assert.match(styles, /\.spu-dim-grid\s*\{[^}]*grid-template-columns:\s*minmax\(0,\s*1fr\);/);
  // 自动保鲜：打开页面增量补算、保存规则后自动全量重算；资产与统计请求互不拖累
  assert.match(page, /startAssociation\("incremental"\)/);
  assert.match(page, /shouldAutoAssociateSpu\(assetsQuery\.data\)/);
  assert.match(queryContracts, /\(assets\.stale_content_count \?\? 0\) > 0/);
  assert.match(page, /车型、人群和场景规则加载失败，请刷新页面重试。/);
  assert.match(page, /统计数据加载失败，请刷新页面重试。/);
  // 车型库翻页：复用全站 Pagination 组件、放在表格上方（账号页同款），前端切片+末页夹紧
  assert.match(page, /import \{ Pagination \} from "\.\.\/components\/Pagination"/);
  assert.match(page, /import \{ sortVehicleCatalogRows \} from "\.\/vehicleCatalogSort"/);
  assert.match(page, /import\("\.\/vehicleCatalogSearch"\)/);
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
  assert.match(page, /<th>主要用车场景<\/th>/);
  assert.match(page, /coreScenesByAudience/);
  // 页面读写走 v8 API，统计为 GET（只读副本可用），关联为 POST（副本被写保护拦截）
  assert.match(page, /useQuery\(spuAssetsQueryOptions\(\)\)/);
  assert.match(page, /useQuery\(spuStatsQueryOptions\(statWindow, statPlatform\)\)/);
  assert.match(queries, /readQueryJson<SpuAudienceAssets>\("\/api\/v8\/spu-audience\/assets"\)/);
  assert.match(queries, /readQueryJson<SpuAudienceStats>\(`\/api\/v8\/spu-audience\/stats\?\$\{search\.toString\(\)\}`\)/);
  assert.match(page, /mode === "full"[\s\S]*"\/api\/v8\/spu-audience\/associate"/);
  assert.match(page, /readJson<\{ run_id: number; status: string \}>\(path, \{ method: "POST" \}\)/);
  assert.match(page, /\/api\/v8\/spu-audience\/spu/);
  // 规则校准区 + 口径脚注（脚注挂在车型库面板底部）
  assert.match(page, /已配置但没有内容/);
  assert.match(page, /内容已有但规则未配置/);
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
  assert.match(page, /容易混淆的别名（只有上下文明确时才识别）/);
  assert.match(page, /row\.aliases\.filter\(\(item\) => !item\.ambiguous\)/);
  assert.match(page, /row\.aliases\.filter\(\(item\) => item\.ambiguous\)/);
  assert.match(page, /audience_secondary: form\.audienceSecondary \|\| null,\s*aliases,/);
  // 内容列表按已确认设计聚合账号/时间，并把低频字段放在详情内。
  assert.match(contents, /<th scope="col">内容<\/th><th scope="col">卖点<\/th>/);
  const contentRow = contents.match(/<tr key=\{item\.id\}>([\s\S]*?)<\/tr>/)?.[1];
  assert.ok(contentRow, "the content table must retain its data row");
  const contentCells = [...contentRow.matchAll(/<td(?:\s[^>]*)?>([\s\S]*?)<\/td>/g)].map(([, cell]) => cell);
  assert.equal(contentCells.length, 7);
  assert.match(contentCells[0], /<ContentMediaBox item=\{item\} thumbnail=\{thumbnailsQuery\.data\?\.items\[item\.id\]\} onOpen=\{setMediaItem\} showPlatformMark=\{false\} \/>/);
  assert.match(contentCells[0], /<ContentTitle text=\{item\.title \|\| "标题缺失"\} href=\{item\.canonical_url\} \/>/);
  assert.match(contentCells[0], /<ContentMediaMark platform=\{item\.platform\} \/>/);
  assert.match(contentCells[0], /item\.raw_account_name/);
  assert.match(contentCells[0], /contentDateTime\(item\.published_at\)/);
  const [mediaBox, contentMedia] = await Promise.all([
    readFile(new URL("../app/contents/ContentMediaBox.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/contentMedia.ts", import.meta.url), "utf8"),
  ]);
  for (const platform of ["douyin", "xiaohongshu", "wechat-channels", "kuaishou"]) {
    assert.match(contentMedia, new RegExp(`/${platform === "wechat-channels" ? "brand-wechat-channels" : `brand-${platform}`}-official\\.png`));
    await access(new URL(`../public/brand-${platform}-official.png`, import.meta.url));
  }
  assert.match(mediaBox, /className="content-media-mark" data-official-logo=\{logoPath \? "true" : undefined\} role="img" aria-label=\{`\$\{label\(platform\)\}平台`\}/);
  assert.match(mediaBox, /<Image src=\{publicAssetPath\(logoPath\)\} alt="" width=\{14\} height=\{14\} unoptimized \/>/);
  // 第三级（原帖）是新标签链接而不是按钮；前两级是按钮，点击交给页面打开弹窗。
  // 链接带 title 悬停说明与中文无障碍名称（去抖音查看原作品：标题），角标只放一个外链箭头图标，不再写"原帖"。
  assert.match(mediaBox, /<a className="content-media-box" data-action=\{action\.kind\} href=\{action\.href \?\? item\.canonical_url\} target="_blank" rel="noreferrer" title=\{originalPostHint\(platformName\)\} aria-label=\{`\$\{originalPostAction\(platformName\)\}：\$\{title\}`\}>/);
  assert.match(mediaBox, /const platformName = label\(item\.platform\);/);
  assert.match(mediaBox, /<button type="button" className="content-media-box" data-action=\{action\.kind\} onClick=\{\(\) => onOpen\(item\)\} aria-label=\{`\$\{action\.label\}：\$\{title\}`\}>/);
  assert.match(mediaBox, /\{action\.kind === "original" && <span className="content-media-badge" aria-hidden="true"><ArrowSquareOutIcon weight="bold" \/><\/span>\}/);
  assert.doesNotMatch(mediaBox, />原帖<|"原帖"|action\.badge/);
  assert.doesNotMatch(mediaBox, /readQueryJson|readJson|fetch\(|<video|<iframe/);
  assert.match(contentMedia, /export const DOUYIN_PLAYER_ORIGIN = "https:\/\/open\.douyin\.com";/);
  assert.match(contentMedia, /\$\{DOUYIN_PLAYER_ORIGIN\}\/player\/video\?vid=\$\{platformContentId\}&autoplay=0&mode=mobile&width=100vw&height=100vh/);
  const contentTitle = await readFile(new URL("../app/contents/ContentTitle.tsx", import.meta.url), "utf8");
  assert.match(contentTitle, /<a[^>]+href=\{href\}[^>]+target="_blank"[^>]+aria-label=\{text\}/);
  // 展开/收起仍是链接外的独立按钮，避免点击它时跳转作品页。
  assert.match(contentTitle, /<\/a>[\s\S]*<button[^>]+className="content-title-toggle"[^>]+aria-expanded=\{expanded\}[^>]+aria-controls=\{titleId\}/);
  assert.match(contentCells[1], /item\.primary_selling_point_code/);
  assert.match(contentCells[2], /contentMetric\(item\.view_count\)/);
  assert.match(contentCells[3], /contentMetric\(item\.comment_count\)/);
  assert.match(contentCells[4], /contentMetric\(item\.like_count\)/);
  assert.match(contentCells[5], /账号类型[\s\S]*内容方向/);
  assert.match(contentCells[6], /setDetailSelection\(item\)/);
  // 编号和原有标签不能因首屏精简而丢失；在详情中仍可核对。
  const details = contents.slice(contents.indexOf("function ContentDetails"), contents.indexOf("export default function ContentsPage"));
  for (const field of ["platform_content_id", "raw_account_uid", "audience", "scenes", "evidence_level", "content_automotive_score", "duplicate_original_link_id"]) {
    assert.ok(details.includes(`item.${field}`), `${field} remains available in content details`);
  }
  assert.match(details, /<ContentDialog title="内容详情" busy=\{saving\} onClose=\{onClose\}/);
  assert.match(details, /onClick=\{onEvidence\}>查看依据/);
  assert.match(details, /onClick=\{onUpdate\}/);
  assert.match(details, /onClick=\{onEdit\}>修改/);
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
  // 资料完整度先说人话，V 码保留在括号里用于核对；未知等级不再裸露内部值。
  assert.match(contents, /V3: "信息完整（V3）",\s*V2: "有媒体资料（V2）",\s*V1: "只有文字（V1）",\s*V0: "资料不可用（V0）",/);
  assert.match(contents, /\{EVIDENCE_LEVEL_LABELS\[item\.evidence_level\] \?\? item\.evidence_level\}/);
  // 卖点列多行完整显示：不再用单行省略号，V 值也不再挤在卖点下方的 cell-subline 里
  assert.match(styles, /\.content-table \.selling-point-name \{ display: block; white-space: normal; overflow-wrap: anywhere; \}/);
  assert.doesNotMatch(styles, /\.content-table \.selling-point-name \{[^}]*text-overflow/);
  assert.doesNotMatch(contents, /<span className="cell-subline">\{item\.evidence_level/);
  assert.match(contents, /buildContentSearchRequest/);
  assert.match(contents, /useQuery\(contentSearchQueryOptions\(appliedRequest\)\)/);
  assert.match(queryContracts, /spu_series: filters\.spuSeries \|\| null,[\s\S]*audience: filters\.audience \|\| null,[\s\S]*scene: filters\.scene \|\| null/);
  assert.match(contents, /车型不确定/);
  // v16 起系统无人工复核：内容页不再有复核状态筛选、待复核/再次复核入口
  assert.doesNotMatch(contents, /复核/);
  assert.doesNotMatch(contents, /review_status|review_queue_id|pending_review_count/);
  assert.match(contents, /未细化/);
  assert.match(contents, /content-scene-cell/);
  assert.doesNotMatch(contents, /useQuery\(spuAssetsQueryOptions\(\)\)/);
  assert.match(queries, /queryFn: \(\) => readQueryJson<SpuAudienceAssets>\("\/api\/v8\/spu-audience\/assets"\)/);
  assert.match(types, /spu: ContentTagSpu \| null;/);
  assert.match(types, /audience: ContentTagAudience \| null;/);
  assert.match(types, /scenes: ContentTagScene\[\];/);
  assert.match(formatSource, /content_explicit: "内容中直接提到", rule_prior: "系统按规则判断"/);
  assert.match(styles, /\.main-area\[data-section="spu-audience"\]/);
  // 后端：v15 关联域（v14 + LLM 辅助）+ 端点 + 内容检索标签
  assert.match(storageSource, /SCHEMA_VERSION = 19/);
  assert.match(storageSource, /CURRENT_SCHEMA_MIGRATION_NAME = "dual-acquisition-profile-roster-v1"/);
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
  assert.doesNotMatch(page, /window\.setInterval|setInterval\(/);
  assert.match(queries, /last_run\?\.status === "running" \? 5_000 : false/);
  assert.match(queries, /last_run\?\.status === "running" \? "always" : false/);
  assert.match(page, /previousRunStatusRef\.current = "running";[\s\S]*invalidateQueries\(\{ queryKey: queryKeys\.spuAssets, exact: true \}\)/);
  assert.match(page, /didSpuRunReachTerminal\(previousStatus, lastRunStatus\)/);
  assert.match(page, /queryKeys\.spuStatsPrefix[\s\S]*queryKeys\.contents/);
  assert.match(page, /只处理资料较完整、可以自动评估的内容；其他时间范围的数据不会改变/);
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
  assert.match(formatSource, /llm: "系统智能判断"/);
  // 「刷新数据」弹窗四选一：先选范围再确认，范围按发布时间、与统计窗口同口径（_window_bounds 同源）
  for (const label of ["昨天", "本周", "上周", "全部内容"]) assert.match(page, new RegExp(`label: "${label}"`));
  assert.match(page, /选择要重新识别的内容范围/);
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
  assert.match(page, /开始重新识别/);
  assert.match(page, /await startAssociation\(scope\)/);
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
  const [shell, accounts, pagination, contents, evidence, tasks, taskDetail, sellingPoints, apiSource, operationsSource, formatSource, layout, packageJson, queryContracts] = await Promise.all([
    readWorkbenchShell(),
    readFile(new URL("../app/accounts/AccountsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/components/Pagination.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/ContentsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/EvidenceModal.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/tasks/TasksPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/tasks/[id]/TaskDetailPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/selling-points/SellingPointsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/api.py", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/operations.py", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/format.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/layout.tsx", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/queryContracts.ts", import.meta.url), "utf8"),
  ]);
  for (const href of ["/overview", "/tasks", "/accounts", "/contents", "/selling-points", "/spu-audience"]) assert.match(shell, new RegExp(`href: "${href}"`));
  assert.doesNotMatch(accounts, /pending-identity|pending_platform_identit|待匹配/);
  assert.match(accounts, /一个平台账号一行/);
  assert.match(accounts, /<table className=\{styles\.memberTable\}>/);
  assert.match(accounts, /styles\.accountPanel/);
  assert.match(accounts, /<AccountsPagination page=\{appliedRequest\.page\} pageSize=\{appliedRequest\.page_size\} total=\{total\} busy=\{accountsQuery\.isFetching \|\| saving\} onChange=\{\(next\) => applySearch\(\{ page: next\.page, pageSize: next\.pageSize \}\)\}/);
  assert.equal((accounts.match(/<AccountsPagination\b/g) ?? []).length, 1, "account pagination appears once below the table");
  assert.ok(accounts.indexOf("<AccountsPagination") > accounts.indexOf("</table>"));
  assert.match(accounts, /buildAccountSearchRequest/);
  assert.match(accounts, /applySearch\(\{ page: 1 \}\)/);
  assert.match(accounts, /if \(!result \|\| accountsQuery\.isPlaceholderData\) return;[\s\S]*lastPageFor\(result\.total, appliedRequest\.page_size\)[\s\S]*\{ \.\.\.current, page: lastPage \}/);
  assert.match(queryContracts, /page_size: positiveInteger\(pageSize\)/);
  assert.match(pagination, /aria-label=\{ariaLabel\}/);
  assert.match(pagination, /首页.*上一页/s);
  assert.match(pagination, /下一页.*末页/s);
  assert.match(pagination, /pagination-\$\{placement\}/);
  assert.match(contents, /<Pagination page=\{appliedRequest\.page\} pageSize=\{appliedRequest\.page_size\} total=\{total\} busy=\{contentsQuery\.isFetching \|\| saving\}/);
  assert.match(contents, /if \(!contentsQuery\.data \|\| contentsQuery\.isPlaceholderData\) return;[\s\S]*lastPageFor\(contentsQuery\.data\.total, appliedRequest\.page_size\)[\s\S]*\{ \.\.\.current, page: lastPage \}/);
  assert.doesNotMatch(contents, /function pageWindow/);
  assert.match(accounts, /managedMode \? "系统托管账号名单" : "矩阵通账号名单"/);
  for (const action of ["批量上传账号", "下载账号表格"]) assert.match(accounts, new RegExp(action));
  assert.doesNotMatch(accounts, /去矩阵通管理|上传官方导出|立即同步|准备系统名单|同步差异|抖音授权管理|roster\?\.message|全部本地档案/);
  assert.doesNotMatch(accounts, /新增账号<\/button>|parseCsv|\/api\/v8\/accounts\/import|platform-editor/);
  assert.match(accounts, /readJson<\{ message: string \}>\(\x60\/api\/v8\/accounts\/\$\{form\.id\}\x60, jsonRequest\(\{\s*\.\.\.body,[\s\S]*?status_request_id:[\s\S]*?\}, "PATCH"\)\)/);
  // 授权状态列已删除：列表不再读取抖音开平授权状态，仅在行菜单保留锁定 account_id+platform_uid 的授权入口
  assert.doesNotMatch(accounts, /useQuery\(douyinAuthorizationStatusesQueryOptions\(\)\)|AccountAuthorizationStatus|authorizationStatusesReady|BroadcastChannel|data-authorization-state/);
  assert.doesNotMatch(accounts, /查询中|状态异常|不可授权/);
  assert.match(accounts, /const douyinAuthorizationHref = identity\?\.platform === "douyin" && identity\.uid \? `\/accounts\/douyin-authorization\?account_id=\$\{encodeURIComponent\(String\(item\.id\)\)\}&platform_uid=\$\{encodeURIComponent\(identity\.uid\)\}` : ""/);
  assert.match(accounts, /<th scope="col" rowSpan=\{2\} className=\{styles\.accountHeading\}>账号<\/th>/);
  assert.match(accounts, /<th scope="colgroup" colSpan=\{3\}>数据规模<\/th>/);
  const accountRow = accounts.match(/<tr key=\{item\.id\} data-account-id=\{item\.id\}>([\s\S]*?)<\/tr>/)?.[1];
  assert.ok(accountRow, "the account table must retain its data row");
  assert.equal((accountRow.match(/<(?:td|th)\b/g) ?? []).length, 9, "grouped headers align with all 9 account data columns");
  assert.match(accountRow, /<th scope="row" className=\{styles\.identityCell\}>/);
  assert.doesNotMatch(accounts, /platformKeys\.map\(\(key\) => <th|account-group-row/);
  assert.match(accounts, /const identity = item\.platforms\[0\]/);
  assert.match(accounts, /手机号可留空，也可以由多个账号共用/);
  assert.match(accounts, /<PlatformHeaderMark platformKey=\{identity\.platform\} \/>/);
  assert.doesNotMatch(accounts, /AccountScope|defaultScopeFilter|setScope|appliedRequest\.scope|名单范围|当前成员|历史档案|待身份对齐/);
  assert.match(formatSource, /platformKeys = \["douyin", "xiaohongshu", "wechat_channels", "kuaishou"\]/);
  for (const column of ["账号状态", "手机号", "总粉丝", "平台作品总量", "本地收录量", "运营人员", "账号分类", "操作"]) assert.match(accounts, new RegExp(column));
  assert.doesNotMatch(accounts, />授权状态<|>运营信息<|>数据更新<|>采集状态<|名单状态|矩阵监测|运营中|上游：|矩阵：/);
  assert.match(accounts, /<th scope="col" rowSpan=\{2\}>手机号<\/th><th scope="colgroup" colSpan=\{3\}>数据规模<\/th><th scope="col" rowSpan=\{2\}>运营人员<\/th>/);
  assert.match(accounts, /const rowStatus = item\.account_status \|\| "unmarked"/);
  assert.match(accountRow, /data-state=\{rowStatus\} title=\{accountStatusHints\[rowStatus\]\}/);
  assert.doesNotMatch(accountRow, /roster_state|未在当前生效名单|待补充平台 UID/);
  assert.match(accounts, /const metricTitle = identity\?\.data_status && identity\.data_status !== "not_collected" && identity\?\.data_date \? `\$\{statusLabels\[identity\.data_status\] \|\| identity\.data_status\} · 数据日期 /);
  assert.equal((accountRow.match(/title=\{metricTitle\}/g) ?? []).length, 2, "fans and platform work counts carry the data-date tooltip");
  assert.match(accountRow, /title=\{`平台 UID：[\s\S]*短号：/);
  assert.match(accountRow, /aria-label=\{`复制\$\{identity\.nickname \|\| "账号"\}的平台 UID`\}[\s\S]*copyUid\(identity\.uid\)/);
  for (const field of ["item.operator_name", "item.account_type", "item.content_direction", "item.phone"]) assert.ok(accountRow.includes(field), `${field} remains accessible in the grouped account row`);
  for (const field of ["identity?.data_date", "identity?.data_status"]) assert.ok(accounts.includes(field), `${field} still feeds the account row tooltip`);
  assert.match(accountRow, /title=\{`账号类型：\$\{label\(item\.account_type\)\}；内容方向：\$\{label\(item\.content_direction\)\}`\}/);
  assert.match(accountRow, /aria-label=\{`修改\$\{identity\?\.nickname \|\| "账号"\}的运营信息`\} onClick=\{\(\) => edit\(item\)\}/);
  assert.match(accountRow, /pauseManagedAccount\(item\)/);
  assert.match(accounts, /const canPause = managedMode && rowStatus !== "paused"/);
  assert.match(accounts, /readJson<\{ message: string \}>\(`\/api\/v8\/accounts\/\$\{account\.id\}`, jsonRequest\(\{\s*account_status: "paused", status_request_id: statusRequests\.current\.get\(requestKey\),\s*\}, "PATCH"\)\)/);
  assert.match(accounts, /暂停后将停止采集、退出当前生效名单，相关数据不进入统计；历史数据保留/);
  assert.doesNotMatch(accounts, /removeManagedAccount|移出名单|method: "DELETE"/);
  assert.match(accountRow, /\(douyinAuthorizationHref \|\| canPause\) && <details className=\{styles\.rowMenu\}/);
  assert.match(accountRow, /<DotsThreeVerticalIcon weight="bold" aria-hidden="true" \/><\/summary><div className=\{styles\.menuPanel\}>\{douyinAuthorizationHref && <Link href=\{douyinAuthorizationHref\}>抖音授权<\/Link>\}\{canPause && <button/);
  assert.doesNotMatch(accounts, /DotsThreeIcon\b/);
  assert.doesNotMatch(accounts, /form\.enabled|主动采集启用/);
  assert.match(accounts, /form\.accountStatus && form\.accountStatus !== form\.originalAccountStatus \? \{ account_status: form\.accountStatus \} : \{\}/);
  assert.match(accounts, /setForm\(null\); await invalidateAccountData\(\); setMessage\(response\.message\)/);
  for (const key of ["accounts", "contents", "overview", "sellingPoints", "spu"]) {
    assert.match(accounts, new RegExp(`invalidateQueries\\(\\{ queryKey: queryKeys\\.${key} \\}\\)`));
  }
  assert.match(accounts, /aria-label="账号状态筛选"/);
  assert.match(accounts, /setAccountStatus\(nextStatus\); applySearch\(\{ accountStatus: nextStatus, page: 1 \}\)/);
  assert.match(accounts, /accountManagementVersion=\{accountManagementVersion\}/);
  assert.match(accounts, /account_status: appliedRequest\.account_status/);
  const editForm = accounts.match(/\{form && <div[\s\S]*?<\/section><\/div>\}/)?.[0];
  assert.ok(editForm);
  for (const [status, text] of [["daily", "日更"], ["weekly", "周更"], ["paused", "暂停"]]) {
    assert.ok(editForm.includes(`<option value="${status}">${text}</option>`));
  }
  assert.match(editForm, /<option value="" disabled>待标记<\/option>/);
  assert.doesNotMatch(editForm, /<option value="unmarked"/);
  assert.match(editForm, /日更、周更仅标注作品更新频率，采集规则不变/);
  assert.match(editForm, /暂停将停止采集、退出当前生效名单，相关数据不进入统计；历史数据保留/);
  assert.match(accounts, /formatIdentityCount\(identity\?\.follower_count\)/);
  assert.match(accounts, /formatIdentityCount\(identity\?\.platform_work_count\)/);
  assert.match(accounts, /formatIdentityCount\(identity\?\.content_count \?\? 0\)/);
  assert.match(accountRow, /<td><span className=\{styles\.phone\}>\{item\.phone \|\| "—"\}<\/span><\/td>/);
  assert.match(accountRow, /<td>\{item\.operator_name \|\| "未填写"\}<\/td>/);
  assert.match(accounts, /douyin_authorization_targets: authorizationTargets/);
  assert.match(accounts, /item\.status === "active" && item\.account_id != null && item\.platform_uid != null/);
  assert.match(accounts, /state: item\.authorized \? "authorized" as const : "needs_reauthorization" as const/);
  assert.match(accounts, /response\.blob\(\)[\s\S]*URL\.createObjectURL\(blob\)[\s\S]*workbookFilename\(response\.headers\.get\("Content-Disposition"\)\)/);
  assert.doesNotMatch(accounts, /douyin_authorized_account_ids/);
  assert.match(accounts, /<ReadErrorState title="账号读取失败" retrying=\{retrying\} onRetry=\{retryAccountsRead\} \/>/);
  assert.doesNotMatch(accounts, /<td[^>]*className="table-read-error"/);
  assert.match(accounts, /className=\{styles\.empty\} colSpan=\{9\}/);
  assert.match(apiSource, /account_read_model/);
  assert.match(operationsSource, /ACCOUNT_IDENTITY_STATS_SQL/);
  assert.match(operationsSource, /NULL follower_count/);
  assert.match(operationsSource, /COUNT\(c\.id\) content_count/);
  assert.match(accounts, /\/api\/v8\/account-roster\/import/);
  assert.match(apiSource, /prepare_candidate\([\s\S]*accept_candidate\(/);
  assert.match(apiSource, /"status": "manual_export_required"/);
  assert.match(accounts, /\/api\/v8\/accounts\/export/);
  assert.doesNotMatch(contents, /\/api\/v8\/contents\/(?:validate|import)/);
  assert.match(contents, /contentUpdates\.submit\(item\)/);
  assert.doesNotMatch(contents, /\/update-data/);
  assert.match(contents, /查看依据/);
  assert.match(contents, /结果需更新/);
  assert.match(taskDetail, /display_effective_revision/);
  assert.match(taskDetail, /这份报告使用的是旧规则，请重新生成后再使用/);
  assert.match(taskDetail, /revision_state === "current"/);
  assert.match(taskDetail, /revision_state === "stale"/);
  assert.doesNotMatch(taskDetail, /revisions\.find\(\(item\) => !item\.invalidated_at\)/);
  assert.match(tasks, /current_valid_revision/);
  assert.match(tasks, /stale_display_revision/);
  assert.match(tasks, /historical_revision_count/);
  assert.match(evidence, /display_evaluation_id/);
  assert.match(evidence, /结果需更新/);
  assert.match(evidence, /这条内容还没有按最新规则完成评估，目前展示的是旧结果/);
  assert.match(evidence, /当前评估摘要/);
  assert.match(evidence, /语音转写/);
  assert.match(evidence, /画面文字识别/);
  assert.match(evidence, /评论摘要/);
  assert.match(evidence, /媒体处理记录/);
  assert.match(evidence, /evidence_ready: "已完成"/);
  assert.match(evidence, /retryable_failed: "处理失败，可以重试"/);
  assert.match(evidence, /terminal_failed: "处理失败"/);
  assert.match(evidence, /return processingStatusLabels\[value\] \?\? "状态未知"/);
  assert.match(evidence, /automatic: "系统自动评估"/);
  assert.match(evidence, /manual_review: "人工复核"/);
  assert.match(evidence, /migrated_from_v5: "历史结果"/);
  assert.match(evidence, /V3: "信息完整（V3）"/);
  assert.match(evidence, /return labels\[value\] \?\? "资料状态未知"/);
  assert.match(evidence, /allow_paid_refresh/);
  assert.match(evidence, /\/evidence/);
  assert.match(evidence, /\/media\/retry/);
  // v16 起证据弹窗是纯只读面板：可以展示历史评估方式，但没有复核提交接口。
  assert.doesNotMatch(evidence, /\/api\/v8\/reviews/);
  assert.match(evidence, /资料或规则更新后，系统会自动重新评估并保留历史结果/);
  assert.match(tasks, /新建自定义报告/);
  assert.match(taskDetail, /\/cancel/);
  assert.match(taskDetail, /\/resume/);
  assert.match(taskDetail, /文件与日志/);
  assert.match(taskDetail, /!superseded/);
  assert.match(taskDetail, /技术文件（供排查）/);
  assert.match(sellingPoints, /useQuery\(activeSellingPointsQueryOptions\(\)\)/);
  assert.match(apiSource, /\/api\/v8\/media-processing\/search/);
  assert.match(apiSource, /\/api\/v8\/contents\/\{content_id\}\/evidence/);
  assert.match(apiSource, /LEGACY_REPORT_VERSION = "channel-structured-conclusions-v7\.0"/);
  assert.match(layout, /DCar Insight · 内容运营工作台/);
  assert.doesNotMatch(packageJson, /react-loading-skeleton/);
  await assert.rejects(access(new URL("../app/InsightDashboard.tsx", import.meta.url)));
  await assert.rejects(access(new URL("../app/_sites-preview", import.meta.url)));
});

test("task list shows background generation progress on cards instead of jumping away", async () => {
  const [tasks, taskDetail, styles, apiSource, reportsSource, queries, queryContracts] = await Promise.all([
    readFile(new URL("../app/tasks/TasksPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/tasks/[id]/TaskDetailPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/api.py", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/reports.py", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/queries.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/queryContracts.ts", import.meta.url), "utf8"),
  ]);
  // 点“生成报告”后留在列表：任务以卡片形式出现，右侧是查看详情，卡片自己显示进度
  assert.doesNotMatch(tasks, /useRouter|router\.push/);
  assert.match(tasks, /className="task-card-list"/);
  assert.match(tasks, /查看详情/);
  assert.match(tasks, /role="progressbar"/);
  assert.match(tasks, /isGeneratingTaskStatus\(task\.task_status\)/);
  assert.match(taskDetail, /isGeneratingTaskStatus\(detail\.task_status\)/);
  assert.match(queryContracts, /new Set\(\["queued", "running", "cancel_requested"\]\)/);
  assert.doesNotMatch(tasks, /setInterval\(/);
  assert.doesNotMatch(tasks, /<table>|<thead>/);
  assert.match(taskDetail, /task-progress-panel/);
  assert.doesNotMatch(taskDetail, /setInterval\(/);
  assert.match(queries, /items\.some\(\(task\) => isGeneratingTaskStatus\(task\.task_status\)\) \? 1_500 : false/);
  assert.match(queries, /isGeneratingTaskStatus\(query\.state\.data\?\.task_status\) \? 1_500 : false/);
  assert.match(queries, /isGeneratingTaskStatus\(query\.state\.data\?\.task_status\) \? "always" : false/);
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
  assert.match(formatSource, /部分用户身份信息不足/);
  assert.match(formatSource, /用户分类未完成/);
  assert.match(formatSource, /互动用户少于 30 人/);
  assert.match(formatSource, /metric\.status === "sample_only" \? `\$\{value\}（仅供参考）` : value/);
  assert.match(taskDetail, /同一互动用户只计算一次/);
  assert.doesNotMatch(taskDetail, /channel_conclusions\.csv/);
  assert.match(taskDetail, /详细人数和覆盖率可在「文件与日志」中下载查看/);
  assert.match(taskDetail, /这份旧报告没有平台和场景数据/);
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
  for (const name of ["账号采集完成率", "内容详情采集完成率", "播放和互动数据更新率", "卖点评估完成率", "语音和画面文字识别完成率", "视频和图片处理完成率", "重复内容识别完成率", "重复内容规则校验", "评论采集完成率"]) {
    assert.match(source, new RegExp(name));
  }
  assert.doesNotMatch(source, /<dt>\{key\}<\/dt>|<dt>discovery_coverage<\/dt>|definition-list/);
  // 周评论检查只对周报有实际意义，其他任务不显示这行恒为 100% 的占位
  assert.match(source, /key !== "weekly_comment_coverage" \|\| detail\?\.task_type === "weekly"/);
  // 文件与日志：产物名称、大小、事件类型全部转中文，raw 值只留在 title/接口里
  assert.match(source, /fileKindLabels/);
  assert.match(source, /渠道结论表（CSV）/);
  assert.match(source, /formatBytes\(file\.byte_size\)/);
  assert.doesNotMatch(source, /\{file\.file_kind\} · /);
  assert.match(source, /taskEventLabels/);
  assert.match(source, /请求重新生成/);
  assert.doesNotMatch(source, /<strong>\{event\.event_type\}<\/strong>/);
});

test("private account searches stay in POST bodies and obsolete static assets remain absent", async () => {
  const [accounts, contents, queries, apiSource] = await Promise.all([
    readFile(new URL("../app/accounts/AccountsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/ContentsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/queries.ts", import.meta.url), "utf8"),
    readFile(new URL("../../../src/dcar_eval/v8/api.py", import.meta.url), "utf8"),
  ]);
  assert.match(accounts, /useQuery\(accountSearchQueryOptions\(appliedRequest\)\)/);
  assert.doesNotMatch(accounts, /\?phone=|URLSearchParams/);
  assert.match(contents, /useQuery\(contentSearchQueryOptions\(appliedRequest\)\)/);
  assert.match(queries, /readQueryJson<AccountSearchResult>\("\/api\/v8\/accounts\/search", jsonRequest\(/);
  assert.match(queries, /readQueryJson<ContentSearchResult>\(CONTENT_SEARCH_PATH, jsonRequest\(request\)\)/);
  assert.doesNotMatch(queries, /\/api\/v8\/(?:accounts|contents)\/search\?/);
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

test("user management is gated by role in the shell and served by the gateway contract", async () => {
  const [shell, page, queries, api, douyin, hook, styles, usersCss] = await Promise.all([
    readWorkbenchShell(),
    readFile(new URL("../app/users/UsersPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/queries.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/api.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/accounts/douyin-authorization/DouyinAuthorizationPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/components/useDialogFocus.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../app/users/UsersPage.module.css", import.meta.url), "utf8"),
  ]);
  // 侧栏：会话查询决定是否渲染"用户管理&质检"分组；入口与其它一级入口一样预取
  assert.match(shell, /useQuery\(sessionQueryOptions\(\)\)/);
  assert.match(shell, /<p>用户管理&质检<\/p>/);
  assert.match(shell, /href="\/users"[\s\S]*用户权限/);
  assert.match(shell, /case "users":\s*return queryClient\.prefetchQuery\(usersQueryOptions\(\)\)/);
  assert.match(shell, /users: \{ eyebrow: "用户管理&质检", title: "用户权限"/);
  assert.match(queries, /session: \["auth", "session"\] as const/);
  assert.match(queries, /readQueryJson<AuthSession>\("\/auth\/session"\)/);
  assert.match(queries, /readQueryJson<ManagedUsersResult>\("\/auth\/users"\)/);
  assert.match(douyin, /useQuery\(sessionQueryOptions\(\)\)/);
  assert.doesNotMatch(douyin, /queryKey: \["auth", "session"\]/);
  // 401 统一整页跳登录（readJson 与 readDownload 都走）
  assert.match(api, /export function redirectToLogin\(\)/);
  assert.match(api, /window\.location\.replace\(LOGIN_PATH \+ "\?return_to=" \+ encodeURIComponent\(returnTo\)\)/);
  assert.match(api, /const LOGIN_PATH = `\$\{process\.env\.NEXT_PUBLIC_DCAR_BASE_PATH \?\? ""\}\/login`/);
  assert.equal((api.match(/if \(response\.status === 401\) redirectToLogin\(\);/g) ?? []).length, 2);
  // 页面合同
  assert.match(page, /<AppShell active="users">/);
  assert.match(page, /aria-label="修改用户"/);
  assert.match(page, /aria-label="删除用户"/);
  assert.match(page, /markedJsonRequest\([^)]*"user-update"\)/);
  assert.match(page, /markedJsonRequest\(\{ username: pendingDelete\.username \}, "user-delete"\)/);
  assert.match(page, /invalidateQueries\(\{ queryKey: queryKeys\.users, exact: true \}\)/);
  assert.match(page, /invalidateQueries\(\{ queryKey: queryKeys\.session, exact: true \}\)/);
  assert.match(page, /autoComplete="new-password"/);
  assert.doesNotMatch(page, /window\.confirm/);
  assert.match(page, /const usersReadFailed = usersQuery\.isLoadingError \|\| retrying/);
  assert.match(page, /usersQuery\.isError && usersQuery\.data && <Notice tone="error">/);
  assert.match(page, /<article className="panel"><div className="empty-state">/);
  assert.doesNotMatch(page, /table-read-error/);
  assert.match(page, /\{!isSelf\(user\) && <button type="button" className="text-button danger"/);
  assert.match(page, /\{!form\.isSelf && <label>新密码/);
  assert.match(page, /disabled=\{saving \|\| form\.isSelf\}/);
  assert.match(page, /className="secondary danger-button"[\s\S]*?>\{saving \? "删除中" : "确认删除"\}/);
  // 弹窗焦点 hook：Effect Event 读取最新的 onClose / busy，effect 只依赖打开状态
  assert.match(hook, /useEffectEvent/);
  assert.match(hook, /const closeIfIdle = useEffectEvent\(\(\) => \{/);
  assert.match(hook, /if \(!options\.busy\) options\.onClose\(\)/);
  assert.match(hook, /event\.key === "Escape"/);
  assert.match(hook, /closeIfIdle\(\)/);
  assert.match(hook, /previouslyFocused\?\.focus\(\)/);
  assert.match(hook, /\}, \[open, dialogRef\]\);/);
  assert.match(page, /useDialogFocus\(form !== null, editDialogRef/);
  // 底栏导航单行横向滚动；用户表六列全显式
  const narrow = styles.slice(styles.indexOf("@media (max-width: 720px)"));
  assert.match(narrow, /\.sidebar nav \{ display: flex; overflow-x: auto;/);
  assert.doesNotMatch(narrow, /\.sidebar nav \{ display: grid; grid-template-columns: repeat\(5, 1fr\); \}/);
  for (const column of [1, 2, 3, 4, 5, 6]) assert.match(usersCss, new RegExp(`\\.table thead th:nth-child\\(${column}\\) \\{ width: \\d+%; \\}`));
  // 列宽只在页面模块里声明一处，globals 不再放同名副本，避免两处打架
  assert.doesNotMatch(styles, /\.user-table/);
  // 保留线上已发布的全宽表格，与页头对齐。
  assert.match(page, /<section className="page-stack wide-stack">/);
  assert.doesNotMatch(usersCss, /max-width: 1080px;/);
  assert.match(usersCss, /justify-self: start;/);
  assert.doesNotMatch(usersCss, /padding: 8px 16px 6px;/);
});
