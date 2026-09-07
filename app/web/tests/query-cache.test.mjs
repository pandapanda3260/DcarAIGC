import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  ApiRequestError,
  readJson,
  readQueryJson,
  shouldRetryQuery,
} from "../app/lib/api.ts";
import {
  buildAccountSearchRequest,
  buildContentSearchRequest,
  didSpuRunReachTerminal,
  isGeneratingTaskStatus,
  lastPageFor,
  shouldAutoAssociateSpu,
} from "../app/lib/queryContracts.ts";

const emptyContentFilters = {
  query: "",
  platform: "",
  accountType: "",
  direction: "",
  sellingPoint: "",
  spuSeries: "",
  audience: "",
  scene: "",
};

test("search request builders produce one canonical key payload", () => {
  assert.deepEqual(buildContentSearchRequest(emptyContentFilters, 0, 50), {
    page: 1,
    page_size: 50,
    query: "",
    platform: null,
    account_type: null,
    content_direction: null,
    selling_point: null,
    spu_series: null,
    audience: null,
    scene: null,
  });
  assert.deepEqual(buildContentSearchRequest({
    ...emptyContentFilters,
    query: "问界",
    platform: "douyin",
    accountType: "original",
    direction: "used_car",
    sellingPoint: "SP01",
    spuSeries: "aito-m7",
    audience: "family",
    scene: "commute",
  }, 3.9, 20.8), {
    page: 3,
    page_size: 20,
    query: "问界",
    platform: "douyin",
    account_type: "original",
    content_direction: "used_car",
    selling_point: "SP01",
    spu_series: "aito-m7",
    audience: "family",
    scene: "commute",
  });
  assert.deepEqual(buildAccountSearchRequest({
    query: "账号",
    platform: "xiaohongshu",
    accountType: "boutique_ip",
    direction: "new_car",
  }, 2, 100), {
    page: 2,
    page_size: 100,
    query: "账号",
    platform: "xiaohongshu",
    account_type: "boutique_ip",
    content_direction: "new_car",
    account_status: null,
  });
  assert.deepEqual(buildAccountSearchRequest({
    query: "", platform: "", accountType: "", direction: "",
  }, 1, 50), {
    page: 1,
    page_size: 50,
    query: "",
    platform: null,
    account_type: null,
    content_direction: null,
    account_status: null,
  });
});

test("account status is the only account lifecycle filter", () => {
  for (const accountStatus of ["daily", "weekly", "paused", "unmarked"]) {
    const request = buildAccountSearchRequest({
      query: "", platform: "", accountType: "", direction: "", accountStatus,
    }, 1, 50);
    assert.equal(request.account_status, accountStatus);
    assert.equal("scope" in request, false);
  }
  const request = buildAccountSearchRequest({
    query: "", platform: "", accountType: "", direction: "", accountStatus: "",
  }, 1, 50);
  assert.equal(request.account_status, null);
  assert.equal("scope" in request, false);
});

test("pagination and runtime predicates cover their boundaries", () => {
  assert.equal(lastPageFor(0, 50), 1);
  assert.equal(lastPageFor(100, 50), 2);
  assert.equal(lastPageFor(101, 50), 3);
  assert.equal(lastPageFor(-1, 0), 1);

  for (const status of ["queued", "running", "cancel_requested"]) {
    assert.equal(isGeneratingTaskStatus(status), true, status);
  }
  for (const status of ["succeeded", "failed", "cancelled", "interrupted", null, undefined]) {
    assert.equal(isGeneratingTaskStatus(status), false, String(status));
  }

  const assets = (overrides = {}) => ({ ready: true, stale_content_count: 2, last_run: null, ...overrides });
  assert.equal(shouldAutoAssociateSpu(assets()), true);
  assert.equal(shouldAutoAssociateSpu(assets({ ready: false })), false);
  assert.equal(shouldAutoAssociateSpu(assets({ stale_content_count: 0 })), false);
  assert.equal(shouldAutoAssociateSpu(assets({ last_run: { status: "running" } })), false);
  assert.equal(didSpuRunReachTerminal("running", "succeeded"), true);
  assert.equal(didSpuRunReachTerminal("running", "failed"), true);
  assert.equal(didSpuRunReachTerminal("running", "running"), false);
  assert.equal(didSpuRunReachTerminal(null, "succeeded"), false);
});

test("query retry policy skips aborts and 4xx but retries network and 5xx once", () => {
  const aborted = new DOMException("stopped", "AbortError");
  const unauthorized = new ApiRequestError("登录已过期", { status: 401 });
  const unavailable = new ApiRequestError("服务暂时不可用", { status: 503, retryable: true });
  const network = new ApiRequestError("无法连接", { retryable: true });

  assert.equal(shouldRetryQuery(0, aborted), false);
  assert.equal(shouldRetryQuery(0, unauthorized), false);
  assert.equal(shouldRetryQuery(0, unavailable), true);
  assert.equal(shouldRetryQuery(1, unavailable), false);
  assert.equal(shouldRetryQuery(0, network), true);
  assert.equal(shouldRetryQuery(0, new Error("unknown")), false);
});

test("readJson preserves retry metadata and abort identity", async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async () => Response.json({ detail: "expired" }, { status: 401 });
    await assert.rejects(readJson("/unauthorized"), (error) => (
      error instanceof ApiRequestError
      && error.status === 401
      && error.retryable === false
    ));

    globalThis.fetch = async () => Response.json({ detail: "down" }, { status: 503 });
    await assert.rejects(readJson("/unavailable"), (error) => (
      error instanceof ApiRequestError
      && error.status === 503
      && error.retryable === true
    ));

    globalThis.fetch = async () => Response.json(
      { detail: "upstream down", code: "upstream_unavailable" },
      { status: 502 },
    );
    await assert.rejects(readJson("/upstream-unavailable"), (error) => (
      error instanceof ApiRequestError
      && error.status === 502
      && error.code === "upstream_unavailable"
      && error.retryable === false
    ));

    globalThis.fetch = async () => { throw new TypeError("network"); };
    await assert.rejects(readJson("/network"), (error) => (
      error instanceof ApiRequestError
      && error.status === null
      && error.retryable === true
    ));

    const aborted = new DOMException("stopped", "AbortError");
    globalThis.fetch = async () => { throw aborted; };
    await assert.rejects(readJson("/abort"), (error) => error === aborted);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("readQueryJson bounds reads without creating an automatic retry", async () => {
  const originalFetch = globalThis.fetch;
  let fetchCalls = 0;
  try {
    globalThis.fetch = async (_input, init) => {
      fetchCalls += 1;
      return await new Promise((_resolve, reject) => {
        init?.signal?.addEventListener(
          "abort",
          () => reject(new DOMException("stopped", "AbortError")),
          { once: true },
        );
      });
    };
    const timeout = await readQueryJson("/slow", undefined, 5).catch((error) => error);
    assert.equal(timeout?.name, "AbortError");
    assert.equal(timeout?.message, "读取超时，请重新加载。");
    assert.equal(fetchCalls, 1);
    assert.equal(shouldRetryQuery(0, timeout), false);

    globalThis.fetch = async () => Response.json({ detail: "down" }, { status: 503 });
    const unavailable = await readQueryJson("/unavailable", undefined, 100).catch((error) => error);
    assert.ok(unavailable instanceof ApiRequestError);
    assert.equal(unavailable.status, 503);
    assert.equal(shouldRetryQuery(0, unavailable), true);
    assert.equal(shouldRetryQuery(1, unavailable), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("provider is SSR-safe and cached read query functions do not consume AbortSignal", async () => {
  const [layout, providers, client, queries, packageJson] = await Promise.all([
    readFile(new URL("../app/layout.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/components/Providers.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/queryClient.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/queries.ts", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);

  assert.match(layout, /<Providers>\{children\}<\/Providers>/);
  assert.doesNotMatch(layout, /\btypeof\s+window\b/);
  assert.match(providers, /^"use client";/);
  assert.match(providers, /<QueryClientProvider client=\{queryClient\}><ContentUpdateJobsProvider>\{children\}<\/ContentUpdateJobsProvider><\/QueryClientProvider>/);
  assert.doesNotMatch(providers, /\btypeof\s+window\b/);
  assert.match(client, /if \(isServer\) return makeQueryClient\(\)/);
  assert.match(client, /let browserQueryClient: QueryClient \| undefined/);
  assert.match(client, /staleTime:\s*60_000/);
  assert.match(client, /gcTime:\s*isServer \? Infinity : 10 \* 60_000/);
  assert.match(client, /refetchOnWindowFocus:\s*false/);
  assert.match(client, /retry:\s*shouldRetryQuery/);
  assert.match(client, /retryDelay:\s*250/);

  assert.doesNotMatch(queries, /\bAbortController\b|\bsignal\b|QueryFunctionContext/);
  assert.equal((queries.match(/queryFn:\s*\(\)\s*=>/g) ?? []).length, 12);
  assert.equal((queries.match(/readQueryJson</g) ?? []).length, 15);
  assert.doesNotMatch(queries, /queryKey:[^\n]*(?:timeout|signal)/);
  assert.match(queries, /spuAssetsQueryOptions[\s\S]*refetchInterval:\s*\(query\) => !query\.state\.error/);
  assert.match(queries, /tasksListQueryOptions[\s\S]*refetchInterval:\s*\(query\) => !query\.state\.error/);
  assert.match(queries, /taskDetailQueryOptions[\s\S]*refetchInterval:\s*\(query\) => !query\.state\.error/);
  assert.match(queries, /activeSellingPointsQueryOptions[\s\S]*?staleTime:\s*60_000,[\s\S]*?refetchOnWindowFocus:\s*true/);
  assert.match(queries, /taskReportQueryOptions[\s\S]*?enabled:\s*Boolean\(revision\),[\s\S]*?staleTime:\s*Infinity/);
  assert.equal(JSON.parse(packageJson).dependencies["@tanstack/react-query"], "5.101.4");
});

test("sidebar navigation prefetches only the destination page primary query", async () => {
  const [shell, queries, contents, accounts] = await Promise.all([
    readFile(new URL("../app/components/AppShell.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/queries.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/ContentsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/accounts/AccountsPage.tsx", import.meta.url), "utf8"),
  ]);

  assert.match(shell, /^"use client";/);
  assert.match(shell, /const queryClient = useQueryClient\(\)/);
  assert.equal((shell.match(/queryClient\.prefetchQuery\(/g) ?? []).length, 7);
  for (const query of [
    "overviewQueryOptions",
    "contentSearchQueryOptions",
    "accountSearchQueryOptions",
    "activeSellingPointsQueryOptions",
    "spuAssetsQueryOptions",
    "tasksListQueryOptions",
    "usersQueryOptions",
  ]) {
    assert.match(shell, new RegExp(`prefetchQuery\\(${query}\\(`));
  }
  assert.match(shell, /setTimeout\(\(\) => \{[\s\S]*void prefetchSection\(section\);[\s\S]*\}, 120\)/);
  assert.match(shell, /onPointerEnter=\{\(\) => schedulePrefetch\(item\.id\)\}/);
  assert.match(shell, /onPointerLeave=\{cancelScheduledPrefetch\}/);
  assert.match(shell, /onPointerDown=\{\(event\) => \{[\s\S]*event\.button !== 0[\s\S]*void prefetchSection\(item\.id\);[\s\S]*\}\}/);
  assert.match(shell, /onFocus=\{\(\) => schedulePrefetch\(item\.id\)\}/);
  assert.match(shell, /onBlur=\{cancelScheduledPrefetch\}/);
  assert.match(queries, /export const defaultContentSearchRequest = buildContentSearchRequest/);
  assert.match(queries, /export const defaultAccountSearchRequest = buildAccountSearchRequest/);
  assert.match(shell, /contentSearchQueryOptions\(defaultContentSearchRequest\)/);
  assert.match(shell, /accountSearchQueryOptions\(defaultAccountSearchRequest\)/);
  assert.match(contents, /useState\(\(\) => \(\{ \.\.\.defaultContentSearchRequest \}\)\)/);
  assert.match(accounts, /useState\(\(\) => \(\{ \.\.\.defaultAccountSearchRequest \}\)\)/);
});

test("background refreshes do not render layout-shifting status banners", async () => {
  const pages = await Promise.all([
    "../app/overview/OverviewPage.tsx",
    "../app/accounts/AccountsPage.tsx",
    "../app/contents/ContentsPage.tsx",
    "../app/selling-points/SellingPointsPage.tsx",
    "../app/spu-audience/SpuAudiencePage.tsx",
    "../app/tasks/TasksPage.tsx",
    "../app/tasks/[id]/TaskDetailPage.tsx",
  ].map((path) => readFile(new URL(path, import.meta.url), "utf8")));

  for (const page of pages) {
    assert.doesNotMatch(page, /<Notice>正在更新数据<\/Notice>/);
  }

  const lifecycle = await readFile(new URL("../app/tasks/MediaLifecyclePanel.tsx", import.meta.url), "utf8");
  assert.match(lifecycle, /refetchInterval:\s*\(current\) => current\.state\.error \? false : 30000/);
  const evidence = await readFile(new URL("../app/contents/EvidenceModal.tsx", import.meta.url), "utf8");
  assert.doesNotMatch(evidence, /setInterval\(\(\) => \{[\s\S]*readQueryJson<EvidenceBundle>/);
  assert.match(evidence, /async function poll\(\)[\s\S]*await readQueryJson<EvidenceBundle>[\s\S]*setTimeout\(\(\) => void poll\(\), 5000\)/);
});

test("list read failures stay distinct from real empty states and expose retry", async () => {
  const [accounts, contents, tasks, feedback, styles] = await Promise.all([
    readFile(new URL("../app/accounts/AccountsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/ContentsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/tasks/TasksPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/components/Feedback.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);

  assert.match(accounts, /const accountsReadFailed = accountsQuery\.isLoadingError \|\| retrying/);
  assert.match(accounts, /accountsReadFailed \? "读取失败" : `\$\{total\} 个账号`/);
  assert.match(accounts, /className=\{`\$\{styles\.accountPanel\}\$\{accountsReadFailed \? " has-read-error" : ""\}`\}/);
  assert.match(accounts, /<\/table><\/div>\s*\{accountsReadFailed && <ReadErrorState title="账号读取失败" retrying=\{retrying\} onRetry=\{retryAccountsRead\} \/>\}/);
  assert.doesNotMatch(accounts, /<td[^>]*className="table-read-error"/);
  assert.match(accounts, /setRetrying\(true\);[\s\S]*accountsQuery\.refetch\(\)\.finally\(\(\) => setRetrying\(false\)\)/);
  assert.match(accounts, /accountsQuery\.isPending && !accountsQuery\.data && !accountsReadFailed/);
  assert.match(accounts, /!accountsReadFailed && accountsQuery\.data && <AccountsPagination/);
  assert.match(accounts, /!accountsReadFailed && !items\.length && <tr><td className=\{styles\.empty\} colSpan=\{9\}/);
  assert.match(accounts, /!accountsReadFailed && items\.map/);

  assert.match(contents, /const contentsReadFailed = contentsQuery\.isLoadingError \|\| retrying/);
  assert.match(contents, /contentsReadFailed \? "读取失败" : listLoading \? "正在读取…" : `\$\{total\} 条内容`/);
  assert.match(contents, /className=\{`panel table-panel \$\{styles\.listPanel\}\$\{contentsReadFailed \? " has-read-error" : ""\}`\}/);
  assert.match(contents, /\{contentsReadFailed && <ReadErrorState title="内容读取失败" retrying=\{retrying\} onRetry=\{retryContentsRead\} \/>\}/);
  assert.match(contents, /\{!contentsReadFailed && !listLoading && <div className=\{styles\.listBody\}>/);
  assert.doesNotMatch(contents, /<td[^>]*className="table-read-error"/);
  assert.match(contents, /setRetrying\(true\);[\s\S]*contentsQuery\.refetch\(\)\.finally\(\(\) => setRetrying\(false\)\)/);
  assert.match(contents, /contentsQuery\.isPending && !contentsQuery\.data && !contentsReadFailed/);
  assert.match(contents, /!contentsReadFailed && contentsQuery\.data && <Pagination/);

  assert.match(tasks, /const tasksReadFailed = tasksQuery\.isLoadingError \|\| retrying/);
  assert.match(tasks, /tasksReadFailed \? "读取失败"/);
  assert.match(tasks, /setRetrying\(true\);[\s\S]*tasksQuery\.refetch\(\)\.finally\(\(\) => setRetrying\(false\)\)/);
  assert.match(tasks, /tasksReadFailed \? <article className="panel"><ReadErrorState title="任务读取失败" retrying=\{retrying\} onRetry=\{retryTasksRead\} \/><\/article> : <>[\s\S]*<MediaLifecyclePanel \/>/);
  assert.match(tasks, /tasksQuery\.isPending && !tasksQuery\.data && !tasksReadFailed/);
  assert.match(tasks, /tasksQuery\.data && tasks\.length === 0/);

  assert.match(feedback, /export function ReadErrorState\(/);
  assert.match(feedback, /className="read-error-state" role="region" aria-label=\{title\} aria-busy=\{retrying\}/);
  assert.match(feedback, /disabled=\{retrying\} onClick=\{onRetry\}>\s*\{retrying \? "正在重新加载…" : "重新加载"\}/);
  assert.match(styles, /\.read-error-state\s*\{[^}]*width:\s*100%;[^}]*display:\s*grid;[^}]*place-content:\s*center;[^}]*justify-items:\s*center;[^}]*text-align:\s*center;/);
  assert.match(styles, /\.table-panel\.has-read-error \.table-scroll\s*\{[^}]*overflow-x:\s*hidden;/);
  assert.match(styles, /\.read-error-retry\s*\{[^}]*justify-self:\s*center;/);
  assert.doesNotMatch(styles, /\.table-read-error/);
});
