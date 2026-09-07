import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import React from "react";
import { QueryClient, QueryObserver } from "@tanstack/react-query";
import ts from "typescript";

const require = createRequire(import.meta.url);

// Compile in memory so these component tests do not rebuild the running Web app.
function loadComponent(relativePath, imports = {}) {
  const source = readFileSync(new URL(relativePath, import.meta.url), "utf8");
  const { outputText } = ts.transpileModule(source, {
    fileName: relativePath,
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2022,
      jsx: ts.JsxEmit.ReactJSX,
      esModuleInterop: true,
    },
  });
  const compiledModule = { exports: {} };
  const localRequire = (name) => {
    if (Object.hasOwn(imports, name)) return imports[name];
    if (name === "react" || name === "react/jsx-runtime") return require(name);
    throw new Error(`Unexpected component dependency: ${name}`);
  };
  new Function("require", "module", "exports", outputText)(localRequire, compiledModule, compiledModule.exports);
  return compiledModule.exports;
}

const feedback = loadComponent("../app/components/Feedback.tsx");

function allElements(node) {
  if (Array.isArray(node)) return node.flatMap(allElements);
  if (!React.isValidElement(node)) return [];
  return [node, ...allElements(node.props.children)];
}

function textContent(node) {
  if (Array.isArray(node)) return node.map(textContent).join("");
  if (React.isValidElement(node)) return textContent(node.props.children);
  return typeof node === "string" || typeof node === "number" ? String(node) : "";
}

function dashboard(tree) {
  return allElements(tree).find((node) => node.props.className === "page-stack overview-dashboard");
}

function errorState(tree) {
  const component = allElements(tree).find((node) => node.type === feedback.ReadErrorState);
  if (!component) return null;
  const rendered = feedback.ReadErrorState(component.props);
  return {
    title: component.props.title,
    text: textContent(rendered),
    button: allElements(rendered).find((node) => node.type === "button"),
  };
}

function hasLoading(tree) {
  return allElements(tree).some((node) => node.type === feedback.Loading);
}

function makeOverview(count = 37) {
  const window = {
    channels: { douyin: {}, xiaohongshu: {} },
    period_start: "2026-08-24",
    period_end: "2026-08-31",
    metrics: { publication_count: { value: count } },
    eligible_count: 25,
    unassociated_content_count: 3,
  };
  return {
    windows: { yesterday: window, this_week: window, last_week: window },
    data_quality: {
      missing_published_at: 2,
      duplicate_fingerprint_coverage: 100,
      confirmed_duplicate_count: 1,
      duplicate_calibration_ready: true,
    },
  };
}

function setup(t, cachedOverview) {
  const requests = [];
  const options = {
    queryKey: ["overview"],
    staleTime: 60_000,
    retry: false,
    queryFn: () => new Promise((resolve, reject) => requests.push({ resolve, reject })),
  };
  const client = new QueryClient({ defaultOptions: { queries: { gcTime: Infinity } } });
  if (cachedOverview) client.setQueryData(options.queryKey, cachedOverview);
  const observer = new QueryObserver(client, options);
  const unsubscribe = observer.subscribe(() => {});
  t.after(() => { unsubscribe(); client.clear(); });

  // Keep hook state across renders while using the real QueryObserver transitions.
  const state = [];
  let cursor = 0;
  const react = {
    ...React,
    useState(initial) {
      const index = cursor++;
      if (!(index in state)) state[index] = typeof initial === "function" ? initial() : initial;
      return [state[index], (value) => {
        state[index] = typeof value === "function" ? value(state[index]) : value;
      }];
    },
    useRef(initial) {
      const index = cursor++;
      if (!(index in state)) state[index] = { current: initial };
      return state[index];
    },
  };
  const { default: OverviewPage } = loadComponent("../app/overview/OverviewPage.tsx", {
    react,
    "@tanstack/react-query": { useQuery: () => observer.getCurrentResult() },
    "@phosphor-icons/react": new Proxy({}, { get: () => "svg" }),
    "next/image": "img",
    "../components/AppShell": "main",
    "../components/Feedback": feedback,
    "../lib/format": { formatDate: (date) => date, formatDateTime: (date) => date },
    "../lib/paths": { publicAssetPath: (path) => path },
    "../lib/queries": { overviewQueryOptions: () => options },
    "./OverviewReport": { OverviewChannelReport: "overview-channel" },
    "./OverviewReport.module.css": new Proxy({}, { get: (_, key) => String(key) }),
  });
  return {
    requests,
    observer,
    render() { cursor = 0; return OverviewPage(); },
  };
}

// Drain query completion and refetch.finally callbacks without real timers/network.
async function settle() {
  for (let index = 0; index < 12; index += 1) await Promise.resolve();
}

test("a first load shows loading and no placeholder quality cards", (t) => {
  const view = setup(t);
  const tree = view.render();
  assert.equal(view.requests.length, 1);
  assert.equal(hasLoading(tree), true);
  assert.equal(errorState(tree), null);
  assert.equal(dashboard(tree), undefined);
});

test("first-load errors persist through a failed retry and one click starts one request", async (t) => {
  const view = setup(t);
  view.requests[0].reject(new DOMException("读取超时，请重新加载。", "AbortError"));
  await settle();
  let tree = view.render();
  let failure = errorState(tree);
  assert.equal(failure.title, "概览读取失败");
  assert.match(failure.text, /读取超时/);
  assert.equal(failure.button.props.disabled, false);
  assert.equal(hasLoading(tree), false);
  assert.equal(dashboard(tree), undefined);

  failure.button.props.onClick();
  failure.button.props.onClick();
  assert.equal(view.requests.length, 2);
  assert.equal(view.observer.getCurrentResult().isPending, true);
  tree = view.render();
  failure = errorState(tree);
  assert.equal(failure.title, "概览读取失败");
  assert.equal(failure.button.props.disabled, true);
  assert.equal(hasLoading(tree), false);
  assert.equal(dashboard(tree), undefined);

  view.requests[1].reject(new Error("服务暂时不可用，请稍后重试。"));
  await settle();
  tree = view.render();
  failure = errorState(tree);
  assert.match(failure.text, /服务暂时不可用/);
  assert.equal(failure.button.props.disabled, false);
  assert.equal(hasLoading(tree), false);
  assert.equal(dashboard(tree), undefined);
});

test("a successful manual retry replaces the first-load error with real data", async (t) => {
  const view = setup(t);
  view.requests[0].reject(new Error("暂时无法读取。"));
  await settle();
  errorState(view.render()).button.props.onClick();
  view.requests[1].resolve(makeOverview());
  await settle();
  const tree = view.render();
  assert.equal(errorState(tree), null);
  assert.equal(hasLoading(tree), false);
  assert.ok(dashboard(tree));
  assert.match(textContent(dashboard(tree)), /37 条/);
});

test("a failed background refresh keeps cached data and a persistent retry action", async (t) => {
  const view = setup(t, makeOverview());
  assert.equal(view.requests.length, 0);
  const refresh = view.observer.refetch();
  view.requests[0].reject(new Error("数据读取失败。"));
  await refresh;
  await settle();
  let tree = view.render();
  let failure = errorState(tree);
  assert.equal(failure.title, "数据刷新失败，当前显示上次数据。");
  assert.match(textContent(dashboard(tree)), /37 条/);
  assert.equal(hasLoading(tree), false);

  failure.button.props.onClick();
  tree = view.render();
  failure = errorState(tree);
  assert.equal(failure.button.props.disabled, true);
  assert.match(textContent(dashboard(tree)), /37 条/);
  view.requests[1].resolve(makeOverview(42));
  await settle();
  tree = view.render();
  assert.equal(errorState(tree), null);
  assert.match(textContent(dashboard(tree)), /42 条/);
});

test("switching the existing date window uses its cached channel data without another request", (t) => {
  const overview = makeOverview();
  overview.windows.yesterday = { ...overview.windows.yesterday,
    metrics: { publication_count: { value: 8 } }, channels: { douyin: { sample: "yesterday" }, xiaohongshu: {} } };
  const view = setup(t, overview);
  let tree = view.render();
  assert.match(textContent(dashboard(tree)), /37 条/);
  const buttons = allElements(tree.props.actions).filter((node) => node.type === "button");
  assert.deepEqual(buttons.map((node) => textContent(node)), ["昨天", "本周", "上周"]);
  buttons[0].props.onClick();
  tree = view.render();
  assert.match(textContent(dashboard(tree)), /8 条/);
  const channels = allElements(dashboard(tree)).filter((node) => node.type === "overview-channel");
  assert.equal(channels[0].props.channel.sample, "yesterday");
  assert.match(channels[0].key, /^yesterday-/);
  assert.equal(view.requests.length, 0);
});

test("this-week cutoff includes the current day and its actual time", (t) => {
  const overview = makeOverview();
  overview.windows.this_week = { ...overview.windows.this_week, period_end: "2026-09-06T14:35:00+08:00" };
  const view = setup(t, overview);
  const tree = view.render();
  const buttons = allElements(tree.props.actions).filter((node) => node.type === "button");
  buttons[1].props.onClick();
  const content = textContent(dashboard(view.render()));
  assert.match(content, /统计截止（北京时间）2026-09-06T14:35:00\+08:00/);
  assert.doesNotMatch(content, /统计到此日期前一天/);
});
