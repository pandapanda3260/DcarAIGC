// Run with DCAR_TEST_DOM_MODULE pointing at an installed linkedom package.
// This uses ReactDOM's real reconciler/effects without a browser or a production build.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import React, { act } from "react";
import { QueryClient, QueryClientProvider, useQuery, useQueryClient } from "@tanstack/react-query";
import ts from "typescript";

const require = createRequire(import.meta.url);
const domModule = process.env.DCAR_TEST_DOM_MODULE;
assert.ok(domModule, "Set DCAR_TEST_DOM_MODULE to the installed linkedom module path");
const { parseHTML } = require(domModule);
const { window } = parseHTML("<!doctype html><html><body><div id=\"root\"></div></body></html>");
window.location = new URL("https://workbench.example/overview");
for (const [name, value] of Object.entries({ window, document: window.document, HTMLElement: window.HTMLElement, Node: window.Node, Event: window.Event, navigator: { userAgent: "node" }, IS_REACT_ACT_ENVIRONMENT: true })) {
  Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
}
const { createRoot } = require("react-dom/client");
const appRoot = fileURLToPath(new URL("../app/", import.meta.url));

function setup({ simulateNavigation = false } = {}) {
  let pathname = "/overview";
  const counts = { session: 0, health: 0 };
  const prefetches = [], modules = [], mounts = {}, unmounts = {};
  const client = new QueryClient({ defaultOptions: { queries: { staleTime: 60_000, retry: false, gcTime: Infinity } } });
  const health = { status: "ok", read_only: true };
  const sessionOptions = { queryKey: ["auth", "session"], queryFn: async () => { counts.session++; return { username: "test-admin", role: "admin", authenticated: true }; } };
  const queries = {
    sessionQueryOptions: () => sessionOptions,
    defaultAccountSearchRequest: {}, defaultContentSearchRequest: {},
  };
  for (const [name, key] of Object.entries({ overviewQueryOptions: "overview", contentSearchQueryOptions: "contents", accountSearchQueryOptions: "accounts", activeSellingPointsQueryOptions: "selling-points", spuAssetsQueryOptions: "spu-assets", spuStatsQueryOptions: "spu-stats", tasksListQueryOptions: "tasks", usersQueryOptions: "users" })) {
    queries[name] = (...parameters) => ({ queryKey: [key, ...parameters], queryFn: async () => { prefetches.push([key, ...parameters]); return {}; } });
  }
  function Probe({ name, children }) {
    React.useEffect(() => {
      mounts[name] = (mounts[name] ?? 0) + 1;
      return () => { unmounts[name] = (unmounts[name] ?? 0) + 1; };
    }, [name]);
    return children ?? null;
  }
  const rsc = [];
  const router = { prefetch: (url) => rsc.push(url) };
  let navigationId = 0;
  const Link = (props) => {
    const attributes = { ...props };
    delete attributes.prefetch;
    attributes.onClick = (event) => {
      props.onClick?.(event);
      if (!simulateNavigation || event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.altKey || event.shiftKey) return;
      // Model the real router's start event, with the network left unresolved.
      React.startTransition(() => feedback.start(++navigationId, props.href, "", "navigate"));
    };
    return React.createElement("a", attributes);
  };
  const Image = (props) => { const attributes = { ...props }; delete attributes.unoptimized; return React.createElement("img", attributes); };
  const cache = new Map();
  function load(filename) {
    if (cache.has(filename)) return cache.get(filename).exports;
    const compiled = { exports: {} };
    cache.set(filename, compiled);
    const source = readFileSync(filename, "utf8");
    const output = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true } }).outputText;
    function localRequire(specifier) {
      if (specifier === "react" || specifier === "react/jsx-runtime") return require(specifier);
      if (specifier === "@tanstack/react-query") return { QueryClientProvider, useQuery, useQueryClient };
      if (specifier === "next/link") return Object.assign(Link, { useLinkStatus: () => ({ pending: false }) });
      if (specifier === "next/image") return Image;
      if (specifier === "next/navigation") return { usePathname: () => pathname, useRouter: () => router };
      const resolved = path.resolve(path.dirname(filename), specifier);
      if (resolved.endsWith(".module.css")) return new Proxy({}, { get: (_, key) => key });
      if (resolved === path.join(appRoot, "lib/queries")) return queries;
      if (resolved === path.join(appRoot, "lib/queryClient")) return { getQueryClient: () => client };
      if (resolved === path.join(appRoot, "lib/api")) return { readQueryJson: async () => { counts.health++; return health; } };
      if (resolved === path.join(appRoot, "components/ContentUpdateJobsProvider")) return function JobsProvider({ children }) { return React.createElement(Probe, { name: "jobs" }, children); };
      if (resolved === path.join(appRoot, "components/BackToTop")) return function BackToTop() { return React.createElement(Probe, { name: "back-to-top" }); };
      if (resolved === path.join(appRoot, "components/LogoutButton")) return function LogoutButton() { return React.createElement("button", { type: "button" }, "退出登录"); };
      if (resolved === path.join(appRoot, "components/Feedback")) return {
        ToastViewport: () => React.createElement(Probe, { name: "toasts" }),
        Loading: ({ label }) => React.createElement("div", { className: "loading-screen", role: "status" }, label),
        ReadErrorState: ({ title, description, onRetry }) => React.createElement("div", null, title, description, React.createElement("button", { onClick: onRetry }, "重新加载")),
      };
      if (/\/(overview|contents|accounts|selling-points|spu-audience|tasks|users)\/\w+Page$/.test(resolved)) { modules.push(specifier); return { default: () => null }; }
      const extension = path.extname(resolved) ? "" : /WorkbenchContext|WorkbenchChrome|AppShell|Providers|RouteLoading$/.test(resolved) ? ".tsx" : ".ts";
      return load(resolved + extension);
    }
    new Function("require", "module", "exports", output)(localRequire, compiled, compiled.exports);
    return compiled.exports;
  }
  const feedback = load(fileURLToPath(new URL("../scripts/navigation-feedback.mjs", import.meta.url))).navigationFeedbackStore;
  const Providers = load(path.join(appRoot, "components/Providers.tsx")).default;
  const AppShell = load(path.join(appRoot, "components/AppShell.tsx")).default;
  const RouteLoading = load(path.join(appRoot, "components/RouteLoading.tsx")).default;
  const ErrorPage = load(path.join(appRoot, "error.tsx")).default;
  const serviceState = load(path.join(appRoot, "lib/serviceStatus.ts")).dataServiceStatus(health, false);
  const { workbenchSection } = load(path.join(appRoot, "components/WorkbenchContext.tsx"));
  function PageContent({ url }) {
    const [filter, setFilter] = React.useState("全部");
    return React.createElement(React.Fragment, null,
      React.createElement("p", null, url),
      React.createElement("button", { "data-filter": true, onClick: () => setFilter("已筛选") }, filter));
  }
  const root = createRoot(document.getElementById("root"));
  return {
    client, counts, prefetches, modules, mounts, unmounts, rsc, workbenchSection, serviceState, feedback,
    async render(nextPath, state = "page") {
      pathname = nextPath;
      window.location = new URL(nextPath, "https://workbench.example");
      const active = workbenchSection(nextPath);
      const child = state === "loading" ? React.createElement(RouteLoading)
        : state === "error" ? React.createElement(ErrorPage, { error: new Error("private diagnostics"), reset: () => {} })
        : active ? React.createElement(AppShell, { active, key: nextPath }, React.createElement(Probe, { name: `page:${nextPath}` }, React.createElement(PageContent, { url: nextPath })))
        : React.createElement("p", null, "Independent route");
      await act(async () => { root.render(React.createElement(Providers, null, child)); });
      // Query notifications use timers; flush those in a separate act after effects start reads.
      await act(async () => { await new Promise((resolve) => setTimeout(resolve, 10)); });
    },
    async event(href, type, properties = {}) {
      const event = new window.Event(type, { bubbles: true, cancelable: true });
      Object.assign(event, { button: 0, detail: type === "click" ? 0 : 1, ...properties });
      await act(async () => { document.querySelector(`a[href="${href}"]`).dispatchEvent(event); });
    },
    async finish(id) { await act(async () => feedback.finish(id)); },
    async close() { await act(async () => root.unmount()); client.clear(); },
  };
}

test("real React lifecycle preserves sidebar, global UI and health observers while route content is replaced", async () => {
  const view = setup();
  try {
    await view.render("/overview");
    const sidebar = document.querySelector("aside");
    assert.ok(sidebar);
    assert.equal(document.querySelectorAll("main").length, 1);
    assert.ok(sidebar.textContent.includes(view.serviceState.label));
    assert.ok(sidebar.querySelector('[role="status"]').getAttribute("title"));
    assert.equal(sidebar.querySelector('[role="status"]').getAttribute("aria-label"), [view.serviceState.label, view.serviceState.description].filter(Boolean).join("。"));
    for (const url of ["/contents", "/tasks/task-1", "/accounts/douyin-authorization"]) {
      await view.render(url);
      assert.ok(document.querySelector("aside") === sidebar, "the existing sidebar DOM node must survive the route update");
      assert.equal(document.querySelectorAll("main").length, 1);
      assert.equal(document.querySelector('[aria-current="page"]').getAttribute("href"), `/${view.workbenchSection(url)}`);
    }
    assert.deepEqual(view.counts, { session: 1, health: 1 });
    assert.equal(view.client.getQueryCache().find({ queryKey: ["system", "health"] }).getObserversCount(), 1);
    assert.equal(view.mounts.toasts, 1);
    assert.equal(view.mounts["back-to-top"], 1);
    assert.equal(view.mounts.jobs, 1);
    assert.equal(view.unmounts["page:/overview"], 1);
    assert.equal(view.unmounts.toasts, undefined);
    await view.render("/contents", "loading");
    assert.ok(document.querySelector("aside") === sidebar, "the existing sidebar DOM node must survive the route update");
    assert.doesNotMatch(document.getElementById("root").textContent, /正在打开页面/);
    await view.render("/contents");
    await view.render("/contents", "error");
    assert.ok(document.querySelector("aside") === sidebar, "the existing sidebar DOM node must survive the route update");
    assert.match(document.querySelector("main").textContent, /页面暂时无法打开/);
    assert.doesNotMatch(document.querySelector("main").textContent, /private diagnostics/);
    await view.render("/login");
    assert.ok(document.querySelector("aside") === null, "independent routes must not include a sidebar");
    assert.equal(view.client.getQueryCache().find({ queryKey: ["system", "health"] }).getObserversCount(), 0);
    for (const url of [null, "/", "/unknown", "/contents/unknown", "/tasks/1/unknown"]) assert.equal(view.workbenchSection(url), null);
  } finally { await view.close(); }
});

test("pending navigation urgently replaces old content, handles rapid clicks and commits the final page", async () => {
  const view = setup({ simulateNavigation: true });
  try {
    await view.render("/overview");
    const sidebar = document.querySelector("aside");
    await view.event("/contents", "click");
    const firstId = view.feedback.getSnapshot().id;
    assert.equal(document.querySelector('main').dataset.section, "contents");
    assert.equal(document.querySelector('[aria-current="page"]').getAttribute("href"), "/contents");
    assert.equal(document.querySelector("h1").textContent, "发布内容明细");
    assert.ok(document.querySelector('[data-navigation-pending="contents"]'));
    assert.doesNotMatch(document.querySelector("main").textContent, /\/overview/);
    assert.equal(view.unmounts["page:/overview"], 1, "old page must unmount before the navigation response arrives");
    assert.equal(document.querySelectorAll("main").length, 1);
    assert.equal(document.querySelector("aside"), sidebar);

    await view.event("/tasks", "click");
    const secondId = view.feedback.getSnapshot().id;
    await view.finish(firstId);
    assert.equal(document.querySelector("h1").textContent, "日报、周报与自定义报告");
    assert.ok(document.querySelector('[data-navigation-pending="tasks"]'));
    await view.event("/overview", "click");
    const returnId = view.feedback.getSnapshot().id;
    assert.ok(returnId > secondId, "a click back to the still committed source is a new navigation");
    await view.finish(secondId);
    assert.ok(document.querySelector('[data-navigation-pending="overview"]'));
    await view.render("/overview");
    await view.finish(returnId);
    assert.ok(!document.querySelector("[data-navigation-pending]"));
    assert.match(document.querySelector("main").textContent, /\/overview/);

    await view.event("/contents", "click");
    const finalId = view.feedback.getSnapshot().id;
    await view.render("/contents");
    assert.ok(document.querySelector("[data-navigation-pending]"), "feedback remains until the router acknowledges its actual commit");
    await view.finish(finalId);
    assert.equal(document.querySelector("aside"), sidebar);
    assert.match(document.querySelector("main").textContent, /\/contents/);
    assert.ok(!document.querySelector("[data-navigation-pending]"));
    assert.deepEqual(view.counts, { session: 1, health: 1 });
  } finally { await view.close(); }
});

test("modified clicks leave the current page and permission changes cannot expose old content behind feedback", async () => {
  const view = setup({ simulateNavigation: true });
  try {
    await view.render("/users");
    for (const properties of [{ ctrlKey: true }, { metaKey: true }, { shiftKey: true }, { altKey: true }, { button: 1 }]) {
      await view.event("/tasks", "click", properties);
      assert.equal(view.feedback.getSnapshot(), null);
      assert.match(document.querySelector("main").textContent, /\/users/);
    }
    await view.event("/accounts", "click");
    assert.ok(document.querySelector('[data-navigation-pending="accounts"]'));
    await act(async () => view.client.setQueryData(["auth", "session"], { username: "different-operator", role: "operator", authenticated: true }));
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 10)); });
    assert.equal(document.querySelector('a[href="/accounts"]'), null);
    assert.equal(document.querySelector('a[href="/users"]'), null);
    assert.doesNotMatch(document.querySelector("main").textContent, /\/users/);
    assert.equal(view.unmounts["page:/users"], 1);
    assert.ok(document.querySelector('[data-navigation-pending="accounts"]'), "only public route metadata remains while the gateway resolves access");
  } finally { await view.close(); }
});

test("reselecting the settled current page preserves its real React filter state", async () => {
  const view = setup({ simulateNavigation: true });
  try {
    await view.render("/contents");
    const filter = document.querySelector("[data-filter]");
    await act(async () => filter.dispatchEvent(new window.Event("click", { bubbles: true })));
    assert.equal(filter.textContent, "已筛选");
    await view.event("/contents", "click");
    assert.equal(view.feedback.getSnapshot(), null);
    assert.equal(document.querySelector("[data-filter]"), filter);
    assert.equal(filter.textContent, "已筛选");
    assert.equal(view.mounts["page:/contents"], 1);
    assert.equal(view.unmounts["page:/contents"], undefined);
  } finally { await view.close(); }
});

test("pointer and keyboard navigation warm one permitted destination with RSC, JS and matching SPU queries", async () => {
  const view = setup();
  try {
    await view.render("/overview");
    assert.deepEqual(view.prefetches, []);
    await view.event("/spu-audience", "pointerdown");
    await view.event("/spu-audience", "click", { detail: 1 });
    assert.deepEqual(view.rsc, ["/spu-audience"]);
    assert.deepEqual(view.prefetches, [["spu-assets"], ["spu-stats", "last_week", ""]]);
    assert.equal(view.modules.length, 1);
    await view.event("/contents", "click");
    assert.deepEqual(view.rsc, ["/spu-audience", "/contents"]);
    assert.equal(view.prefetches.at(-1)[0], "contents");
    await view.event("/tasks", "click", { ctrlKey: true });
    assert.equal(view.rsc.length, 2);
    await view.render("/tasks/task-1");
    await view.event("/tasks", "click");
    assert.equal(view.rsc.at(-1), "/tasks");
    await view.event("/accounts", "focusin");
    await act(async () => view.client.setQueryData(["auth", "session"], { username: "test-admin", role: "operator", authenticated: true }));
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 10)); });
    assert.ok(document.querySelector('a[href="/accounts"]') === null, "account entry must be hidden from operators");
    assert.ok(document.querySelector('a[href="/users"]') === null, "user-management entry must be hidden from operators");
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 140)); });
    assert.ok(!view.rsc.includes("/accounts"), "permission changes cancel a pending hover/focus prefetch");
  } finally { await view.close(); }
});
