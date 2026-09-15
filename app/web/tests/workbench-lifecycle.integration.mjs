// Run with DCAR_TEST_DOM_MODULE pointing at an installed linkedom package.
// This uses ReactDOM's real reconciler/effects without a browser or a production build.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import React, { act } from "react";
import { QueryClient, QueryClientProvider, skipToken, useQuery, useQueryClient } from "@tanstack/react-query";
import ts from "typescript";

const require = createRequire(import.meta.url);
const domModule = process.env.DCAR_TEST_DOM_MODULE;
assert.ok(domModule, "Set DCAR_TEST_DOM_MODULE to the installed linkedom module path");
const { parseHTML } = require(domModule);
const { window } = parseHTML("<!doctype html><html><body><div id=\"root\"></div></body></html>");
// Linkedom has no native text-selection engine; retain the requested range so
// the real nickname editor can run its focus effect in this DOM harness.
window.HTMLTextAreaElement.prototype.setSelectionRange = function (start, end) {
  this.selectionStart = start;
  this.selectionEnd = end;
};
window.location = new URL("https://workbench.example/overview");
window.requestAnimationFrame = (callback) => setTimeout(callback, 0);
window.cancelAnimationFrame = clearTimeout;
for (const [name, value] of Object.entries({ window, document: window.document, HTMLElement: window.HTMLElement, Node: window.Node, Event: window.Event, navigator: { userAgent: "node" }, IS_REACT_ACT_ENVIRONMENT: true })) {
  Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
}
const { createRoot } = require("react-dom/client");
const appRoot = fileURLToPath(new URL("../app/", import.meta.url));

function setup({ simulateNavigation = false, profileFailure = false, includeFreshness = false, initialHealth = { status: "ok", read_only: true } } = {}) {
  let pathname = "/overview";
  const counts = { session: 0, health: 0 };
  const prefetches = [], modules = [], mounts = {}, unmounts = {};
  const profileRequests = [];
  const workbenchStates = [];
  const client = new QueryClient({ defaultOptions: { queries: { staleTime: 60_000, retry: false, gcTime: Infinity } } });
  let health = initialHealth;
  let healthFailure = false;
  const sessionOptions = { queryKey: ["auth", "session"], queryFn: async () => { counts.session++; return { username: "test-admin", role: "admin", authenticated: true }; } };
  const queries = {
    sessionQueryOptions: () => sessionOptions,
    queryKeys: { session: ["auth", "session"], users: ["auth", "users"] },
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
      if (specifier === "@tanstack/react-query") return { QueryClientProvider, skipToken, useQuery, useQueryClient };
      if (specifier === "next/link") return Object.assign(Link, { useLinkStatus: () => ({ pending: false }) });
      if (specifier === "next/image") return Image;
      if (specifier === "next/navigation") return { usePathname: () => pathname, useRouter: () => router };
      const resolved = path.resolve(path.dirname(filename), specifier);
      if (resolved.endsWith(".module.css")) return new Proxy({}, { get: (_, key) => key });
      if (resolved === path.join(appRoot, "lib/queries")) return queries;
      if (resolved === path.join(appRoot, "lib/queryClient")) return { getQueryClient: () => client };
      if (resolved === path.join(appRoot, "lib/api")) return {
        isAbortError: (reason) => reason?.name === "AbortError",
        requireApprovedSession: (session) => session,
        markedJsonRequest: (body) => ({ body: JSON.stringify(body) }),
        readQueryJson: async (url, options) => {
          if (url === "/auth/profile") {
            const body = JSON.parse(options.body);
            profileRequests.push(body);
            if (profileFailure) throw new Error("保存失败，请重试。");
            return { authenticated: true, username: "test-admin", role: "admin", ...body };
          }
          counts.health++;
          if (healthFailure) throw new Error("Health service unavailable");
          return health;
        },
      };
      if (resolved === path.join(appRoot, "components/ContentUpdateJobsProvider")) return function JobsProvider({ children }) { return React.createElement(Probe, { name: "jobs" }, children); };
      if (resolved === path.join(appRoot, "components/BackToTop")) return function BackToTop() { return React.createElement(Probe, { name: "back-to-top" }); };
      if (resolved === path.join(appRoot, "components/LogoutButton")) return function LogoutButton() { return React.createElement("button", { type: "button" }, "退出登录"); };
      if (resolved === path.join(appRoot, "components/Feedback")) return {
        showToast() {},
        ToastViewport: () => React.createElement(Probe, { name: "toasts" }),
        Loading: ({ label }) => React.createElement("div", { className: "loading-screen", role: "status" }, label),
        ReadErrorState: ({ title, description, onRetry }) => React.createElement("div", null, title, description, React.createElement("button", { onClick: onRetry }, "重新加载")),
      };
      if (/\/(overview|contents|accounts|selling-points|spu-audience|tasks|users)\/\w+Page$/.test(resolved)) { modules.push(specifier); return { default: () => null }; }
      const extension = path.extname(resolved) ? "" : /WorkbenchContext|WorkbenchChrome|AppShell|Providers|RouteLoading|InlineNicknameEditor|DataFreshnessNote$/.test(resolved) ? ".tsx" : ".ts";
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
  const DataFreshnessNote = load(path.join(appRoot, "components/DataFreshnessNote.tsx")).default;
  const serviceState = load(path.join(appRoot, "lib/serviceStatus.ts")).dataServiceStatus(health, false);
  const { workbenchSection, useWorkbench } = load(path.join(appRoot, "components/WorkbenchContext.tsx"));
  function PageContent({ url }) {
    workbenchStates.push(useWorkbench());
    const [filter, setFilter] = React.useState("全部");
    return React.createElement(React.Fragment, null,
      React.createElement("p", null, url),
      includeFreshness && React.createElement("section", { "data-loaded-data": true }, React.createElement(DataFreshnessNote)),
      React.createElement("button", { "data-filter": true, onClick: () => setFilter("已筛选") }, filter));
  }
  const root = createRoot(document.getElementById("root"));
  return {
    client, counts, prefetches, modules, mounts, unmounts, rsc, workbenchSection, serviceState, workbenchStates, feedback, profileRequests,
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
    async refreshHealth(nextHealth, { fail = false } = {}) {
      health = nextHealth;
      healthFailure = fail;
      await act(async () => { await client.refetchQueries({ queryKey: ["system", "health"], exact: true, type: "active" }); });
      await act(async () => { await new Promise((resolve) => setTimeout(resolve, 10)); });
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
    assert.equal(sidebar.textContent.includes(view.serviceState.label), false, "normal system status must not be permanently shown beside the account");
    assert.equal(sidebar.querySelector('[role="status"]'), null);
    assert.deepEqual(view.workbenchStates.at(-1).serviceState, view.serviceState, "health information remains available to page content");
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

test("data freshness notes share health requests, remain dismissed during polling, reset after recovery and defer failed refreshes to the error banner", async () => {
  const current = {
    status: "ok", read_only: false,
    automation: { scheduler_state: "running", paid_dispatch_state: "open" },
    data_freshness: { status: "current", last_successful_capture_at: "2026-09-13T00:00:00Z" },
  };
  const stale = { ...current, data_freshness: { ...current.data_freshness, status: "stale" } };
  const view = setup({ includeFreshness: true, initialHealth: current });
  const note = () => document.querySelector("[data-freshness-note]");
  const click = async (target) => {
    await act(async () => { target.dispatchEvent(new window.Event("click", { bubbles: true })); });
  };
  try {
    await view.render("/overview");
    const query = view.client.getQueryCache().find({ queryKey: ["system", "health"] });
    assert.equal(query.getObserversCount(), 2, "the data note observes the same health query as the shell");
    assert.equal(view.counts.health, 1, "mounting the disabled note observer must not issue another request");
    assert.equal(note(), null, "healthy data remains silent");
    assert.equal(document.querySelector('main [role="status"]'), null);

    await view.refreshHealth(stale);
    assert.equal(view.counts.health, 2);
    assert.ok(note().closest("[data-loaded-data]"), "the notice belongs beside loaded data");
    assert.match(note().textContent, /数据更新延迟，当前展示已有数据/);
    assert.match(note().textContent, /最近成功采集：2026\/09\/13 08:00（北京时间）/);
    assert.equal(document.querySelector('aside [role="status"]'), null);
    await click(note().querySelector('button[aria-label="关闭数据时效提示"]'));
    assert.equal(note(), null);

    await view.refreshHealth({ ...stale, automation: { ...stale.automation, report_from_date: "2026-09-01" } });
    assert.equal(view.counts.health, 3);
    assert.equal(note(), null, "polling with unchanged notice text preserves dismissal");
    await view.refreshHealth(current);
    assert.equal(note(), null);
    await view.refreshHealth(stale);
    assert.equal(view.counts.health, 5);
    assert.ok(note(), "a confirmed recovery allows the same delay to be reported again");

    await view.refreshHealth(stale, { fail: true });
    assert.equal(view.counts.health, 6, "each explicit health refresh sends one request even with two observers");
    assert.equal(query.state.data.data_freshness.status, "stale", "a failed request leaves stale data in cache");
    assert.equal(query.state.status, "error");
    assert.equal(note(), null, "cached stale health must not masquerade as a confirmed data delay after a failed request");
    const banner = document.querySelector('main > [role="status"]');
    assert.ok(banner, "the existing page-level service failure banner is preserved");
    assert.match(banner.textContent, /服务异常/);
    assert.match(banner.textContent, /数据服务不可用/);
    assert.equal(banner.hidden, false);
    await click(banner.querySelector('button[aria-label="关闭提示"]'));
    assert.equal(banner.hidden, true, "the existing error notification remains dismissible");
    assert.equal(view.counts.health, 6);
  } finally { await view.close(); }
});

test("real inline editor retains a failed draft through session refresh and navigation, handles IME, and cancels in place", async () => {
  const view = setup({ profileFailure: true });
  const dispatch = async (target, type, properties = {}) => {
    const event = new window.Event(type, { bubbles: true, cancelable: true });
    Object.assign(event, properties);
    await act(async () => target.dispatchEvent(event));
    return event;
  };
  try {
    await view.render("/overview");
    const pencil = document.querySelector('button[aria-label="编辑昵称"]');
    assert.ok(pencil);
    assert.equal(pencil.closest("button"), pencil, "only the small edit control is a button");
    assert.equal(pencil.previousElementSibling.hasAttribute("title"), false);
    await dispatch(pencil, "click");
    const field = document.querySelector('textarea[aria-label="昵称"]');
    assert.ok(field);
    assert.equal(document.querySelector('[role="dialog"]'), null);
    assert.equal(document.querySelector('button[aria-label="编辑昵称"]'), pencil, "editing keeps the identity row and pencil's layout slot");
    assert.equal(pencil.previousElementSibling.textContent, "test-admin");
    assert.equal(pencil.getAttribute("aria-hidden"), "true");
    // Linkedom has no browser input/composition engine. Drive React's handlers
    // attached to the real textarea; its reconciler, state and effects stay real.
    const fieldProps = () => field[Object.keys(field).find((key) => key.startsWith("__reactProps$"))];
    const sendKey = async (key, properties = {}) => act(async () => fieldProps().onKeyDown({ key, shiftKey: false, nativeEvent: { keyCode: key === "Enter" ? 13 : 27 }, preventDefault() {}, ...properties }));
    await act(async () => fieldProps().onChange({ target: { value: "草稿🚗\n第二行" } }));
    await act(async () => fieldProps().onCompositionStart());
    await sendKey("Enter");
    await sendKey("Escape");
    assert.equal(view.profileRequests.length, 0);
    assert.equal(document.querySelector("textarea"), field);
    await act(async () => fieldProps().onCompositionEnd());
    await sendKey("Enter", { nativeEvent: { keyCode: 229 } });
    await sendKey("Enter", { shiftKey: true });
    assert.equal(view.profileRequests.length, 0);
    await act(async () => fieldProps().onBlur?.({}));
    await act(async () => view.client.setQueryData(["auth", "session"], { authenticated: true, username: "test-admin", role: "admin", display_name: "后台新昵称" }));
    await view.render("/contents");
    assert.equal(document.querySelector("textarea"), field);
    assert.equal(field.value, "草稿🚗\n第二行");
    await sendKey("Enter");
    assert.deepEqual(view.profileRequests, [{ display_name: "草稿🚗\n第二行" }]);
    assert.equal(field.value, "草稿🚗\n第二行");
    assert.equal(field.disabled, false);
    assert.match(document.querySelector('[role="alert"]').textContent, /保存失败/);
    await sendKey("Escape");
    assert.equal(document.querySelector("textarea"), null);
    assert.equal(document.querySelector('button[aria-label="编辑昵称"]').previousElementSibling.textContent, "后台新昵称");
    assert.equal(pencil.hasAttribute("aria-hidden"), false);
    assert.equal(view.profileRequests.length, 1);
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
