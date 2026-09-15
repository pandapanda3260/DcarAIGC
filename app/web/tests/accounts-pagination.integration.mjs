// Real React effects and TanStack queries with delayed HTTP responses.
// Run with DCAR_TEST_DOM_MODULE pointing to an installed linkedom package.
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import ts from "typescript";

const require = createRequire(import.meta.url);
assert.ok(process.env.DCAR_TEST_DOM_MODULE, "Set DCAR_TEST_DOM_MODULE to the installed linkedom package");
const { parseHTML } = require(process.env.DCAR_TEST_DOM_MODULE);
const { window } = parseHTML("<!doctype html><html><body><div id='root'></div></body></html>");
window.location = new URL("https://workbench.example/accounts");
window.requestAnimationFrame = (callback) => setTimeout(callback, 0);
window.cancelAnimationFrame = clearTimeout;
for (const [name, value] of Object.entries({ window, document: window.document, HTMLElement: window.HTMLElement,
  Node: window.Node, Event: window.Event, navigator: { userAgent: "node" }, IS_REACT_ACT_ENVIRONMENT: true })) {
  Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
}
const React = require("react"), { act } = React;
const { createRoot } = require("react-dom/client");
const Query = require("@tanstack/react-query");
const appRoot = fileURLToPath(new URL("../app/", import.meta.url));
const requestFor = (page) => ({ page, page_size: 50, query: "", platform: null, account_group: null, business_direction: null, account_status: null });
function account(page) {
  return { id: -page, directory_row_id: page, locator_sha256: "old-cas", directory_identity_status: "identity_missing",
    phone: "", operator_name: "", account_group: "unknown", business_direction: "unknown", account_status: "unmarked", enabled: true,
    platforms: [{ id: page, platform: "douyin", nickname: `页面${page}账号`, uid: null, unique_id: null,
      follower_count: null, platform_work_count: null, content_count: 0 }] };
}
function responseFor(page, options = {}) {
  return { items: [account(page)], total: 665, account_management_version: 2, list_contract_version: 1,
    roster: { source_family: "system", active_profile_id: "integrated_route_v1" }, ...options };
}

async function setup({ access = false } = {}) {
  const client = new Query.QueryClient({ defaultOptions: { queries: { staleTime: 60_000, gcTime: Infinity, retry: false } } });
  const requests = [], modules = new Map();
  let authSession = { username: "first", role: "admin" };
  const oldFetch = globalThis.fetch;
  globalThis.fetch = (url, init = {}) => new Promise((resolve, reject) => {
    if (url === "/auth/session") { resolve(Response.json(authSession)); return; }
    const request = { url, payload: init.body ? JSON.parse(init.body) : null, signal: init.signal, done: false,
      resolve(body, status = 200) { request.done = true; resolve(Response.json(body, { status })); } };
    init.signal?.addEventListener("abort", () => { request.aborted = true; reject(init.signal.reason); }, { once: true });
    requests.push(request);
  });
  function load(filename) {
    if (modules.has(filename)) return modules.get(filename).exports;
    const moduleRecord = { exports: {} }; modules.set(filename, moduleRecord);
    const compiled = ts.transpileModule(readFileSync(filename, "utf8"), { compilerOptions: {
      target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true,
    } }).outputText;
    function localRequire(specifier) {
      if (specifier === "react" || specifier === "react/jsx-runtime") return require(specifier);
      if (specifier === "@tanstack/react-query") return Query;
      if (specifier === "next/image") return function Image(props) { const copy = { ...props }; delete copy.unoptimized; return React.createElement("img", copy); };
      if (specifier === "next/link") return function Link({ children, ...props }) { return React.createElement("a", props, children); };
      if (specifier === "@phosphor-icons/react") return new Proxy({}, { get: () => () => React.createElement("svg") });
      const resolved = path.resolve(path.dirname(filename), specifier);
      if (resolved.endsWith(".module.css")) return new Proxy({}, { get: (_, key) => key });
      if (resolved === path.join(appRoot, "components/AppShell")) return function AppShell({ children, header }) { return React.createElement("main", null, header, children); };
      if (resolved === path.join(appRoot, "components/AccountPageAccess") && !access) return ({ children }) => children;
      if (resolved === path.join(appRoot, "components/DataFreshnessNote")) return () => null;
      if (resolved === path.join(appRoot, "components/Feedback")) return {
        Feedback: ({ error, message }) => React.createElement("aside", null, error, message),
        Loading: ({ label }) => React.createElement("p", null, label),
        Notice: ({ children }) => React.createElement("aside", null, children),
        ReadErrorState: ({ onRetry }) => React.createElement("button", { onClick: onRetry }, "重试"),
      };
      if (resolved === path.join(appRoot, "accounts/AccountDialogLayout")) return function Dialog({ children, onClose }) { return React.createElement("section", { role: "dialog" }, children, React.createElement("button", { onClick: onClose }, "关闭")); };
      if (resolved === path.join(appRoot, "accounts/AccountSummaryDialog")) return function Summary({ account, onClose }) { return React.createElement("section", { role: "dialog", "data-summary": account.directory_row_id }, account.account_summary?.fields?.账号名称, React.createElement("button", { onClick: onClose }, "关闭")); };
      if (resolved === path.join(appRoot, "accounts/AccountOperationsDialog")) return function Operations({ account, form, onClose }) { return React.createElement("section", { role: "dialog", "data-edit": account.directory_row_id }, form.phone, React.createElement("button", { onClick: onClose }, "关闭")); };
      if (resolved === path.join(appRoot, "accounts/RepairAccountIdentityDialog")) return function Repair({ account, onClose }) { return React.createElement("section", { role: "dialog", "data-repair": account.directory_row_id }, account.locator_sha256, React.createElement("button", { onClick: onClose }, "关闭")); };
      if (resolved === path.join(appRoot, "accounts/CreateAccountDialog")) return () => null;
      const actual = [resolved, `${resolved}.ts`, `${resolved}.tsx`].find((file) => existsSync(file));
      assert.ok(actual, `Missing module ${specifier}`);
      return load(actual);
    }
    new Function("require", "module", "exports", compiled)(localRequire, moduleRecord, moduleRecord.exports);
    return moduleRecord.exports;
  }
  const Component = load(path.join(appRoot, "accounts/AccountsPage.tsx")).default;
  const queries = load(path.join(appRoot, "lib/queries.ts"));
  if (access) {
    load(path.join(appRoot, "lib/queryClient.ts")).bindNavigationSession(client, () => {});
    client.setQueryData(queries.queryKeys.session, authSession);
  }
  const root = createRoot(document.getElementById("root"));
  function seed(page, updatedAt = Date.now(), options = {}) {
    client.setQueryData(queries.queryKeys.accountSearch(requestFor(page)), { ...responseFor(page, options), sourceRequest: requestFor(page) }, { updatedAt });
  }
  const settle = async (ms = 0) => { await act(async () => { await new Promise((resolve) => setTimeout(resolve, ms)); }); };
  const mount = async () => { await act(async () => { root.render(React.createElement(Query.QueryClientProvider, { client }, React.createElement(Component))); }); await settle(); };
  async function click(label) {
    const element = [...document.querySelectorAll("button")].find((button) => button.getAttribute("aria-label") === label || button.textContent === label);
    assert.ok(element, `Missing button: ${label}`); assert.equal(element.disabled, false, `Disabled button: ${label}`);
    await act(async () => { element.dispatchEvent(new window.Event("click", { bubbles: true })); });
    await settle();
  }
  async function intent(label, type = "pointerover", delay = 150) {
    const element = [...document.querySelectorAll("button")].find((button) => button.getAttribute("aria-label") === label);
    assert.ok(element, `Missing intent target: ${label}`);
    await act(async () => { element.dispatchEvent(new window.Event(type, { bubbles: true })); });
    await settle(delay);
  }
  async function filter(group) {
    const element = document.querySelector('[aria-label="账号分组筛选"]');
    Object.defineProperty(element, "value", { configurable: true, value: group });
    await act(async () => { element.dispatchEvent(new window.Event("change", { bubbles: true })); });
    await settle();
  }
  const resolve = async (request, body, status) => { assert.ok(request); await act(async () => { request.resolve(body, status); }); await settle(); };
  return { client, requests, seed, mount, settle, click, intent, filter, resolve,
    async changeSession(next) { authSession = next; await act(async () => { client.setQueryData(queries.queryKeys.session, next); }); await settle(); },
    pageRequest: (page) => requests.findLast((request) => request.payload?.page === page),
    text: () => document.querySelector("main").textContent,
    async close() { await act(async () => { root.unmount(); client.clear(); }); globalThis.fetch = oldFetch; },
  };
}

test("stale cached pages remain interactive; the final page intent wins and cancels obsolete work", async () => {
  const view = await setup();
  try {
    view.seed(1); view.seed(2, Date.now() - 61_000); await view.mount();
    await view.click("第 2 页");
    assert.match(view.text(), /页面2账号/);
    assert.match(view.text(), /第 2 页已显示，正在更新/);
    await view.click("第 3 页");
    assert.equal(view.pageRequest(2).signal.aborted, true);
    assert.match(view.text(), /正在读取第 3 页.*第 2 页/);
    assert.equal(document.querySelector('[aria-label="修改页面2账号的运营信息"]').disabled, true);
    await view.click("第 4 页");
    assert.equal(view.pageRequest(3).signal.aborted, true);
    await view.resolve(view.pageRequest(4), responseFor(4));
    await view.resolve(view.pageRequest(3), responseFor(3));
    assert.match(view.text(), /页面4账号/); assert.doesNotMatch(view.text(), /页面3账号/);
  } finally { await view.close(); }
});

test("a failed page keeps the last completed page and permits returning to it", async () => {
  const view = await setup();
  try {
    view.seed(1); await view.mount(); await view.click("第 2 页");
    await view.resolve(view.pageRequest(2), { detail: "读取失败。" }, 503);
    assert.match(view.text(), /第 2 页读取失败.*第 1 页/); assert.match(view.text(), /页面1账号/);
    await view.click("继续查看第 1 页");
    assert.equal(document.querySelector('[aria-label="第 1 页"]').getAttribute("aria-current"), "page");
    assert.equal(view.requests.filter((request) => request.payload?.page === 1).length, 0, "fresh cached page needs no read");
  } finally { await view.close(); }
});

test("only the next page is prefetched and a foreground click reuses its in-flight query", async () => {
  const view = await setup();
  try {
    view.seed(1); await view.mount(); await view.settle(150);
    assert.deepEqual(view.requests.map((request) => request.payload?.page), [2]);
    assert.equal(view.pageRequest(2).payload.compact, true);
    await view.click("第 2 页");
    assert.equal(view.pageRequest(2).signal.aborted, false);
    assert.equal(view.requests.filter((request) => request.payload?.page === 2).length, 1);
    await view.resolve(view.pageRequest(2), responseFor(2)); await view.settle(150);
    assert.deepEqual(view.requests.map((request) => request.payload?.page), [2, 3]);
    await view.click("第 14 页");
    assert.equal(view.pageRequest(3).signal.aborted, true, "unused next-page work is cancelled on new intent");
  } finally { await view.close(); }
});

test("compact rows fetch full current details by directory id for summary, edit and identity repair", async () => {
  const view = await setup();
  try {
    view.seed(1); await view.mount();
    const full = { ...account(1), phone: "latest-phone", locator_sha256: "latest-cas", account_summary: { fields: { 账号名称: "完整导入资料" } } };
    for (const [button, expected] of [["查看页面1账号的导入资料", "完整导入资料"], ["修改页面1账号的运营信息", "latest-phone"], ["补充身份", "latest-cas"]]) {
      const previousCount = view.requests.length;
      await view.click(button); assert.match(view.text(), /正在读取账号资料/);
      const request = view.requests.slice(previousCount).find((request) => request.url === "/api/v8/accounts/directory/1");
      assert.ok(request, "detail reads use the positive directory id, not negative Account.id");
      await view.resolve(request, full);
      assert.match(document.querySelector('[role="dialog"]').textContent, new RegExp(expected));
      await view.click("关闭");
    }
    await view.click("查看页面1账号的导入资料");
    const request = view.requests.findLast((request) => request.url === "/api/v8/accounts/directory/1");
    await view.click("关闭"); assert.equal(request.signal.aborted, true);
    await view.resolve(request, full);
    assert.equal(document.querySelector('[role="dialog"]'), null, "late completion cannot reopen a closed dialog");
  } finally { await view.close(); }
});

test("permission scope changes discard both cached results and a locally retained failed-page snapshot", async () => {
  const view = await setup({ access: true });
  try {
    view.seed(1); await view.mount(); await view.click("第 2 页");
    await view.resolve(view.pageRequest(2), { detail: "读取失败。" }, 503);
    assert.match(view.text(), /页面1账号/);
    await view.changeSession({ username: "first", role: "superadmin" });
    assert.doesNotMatch(view.text(), /页面1账号/);
    assert.match(view.text(), /正在读取账号库/);
    const currentRead = view.pageRequest(1);
    assert.ok(currentRead, "new scope starts at its own initial page");
    await view.resolve(currentRead, responseFor(1, { items: [{ ...account(1), platforms: [{ ...account(1).platforms[0], nickname: "新权限结果" }] }] }));
    assert.match(view.text(), /新权限结果/);
  } finally { await view.close(); }
});

test("hover and keyboard focus share one speculative slot and preserve only the last queued target", async () => {
  const view = await setup();
  try {
    view.seed(1, Date.now(), { total: 350 }); await view.mount(); await view.settle(150);
    await view.intent("第 7 页");
    await view.intent("第 6 页", "focusin");
    assert.deepEqual(view.requests.map((request) => request.payload?.page), [2], "hover/focus do not compete with the running next-page read");
    await view.resolve(view.pageRequest(2), responseFor(2, { total: 350 }));
    assert.deepEqual(view.requests.map((request) => request.payload?.page), [2, 6], "only the final explicit target starts when the slot is free");
    assert.equal(view.requests.filter((request) => !request.done && !request.signal.aborted).length, 1);
    await view.click("第 6 页");
    assert.equal(view.pageRequest(6).signal.aborted, false);
    assert.equal(view.requests.filter((request) => request.payload?.page === 6).length, 1, "foreground navigation adopts the focused page read");
    await view.resolve(view.pageRequest(6), responseFor(6, { total: 350 }));
    assert.match(view.text(), /页面6账号/);
  } finally { await view.close(); }
});

test("changing filters cancels running speculation and discards a queued hover target", async () => {
  const view = await setup();
  try {
    view.seed(1, Date.now(), { total: 350 }); await view.mount(); await view.settle(150);
    const nextRead = view.pageRequest(2);
    await view.intent("第 7 页");
    await view.filter("innovation");
    assert.equal(nextRead.signal.aborted, true);
    assert.equal(view.requests.some((request) => request.payload?.page === 7), false);
    const filtered = view.pageRequest(1);
    assert.equal(filtered.payload.account_group, "innovation");
    await view.resolve(nextRead, responseFor(2, { total: 350 }));
    await view.settle(150);
    assert.equal(view.requests.some((request) => request.payload?.page === 7), false, "completion from an obsolete generation cannot restart its queued target");
  } finally { await view.close(); }
});

test("legacy list responses enable neither automatic nor hover/focus speculation", async () => {
  const view = await setup();
  try {
    view.seed(1, Date.now(), { total: 350, list_contract_version: undefined }); await view.mount();
    await view.intent("第 7 页"); await view.intent("第 6 页", "focusin");
    assert.equal(view.requests.length, 0);
  } finally { await view.close(); }
});
