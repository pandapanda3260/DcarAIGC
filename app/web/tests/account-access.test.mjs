import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import React from "react";
import * as jsxRuntime from "react/jsx-runtime";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";

const appRoot = fileURLToPath(new URL("../app/", import.meta.url));
const phone = "13212343053";
const protectedName = "仅管理员可见的测试账号";
const accountData = {
  total: 2, page: 1, page_size: 50,
  roster: { active_profile_id: "tikhub_managed_v1" },
  items: [phone, ""].map((number, index) => ({
    id: index + 1, phone: number, operator_name: "测试运营人员",
    account_type: "original", content_direction: "new_car", enabled: true,
    account_status: "daily", roster_state: "current", content_count: 3,
    platforms: [{ platform: "xiaohongshu", uid: `test-platform-uid-${index}`,
      nickname: `${protectedName}${index}`, unique_id: "", data_status: "not_collected" }],
  })),
};

// Render the real page, shell, access rule and query contracts. Only framework
// boundaries are controlled; the accounts cache stays populated for every role.
function harness({ role, sessionState = "ready", basePath = "" } = {}) {
  const queries = [], prefetches = [], links = [], effects = [], destinations = [];
  const timers = new Map();
  let timerId = 0;
  const cache = new Map();
  const schedule = (callback, delay) => {
    const id = ++timerId;
    timers.set(id, { callback, delay });
    return id;
  };
  const cancel = (id) => timers.delete(id);
  const queryClient = {
    prefetchQuery: async (options) => { prefetches.push(options.queryKey); },
    invalidateQueries: async () => {},
  };
  const queryRuntime = {
    queryOptions: (options) => options,
    keepPreviousData: (data) => data,
    useQueryClient: () => queryClient,
    useQuery: (options) => {
      queries.push(options.queryKey);
      const result = { isPending: false, isError: false, isLoadingError: false,
        isPlaceholderData: false, refetch: async () => {} };
      if (options.queryKey[0] === "auth" && options.queryKey[1] === "session") {
        return { ...result,
          data: sessionState === "ready" ? { authenticated: true, role } : undefined,
          isPending: sessionState === "pending", isError: sessionState === "error",
          error: sessionState === "error" ? new Error("session unavailable") : null };
      }
      if (options.queryKey[0] === "accounts") return { ...result, data: accountData };
      if (options.queryKey[0] === "system") return { ...result, data: { status: "ok" } };
      throw new Error(`Unexpected query: ${JSON.stringify(options.queryKey)}`);
    },
  };
  const Link = (props) => {
    links.push(props);
    const { prefetch: _prefetch, ...attributes } = props;
    return React.createElement("a", attributes);
  };
  const Image = (props) => {
    const attributes = { ...props };
    delete attributes.unoptimized;
    return React.createElement("img", attributes);
  };
  const message = ({ children, label, title, description }) => React.createElement(
    "div", { role: "status" }, children ?? label ?? title, description,
  );
  const feedback = { Feedback: () => null, ToastViewport: () => null,
    Loading: message, Notice: message, ReadErrorState: message };
  const api = { ApiRequestError: class extends Error {},
    readQueryJson: () => { throw new Error("Network query unexpectedly executed"); },
    apiErrorMessage: (error) => String(error), apiUrl: (url) => url,
    requireApprovedSession: (session) => session, handleApprovalRequired: () => false,
    jsonRequest: (body) => ({ body }), readJson: () => { throw new Error("Unexpected mutation"); } };
  const context = vm.createContext({
    console, URLSearchParams, process: { env: { NEXT_PUBLIC_DCAR_BASE_PATH: basePath } },
    setTimeout: schedule, clearTimeout: cancel,
    window: { setTimeout: schedule, clearTimeout: cancel,
      location: { replace: (url) => destinations.push(url) } },
  });
  function load(filename) {
    if (cache.has(filename)) return cache.get(filename).exports;
    const moduleRecord = { exports: {} };
    cache.set(filename, moduleRecord);
    const source = readFileSync(filename, "utf8");
    const output = ts.transpileModule(source, { compilerOptions: {
      target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS,
      jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true,
    } }).outputText;
    function requireModule(specifier) {
      if (specifier === "react") return { ...React, useEffect: (effect) => { effects.push(effect); } };
      if (specifier === "react/jsx-runtime") return jsxRuntime;
      if (specifier === "@tanstack/react-query") return queryRuntime;
      if (specifier === "next/link") return Object.assign(Link, { useLinkStatus: () => ({ pending: false }) });
      if (specifier === "next/image") return Image;
      if (specifier === "next/navigation") return { usePathname: () => "/overview", useRouter: () => ({ prefetch: () => {} }) };
      if (specifier === "@phosphor-icons/react") return new Proxy({}, {
        get: () => () => React.createElement("svg", { "aria-hidden": true }),
      });
      assert.ok(specifier.startsWith("."), `Unexpected dependency ${specifier}`);
      const resolved = path.resolve(path.dirname(filename), specifier);
      if (resolved.endsWith(".module.css")) return { __esModule: true, default: new Proxy({}, { get: (_target, key) => key }) };
      if (resolved === path.join(appRoot, "lib/api")) return api;
      if (resolved === path.join(appRoot, "components/Feedback")) return feedback;
      if (resolved === path.join(appRoot, "components/LogoutButton")) return () => null;
      if (resolved === path.join(appRoot, "components/BackToTop")) return () => null;
      if (resolved === path.join(appRoot, "accounts/AccountsPagination")) return { AccountsPagination: () => null };
      const file = [resolved, `${resolved}.ts`, `${resolved}.tsx`].find((candidate) => existsSync(candidate));
      assert.ok(file, `Missing dependency ${specifier}`);
      return load(file);
    }
    vm.runInContext(`(function(require, module, exports) {${output}\n})`, context, { filename })(requireModule, moduleRecord, moduleRecord.exports);
    return moduleRecord.exports;
  }
  return {
    queries, prefetches, links, destinations,
    renderPage: () => renderToStaticMarkup(React.createElement(load(path.join(appRoot, "accounts/AccountsPage.tsx")).default)),
    renderAuthorizationPage: () => renderToStaticMarkup(React.createElement(load(path.join(appRoot, "accounts/douyin-authorization/DouyinAuthorizationPage.tsx")).default)),
    renderShell: () => renderToStaticMarkup(React.createElement(load(path.join(appRoot, "components/WorkbenchChrome.tsx")).default,
      { active: "overview" }, React.createElement("p", null, "普通页面"))),
    accessRule: () => load(path.join(appRoot, "lib/accountAccess.ts")).canAccessAccounts,
    runEffects: () => { for (const effect of effects.splice(0)) effect(); },
    runPrefetchTimers: () => {
      for (const [id, timer] of [...timers]) {
        if (timer.delay <= 120) { timers.delete(id); timer.callback(); }
      }
    },
  };
}

test("account access permits only the two administrator roles", () => {
  const allowed = harness().accessRule();
  for (const role of ["admin", "superadmin"]) assert.equal(allowed(role), true, role);
  for (const role of ["operator", "new_user", undefined, "", "Admin", "future_role"]) {
    assert.equal(allowed(role), false, String(role));
  }
});

for (const role of ["admin", "superadmin", "operator", "new_user", undefined]) {
  const allowed = role === "admin" || role === "superadmin";
  test(`${role ?? "unknown"} navigation and prefetch obey account access`, () => {
    const view = harness({ role });
    const html = view.renderShell();
    assert.equal(/href="\/accounts"/.test(html), allowed);
    assert.match(html, /href="\/contents"/);
    for (const link of view.links) {
      link.onFocus?.();
      view.runPrefetchTimers();
      link.onPointerDown?.({ button: 0 });
    }
    assert.equal(view.prefetches.some((key) => key[0] === "accounts"), allowed);
  });

  test(`${role ?? "unknown"} account page never exposes another role's cached rows`, () => {
    const view = harness({ role, basePath: "/dcar" });
    const html = view.renderPage();
    assert.equal(view.queries.some((key) => key[0] === "accounts"), allowed);
    assert.equal(html.includes(protectedName), allowed);
    if (allowed) {
      assert.match(html, /class="phone">13212343053<\/span>/);
      assert.match(html, /class="phone">—<\/span>/);
      assert.doesNotMatch(html, /\*{4}/);
    } else {
      assert.doesNotMatch(html, /13212343053|测试运营人员|test-platform-uid|账号列表/);
      view.runEffects();
      assert.deepEqual(view.destinations, ["/dcar/overview"]);
    }
  });
}

for (const role of ["operator", "new_user", undefined]) {
  test(`${role ?? "unknown"} cannot mount the account authorization workspace`, () => {
    const view = harness({ role, basePath: "/dcar" });
    const html = view.renderAuthorizationPage();
    assert.equal(view.queries.some((key) => ["accounts", "douyin"].includes(key[0])), false);
    assert.doesNotMatch(html, /13212343053|仅管理员可见|test-platform-uid|抖音开放平台授权/);
    view.runEffects();
    assert.deepEqual(view.destinations, ["/dcar/overview"]);
  });
}

for (const sessionState of ["pending", "error"]) {
  test(`a ${sessionState} session keeps cached account data unmounted`, () => {
    const view = harness({ role: "admin", sessionState });
    const html = view.renderPage();
    assert.equal(view.queries.some((key) => key[0] === "accounts"), false);
    assert.doesNotMatch(html, /13212343053|仅管理员可见|测试运营人员|test-platform-uid/);
    assert.match(html, /权限|身份/);
    view.runEffects();
    assert.deepEqual(view.destinations, []);
  });
}
