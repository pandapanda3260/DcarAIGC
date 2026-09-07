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
const profileUrl = "https://www.xiaohongshu.com/user/profile/example-user";
const created = {
  message: "账号已保存为周更，将于明日零点加入生效名单。",
  account_id: 42, platform: "xiaohongshu", uid: "resolved-platform-uid",
  account_status: "weekly", activation_status: "scheduled",
  scheduled_effective_at: "2026-09-08T00:00:00+08:00", action: "inserted",
};

// Exercise the real components, hooks' retained state, API request serialization
// and account search contracts. No provider, browser server or database is used.
function harness({ page = false, managementVersion = 2, items = [], profileId = "tikhub_managed_v1", sourceFamily = "system" } = {}) {
  const modules = new Map(), instances = new Map();
  const requests = [], completed = [], searches = [], invalidations = [];
  let instance, cursor = 0, uuid = 0, closed = 0;
  let respond = () => Response.json(created);
  const react = {
    ...React,
    useState(initial) {
      const owner = instance, index = cursor++;
      if (!(index in owner)) owner[index] = typeof initial === "function" ? initial() : initial;
      return [owner[index], (value) => { owner[index] = typeof value === "function" ? value(owner[index]) : value; }];
    },
    useRef(initial) {
      const index = cursor++;
      if (!(index in instance)) instance[index] = { current: initial };
      return instance[index];
    },
    useEffect() {},
  };
  const query = {
    queryOptions: (options) => options,
    keepPreviousData: (value) => value,
    useQueryClient: () => ({ invalidateQueries: async (value) => { invalidations.push(value.queryKey); } }),
    useQuery: (options) => {
      assert.equal(options.queryKey[0], "accounts");
      searches.push(JSON.parse(JSON.stringify(options.queryKey[2])));
      return {
        data: { items, total: items.length, account_management_version: managementVersion, roster: { active_profile_id: profileId, source_family: sourceFamily } },
        isPending: false, isError: false, isLoadingError: false,
        isPlaceholderData: false, refetch: async () => {},
      };
    },
  };
  const context = vm.createContext({
    console, process: { env: {} }, URL, Response, Headers, AbortController, DOMException,
    setTimeout, clearTimeout,
    crypto: { randomUUID: () => `00000000-0000-4000-8000-${String(++uuid).padStart(12, "0")}` },
    fetch: async (url, options) => {
      requests.push({ url, method: options.method, body: JSON.parse(options.body) });
      return respond(url, options);
    },
  });
  function load(filename) {
    if (modules.has(filename)) return modules.get(filename).exports;
    const moduleRecord = { exports: {} };
    modules.set(filename, moduleRecord);
    const output = ts.transpileModule(readFileSync(filename, "utf8"), { compilerOptions: {
      target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS,
      jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true,
    } }).outputText;
    function requireModule(specifier) {
      if (specifier === "react") return react;
      if (specifier === "react/jsx-runtime") return jsxRuntime;
      if (specifier === "@tanstack/react-query") return query;
      if (specifier === "next/link") return function Link({ children }) { return React.createElement("a", null, children); };
      if (specifier === "next/image") return () => null;
      if (specifier === "@phosphor-icons/react") return new Proxy({}, { get: () => () => null });
      assert.ok(specifier.startsWith("."), `Unexpected dependency ${specifier}`);
      const resolved = path.resolve(path.dirname(filename), specifier);
      if (resolved.endsWith(".module.css")) return { __esModule: true, default: new Proxy({}, { get: (_target, key) => key }) };
      if (resolved === path.join(appRoot, "components/AppShell")) return function AppShell({ header, children }) { return React.createElement("main", null, header, children); };
      if (resolved === path.join(appRoot, "components/AccountPageAccess")) return ({ children }) => children;
      if (resolved === path.join(appRoot, "components/Feedback")) return {
        Feedback: ({ error, message }) => React.createElement("aside", null, error, message),
        Loading: () => null, Notice: () => null, ReadErrorState: () => null,
      };
      if (resolved === path.join(appRoot, "accounts/AccountsPagination")) return { AccountsPagination: () => null };
      const file = [resolved, `${resolved}.ts`, `${resolved}.tsx`].find((value) => existsSync(value));
      assert.ok(file, `Missing dependency ${specifier}`);
      return load(file);
    }
    vm.runInContext(`(function(require,module,exports){${output}\n})`, context, { filename })(requireModule, moduleRecord, moduleRecord.exports);
    return moduleRecord.exports;
  }
  function expand(element) {
    if (Array.isArray(element)) return React.Children.toArray(element).map(expand);
    if (!React.isValidElement(element)) return element;
    if (typeof element.type === "function") {
      if (!instances.has(element.type)) instances.set(element.type, []);
      const previous = instance, previousCursor = cursor;
      instance = instances.get(element.type); cursor = 0;
      const result = element.type(element.props);
      instance = previous; cursor = previousCursor;
      return expand(result);
    }
    return React.cloneElement(element, {}, ...React.Children.toArray(element.props.children).map(expand));
  }
  const Component = load(path.join(appRoot, page ? "accounts/AccountsPage.tsx" : "accounts/CreateAccountDialog.tsx")).default;
  const props = { onClose: () => { closed++; }, onCreated: (value) => { completed.push(value); } };
  const render = () => expand(React.createElement(Component, { ...props, accountManagementVersion: managementVersion }));
  function nodes() {
    const found = [];
    function walk(value) {
      if (Array.isArray(value)) { value.forEach(walk); return; }
      if (!React.isValidElement(value)) return;
      found.push(value); walk(value.props.children);
    }
    walk(render());
    return found;
  }
  const find = (predicate) => {
    const result = nodes().find(predicate);
    assert.ok(result, "Expected rendered element");
    return result;
  };
  const field = (name) => find((node) => node.props.name === name);
  return {
    requests, completed, searches, invalidations, field, find, nodes,
    setManagementVersion: (value) => { managementVersion = value; },
    readSearch: (overrides = {}) => {
      const queries = load(path.join(appRoot, "lib/queries.ts"));
      return queries.accountSearchQueryOptions({ ...queries.defaultAccountSearchRequest, ...overrides }).queryFn();
    },
    respondWith: (value) => { respond = value; },
    html: () => renderToStaticMarkup(render()),
    change: (name, value) => field(name).props.onChange({ target: { value } }),
    submit: () => find((node) => node.type === "form").props.onSubmit({ preventDefault() {} }),
    open: () => find((node) => node.type === "button" && node.props.children === "新增系统账号").props.onClick(),
    closed: () => closed,
  };
}

const settle = () => new Promise((resolve) => setImmediate(resolve));

test("create form has exactly four fields and no default publishing frequency", () => {
  const view = harness();
  const fields = view.nodes().filter((node) => ["input", "select", "textarea"].includes(node.type));
  assert.deepEqual(fields.map((node) => node.props.name), ["profile_url", "phone", "operator_name", "account_status"]);
  assert.equal(view.field("profile_url").props.required, true);
  assert.equal(view.field("account_status").props.required, true);
  assert.equal(view.field("account_status").props.value, "");
  assert.match(view.html(), /支持抖音、小红书；视频号、快手暂不支持采集/);
  assert.doesNotMatch(view.html(), /sec_user_id|平台 UID|name="nickname"|name="platform"/);
});

test("missing profile or status never submits a request", async () => {
  const view = harness();
  view.submit(); await settle();
  assert.match(view.html(), /请填写账号主页链接/);
  view.change("profile_url", profileUrl);
  view.submit(); await settle();
  assert.match(view.html(), /请选择日更、周更或暂停/);
  assert.equal(view.requests.length, 0);
});

test("POST carries only four user fields and the idempotency key", async () => {
  const view = harness();
  view.change("profile_url", ` ${profileUrl} `);
  view.change("phone", " 13212343053 ");
  view.change("operator_name", " 测试运营 ");
  view.change("account_status", "weekly");
  view.submit(); await settle();
  assert.equal(view.requests.length, 1);
  assert.deepEqual(view.requests[0], {
    url: "/api/v8/accounts", method: "POST",
    body: { profile_url: profileUrl, phone: "13212343053", operator_name: "测试运营",
      account_status: "weekly", request_id: "00000000-0000-4000-8000-000000000001" },
  });
  assert.deepEqual(JSON.parse(JSON.stringify(view.completed)), [created]);
});

test("paused creation allows blank operating fields and explains exclusion", async () => {
  const view = harness();
  view.change("profile_url", profileUrl);
  view.change("account_status", "paused");
  assert.match(view.html(), /不加入当前生效名单，不采集，也不进入统计/);
  assert.match(view.html(), /日更、周更只标注作品更新频率，采集规则不变/);
  view.submit(); await settle();
  assert.equal(view.requests[0].body.phone, "");
  assert.equal(view.requests[0].body.operator_name, "");
  assert.equal(view.requests[0].body.account_status, "paused");
});

test("ambiguous failure retains inputs and reuses the request key until intent changes", async () => {
  const view = harness();
  view.respondWith(() => { throw new TypeError("connection lost"); });
  view.change("profile_url", profileUrl);
  view.change("phone", "13212343053");
  view.change("operator_name", "原输入");
  view.change("account_status", "daily");
  view.submit(); await settle();
  assert.match(view.html(), /无法连接数据服务/);
  for (const [name, value] of Object.entries({ profile_url: profileUrl, phone: "13212343053", operator_name: "原输入", account_status: "daily" })) {
    assert.equal(view.field(name).props.value, value);
  }
  view.submit(); await settle();
  assert.equal(view.requests[0].body.request_id, view.requests[1].body.request_id);
  view.change("account_status", "weekly");
  view.submit(); await settle();
  assert.notEqual(view.requests[1].body.request_id, view.requests[2].body.request_id);
  assert.equal(view.completed.length, 0);
  assert.equal(view.closed(), 0);
});

test("identification in progress prevents duplicate submissions and keeps backend rejection visible", async () => {
  const view = harness();
  let finish;
  view.respondWith(() => new Promise((resolve) => { finish = resolve; }));
  view.change("profile_url", "https://example.test/unsupported-account");
  view.change("account_status", "daily");
  view.submit(); view.submit();
  assert.equal(view.requests.length, 1);
  assert.match(view.html(), /正在识别账号/);
  assert.equal(view.field("profile_url").props.disabled, true);
  finish(Response.json({ detail: "暂不支持该平台，请使用抖音或小红书主页链接。" }, { status: 400 }));
  await settle();
  assert.match(view.html(), /暂不支持该平台，请使用抖音或小红书主页链接/);
  assert.equal(view.field("account_status").props.value, "daily");
});

test("created account is found in all accounts after clearing incompatible filters", async () => {
  const view = harness({ page: true });
  view.find((node) => node.props["aria-label"] === "账号状态筛选").props.onChange({ target: { value: "paused" } });
  for (const value of ["original", "used_car", "douyin"]) {
    view.find((node) => node.type === "select" && React.Children.toArray(node.props.children).some((option) => option.props.value === value))
      .props.onChange({ target: { value } });
  }
  view.open();
  view.change("profile_url", profileUrl);
  view.change("account_status", "weekly");
  view.submit(); await settle();
  const html = view.html();
  assert.match(html, /账号已保存为周更，将于明日零点加入生效名单/);
  assert.doesNotMatch(html, /create-account-title/);
  assert.doesNotMatch(html, /名单范围|当前成员|历史档案|待身份对齐/);
  assert.equal(view.find((node) => node.props["aria-label"] === "账号状态筛选").props.value, "");
  assert.equal(view.find((node) => node.type === "input" && node.props.placeholder?.startsWith("手机号、运营人员")).props.value, created.uid);
  assert.deepEqual(view.searches.at(-1), {
    page: 1, page_size: 50, query: created.uid, platform: null, account_type: null,
    content_direction: null, account_status: null,
  });
  assert.ok(view.invalidations.some((key) => key[0] === "accounts"));
});

test("old service shows the four-field form but cannot submit it", async () => {
  const view = harness({ page: true, managementVersion: 1 });
  view.open();
  view.change("profile_url", profileUrl);
  view.change("account_status", "weekly");
  const fields = view.nodes().filter((node) => ["input", "select", "textarea"].includes(node.type) && node.props.name);
  assert.deepEqual(fields.map((node) => node.props.name), ["profile_url", "phone", "operator_name", "account_status"]);
  assert.match(view.html(), /新增账号暂不可用，服务更新完成后可提交/);
  assert.equal(view.find((node) => node.props.type === "submit").props.disabled, true);
  view.submit(); await settle();
  assert.equal(view.requests.length, 0);
  view.setManagementVersion(2);
  assert.equal(view.find((node) => node.props.type === "submit").props.disabled, false);
  view.submit(); await settle();
  assert.equal(view.requests.length, 1);
  assert.equal(view.requests[0].body.profile_url, profileUrl);
  assert.equal("platforms" in view.requests[0].body, false);
});

test("integrated system profile keeps the four-field creation entry", () => {
  for (const profileId of ["tikhub_managed_v1", "integrated_route_v1"]) {
    const view = harness({ page: true, profileId });
    assert.doesNotMatch(view.html(), /批量上传账号/);
    view.open();
    assert.equal(view.field("profile_url").props.required, true);
    assert.equal(view.find((node) => node.props.type === "submit").props.disabled, false);
  }
  const matrix = harness({ page: true, profileId: "matrix_hybrid_v1", sourceFamily: "matrix" });
  assert.match(matrix.html(), /批量上传账号/);
  assert.doesNotMatch(matrix.html(), /新增系统账号/);
});

test("account rows show account status and ignore historical roster projections", () => {
  const view = harness({ page: true, items: [
    { id: 1, phone: "13212343053", operator_name: "运营", account_type: "original", content_direction: "new_car",
      account_status: "weekly", enabled: true, roster_state: "history", platforms: [] },
    { id: 2, phone: "", operator_name: "", account_type: "unknown", content_direction: "unknown",
      account_status: "paused", enabled: false, roster_state: "unresolved", platforms: [] },
  ] });
  assert.match(view.html(), /data-state="weekly"/);
  assert.match(view.html(), /data-state="paused"/);
  assert.doesNotMatch(view.html(), /名单范围|当前成员|历史档案|待身份对齐|未在当前生效名单|待补充平台 UID/);
  assert.equal("scope" in view.searches.at(-1), false);
  view.find((node) => node.props["aria-label"] === "账号状态筛选").props.onChange({ target: { value: "paused" } });
  view.html();
  assert.equal(view.searches.at(-1).account_status, "paused");
  assert.equal("scope" in view.searches.at(-1), false);
});

test("export keeps the selected account status on v2 and blocks filtered legacy exports", async () => {
  const view = harness({ page: true });
  view.respondWith(() => Response.json({ detail: "离线导出样本" }, { status: 400 }));
  const downloadButton = () => view.find((node) => node.type === "button" && node.props.children === "下载账号表格");
  view.find((node) => node.props["aria-label"] === "账号状态筛选").props.onChange({ target: { value: "weekly" } });
  downloadButton().props.onClick(); await settle();
  assert.deepEqual(view.requests[0].body, { douyin_authorization_targets: null, account_status: "weekly" });
  view.setManagementVersion(1);
  assert.equal(downloadButton().props.disabled, true);
  assert.match(view.html(), /服务更新完成后可按账号状态导出/);
  downloadButton().props.onClick(); await settle();
  assert.equal(view.requests.length, 1);
  view.find((node) => node.props["aria-label"] === "账号状态筛选").props.onChange({ target: { value: "" } });
  assert.equal(downloadButton().props.disabled, false);
  downloadButton().props.onClick(); await settle();
  assert.deepEqual(view.requests[1].body, { douyin_authorization_targets: null, scope: "all" });
});

test("search adapter sends legacy all only until v2 is confirmed and handles rollback", async () => {
  const view = harness();
  view.respondWith(() => Response.json({ items: [], total: 0 }));
  await view.readSearch({ account_status: "paused" });
  assert.equal(view.requests.at(-1).body.scope, "all");
  assert.equal(view.requests.at(-1).body.account_status, "paused");
  view.respondWith(() => Response.json({ items: [], total: 0, account_management_version: 2 }));
  await view.readSearch();
  assert.equal(view.requests.at(-1).body.scope, "all");
  await view.readSearch();
  assert.equal("scope" in view.requests.at(-1).body, false);
  view.respondWith(() => Response.json({ items: [], total: 0 }));
  const beforeRollback = view.requests.length;
  await view.readSearch();
  assert.equal(view.requests.length - beforeRollback, 2);
  assert.equal("scope" in view.requests.at(-2).body, false);
  assert.equal(view.requests.at(-1).body.scope, "all");
});
