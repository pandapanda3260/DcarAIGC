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
  message: "账号已保存为周更，可自动采集。",
  account_id: 42, platform: "xiaohongshu", uid: "resolved-platform-uid",
  account_status: "weekly", activation_status: "active",
  scheduled_effective_at: "2026-09-08T00:00:00+08:00", action: "inserted",
};

// Exercise the real components, hooks' retained state, API request serialization
// and account search contracts. No provider, browser server or database is used.
function harness({ page = false, managementVersion = 2, items = [], profileId = "tikhub_managed_v1", sourceFamily = "system", confirmPause = true } = {}) {
  const modules = new Map(), instances = new Map();
  const requests = [], completed = [], searches = [], invalidations = [];
  const confirmations = [];
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
    window: { confirm: (message) => { confirmations.push(message); return confirmPause; } },
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
      if (specifier === "next/link") return function Link({ children, ...props }) { return React.createElement("a", props, children); };
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
    requests, completed, searches, invalidations, confirmations, field, find, nodes,
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

test("create form has six fields including the shared account classification and no default publishing frequency", () => {
  const view = harness();
  const fields = view.nodes().filter((node) => ["input", "select", "textarea"].includes(node.type));
  assert.deepEqual(fields.map((node) => node.props.name), ["profile_url", "phone", "operator_name", "account_group", "business_direction", "account_status"]);
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

test("POST carries the shared classification fields and the idempotency key", async () => {
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
      account_status: "weekly", account_group: "unknown", business_direction: "unknown", request_id: "00000000-0000-4000-8000-000000000001" },
  });
  assert.deepEqual(JSON.parse(JSON.stringify(view.completed)), [created]);
});

test("paused creation allows blank operating fields and explains exclusion", async () => {
  const view = harness();
  view.change("profile_url", profileUrl);
  view.change("account_status", "paused");
  assert.match(view.html(), /只停止自动采集，历史内容和数据保留/);
  assert.match(view.html(), /日更、周更只标注作品更新频率/);
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
  for (const value of ["innovation", "used_car_c2", "douyin"]) {
    view.find((node) => node.type === "select" && React.Children.toArray(node.props.children).some((option) => option.props.value === value))
      .props.onChange({ target: { value } });
  }
  view.open();
  view.change("profile_url", profileUrl);
  view.change("account_status", "weekly");
  view.submit(); await settle();
  const html = view.html();
  assert.match(html, /账号已保存为周更，可自动采集/);
  assert.doesNotMatch(html, /create-account-title/);
  assert.doesNotMatch(html, /名单范围|当前成员|历史档案|待身份对齐/);
  assert.equal(view.find((node) => node.props["aria-label"] === "账号状态筛选").props.value, "");
  assert.equal(view.find((node) => node.type === "input" && node.props.placeholder?.startsWith("手机号、运营人员")).props.value, created.uid);
  assert.deepEqual(view.searches.at(-1), {
    page: 1, page_size: 50, query: created.uid, platform: null, account_group: null,
    business_direction: null, account_status: null,
  });
  assert.ok(view.invalidations.some((key) => key[0] === "accounts"));
});

test("old service shows the classification form but cannot submit it", async () => {
  const view = harness({ page: true, managementVersion: 1 });
  view.open();
  view.change("profile_url", profileUrl);
  view.change("account_status", "weekly");
  const fields = view.nodes().filter((node) => ["input", "select", "textarea"].includes(node.type) && node.props.name);
  assert.deepEqual(fields.map((node) => node.props.name), ["profile_url", "phone", "operator_name", "account_group", "business_direction", "account_status"]);
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

test("integrated system profile keeps the classification creation entry", () => {
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
    { id: 1, phone: "13212343053", operator_name: "运营", account_group: "innovation", business_direction: "new_car",
      account_status: "weekly", enabled: true, roster_state: "history", platforms: [] },
    { id: 2, phone: "", operator_name: "", account_group: "unknown", business_direction: "unknown",
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
  assert.deepEqual(view.requests[0].body, { douyin_authorization_targets: null, query: "", platform: null, account_group: null, business_direction: null, account_status: "weekly" });
  view.setManagementVersion(1);
  assert.equal(downloadButton().props.disabled, true);
  assert.match(view.html(), /服务更新完成后可按账号状态导出/);
  downloadButton().props.onClick(); await settle();
  assert.equal(view.requests.length, 1);
  view.find((node) => node.props["aria-label"] === "账号状态筛选").props.onChange({ target: { value: "" } });
  assert.equal(downloadButton().props.disabled, false);
  downloadButton().props.onClick(); await settle();
  assert.deepEqual(view.requests[1].body, { douyin_authorization_targets: null, query: "", platform: null, account_group: null, business_direction: null, scope: "all" });
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

const quickStatusAccount = (status, platform = 'douyin', uid = 'sample-uid') => ({
  id: uid ? 52 : -6, directory_row_id: 6,
  directory_identity_status: uid ? 'existing_verified' : 'identity_missing',
  phone: '', operator_name: '', account_group: 'unknown', business_direction: 'unknown',
  account_status: status, enabled: status !== 'paused' && Boolean(uid),
  platforms: [{ platform, uid, nickname: '菜单测试账号' }],
});
const clickStatus = (view, status) => view.find(node => node.props['data-status-action'] === status).props.onClick({
  currentTarget: { closest: () => ({ open: true }) },
});

test('every platform and identity row has a menu with exactly the alternative statuses', () => {
  for (const platform of ['douyin', 'xiaohongshu', 'wechat_channels', 'kuaishou']) {
    for (const uid of ['sample-uid', null]) {
      for (const status of ['daily', 'weekly', 'paused', 'unmarked']) {
        const view = harness({ page: true, items: [quickStatusAccount(status, platform, uid)] });
        assert.equal(view.nodes().filter(node => node.type === 'details').length, 1);
        const choices = view.nodes().filter(node => node.props['data-status-action']).map(node => node.props['data-status-action']);
        assert.deepEqual(choices, ['daily', 'weekly', 'paused'].filter(choice => choice !== status));
        const authorization = view.nodes().filter(node => ['a', 'button'].includes(node.type) && node.props.children === '抖音授权');
        assert.equal(authorization.length, platform === 'douyin' ? 1 : 0);
        if (platform === 'douyin' && uid) {
          assert.equal(authorization[0].type, 'a');
          assert.equal(authorization[0].props.href, '/accounts/douyin-authorization?account_id=52&platform_uid=sample-uid');
        } else if (platform === 'douyin') {
          assert.equal(authorization[0].type, 'button');
          assert.equal(authorization[0].props.disabled, true);
          assert.match(authorization[0].props.title, /需先补充平台 UID/);
          assert.equal(authorization[0].props.onClick, undefined);
        }
      }
    }
  }
});

test('quick status PATCH uses the row identity and chosen status then invalidates list and related data', async () => {
  for (const [before, after, uid] of [['daily', 'weekly', 'sample-uid'], ['weekly', 'daily', 'sample-uid'], ['paused', 'daily', null], ['daily', 'paused', null]]) {
    const view = harness({ page: true, items: [quickStatusAccount(before, 'wechat_channels', uid)] });
    view.respondWith(() => Response.json({ message: '账号状态已保存' }));
    clickStatus(view, after);
    await settle();
    assert.deepEqual(view.requests, [{ url: `/api/v8/accounts/${uid ? 52 : -6}`, method: 'PATCH', body: {
      account_status: after, status_request_id: '00000000-0000-4000-8000-000000000001',
    } }]);
    assert.equal(view.confirmations.length, after === 'paused' ? 1 : 0);
    assert.equal(view.invalidations.length, 5);
    assert.match(view.html(), /账号状态已保存/);
  }
});

test('cancelled pause leaves the account untouched', async () => {
  const view = harness({ page: true, items: [quickStatusAccount('daily')], confirmPause: false });
  clickStatus(view, 'paused'); await settle();
  assert.equal(view.confirmations.length, 1);
  assert.equal(view.requests.length, 0);
  assert.equal(view.invalidations.length, 0);
  assert.match(view.confirmations[0], /只停止自动采集，历史内容和数据保留/);
});

test('blocked automatic capture reason is visible and describes the operating status', () => {
  const item = { ...quickStatusAccount('daily'), automatic_capture: {
    eligible: false, reason_code: 'locator_missing', reason_label: '缺少有效主页标识',
  } };
  const view = harness({ page: true, items: [item] });
  assert.match(view.html(), /暂不自动采集：缺少有效主页标识/);
  const status = view.find(node => node.props['data-state'] === 'daily');
  const reason = view.find(node => node.props.id === status.props['aria-describedby']);
  assert.equal(reason.type, 'span');
  assert.match(React.Children.toArray(reason.props.children).join(''), /缺少有效主页标识/);
  assert.doesNotMatch(view.html(), /采集开关|等待.*名单|明日零点加入/);
});

test('eligible or older API rows do not gain automatic capture noise or controls', () => {
  for (const automatic_capture of [undefined, {
    eligible: true, reason_code: 'eligible', reason_label: '已满足自动采集条件',
  }]) {
    const view = harness({ page: true, items: [{ ...quickStatusAccount('weekly'), automatic_capture }] });
    assert.doesNotMatch(view.html(), /暂不自动采集|已满足自动采集条件|automatic-capture-reason/);
    assert.equal(view.nodes().filter(node => node.props.role === 'switch').length, 0);
    assert.equal(view.nodes().filter(node => node.props['data-status-action']).length, 2);
  }
});

test('in-flight changes disable all status actions; uncertain retry reuses the intent ID', async () => {
  const view = harness({ page: true, items: [quickStatusAccount('daily')] });
  let finish;
  view.respondWith(() => new Promise(resolve => { finish = resolve; }));
  clickStatus(view, 'weekly');
  assert.ok(view.nodes().filter(node => node.props['data-status-action']).every(node => node.props.disabled));
  finish(Response.json({ detail: '暂时无法保存' }, { status: 503 }));
  await settle();
  assert.match(view.html(), /服务暂时不可用，请稍后重试/);
  assert.equal(view.invalidations.length, 0);
  assert.ok(view.nodes().filter(node => node.props['data-status-action']).every(node => !node.props.disabled));
  view.respondWith(() => Response.json({ message: '账号状态已保存' }));
  clickStatus(view, 'weekly'); await settle();
  assert.equal(view.requests.length, 2);
  assert.deepEqual(view.requests[0], view.requests[1]);
});


test("editing classification uses the same values as the row and never resubmits unchanged account status", async () => {
  const item = { ...quickStatusAccount("weekly"), account_group: "image_text", business_direction: "used_car_c2" };
  const view = harness({ page: true, items: [item] });
  assert.match(view.html(), /账号分组：图文号；业务方向：二手车C2/);
  view.find(node => node.type === "button" && node.props.children === "修改").props.onClick();
  assert.equal(view.field("account_group").props.value, "image_text");
  assert.equal(view.field("business_direction").props.value, "used_car_c2");
  view.change("account_group", "innovation");
  view.change("business_direction", "ai_xiaodong");
  view.respondWith((_url, options) => { Object.assign(item, JSON.parse(options.body)); return Response.json({ message: "已保存" }); });
  view.find(node => node.type === "button" && node.props.children === "保存修改").props.onClick();
  await settle();
  assert.deepEqual(view.requests[0].body, { phone: "", operator_name: "", account_group: "innovation", business_direction: "ai_xiaodong" });
  assert.match(view.html(), /账号分组：创新号；业务方向：AI小懂/);
  assert.equal(item.account_status, "weekly");
  assert.doesNotMatch(view.html(), /全部账号类型|全部内容方向|原创/);
});

test("a row without UID can edit classification using only its negative directory ID", async () => {
  const view = harness({ page: true, items: [quickStatusAccount("paused", "douyin", null)] });
  const edit = view.find(node => node.type === "button" && node.props.children === "修改");
  assert.notEqual(edit.props.disabled, true);
  edit.props.onClick();
  view.change("account_group", "image_text");
  view.change("business_direction", "used_car_c1");
  view.respondWith(() => Response.json({ message: "分类已保存" }));
  view.find(node => node.type === "button" && node.props.children === "保存修改").props.onClick();
  await settle();
  assert.deepEqual(view.requests[0], { url: "/api/v8/accounts/-6", method: "PATCH", body: { account_group: "image_text", business_direction: "used_car_c1" } });
  assert.equal(view.confirmations.length, 0);
});

test("creation, filters, and export submit the same classification enums", async () => {
  const create = harness();
  create.change("profile_url", profileUrl);
  create.change("account_status", "daily");
  create.change("account_group", "image_text");
  create.change("business_direction", "used_car_c2");
  create.submit(); await settle();
  assert.equal(create.requests[0].body.account_group, "image_text");
  assert.equal(create.requests[0].body.business_direction, "used_car_c2");
  const view = harness({ page: true });
  view.find(node => node.props["aria-label"] === "账号分组筛选").props.onChange({ target: { value: "image_text" } });
  view.find(node => node.props["aria-label"] === "业务方向筛选").props.onChange({ target: { value: "used_car_c2" } });
  view.html();
  assert.equal(view.searches.at(-1).account_group, "image_text");
  assert.equal(view.searches.at(-1).business_direction, "used_car_c2");
  view.respondWith(() => Response.json({ detail: "导出请求已记录" }, { status: 400 }));
  view.find(node => node.type === "button" && node.props.children === "下载账号表格").props.onClick();
  await settle();
  assert.equal(view.requests[0].body.account_group, "image_text");
  assert.equal(view.requests[0].body.business_direction, "used_car_c2");
  for (const body of [create.requests[0].body, view.requests[0].body, view.searches.at(-1)]) {
    assert.equal("account_type" in body, false);
    assert.equal("content_direction" in body, false);
  }
});

test('returning to a previous quick status after another intent never replays its old request', async () => {
  const account = quickStatusAccount('daily');
  const view = harness({ page: true, items: [account] });
  const receipts = new Map();
  let actualStatus = 'daily';
  view.respondWith((_url, options) => {
    const body = JSON.parse(options.body);
    if (!receipts.has(body.status_request_id)) {
      actualStatus = body.account_status;
      receipts.set(body.status_request_id, { message: `已保存为${actualStatus}` });
      if (receipts.size === 1) throw new TypeError('response lost after commit');
    }
    account.account_status = actualStatus;
    return Response.json(receipts.get(body.status_request_id));
  });
  clickStatus(view, 'weekly'); await settle();
  assert.equal(actualStatus, 'weekly');
  assert.equal(account.account_status, 'daily', 'lost response leaves the visible row unchanged');
  clickStatus(view, 'paused'); await settle();
  assert.equal(actualStatus, 'paused');
  clickStatus(view, 'weekly'); await settle();
  assert.equal(actualStatus, 'weekly', 'returning to weekly must execute a fresh operation');
  assert.equal(new Set(view.requests.map(request => request.body.status_request_id)).size, 3);
});

test('quick actions and the edit dialog replace each other\'s pending intent while same-intent retries remain stable', async () => {
  const view = harness({ page: true, items: [quickStatusAccount('daily')] });
  view.respondWith(() => Response.json({ detail: '暂时无法保存' }, { status: 503 }));
  clickStatus(view, 'weekly'); await settle();
  const firstQuickId = view.requests.at(-1).body.status_request_id;
  const openEdit = () => view.find(node => node.props['aria-label'] === '修改菜单测试账号的运营信息').props.onClick();
  const chooseEditStatus = (status) => view.find(node => node.props['aria-describedby'] === 'account-status-help').props.onChange({ target: { value: status } });
  const saveEdit = () => view.find(node => node.type === 'button' && node.props.children === '保存修改').props.onClick();
  const cancelEdit = () => view.find(node => node.type === 'button' && node.props.children === '取消').props.onClick();
  openEdit(); chooseEditStatus('paused'); saveEdit(); await settle();
  const firstEditId = view.requests.at(-1).body.status_request_id;
  assert.notEqual(firstEditId, firstQuickId);
  saveEdit(); await settle();
  assert.equal(view.requests.at(-1).body.status_request_id, firstEditId, 'unchanged dialog submission reuses its pending request');
  cancelEdit(); clickStatus(view, 'weekly'); await settle();
  const secondQuickId = view.requests.at(-1).body.status_request_id;
  assert.notEqual(secondQuickId, firstQuickId, 'an intervening dialog intent retires the old quick action ID');
  assert.notEqual(secondQuickId, firstEditId);
  openEdit(); chooseEditStatus('paused'); saveEdit(); await settle();
  const secondEditId = view.requests.at(-1).body.status_request_id;
  assert.notEqual(secondEditId, firstEditId, 'an intervening quick action retires the old dialog ID');
  assert.notEqual(secondEditId, secondQuickId);
});
