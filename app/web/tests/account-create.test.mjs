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
function harness({ page = false, managementVersion = 2, items = [], profileId = "tikhub_managed_v1", sourceFamily = "system", confirmPause = true } = {}) {
  const modules = new Map(), instances = new Map();
  const requests = [], completed = [], searches = [], invalidations = [], clipboardWrites = [];
  const confirmations = [];
  let instance, cursor = 0, uuid = 0, closed = 0;
  let respond = () => Response.json(created);
  let writeClipboard = async () => {};
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
    useCallback(callback) { return callback; },
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
    navigator: { clipboard: { writeText: async (value) => { clipboardWrites.push(value); return writeClipboard(value); } } },
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
      // Health observer behavior is exercised by the real React lifecycle suite.
      if (resolved === path.join(appRoot, "components/DataFreshnessNote")) return () => null;
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
    requests, completed, searches, invalidations, confirmations, clipboardWrites, field, find, nodes,
    clipboardRespondWith: (value) => { writeClipboard = value; },
    setManagementVersion: (value) => { managementVersion = value; },
    readSearch: (overrides = {}) => {
      const queries = load(path.join(appRoot, "lib/queries.ts"));
      return queries.accountSearchQueryOptions({ ...queries.defaultAccountSearchRequest, ...overrides }).queryFn({ signal: new AbortController().signal });
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

test("paused creation allows blank operating fields and explains the manual label", async () => {
  const view = harness();
  view.change("profile_url", profileUrl);
  view.change("account_status", "paused");
  assert.match(view.html(), /暂停标签；当前仍按采集条件参与自动采集/);
  assert.match(view.html(), /账号状态由人工维护；当前所有状态均参与自动采集/);
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
  assert.match(html, /账号已保存为周更，将于明日零点加入生效名单/);
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

test('platform UID and platform-specific account number copy their complete original text independently', async () => {
  const uid = '000012345678901234567890123456789012345678901234567890';
  const shortId = '000098765432109876543210';
  for (const [platform, accountLabel] of [['douyin', '抖音号'], ['xiaohongshu', '小红书号'], ['kuaishou', '快手号'], ['wechat_channels', '视频号']]) {
    const item = quickStatusAccount('daily', platform, uid);
    item.platforms[0].unique_id = shortId;
    const original = JSON.stringify(item);
    const view = harness({ page: true, items: [item] });
    const uidValue = view.find(node => node.type === 'span' && node.props.className === 'uid');
    const shortValue = view.find(node => node.type === 'span' && node.props.className === 'shortId');
    assert.equal(uidValue.props.children, uid);
    assert.equal(shortValue.props.children, shortId);
    assert.ok(uidValue.props.title.includes(uid), 'the UID tooltip preserves all characters');
    assert.ok(shortValue.props.title.includes(shortId), 'the account-number tooltip preserves leading zeroes');
    const copyUid = view.find(node => node.type === 'button' && node.props['aria-label'] === '复制菜单测试账号的平台 UID');
    const copyShort = view.find(node => node.type === 'button' && node.props['aria-label'] === `复制菜单测试账号的${accountLabel}`);
    assert.notEqual(copyUid.props['aria-label'], copyShort.props['aria-label']);
    copyUid.props.onClick(); await settle();
    assert.deepEqual(view.clipboardWrites, [uid]);
    assert.match(view.html(), /平台 UID已复制/);
    copyShort.props.onClick(); await settle();
    assert.deepEqual(view.clipboardWrites, [uid, shortId]);
    assert.ok(view.html().includes(`${accountLabel}已复制`));
    assert.deepEqual(view.requests, []);
    assert.equal(JSON.stringify(item), original);
  }
});

test('missing identifiers show a dash and expose only the available identifier copy action', () => {
  for (const [uid, shortId] of [[null, null], ['12345678901234567890', ''], ['', '0000123']]) {
    const item = quickStatusAccount('daily', 'douyin', uid);
    item.platforms[0].unique_id = shortId;
    const view = harness({ page: true, items: [item] });
    assert.equal(view.find(node => node.type === 'span' && node.props.className === 'uid').props.children, uid || '—');
    assert.equal(view.find(node => node.type === 'span' && node.props.className === 'shortId').props.children, shortId || '—');
    const copyNames = view.nodes().filter(node => node.type === 'button' && node.props['aria-label']?.startsWith('复制')).map(node => node.props['aria-label']);
    assert.deepEqual(copyNames, [uid && '复制菜单测试账号的平台 UID', shortId && '复制菜单测试账号的抖音号'].filter(Boolean));
    assert.deepEqual(view.clipboardWrites, []);
    assert.deepEqual(view.requests, []);
  }
});

test('clipboard rejection shows a Chinese error without stale copy success or any business request', async () => {
  const item = quickStatusAccount('daily', 'kuaishou', '000012345678901234567890');
  item.platforms[0].unique_id = '00009876';
  const view = harness({ page: true, items: [item] });
  const copyUid = () => view.find(node => node.type === 'button' && node.props['aria-label'] === '复制菜单测试账号的平台 UID').props.onClick();
  const copyShort = () => view.find(node => node.type === 'button' && node.props['aria-label'] === '复制菜单测试账号的快手号').props.onClick();
  copyUid(); await settle();
  assert.match(view.html(), /平台 UID已复制/);
  view.clipboardRespondWith(async () => { throw new Error('NotAllowedError: clipboard denied'); });
  copyShort(); await settle();
  const failedHtml = view.html();
  assert.match(failedHtml, /复制失败/);
  assert.match(failedHtml, /手动复制/);
  assert.doesNotMatch(failedHtml, /已复制|NotAllowedError|clipboard denied/);
  view.clipboardRespondWith(async () => {});
  copyShort(); await settle();
  assert.match(view.html(), /快手号已复制/);
  assert.doesNotMatch(view.html(), /复制失败/);
  assert.deepEqual(view.clipboardWrites, [item.platforms[0].uid, item.platforms[0].unique_id, item.platforms[0].unique_id]);
  assert.deepEqual(view.requests, []);
  assert.deepEqual(view.invalidations, []);
});

test('imported account details keep all sixteen fields separate and never submit a mutation', () => {
  const headers = ['平台', '运营人员', '账号名称', 'ID（抖音号/快手号/小红书号/视频号）', 'uid', '粉丝',
    '更新状态', '质量标签', '业务标签', '是否开通接单', '手机号', '手机号开卡人姓名', '使用人证件号码', '持卡人', '是否实名', '实名来源'];
  const fields = Object.fromEntries(headers.map(header => [header, null]));
  Object.assign(fields, { 平台: '快手', 运营人员: '测试运营甲', 账号名称: '测试账号',
    [headers[3]]: '0000123', uid: '1234567890123456789', 粉丝: '约1200', 手机号: '01300000000',
    手机号开卡人姓名: '测试开卡乙', 使用人证件号码: '001234567890123456', 持卡人: '测试持卡丙' });
  const item = { ...quickStatusAccount('unmarked'), account_summary: { fields,
    source_name: '测试来源.xlsx', source_sheet: '汇总', source_row: 57,
    imported_at: '2026-09-11T01:02:03Z', comment: '原始来源第57行\n<script>不执行的批注</script>',
    pending_fields: ['是否实名'] } };
  const original = JSON.stringify(item);
  const view = harness({ page: true, items: [item] });
  view.find(node => node.type === 'button' && node.props.children === '资料').props.onClick({ currentTarget: { focus() {} } });
  const cells = view.nodes().filter(node => node.props['data-account-summary-field']);
  assert.deepEqual(cells.map(node => node.props['data-account-summary-field']).sort(), [...headers].sort());
  for (const cell of cells) assert.equal(cell.props.children, fields[cell.props['data-account-summary-field']] ?? '—');
  const dialog = view.find(node => node.props.role === 'dialog');
  const html = renderToStaticMarkup(dialog);
  assert.match(html, /测试来源.xlsx.*汇总.*57.*行/);
  assert.match(html, /2026\/09\/11 09:02:03/);
  assert.match(html.replace(/<[^>]+>/g, ''), /待核实字段.*是否实名/);
  assert.match(html, /&lt;script&gt;不执行的批注&lt;\/script&gt;/);
  assert.doesNotMatch(html, /<(?:input|select|textarea|form)\b/);
  view.find(node => node.type === 'button' && node.props['aria-label'] === '关闭账号资料').props.onClick();
  assert.equal(view.nodes().some(node => node.props.role === 'dialog'), false);
  assert.deepEqual(view.requests, []);
  assert.equal(JSON.stringify(item), original, 'opening and closing read-only details never changes the source record');
});

test('missing imported details remain blank and do not borrow another person or an unknown status', () => {
  for (const accountSummary of [undefined, { fields: { 运营人员: '', 手机号开卡人姓名: null, 是否实名: null },
    source_name: '来源.xlsx', source_sheet: '汇总', source_row: 2, imported_at: '', comment: '' }]) {
    const item = { ...quickStatusAccount('unmarked'), operator_name: '不可借用的运营姓名', phone: '13900000000', account_summary: accountSummary };
    const view = harness({ page: true, items: [item] });
    view.find(node => node.type === 'button' && node.props.children === '资料').props.onClick({ currentTarget: { focus() {} } });
    const cells = view.nodes().filter(node => node.props['data-account-summary-field']);
    assert.equal(cells.length, 16);
    assert.ok(cells.every(node => node.props.children === '—'));
    assert.deepEqual(view.requests, []);
  }
});

test('followers use collected values including zero and decreases, with missing values shown as a dash and no imported metadata', () => {
  for (const [provider, imported, expected] of [
    [284, 265, '284'], [0, '约1200', '0'], [35, 1000, '35'], [35, null, '35'], [35, 35, '35'],
    [null, '约1200', '—'], [undefined, 0, '—'], [null, null, '—'],
  ]) {
    const item = quickStatusAccount('daily');
    item.platforms[0].follower_count = provider;
    item.account_summary = { fields: { 粉丝: imported }, source_name: '来源.xlsx', source_sheet: '汇总', source_row: 2,
      imported_at: '2026-09-11T01:02:03Z', comment: '' };
    const view = harness({ page: true, items: [item] });
    const cell = view.find(node => node.type === 'td' && node.props.className === 'metric');
    const html = renderToStaticMarkup(cell);
    assert.equal(html.replace(/<[^>]+>/g, ''), expected);
    assert.doesNotMatch(html, /导入|采集记录|来源\.xlsx|汇总|2026-09-11|2026\/09\/11|<small\b/);
    assert.deepEqual(view.requests, []);
  }
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
    assert.equal(view.confirmations.length, 0);
    assert.equal(view.invalidations.length, 5);
    assert.match(view.html(), /账号状态已保存/);
  }
});

test('pause is a manual label and saves without a stop-capture confirmation', async () => {
  const view = harness({ page: true, items: [{ ...quickStatusAccount('daily'), automatic_capture: {
    eligible: true, reason_code: 'eligible', reason_label: '可自动采集',
  } }], confirmPause: false });
  view.respondWith(() => Response.json({ message: '账号状态已保存；当前所有状态均参与自动采集' }));
  clickStatus(view, 'paused'); await settle();
  assert.equal(view.confirmations.length, 0);
  assert.equal(view.requests.length, 1);
  assert.equal(view.requests[0].body.account_status, 'paused');
  assert.equal(view.invalidations.length, 5);
  assert.match(view.html(), /当前所有状态均参与自动采集/);
});

test('account rows show only the operating status and retain existing identity repair availability', () => {
  for (const [operatingStatus, statusLabel] of [['daily', '日更'], ['weekly', '周更'], ['paused', '暂停'], ['unmarked', '待标记']]) {
    for (const [state, label] of [['queued', '待接入'], ['running', '正在准备'], ['blocked', '准备失败'], ['ready', '主页准备完成']]) {
      const item = { ...quickStatusAccount(operatingStatus), locator_sha256: 'a'.repeat(64),
        account_preparation: { intake_id: 7, state, label, reason: 'identity_conflict',
          reason_label: '本次返回的账号与查询目标不一致', message: '准备状态说明' },
        automatic_capture: { eligible: false, reason_code: 'identity_unverified', reason_label: '平台身份待核验' } };
      const view = harness({ page: true, items: [item] });
      const status = view.find(node => node.props['data-state'] === operatingStatus);
      assert.equal(renderToStaticMarkup(status).replace(/<[^>]+>/g, ''), statusLabel);
      assert.equal(status.props['aria-describedby'], undefined);
      assert.doesNotMatch(view.html(), /待接入|正在准备|准备失败|主页准备完成|本次返回|准备状态说明|平台身份待核验|暂不自动采集|account-preparation-|automatic-capture-reason-/);
      assert.equal(view.nodes().filter(node => node.props['data-status-action']).length, operatingStatus === 'unmarked' ? 3 : 2);
      assert.equal(view.nodes().some(node => node.type === 'button' && node.props.children === '补充身份'), state === 'blocked');
      assert.deepEqual(view.requests, []);
    }
  }
});

test('account details show preparation failure fallbacks and original evidence with or without imported data', () => {
  for (const [reason, expected] of [
    ['preparation_retry_raw_missing', '缺少可确认的完整原始响应证据'],
    ['preparation_billing_unverified', '计费或退款结果尚未核清'],
  ]) {
    for (const reason_label of [reason, null, undefined, '', '  ', '原请求的完整返回需要进一步核查']) {
      const account_preparation = Object.freeze({ intake_id: 7, state: 'blocked', label: '账号资料补齐失败',
        reason, reason_label, message: '原始准备状态说明' });
      const item = { ...quickStatusAccount('weekly'), account_preparation,
        account_summary: reason_label === reason ? { fields: {}, source_name: '来源.xlsx', source_sheet: '汇总', source_row: 2,
          imported_at: '', comment: '' } : undefined };
      const view = harness({ page: true, items: [item] });
      assert.doesNotMatch(view.html(), /准备失败|原始准备状态说明|preparation_/);
      view.find(node => node.type === 'button' && node.props.children === '资料').props.onClick({ currentTarget: { focus() {} } });
      const dialog = view.find(node => node.props.role === 'dialog');
      const html = renderToStaticMarkup(dialog);
      const text = html.replace(/<[^>]+>/g, '');
      assert.match(text, /采集说明/);
      assert.ok(text.includes(`准备失败：${reason_label?.includes('进一步') ? reason_label : expected}`));
      assert.ok(text.includes(`原因代码：${reason}`));
      assert.match(text, /详细信息.*原始准备状态说明/);
      if (!item.account_summary) assert.match(text, /此账号尚无导入资料/);
      assert.doesNotMatch(html, /<(?:input|select|textarea|form)\b/);
      assert.equal(item.account_preparation, account_preparation);
      assert.equal(account_preparation.reason, reason);
      assert.equal(account_preparation.reason_label, reason_label);
      assert.deepEqual(view.requests, []);
      assert.doesNotMatch(text, /余额不足|重新请求/);
    }
  }
  const unknown = harness({ page: true, items: [{ ...quickStatusAccount('daily'), account_preparation: {
    intake_id: 7, state: 'blocked', label: '账号资料补齐失败',
    reason: 'unknown_preparation_reason', reason_label: 'unknown_preparation_reason', message: '原始说明',
  } }] });
  unknown.find(node => node.type === 'button' && node.props.children === '资料').props.onClick({ currentTarget: { focus() {} } });
  assert.match(renderToStaticMarkup(unknown.find(node => node.props.role === 'dialog')), /准备失败：unknown_preparation_reason/);
});

test('capture restrictions are visible in account details while normal preparation states remain quiet', () => {
  for (const reason_code of ['platform_unsupported', 'platform_policy_unavailable', 'platform_source_unconfigured', 'provider_transport_blocked', 'provider_diagnostic_only']) {
    for (const state of ['queued', 'running', 'blocked', 'ready']) {
      const item = { ...quickStatusAccount('daily'), directory_identity_status: 'identity_missing', locator_sha256: 'a'.repeat(64),
        account_preparation: { intake_id: 7, state, label: '待补齐账号资料', message: '准备状态说明' },
        automatic_capture: { eligible: false, reason_code, reason_label: '当前平台数据源能力未开放' } };
      const view = harness({ page: true, items: [item] });
      assert.doesNotMatch(view.html(), /当前平台数据源能力未开放|待补齐账号资料|准备状态说明|暂不自动采集/);
      assert.ok(view.nodes().some(node => node.type === 'button' && node.props.children === '补充身份'));
      view.find(node => node.type === 'button' && node.props.children === '资料').props.onClick({ currentTarget: { focus() {} } });
      const text = renderToStaticMarkup(view.find(node => node.props.role === 'dialog')).replace(/<[^>]+>/g, '');
      assert.match(text, /采集说明.*暂不自动采集：当前平台数据源能力未开放/);
      assert.ok(text.includes(`原因代码：${reason_code}`));
      assert.equal(text.includes('准备状态说明'), state === 'blocked');
      assert.deepEqual(view.requests, []);
    }
  }
  for (const [state, label] of [['queued', '待接入'], ['running', '正在准备'], ['ready', '主页准备完成']]) {
    const view = harness({ page: true, items: [{ ...quickStatusAccount('weekly'),
      account_preparation: { intake_id: 7, state, label, reason: null, reason_label: null, message: label },
      automatic_capture: { eligible: true, reason_code: 'eligible', reason_label: '可自动采集' } }] });
    view.find(node => node.type === 'button' && node.props.children === '资料').props.onClick({ currentTarget: { focus() {} } });
    const text = renderToStaticMarkup(view.find(node => node.props.role === 'dialog')).replace(/<[^>]+>/g, '');
    assert.doesNotMatch(text, /采集说明|待接入|正在准备|主页准备完成|可自动采集/);
    assert.deepEqual(view.requests, []);
  }
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
  const operatingInputs = view.nodes().filter(node => node.type === "input" && node.props.value === "");
  assert.equal(operatingInputs.filter(node => node.props.disabled).length, 2, "directory-only rows cannot edit phone or operator");
  assert.equal(view.nodes().some(node => node.props["aria-describedby"] === "account-status-help"), false, "directory-only rows cannot edit account status");
  view.change("account_group", "image_text");
  view.change("business_direction", "used_car_c1");
  view.respondWith(() => Response.json({ message: "分类已保存" }));
  view.find(node => node.type === "button" && node.props.children === "保存修改").props.onClick();
  await settle();
  assert.deepEqual(view.requests[0], { url: "/api/v8/accounts/-6", method: "PATCH", body: { account_group: "image_text", business_direction: "used_car_c1" } });
  assert.equal(view.confirmations.length, 0);
});

test("failed operating edits retain the full draft and do not close or invalidate account data", async () => {
  const item = { ...quickStatusAccount("unmarked"), phone: "13800000000", operator_name: "原运营" };
  const view = harness({ page: true, items: [item] });
  view.find(node => node.props["aria-label"] === "修改菜单测试账号的运营信息").props.onClick();
  view.find(node => node.type === "input" && node.props.value === "13800000000").props.onChange({ target: { value: "13900000000" } });
  view.find(node => node.type === "input" && node.props.value === "原运营").props.onChange({ target: { value: "新运营" } });
  view.change("account_group", "innovation");
  view.change("business_direction", "ai_xiaodong");
  const status = view.find(node => node.props["aria-describedby"] === "account-status-help");
  assert.equal(status.props.value, "");
  const options = React.Children.toArray(status.props.children);
  assert.equal(options.find(option => option.props.value === "").props.disabled, true);
  assert.equal(options.some(option => option.props.value === "unmarked"), false);
  status.props.onChange({ target: { value: "weekly" } });
  let finish;
  view.respondWith(() => new Promise(resolve => { finish = resolve; }));
  view.find(node => node.type === "button" && node.props.children === "保存修改").props.onClick();
  assert.equal(view.requests.length, 1);
  const dialog = view.find(node => node.props["aria-label"] === "编辑账号");
  assert.equal(dialog.props["aria-busy"], true);
  finish(Response.json({ detail: "暂时无法保存" }, { status: 503 }));
  await settle();
  assert.match(renderToStaticMarkup(view.find(node => node.props["aria-label"] === "编辑账号")), /服务暂时不可用，请稍后重试/);
  assert.equal(view.find(node => node.type === "input" && node.props.value === "13900000000").props.disabled, false);
  assert.equal(view.find(node => node.type === "input" && node.props.value === "新运营").props.disabled, false);
  assert.equal(view.field("account_group").props.value, "innovation");
  assert.equal(view.field("business_direction").props.value, "ai_xiaodong");
  assert.equal(view.find(node => node.props["aria-describedby"] === "account-status-help").props.value, "weekly");
  assert.deepEqual(view.invalidations, []);
  assert.deepEqual(view.requests[0].body, {
    phone: "13900000000", operator_name: "新运营", account_group: "innovation", business_direction: "ai_xiaodong",
    account_status: "weekly", status_request_id: "00000000-0000-4000-8000-000000000001",
  });
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

test("unified create accepts four platforms and text UID without claiming capture success", async () => {
  const view = harness({ managementVersion: 3 });
  assert.equal(view.field("profile_url").props.required, false);
  assert.match(view.html(), /快手/);
  assert.match(view.html(), /视频号/);
  view.submit(); await settle();
  assert.match(view.html(), /请填写 UID、账号 ID 或官方主页链接中的一项/);
  view.change("uid", "0001234567890123456789");
  view.submit(); await settle();
  assert.match(view.html(), /请选择账号平台/);
  view.change("platform", "kuaishou");
  view.change("account_status", "paused");
  const accepted = { intake_id: 7, account_id: null, platform: "kuaishou", uid: "0001234567890123456789", status: "accepted", message: "资料已接收，系统将自动准备账号并接入采集。" };
  view.respondWith(() => Response.json(accepted));
  view.submit(); await settle();
  assert.equal(view.requests.length, 1);
  assert.equal(view.requests[0].body.uid, "0001234567890123456789");
  assert.equal(view.requests[0].body.platform, "kuaishou");
  assert.equal(view.requests[0].body.account_status, "paused");
  assert.equal(view.completed[0].status, "accepted");
  assert.equal(view.completed[0].account_id, null);
});

test("unified create distinguishes display ID from UID and can infer a profile platform", async () => {
  const view = harness({ managementVersion: 3 });
  view.change("platform", "wechat_channels");
  view.change("display_account_id", "sph0123456789");
  view.change("account_status", "daily");
  view.submit(); await settle();
  assert.equal(view.requests[0].body.uid, "");
  assert.equal(view.requests[0].body.display_account_id, "sph0123456789");
  const linked = harness({ managementVersion: 3 });
  linked.change("profile_url", profileUrl); linked.change("account_status", "daily");
  linked.submit(); await settle();
  assert.equal(linked.requests[0].body.platform, null);
  assert.equal(linked.requests[0].body.profile_url, profileUrl);
});
