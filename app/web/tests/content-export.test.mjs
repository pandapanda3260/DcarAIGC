import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";
import test from "node:test";
import React from "react";
import * as jsxRuntime from "react/jsx-runtime";
import ts from "typescript";
import * as exportsClient from "../app/contents/contentExport.ts";

const { exportFilters, exportFilterKey, exportGenerationBlock, hasExportFilters, matchingActiveExport, readPendingExport, isContentExportJob } = exportsClient;
const emptyFilters = { page: 1, page_size: 50, query: "", platform: null, account_group: null, business_direction: null,
  content_direction: null, selling_point: null, spu_series: null, audience: null, scene: null };
const filters = { ...emptyFilters, platform: "douyin", published_from: "2026-09-01", published_to: "2026-09-07" };
const requestId = "12345678-1234-4234-8234-123456789abc";
const job = (overrides = {}) => ({ id: "c361cb0d-f3ca-4304-ad33-32b29bdc4db6", status: "running", filters,
  created_at: "2026-09-13T10:00:00Z", started_at: null, completed_at: null, total: 120, completed_rows: 40,
  filename: "内容筛选结果.xlsx", error: null, request_id: requestId, ...overrides });

test("export scope retains every applied filter and omits list pagination from identity", () => {
  const source = { ...filters, page: 9, page_size: 20, query: "00123456789012345678", account_group: "self_operated",
    business_direction: "new_car", content_direction: "used_car", selling_point: "A1", spu_series: "车型", audience: "new", scene: "commute" };
  const normalized = exportFilters(source);
  assert.deepEqual(normalized, { ...source, page: 1, page_size: 100 });
  assert.equal(exportFilterKey(source), exportFilterKey({ ...source, page: 1, page_size: 100 }));
  assert.notEqual(exportFilterKey(source), exportFilterKey({ ...source, published_to: "2026-09-08" }));
  assert.equal(hasExportFilters(emptyFilters), false);
  assert.equal(hasExportFilters({ ...emptyFilters, query: "  " }), false);
  assert.equal(matchingActiveExport([job({ status: "failed" }), job()], { ...filters, page: 8 }).status, "running");
});

test("generation rejects stale, failed, unapplied, missing and zero result counts", () => {
  const input = { filters, total: 50, queryPending: false, queryError: false, unappliedQuery: false };
  assert.equal(exportGenerationBlock(input), "");
  for (const extra of [{ total: null }, { total: NaN }, { total: -1 }, { total: 0 }, { queryPending: true },
    { queryError: true }, { unappliedQuery: true }, { filters: emptyFilters }]) assert.ok(exportGenerationBlock({ ...input, ...extra }));
  assert.equal(readPendingExport(JSON.stringify({ request_id: requestId, filters }))?.request_id, requestId);
  assert.equal(readPendingExport(JSON.stringify({ request_id: "bad", filters })), null);
  assert.equal(isContentExportJob(job()), true);
  assert.equal(isContentExportJob(job({ total: "120" })), false);
  assert.equal(isContentExportJob(job({ id: "../another-user" })), false);
});

const source = readFileSync(new URL("../app/contents/ContentExportControl.tsx", import.meta.url), "utf8");
const javascript = ts.transpileModule(source, { compilerOptions: { jsx: ts.JsxEmit.ReactJSX,
  target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, esModuleInterop: true } }).outputText;

function harness(options = {}) {
  const states = [], effects = [], cleanups = [], calls = [], stored = options.storage ?? new Map(), downloads = [], appliedDates = [];
  let cursor = 0, sequence = 0, jobs = options.jobs ?? [], changed = false, queryOptions, dialogOptions;
  const session = { data: { username: "fixture", role: "admin" }, isError: false };
  let props = { filters, total: 120, queryPending: false, queryError: false, unappliedQuery: false,
    onApplyQuery() { calls.push({ applyQuery: true }); }, onApplyDates(...value) { appliedDates.push(value); }, sellingPoints: [], owner: "fixture", ...options.props };
  class ApiRequestError extends Error { constructor(message, { status } = {}) { super(message); this.status = status; } }
  const hooks = { ...React,
    useState(initial) { const id = cursor++; if (!(id in states)) states[id] = typeof initial === "function" ? initial() : initial;
      return [states[id], (next) => { const value = typeof next === "function" ? next(states[id]) : next; if (!Object.is(states[id], value)) { states[id] = value; changed = true; } }]; },
    useRef(initial) { const id = cursor++; if (!(id in states)) states[id] = { current: initial }; return states[id]; },
    useId() { return "export-heading"; },
    useEffect(callback, deps) { const id = cursor++; if (!(id in states) || deps.some((value, index) => !Object.is(value, states[id][index]))) { states[id] = deps; effects.push(() => { cleanups[id]?.(); cleanups[id] = callback(); }); } },
  };
  const loadedModule = { exports: {} };
  vm.runInNewContext(javascript, { module: loadedModule, exports: loadedModule.exports, localStorage: {
    getItem: (key) => stored.get(key) ?? null,
    setItem: (key, value) => { if (options.storageFailure) throw new Error("blocked"); stored.set(key, value); },
    removeItem: (key) => stored.delete(key),
  }, crypto: { randomUUID: () => `12345678-1234-4234-8234-${String(++sequence).padStart(12, "0")}` },
  require(name) {
    if (name === "react") return hooks;
    if (name === "react/jsx-runtime") return jsxRuntime;
    if (name.endsWith(".module.css")) return { __esModule: true, default: {} };
    if (name === "@tanstack/react-query") return {
      useQuery(value) {
        if (value.session) return session;
        queryOptions = value;
        return { data: jobs, isPending: false, isError: false, isSuccess: true, isFetching: false, refetch: async () => {} };
      },
      useQueryClient: () => ({ cancelQueries: async () => {}, setQueryData(_key, updater) { jobs = updater(jobs); changed = true; } }),
    };
    if (name.endsWith("/lib/queries")) return { sessionQueryOptions: () => ({ session: true }) };
    if (name.endsWith("/lib/api")) return {
      ApiRequestError,
      jsonRequest: (body) => ({ method: "POST", body: JSON.stringify(body) }),
      readQueryJson: async (url, request) => {
        const body = JSON.parse(request.body); calls.push({ url, body });
        if (options.post) return options.post(body, calls.length, ApiRequestError);
        return { job: job({ filters: body.filters, request_id: body.request_id }), reused: false };
      },
      readDownload: async (...args) => { calls.push({ download: args }); if (options.download) return options.download(); return { blob: new Blob(["file"]), filename: "导出.xlsx" }; },
      saveDownload: (file) => downloads.push(file),
    };
    if (name.endsWith("/lib/accountClassification")) return { accountGroupLabel: (v) => v, businessDirectionLabel: (v) => v };
    if (name.endsWith("/lib/format")) return { label: (v) => v, formatDateTime: (v) => v };
    if (name.endsWith("/components/useDialogFocus")) return { useDialogFocus(_open, _ref, value) { dialogOptions = value; } };
    if (name === "./ContentDateFilter") return { __esModule: true, default: function DateFilter() { return null; } };
    if (name === "./contentExport") return exportsClient;
    throw new Error(`unexpected import: ${name}`);
  } });
  let tree;
  function render() {
    let passes = 0;
    do { changed = false; cursor = 0; tree = loadedModule.exports.OwnedContentExportControl(props); while (effects.length) effects.shift()(); } while (changed && ++passes < 10);
    return tree;
  }
  function find(node, predicate) {
    if (!node || typeof node !== "object") return null;
    if (Array.isArray(node)) { for (const item of node) { const hit = find(item, predicate); if (hit) return hit; } return null; }
    return predicate(node) ? node : find(node.props?.children, predicate);
  }
  function text(node) { if (node == null || typeof node === "boolean") return ""; if (typeof node !== "object") return String(node);
    if (Array.isArray(node)) return node.map(text).join(""); return text(node.props?.children); }
  render();
  return { calls, stored, downloads, appliedDates, render, session,
    get props() { return props; }, update(value) { props = { ...props, ...value }; return render(); },
    find(predicate) { return find(render(), predicate); },
    button(label) { return this.find((node) => node.type === "button" && text(node) === label); },
    async click(label) { const button = this.button(label); assert.ok(button, `button ${label}`); if (button.props.disabled) return;
      button.props.onClick(); await new Promise((resolve) => setImmediate(resolve)); render(); },
    get queryKey() { return queryOptions.queryKey; },
    wrapper() { cursor = 0; return loadedModule.exports.default(props); },
    escape() { dialogOptions.onClose(); render(); },
    unmount() { cleanups.forEach((cleanup) => cleanup?.()); },
  };
}

test("date selection is required only for an unfiltered export and stale counts cannot submit", async () => {
  const h = harness({ props: { filters: emptyFilters, total: 64046 } });
  assert.equal(h.button("最近导出"), null);
  await h.click("导出筛选结果");
  assert.ok(h.button("最近导出"));
  assert.equal(h.button("生成 Excel").props.disabled, true);
  const date = h.find((node) => node.type?.name === "DateFilter");
  assert.ok(date); date.props.onChange("2026-09-01", "2026-09-07");
  assert.deepEqual(h.appliedDates, [["2026-09-01", "2026-09-07"]]);
  h.update({ filters, total: 64046, queryPending: true });
  await h.click("生成 Excel"); assert.equal(h.calls.length, 0);
  h.update({ total: 0, queryPending: false }); assert.equal(h.button("生成 Excel").props.disabled, true);
  h.update({ total: 3, unappliedQuery: true }); assert.equal(h.button("生成 Excel").props.disabled, true);
  await h.click("应用搜索词"); assert.deepEqual(h.calls, [{ applyQuery: true }]);
});

test("history returns to confirmation, active jobs reuse work and failed jobs get a new id", async () => {
  const active = harness({ jobs: [job()] }); await active.click("导出筛选结果"); await active.click("查看生成进度");
  assert.equal(active.calls.length, 0);
  assert.ok(active.find((node) => node.type === "h2" && node.props.children === "最近导出"));
  await active.click("返回");
  assert.ok(active.find((node) => node.type === "h2" && node.props.children === "导出筛选结果"));
  assert.ok(active.button("查看生成进度"));
  await active.click("最近导出");
  assert.ok(active.button("返回"));
  assert.equal(active.calls.length, 0);
  const failed = harness({ jobs: [job({ status: "failed", error: "文件生成失败" })] });
  await failed.click("导出筛选结果"); await failed.click("最近导出"); await failed.click("重新生成");
  assert.equal(failed.calls.length, 1); assert.notEqual(failed.calls[0].body.request_id, requestId);
  assert.deepEqual(failed.calls[0].body.filters, exportFilters(filters));
});

test("uncertain submission survives remount and repeats the same id and filters", async () => {
  const stored = new Map();
  const h = harness({ storage: stored, post: async () => { throw new Error("连接中断"); } });
  await h.click("导出筛选结果"); await h.click("生成 Excel");
  assert.equal(h.calls.length, 1); assert.equal(stored.size, 1);
  const request = h.calls[0].body; h.unmount();
  const restored = harness({ storage: stored, props: { filters: { ...filters, platform: "xiaohongshu" } } });
  await restored.click("导出筛选结果"); await restored.click("最近导出"); await restored.click("重新确认结果");
  assert.deepEqual(restored.calls[0].body, request); assert.equal(stored.size, 0);
});

test("a double click and unavailable browser storage cannot create extra submissions", async () => {
  let complete;
  const h = harness({ post: (body) => new Promise((resolve) => { complete = () => resolve({ job: job({ request_id: body.request_id, filters: body.filters }), reused: false }); }) });
  await h.click("导出筛选结果");
  const button = h.button("生成 Excel"); button.props.onClick(); button.props.onClick();
  assert.equal(h.calls.length, 1); complete(); await new Promise((resolve) => setImmediate(resolve));
  const blocked = harness({ storageFailure: true }); await blocked.click("导出筛选结果"); await blocked.click("生成 Excel");
  assert.equal(blocked.calls.length, 0);
});

test("explicit 4xx rejections release pending state but uncertain failures keep the same id", async () => {
  for (const status of [400, 403, 413, 422, 429]) {
    const h = harness({ post: async (_body, _count, ErrorType) => { throw new ErrorType("请求被拒绝", { status }); } });
    await h.click("导出筛选结果"); await h.click("生成 Excel");
    assert.equal(h.stored.size, 0, `HTTP ${status}`);
    assert.ok(h.find((node) => node.props?.role === "alert"));
  }
  for (const status of [408, 503]) {
    const h = harness({ post: async (_body, _count, ErrorType) => { throw new ErrorType("结果待确认", { status }); } });
    await h.click("导出筛选结果"); await h.click("生成 Excel");
    assert.equal(h.stored.size, 1, `HTTP ${status}`);
  }
});

test("Escape in the nested date picker leaves the outer export dialog open", async () => {
  const h = harness({ props: { filters: emptyFilters } }); await h.click("导出筛选结果");
  const dialog = h.find((node) => node.props?.role === "dialog");
  dialog.props.ref.current = { querySelector: () => ({}) };
  h.escape(); assert.ok(h.find((node) => node.props?.role === "dialog"));
  dialog.props.ref.current = { querySelector: () => null };
  h.escape(); assert.equal(h.find((node) => node.props?.role === "dialog"), null);
});

test("download errors stay in the dialog and late results cannot download for another identity", async () => {
  const failed = harness({ jobs: [job({ status: "succeeded" })], download: async () => { throw new Error("文件已失效"); } });
  await failed.click("导出筛选结果"); await failed.click("最近导出"); await failed.click("下载 Excel");
  assert.equal(failed.downloads.length, 0); assert.ok(failed.find((node) => node.props?.role === "alert"));
  let finish;
  const late = harness({ jobs: [job({ status: "succeeded" })], download: () => new Promise((resolve) => { finish = resolve; }) });
  await late.click("导出筛选结果"); await late.click("最近导出"); await late.click("下载 Excel"); late.unmount(); finish({ blob: new Blob(["file"]), filename: "old.xlsx" });
  await new Promise((resolve) => setImmediate(resolve)); assert.equal(late.downloads.length, 0);
});

test("identity keys isolate caches, remount controls and never load another user's pending request", async () => {
  const storage = new Map([["dcar-content-export-pending-v1:fixture", JSON.stringify({ request_id: requestId, filters })]]);
  const h = harness({ storage, props: { owner: "other" } });
  assert.deepEqual([...h.queryKey], ["content-exports", "other"]);
  assert.equal(h.button("最近导出"), null);
  await h.click("导出筛选结果"); await h.click("最近导出");
  assert.equal(h.button("重新确认结果"), null);
  const previous = h.wrapper().key; h.session.data.username = "another";
  assert.notEqual(h.wrapper().key, previous);
  h.session.isError = true; assert.equal(h.wrapper().props.disabled, true);
});
