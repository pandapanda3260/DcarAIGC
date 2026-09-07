import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import React from "react";
import ts from "typescript";
import * as catalogSearch from "../app/spu-audience/vehicleCatalogSearch.ts";
import { sortVehicleCatalogRows } from "../app/spu-audience/vehicleCatalogSort.ts";

const require = createRequire(import.meta.url);
const pageSource = readFileSync(new URL("../app/spu-audience/SpuAudiencePage.tsx", import.meta.url), "utf8");

function elements(node) {
  if (Array.isArray(node)) return node.flatMap(elements);
  if (!React.isValidElement(node)) return [];
  return [node, ...elements(node.props.children)];
}

function text(node) {
  if (Array.isArray(node)) return node.map(text).join("");
  if (React.isValidElement(node)) return text(node.props.children);
  return typeof node === "string" || typeof node === "number" ? String(node) : "";
}

function row(id, brand, series, aliases = []) {
  return {
    spu_id: id, series_slug: id, brand, series, aliases, is_series_node: true,
    trim_label: null, model_year: null, powertrain: "", body_style: "",
    price_low: null, price_high: null, audience_primary: null, audience_secondary: null,
  };
}

function setup() {
  const requests = [];
  const assets = {
    ready: true,
    spu: [row("byd", "比亚迪", "汉"), row("m7", "问界", "问界M7", [{ alias: "AITO M7", ambiguous: false }]), row("m9", "问界", "问界M9")],
    audiences: [], scenes: [], audience_scene_map: [], last_run: null, stale_content_count: 0,
  };
  // Keep the page's actual hook state across renders; only the network/module completion is controlled.
  const state = [];
  const mountEffects = [];
  let unmounted = false;
  let updatesAfterUnmount = 0;
  let cursor = 0;
  const react = {
    ...React,
    useState(initial) {
      const index = cursor++;
      if (!(index in state)) state[index] = typeof initial === "function" ? initial() : initial;
      return [state[index], (next) => {
        if (unmounted) updatesAfterUnmount++;
        state[index] = typeof next === "function" ? next(state[index]) : next;
      }];
    },
    useRef(initial) {
      const index = cursor++;
      if (!(index in state)) state[index] = { current: initial };
      return state[index];
    },
    useMemo: (compute) => compute(),
    useCallback: (callback) => callback,
    useEffect(effect, dependencies) {
      const index = cursor++;
      if (!(index in state)) {
        state[index] = true;
        if (dependencies?.length === 0) mountEffects.push({ effect, cleanup: null });
      }
    },
  };
  const modules = {
    react,
    "react/jsx-runtime": require("react/jsx-runtime"),
    "@tanstack/react-query": {
      useQuery: (options) => ({ data: options === "assets" ? assets : null, isPending: false, isError: false }),
      useQueryClient: () => ({}),
    },
    "@phosphor-icons/react": new Proxy({}, { get: () => "svg" }),
    "../components/AppShell": "main",
    "../components/Feedback": { Feedback: "feedback", Loading: "loading", Notice: "notice" },
    "../components/Pagination": { Pagination: "pagination" },
    "../lib/api": {},
    "../lib/queryContracts": {},
    "../lib/queries": { spuAssetsQueryOptions: () => "assets", spuStatsQueryOptions: () => "stats" },
    "./VehicleBrandLogo": { VehicleBrandLogo: "brand-logo" },
    "./vehicleCatalogSort": { sortVehicleCatalogRows },
  };
  const source = pageSource.replace('catalogSearchPromise = import("./vehicleCatalogSearch")', "catalogSearchPromise = __loadSearch()");
  const { outputText } = ts.transpileModule(source, {
    fileName: "SpuAudiencePage.tsx",
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true },
  });
  const compiled = { exports: {} };
  new Function("require", "module", "exports", "__loadSearch", outputText)(
    (name) => { assert.ok(Object.hasOwn(modules, name), `Unexpected static page dependency: ${name}`); return modules[name]; },
    compiled, compiled.exports,
    () => new Promise((resolve, reject) => requests.push({ resolve, reject })),
  );
  return {
    requests,
    get updatesAfterUnmount() { return updatesAfterUnmount; },
    unmount() {
      unmounted = true;
      for (const entry of mountEffects) entry.cleanup?.();
    },
    replayMountEffects() {
      for (const entry of mountEffects) { entry.cleanup?.(); entry.cleanup = entry.effect(); }
    },
    render() {
      cursor = 0;
      const tree = compiled.exports.default();
      for (const entry of mountEffects) if (!entry.cleanup) entry.cleanup = entry.effect();
      const nodes = elements(tree);
      return {
        input: nodes.find((node) => node.type === "input" && node.props.type === "search"),
        status: text(nodes.find((node) => node.props.id === "spu-catalog-search-status")),
        rows: nodes.filter((node) => node.props.className === "spu-series-row").map(text),
        retry: nodes.find((node) => node.type === "button" && text(node) === "重试搜索"),
        table: nodes.find((node) => node.props["aria-label"] === "车系与款型库表格"),
      };
    },
  };
}

async function settle() {
  await new Promise((resolve) => setImmediate(resolve));
}

test("the default catalog imports no search dictionary and focus prepares one shared request", async () => {
  const order = readFileSync(new URL("../app/spu-audience/vehicleCatalogSort.ts", import.meta.url), "utf8");
  const pageAst = ts.createSourceFile("page.tsx", pageSource, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const runtimeImports = pageAst.statements.filter((statement) => ts.isImportDeclaration(statement) && !statement.importClause?.isTypeOnly).map((statement) => statement.moduleSpecifier.text);
  assert.ok(!runtimeImports.some((name) => /pinyin-pro|vehicleCatalogSearch/.test(name)));
  assert.doesNotMatch(order, /pinyin-pro|vehicleCatalogSearch/);
  const page = setup();
  let view = page.render();
  assert.equal(view.rows.length, 3);
  assert.equal(page.requests.length, 0);
  view.input.props.onFocus();
  page.render().input.props.onFocus();
  assert.equal(page.requests.length, 1);
  page.requests[0].resolve(catalogSearch);
  await settle();
  assert.equal(page.render().rows.length, 3);
});

test("pending search preserves the directory and applies the latest query after loading", async () => {
  const page = setup();
  page.render().input.props.onChange({ target: { value: "han" } });
  page.render().input.props.onChange({ target: { value: "wenjiem7" } });
  let view = page.render();
  assert.equal(page.requests.length, 1);
  assert.equal(view.rows.length, 3);
  assert.match(view.status, /正在准备搜索/);
  assert.equal(view.table.props["aria-busy"], true);
  page.requests[0].resolve(catalogSearch);
  await settle();
  view = page.render();
  assert.equal(view.input.props.value, "wenjiem7");
  assert.equal(view.rows.length, 1);
  assert.match(view.rows[0], /问界M7/);
  assert.equal(view.status, "1 个结果");
  assert.equal(view.table.props["aria-busy"], false);
  view.input.props.onChange({ target: { value: "wj" } });
  assert.equal(page.render().rows.length, 2);
  assert.equal(page.requests.length, 1);
});

test("clearing the input before search arrives cannot restore obsolete results", async () => {
  const page = setup();
  page.render().input.props.onChange({ target: { value: "wenjiem7" } });
  page.render().input.props.onChange({ target: { value: "" } });
  page.requests[0].resolve(catalogSearch);
  await settle();
  const view = page.render();
  assert.equal(view.input.props.value, "");
  assert.equal(view.status, "");
  assert.equal(view.rows.length, 3);
});

test("failed loading offers retry without pretending the query had no matches", async () => {
  const page = setup();
  page.render().input.props.onChange({ target: { value: "ＡＩＴＯ　Ｍ７" } });
  page.requests[0].reject(new Error("temporary chunk failure"));
  await settle();
  let view = page.render();
  assert.match(view.status, /搜索暂不可用/);
  assert.equal(view.rows.length, 3);
  assert.ok(view.retry);
  view.retry.props.onClick();
  assert.equal(page.requests.length, 2);
  page.requests[1].resolve(catalogSearch);
  await settle();
  view = page.render();
  assert.equal(view.status, "1 个结果");
  assert.match(view.rows[0], /问界M7/);
  assert.equal(view.retry, undefined);
});

test("search completion does not update a page that was navigated away from", async () => {
  const page = setup();
  page.render().input.props.onChange({ target: { value: "wenjiem7" } });
  page.unmount();
  page.requests[0].resolve(catalogSearch);
  await settle();
  assert.equal(page.updatesAfterUnmount, 0);
});

test("StrictMode mount cleanup and setup still allow the pending search to complete", async () => {
  const page = setup();
  page.render().input.props.onChange({ target: { value: "wenjiem7" } });
  page.replayMountEffects();
  page.requests[0].resolve(catalogSearch);
  await settle();
  const view = page.render();
  assert.equal(view.rows.length, 1);
  assert.match(view.rows[0], /问界M7/);
  assert.equal(view.table.props["aria-busy"], false);
});
