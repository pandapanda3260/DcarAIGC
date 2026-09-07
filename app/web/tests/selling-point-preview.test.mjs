import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";
import * as React from "react";
import * as jsxRuntime from "react/jsx-runtime";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";

const source = await readFile(new URL("../app/selling-points/SellingPointsPage.tsx", import.meta.url), "utf8");
const pageCode = ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX },
}).outputText;

function publishedData() {
  const scenes = { E1: "used_car", X1: "new_car", M1: "media" };
  return {
    taxonomy: { version: "selling-points-v5.2", status: "published" },
    items: Object.entries(scenes).map(([code, scene]) => ({
      code, label: `正式标准 ${code}`, definition: `正式定义 ${code}`, tier: "core", scenes: [scene],
      scene_hits: { [scene]: { primary_hits: 120, total_hits: 140 } },
      window_hits: {
        last_week: { [scene]: { primary_hits: 13, total_hits: 20, channels: { douyin: { primary_hits: 2, primary_views: 100 } } } },
        this_week: { [scene]: { primary_hits: 4, total_hits: 7, channels: { douyin: { primary_hits: 3, primary_views: 50 } } } },
      },
    })),
    windows: Object.fromEntries(["last_week", "this_week"].map((window) => [window, {
      scene_denominators: Object.fromEntries(Object.values(scenes).map((scene) => [scene, {
        douyin: window === "last_week" ? { publication_count: 10, valid_exposure_views: 400 } : { publication_count: 5, valid_exposure_views: 500 },
        xiaohongshu: { publication_count: 0, valid_exposure_views: 0 },
      }])),
    }])),
  };
}

function isolatedPage(data) {
  const states = [];
  const queries = [];
  const activeOptions = { queryKey: ["selling-points", "active"] };
  let stateIndex = 0;
  const modules = {
    "react/jsx-runtime": jsxRuntime,
    react: { ...React, useState: (initial) => {
      const index = stateIndex++;
      if (!(index in states)) states[index] = initial;
      return [states[index], (value) => { states[index] = value; }];
    } },
    "@tanstack/react-query": { useQuery: (options) => {
      queries.push(options);
      assert.equal(options, activeOptions, "the page reads only the published standards query");
      return { data, isPending: false, isError: false };
    } },
    "@phosphor-icons/react": Object.fromEntries(["CarIcon", "GameControllerIcon", "StarFourIcon", "TelevisionIcon"].map((name) => [name, () => null])),
    "../components/AppShell": { default: ({ children, actions }) => React.createElement("main", null, actions, children) },
    "../components/Feedback": {
      Loading: ({ label }) => React.createElement("p", null, label),
      Notice: ({ children }) => React.createElement("p", null, children),
    },
    "../lib/format": { label: (value) => ({ used_car: "二手车", new_car: "新车", media: "媒体" })[value] ?? value },
    "../lib/queries": { activeSellingPointsQueryOptions: () => activeOptions },
  };
  const context = vm.createContext({ exports: {}, require: (name) => {
    assert.ok(Object.hasOwn(modules, name), `unexpected page dependency: ${name}`);
    return modules[name];
  } });
  vm.runInContext(pageCode, context);
  return { queries, render: () => {
    stateIndex = 0;
    const tree = context.exports.default();
    return { tree, html: renderToStaticMarkup(tree) };
  } };
}

function selectElements(node) {
  if (Array.isArray(node)) return node.flatMap(selectElements);
  if (!React.isValidElement(node)) return [];
  return [...(node.type === "select" ? [node] : []), ...selectElements(node.props.children)];
}

test("published selling point standards render without authoring controls or an empty action column", () => {
  const page = isolatedPage(publishedData());
  const { html, tree } = page.render();
  assert.equal(page.queries.length, 1);
  assert.doesNotMatch(html, /<(?:button|textarea|input|form)\b|role="dialog"|新增卖点|编辑|删除|草稿|匹配规则|查看正式生效版/);
  assert.equal(selectElements(tree).length, 3);
  const tables = html.match(/<table\b[\s\S]*?<\/table>/g);
  assert.equal(tables.length, 3);
  for (const [index, code] of ["E1", "X1", "M1"].entries()) {
    const table = tables[index];
    assert.ok(table.includes(`正式标准 ${code}`));
    assert.ok(table.includes(`正式定义 ${code}`));
    assert.equal((table.match(/<col\b/g) ?? []).length, 8);
    for (const row of table.match(/<tr\b[\s\S]*?<\/tr>/g)) {
      assert.equal((row.match(/<(?:th|td)\b/g) ?? []).length, 8);
    }
    assert.match(table, /<strong>13<\/strong><small>全部 20<\/small>/);
    assert.match(table, /title="2 \/ 10 条发布">20\.0%/);
    assert.match(table, /title="100 \/ 400 次有效曝光">25\.0%/);
    assert.match(table, /title="小红书窗口内无发布">—/);
  }
});

test("changing a statistics window updates counts and both ratios across the three scenes", () => {
  const page = isolatedPage(publishedData());
  selectElements(page.render().tree)[0].props.onChange({ target: { value: "this_week" } });
  const { tree, html } = page.render();
  assert.ok(selectElements(tree).every((select) => select.props.value === "this_week"));
  assert.equal((html.match(/本周 4 次主要卖点命中/g) ?? []).length, 3);
  assert.equal((html.match(/<strong>4<\/strong><small>全部 7<\/small>/g) ?? []).length, 3);
  assert.equal((html.match(/title="3 \/ 5 条发布">60\.0%/g) ?? []).length, 3);
  assert.equal((html.match(/title="50 \/ 500 次有效曝光">10\.0%/g) ?? []).length, 3);
});

test("standards retain scene totals when window statistics are unavailable", () => {
  const data = publishedData();
  delete data.windows;
  const { html } = isolatedPage(data).render();
  assert.equal((html.match(/<strong>120<\/strong><small>全部 140<\/small>/g) ?? []).length, 3);
  assert.doesNotMatch(html, /selling-point-share-value/);
});
