import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import test from "node:test";
import React from "react";
import * as jsxRuntime from "react/jsx-runtime";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";

const modules = new Map();
function load(filename) {
  if (modules.has(filename)) return modules.get(filename).exports;
  const compiledModule = { exports: {} };
  modules.set(filename, compiledModule);
  const source = ts.transpileModule(readFileSync(filename, "utf8"), { compilerOptions: {
    target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS,
    jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true,
  } }).outputText;
  vm.runInNewContext(source, { module: compiledModule, exports: compiledModule.exports, Intl, Date, process: { env: {} }, require(name) {
    if (name === "react/jsx-runtime") return jsxRuntime;
    assert.ok(name.startsWith("."), name);
    const resolved = path.resolve(path.dirname(filename), name);
    const file = [resolved, `${resolved}.ts`, `${resolved}.tsx`].find(existsSync);
    assert.ok(file, name);
    return load(file);
  } }, { filename });
  return compiledModule.exports;
}
const ReportDimensions = load(fileURLToPath(new URL("../app/tasks/[id]/ReportDimensions.tsx", import.meta.url))).default;
const render = report => renderToStaticMarkup(React.createElement(ReportDimensions, { report }));
const direction = [{ key: "new_car", count: 2, percentage: 100 }];

test("report displays independent account groups, business directions and work directions", () => {
  const html = render({ account_group_dimensions: [{ key: "image_text", count: 2, percentage: 100 }],
    business_direction_dimensions: [{ key: "used_car_c2", count: 2, percentage: 100 }], content_direction_dimensions: direction });
  for (const text of ["账号分组", "图文号", "业务方向", "二手车C2", "作品内容方向", "新车", "2 条 · 100%"]) assert.ok(html.includes(text), text);
  assert.doesNotMatch(html, /账号类型|原创/);
});

test("historical payload cannot reintroduce old labels or infer a group from original", () => {
  const html = render({ account_type_dimensions: [{ key: "original", count: 2, percentage: 100 }], content_direction_dimensions: direction });
  assert.match(html, /这份报告尚未记录账号分组和业务方向/);
  assert.match(html, /作品内容方向/);
  assert.doesNotMatch(html, /账号类型|原创|创新号/);
});

test("unknown historical classification remains unfilled and pending reports render safely", () => {
  const html = render({ account_group_dimensions: [{ key: "unknown", count: 2, percentage: 100 }],
    business_direction_dimensions: [{ key: "unknown", count: 2, percentage: 100 }], content_direction_dimensions: direction });
  assert.equal((html.match(/未填写/g) || []).length, 2);
  assert.doesNotThrow(() => render(undefined));
});
