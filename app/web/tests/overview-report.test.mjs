import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";

const require = createRequire(import.meta.url);

// Exercise source modules in memory without rebuilding or starting a page server.
function loadSource(relativePath, imports = {}) {
  const source = readFileSync(new URL(relativePath, import.meta.url), "utf8");
  const { outputText } = ts.transpileModule(source, {
    fileName: relativePath,
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2022,
      jsx: ts.JsxEmit.ReactJSX,
      esModuleInterop: true,
    },
  });
  const compiledModule = { exports: {} };
  const localRequire = (name) => {
    if (Object.hasOwn(imports, name)) return imports[name];
    if (name === "react" || name === "react/jsx-runtime") return require(name);
    throw new Error(`Unexpected overview dependency: ${name}`);
  };
  new Function("require", "module", "exports", outputText)(localRequire, compiledModule, compiledModule.exports);
  return compiledModule.exports;
}

const model = loadSource("../app/overview/overviewModel.ts");
const format = loadSource("../app/lib/format.ts");
const Icon = (props) => React.createElement("svg", { ...props, "aria-hidden": true });
const Image = (props) => {
  const imageProps = { ...props };
  delete imageProps.unoptimized;
  return React.createElement("img", imageProps);
};
const styles = new Proxy({}, { get: (_target, property) => String(property) });
const { OverviewChannelReport } = loadSource("../app/overview/OverviewReport.tsx", {
  "next/image": Image,
  "@phosphor-icons/react": { CaretDownIcon: Icon, CaretUpIcon: Icon, InfoIcon: Icon },
  "../lib/format": format,
  "../lib/paths": { publicAssetPath: (path) => path },
  "./overviewModel": model,
  "./OverviewReport.module.css": { __esModule: true, default: styles },
});

const retainedLabels = ["卖点条数占比", "核心卖点条数占比", "卖点曝光占比", "核心卖点曝光占比"];

function quantity(value, status = "available", overrides = {}) {
  return { kind: "quantity", value, unit: "次", status, reason: "", ...overrides };
}

function ratio(numerator, denominator = 100, status = "available", overrides = {}) {
  return {
    kind: "ratio", numerator, denominator,
    percentage: denominator > 0 ? numerator * 100 / denominator : null,
    unit: "%", status, reason: "", ...overrides,
  };
}

function metricGroup(selling, core, sellingViews, coreViews) {
  return {
    selling_point_count_share: ratio(selling),
    core_selling_point_count_share: ratio(core),
    selling_point_exposure_share: ratio(sellingViews, 1000),
    core_selling_point_exposure_share: ratio(coreViews, 1000),
    // Keep legacy payload fields in the fixture to verify they are omitted by the UI.
    content_verticality: { kind: "score", value: 78, scale: 100, unit: "分", status: "available", reason: "内容垂直度" },
    automotive_user_rate: ratio(45, 100, "available", { reason: "互动用户汽车兴趣占比" }),
    acquisition_potential: { kind: "score", value: 65, scale: 100, unit: "分", status: "available", reason: "内容拉新效果预估" },
  };
}

function point(code, publicationCount, views, status = "available", overrides = {}) {
  return {
    code, label: `卖点 ${code}`, tier: "other", publication_count: publicationCount,
    count_share: ratio(publicationCount), view_count: quantity(views, status),
    exposure_share: ratio(views ?? 0, 1000, status, { percentage: views == null ? null : views / 10 }),
    provided_view_items: status === "available" ? publicationCount : 0,
    missing_view_items: status === "missing" ? publicationCount : 0,
    stale_view_items: status === "stale" ? publicationCount : 0,
    ...overrides,
  };
}

function channel() {
  return {
    platform: "douyin", label: "抖音", publication_count: 100,
    evidence_coverage_percentage: 94, valid_exposure_items: 90, exposure_coverage_percentage: 96,
    summary: { label: "渠道汇总", publication_count: 100, metrics: metricGroup(70, 30, 700, 300) },
    scenes: {
      used_car: { label: "二手车", publication_count: 30, metrics: metricGroup(20, 5, 100, 50) },
      new_car: { label: "新车", publication_count: 55, metrics: metricGroup(40, 20, 500, 200) },
      media: { label: "媒体-AI小懂", publication_count: 15, metrics: metricGroup(10, 5, 100, 50) },
    },
  };
}

function renderChannel(value) {
  return renderToStaticMarkup(React.createElement(OverviewChannelReport, { channel: value }));
}

function card(html, label) {
  const result = html.match(new RegExp(`<article[^>]*aria-label="${label}"[^>]*>([\\s\\S]*?)</article>`));
  assert.ok(result, `summary card missing: ${label}`);
  return result[1];
}

function tableBody(html) {
  const result = html.match(/<tbody>([\s\S]*?)<\/tbody>/);
  assert.ok(result, "selling-point table missing");
  return result[1];
}

test("publishable metrics preserve actual zero and read ratio percentages rather than quantity values", () => {
  assert.equal(model.visibleMetricNumber(quantity(0)), 0);
  assert.equal(model.visibleMetricNumber(quantity(0, "sample_only")), 0);
  assert.equal(model.visibleMetricNumber(ratio(0)), 0);
  assert.equal(model.visibleNumerator(ratio(0)), 0);
  assert.equal(model.visibleMetricNumber(ratio(7, 20, "available", { value: 999 })), 35);
});

test("all withheld statuses hide numeric payloads and raw numerators", () => {
  for (const status of ["below_threshold", "not_applicable", "not_calculable", "missing", "stale", "unknown"]) {
    assert.equal(model.visibleMetricNumber(quantity(9876543, status)), null, status);
    assert.equal(model.visibleMetricNumber(ratio(77, 100, status)), null, status);
    assert.equal(model.visibleNumerator(ratio(77, 100, status)), null, status);
  }
  assert.equal(model.visibleMetricNumber(undefined), null);
  assert.equal(model.visibleNumerator(undefined), null);
});

test("non-numeric, negative and non-finite values never become published numbers", () => {
  for (const value of [null, undefined, -1, NaN, Infinity, -Infinity, "12"]) {
    assert.equal(model.visibleMetricNumber(quantity(value)), null, String(value));
    assert.equal(model.visibleMetricNumber(ratio(12, 100, "available", { percentage: value })), null, String(value));
    assert.equal(model.visibleNumerator(ratio(value, 100, "available", { percentage: 12 })), null, String(value));
  }
});

test("zero, missing and tiny positive ratios have distinct presentation", () => {
  assert.equal(model.numberText(0), "0");
  assert.equal(model.numberText(null), "—");
  assert.equal(model.percentageText(0), "0.0%");
  assert.equal(model.percentageText(null), "—");
  assert.equal(model.percentageText(0.04), "<0.1%");
  assert.equal(model.percentageText(NaN), "—");
  assert.equal(model.progressWidth(null), "0%");
  assert.equal(model.progressWidth(-5), "0%");
  assert.equal(model.progressWidth(120), "100%");
});

test("ratio evidence retains a real zero but is hidden when the metric is withheld", () => {
  assert.equal(model.ratioEvidence(ratio(0), false), "0 / 100 条");
  assert.equal(model.ratioEvidence(ratio(0, 20000), true), "0 / 2 万 VV");
  assert.equal(model.ratioEvidence(ratio(70, 100, "stale"), false), null);
  assert.equal(model.ratioEvidence(ratio(70, 100, "below_threshold"), false), null);
  assert.equal(model.ratioEvidence(ratio(70, 100, "available", { denominator: undefined }), false), null);
});

test("content structure divides one cohort into core, other selling points and remaining content", () => {
  const structure = model.contentStructure(channel());
  assert.deepEqual(structure, {
    total: 100, selling: 70, core: 30, other: 40, rest: 30,
    sellingPercentage: 70, corePercentage: 30,
  });
  assert.equal(structure.core + structure.other + structure.rest, structure.total);
  assert.equal(structure.core + structure.other, structure.selling);
});

test("a channel with no selling points still has a valid all-remaining content structure", () => {
  const value = channel();
  value.summary.metrics.selling_point_count_share = ratio(0);
  value.summary.metrics.core_selling_point_count_share = ratio(0);
  assert.deepEqual(model.contentStructure(value), {
    total: 100, selling: 0, core: 0, other: 0, rest: 100,
    sellingPercentage: 0, corePercentage: 0,
  });
});

test("content structure refuses empty, invalid and mismatched denominators", () => {
  for (const total of [0, -1, 1.5, NaN, Infinity]) {
    const value = channel();
    value.publication_count = total;
    assert.equal(model.contentStructure(value), null, `total ${total}`);
  }
  for (const denominator of [0, -1, 99, 101, undefined, NaN, Infinity]) {
    for (const key of ["selling_point_count_share", "core_selling_point_count_share"]) {
      const value = channel();
      value.summary.metrics[key].denominator = denominator;
      assert.equal(model.contentStructure(value), null, `${key}: ${denominator}`);
    }
  }
});

test("content structure refuses impossible or fractional content counts", () => {
  for (const [selling, core] of [[101, 30], [70, 71], [-1, 0], [70, -1], [70.5, 30], [70, 30.5]]) {
    const value = channel();
    value.summary.metrics.selling_point_count_share = ratio(selling);
    value.summary.metrics.core_selling_point_count_share = ratio(core);
    assert.equal(model.contentStructure(value), null, `selling ${selling}, core ${core}`);
  }
});

test("raw prior counts cannot draw a content ring when a count metric is withheld", () => {
  for (const status of ["below_threshold", "missing", "stale", "not_calculable"]) {
    for (const key of ["selling_point_count_share", "core_selling_point_count_share"]) {
      const value = channel();
      value.summary.metrics[key].status = status;
      assert.equal(model.contentStructure(value), null, `${key}: ${status}`);
    }
  }
});

test("ranking prioritizes current available exposure and never promotes stale or partial values", () => {
  const input = [
    point("STALE", 8, 900000, "stale"), point("SMALL", 4, 100),
    point("PARTIAL", 12, 800000, "sample_only"), point("MISSING", 20, null, "missing"),
    point("BIG", 6, 500),
  ];
  const originalOrder = input.map((item) => item.code);
  const ranked = model.sortedSellingPoints(input);
  assert.deepEqual(ranked.items.map((item) => item.code), ["BIG", "SMALL", "MISSING", "PARTIAL", "STALE"]);
  assert.equal(ranked.ordering, "先看曝光可用项");
  assert.deepEqual(input.map((item) => item.code), originalOrder, "caller array must not be mutated");
});

test("when no exposure is current, ranking uses content counts with a deterministic code tie break", () => {
  const ranked = model.sortedSellingPoints([
    point("C", 5, 99999, "stale"), point("B", 10, null, "missing"),
    point("A", 10, 88888, "sample_only"),
  ]);
  assert.deepEqual(ranked.items.map((item) => item.code), ["A", "B", "C"]);
  assert.equal(ranked.ordering, "按内容数排序");
});

test("an available zero remains sortable while invalid numeric payloads remain unavailable", () => {
  const ranked = model.sortedSellingPoints([
    point("NULL", 20, null), point("NAN", 30, NaN),
    point("ZERO", 1, 0), point("POSITIVE", 2, 50),
  ]);
  assert.deepEqual(ranked.items.map((item) => item.code), ["POSITIVE", "ZERO", "NAN", "NULL"]);
  assert.equal(ranked.ordering, "先看曝光可用项");
  assert.equal(model.sortedSellingPoints([point("ZERO", 1, 0)]).ordering, "按累计 VV 排序");
});

test("SSR keeps the four summary metrics and the same four metrics in each of three scenes", () => {
  const html = renderChannel(channel());
  for (const label of retainedLabels) {
    assert.ok(card(html, label));
    assert.equal([...html.matchAll(new RegExp(`aria-label="${label}"`, "g"))].length, 4, label);
  }
  for (const label of ["二手车", "新车", "媒体-AI小懂"]) {
    assert.ok(html.includes(`<h4>${label}</h4>`), label);
  }
  assert.match(html, /100<\/strong> 条发布/);
  assert.match(html, /可评估内容/);
  assert.match(html, /有曝光数据/);
  assert.match(html, /已完成曝光分类/);
});

test("SSR omits the removed metrics even when legacy payloads still contain their data", () => {
  const html = renderChannel(channel());
  for (const label of ["内容垂直度", "互动用户汽车兴趣占比", "内容拉新效果预估", "拉活", "拉新率", "拉活率"]) {
    assert.equal(html.includes(label), false, label);
  }
});

test("SSR calls the remaining content group 其余内容 and does not claim it is all 无卖点", () => {
  const html = renderChannel(channel());
  assert.match(html, /核心卖点 30 条，其他卖点 40 条，其余内容 30 条/);
  assert.match(html, /其余含待评估内容/);
  assert.doesNotMatch(html, /无卖点/);
});

test("Xiaohongshu unsupported exposure stays unavailable in summary, scenes and point rows", () => {
  const value = channel();
  value.platform = "xiaohongshu";
  value.label = "小红书";
  value.valid_exposure_items = 0;
  value.exposure_coverage_percentage = null;
  for (const group of [value.summary, ...Object.values(value.scenes)]) {
    for (const key of ["selling_point_exposure_share", "core_selling_point_exposure_share"]) {
      group.metrics[key] = ratio(777, 1000, "not_calculable", { reason: "小红书接口未提供阅读数" });
    }
  }
  value.selling_points = [point("XHS", 20, null, "not_calculable", {
    view_count: quantity(null, "not_calculable", { reason: "小红书接口未提供阅读数" }),
    exposure_share: ratio(0, 0, "not_calculable", { reason: "小红书接口未提供阅读数" }),
  })];
  const html = renderChannel(value);
  for (const label of ["卖点曝光占比", "核心卖点曝光占比"]) {
    assert.match(card(html, label), /class="metricValue">—<\/strong>/);
  }
  assert.equal([...html.matchAll(/aria-label="(?:核心)?卖点曝光占比">—<\/span>/g)].length, 6);
  assert.match(tableBody(html), />—<span class="rowStatus">平台未提供阅读数/);
  assert.match(html, /小红书接口未提供阅读数，曝光指标暂不可用/);
  assert.doesNotMatch(html, /77\.7/);
  assert.doesNotMatch(html, /NaN|Infinity/);
});

test("legacy backends without selling_points show an unavailable detail section while retaining summary", () => {
  const value = channel();
  const html = renderChannel(value);
  assert.match(html, /卖点明细暂不可用/);
  assert.doesNotMatch(html, /所选时间内没有可计入的卖点内容/);
  assert.doesNotMatch(html, /<table/);
  assert.ok(card(html, "卖点条数占比"));
});

test("an explicit empty selling_points array shows no-results rather than a backend availability error", () => {
  const value = channel();
  value.selling_points = [];
  const html = renderChannel(value);
  assert.match(html, /所选时间内没有可计入的卖点内容/);
  assert.doesNotMatch(html, /卖点明细暂不可用/);
  assert.doesNotMatch(html, /<table/);
});

test("selling-point details initially render only the top three with an accessible expand control", () => {
  const value = channel();
  value.selling_points = [point("P1", 10, 100), point("P4", 10, 400), point("P2", 10, 200), point("P3", 10, 300)];
  const html = renderChannel(value);
  const body = tableBody(html);
  assert.equal([...body.matchAll(/<tr>/g)].length, 3);
  assert.match(body, />P4<\/span>/);
  assert.match(body, />P3<\/span>/);
  assert.match(body, />P2<\/span>/);
  assert.doesNotMatch(body, />P1<\/span>/);
  assert.match(html, /aria-controls="overview-selling-points-douyin" aria-expanded="false"/);
  assert.match(html, /查看全部 4 项/);
  assert.match(html, /卖点明细显示 3 项，共 4 项/);
  assert.match(body, /所选内容截至采集时的累计 VV：400/);
  assert.doesNotMatch(body, /所选时间内还无法得出结果/);
});

test("stale point exposure is not printed as a current VV or promoted above an available zero", () => {
  const value = channel();
  value.selling_points = [point("STALE", 20, 9876543, "stale"), point("ZERO", 1, 0)];
  const html = renderChannel(value);
  const body = tableBody(html);
  assert.ok(body.indexOf(">ZERO</span>") < body.indexOf(">STALE</span>"));
  assert.match(body, />0<span class="visually-hidden">/);
  assert.match(body, />—<span class="rowStatus">数据需要更新/);
  assert.doesNotMatch(body, /9,876,543|9876543/);
});

test("unclassified selling-point rows retain their counts without exposing internal bucket codes", () => {
  const value = channel();
  value.selling_points = [point("__unclassified_core__", 3, 0, "available", {
    code_missing: true, label: "卖点编码缺失（待核对）",
  })];
  const body = tableBody(renderChannel(value));
  assert.match(body, /待核对/);
  assert.match(body, /卖点编码缺失（待核对）/);
  assert.match(body, />3<\/td>/);
  assert.doesNotMatch(body, /__unclassified_core__/);
});

test("partial summary metrics remain explicitly qualified and withheld metrics do not expose stale ratios", () => {
  const value = channel();
  value.summary.metrics.selling_point_count_share.status = "sample_only";
  value.summary.metrics.selling_point_exposure_share = ratio(999, 1000, "below_threshold", { reason: "有曝光数据尚未达到要求" });
  const html = renderChannel(value);
  assert.match(card(html, "卖点条数占比"), /仅供参考/);
  assert.match(card(html, "卖点曝光占比"), /class="metricValue">—<\/strong>/);
  assert.doesNotMatch(card(html, "卖点曝光占比"), /99\.9|999/);
});

test("an empty publication window renders a clear content empty state without NaN or fabricated percentages", () => {
  const value = channel();
  value.publication_count = 0;
  for (const group of [value.summary, ...Object.values(value.scenes)]) {
    group.publication_count = 0;
    for (const [key] of model.overviewMetrics) group.metrics[key] = ratio(0, 0, "not_applicable");
  }
  value.selling_points = [];
  const html = renderChannel(value);
  assert.match(html, /所选时间内没有发布内容/);
  assert.doesNotMatch(html, /NaN|Infinity/);
  for (const label of retainedLabels) assert.match(card(html, label), /class="metricValue">—<\/strong>/);
});
