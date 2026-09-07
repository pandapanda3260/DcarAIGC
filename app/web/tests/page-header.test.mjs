import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import postcss from "postcss";
import ts from "typescript";

const styles = await readFile(new URL("../app/globals.css", import.meta.url), "utf8");
const stylesheet = postcss.parse(styles);

function rulesFor(selector) {
  const rules = [];
  stylesheet.walkRules((rule) => {
    if (rule.selectors.includes(selector)) rules.push(rule);
  });
  return rules;
}

function declarations(rule) {
  return Object.fromEntries(rule.nodes.filter((node) => node.type === "decl").map((node) => [node.prop, node.value]));
}

function baseStyle(selector) {
  const rules = rulesFor(selector).filter((rule) => rule.parent.type === "root");
  assert.equal(rules.length, 1, `${selector} must have one shared base rule`);
  return declarations(rules[0]);
}

function assertProperties(actual, expected) {
  for (const [property, value] of Object.entries(expected)) assert.equal(actual[property], value, property);
}

async function sourceFile(path) {
  const source = await readFile(new URL(`../app/${path}`, import.meta.url), "utf8");
  return ts.createSourceFile(path, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
}

function collect(node, predicate) {
  const found = [];
  function visit(child) {
    if (predicate(child)) found.push(child);
    ts.forEachChild(child, visit);
  }
  visit(node);
  return found;
}

function classElements(node, className) {
  return collect(node, (child) => ts.isJsxElement(child) && child.openingElement.attributes.properties.some((attribute) =>
    ts.isJsxAttribute(attribute) && attribute.name.text === "className"
    && attribute.initializer && ts.isStringLiteral(attribute.initializer)
    && attribute.initializer.text.split(/\s+/).includes(className),
  ));
}

function textOf(element) {
  return element.children.filter(ts.isJsxText).map((node) => node.text).join("").trim();
}

test("all six page titles use the approved shared typography and neutral background", () => {
  assertProperties(baseStyle(":root"), { "--ink": "#13262d", "--canvas": "#f3f6f6" });
  assertProperties(baseStyle(".page-header"), {
    background: "var(--canvas)", "font-family": "var(--font-geist-sans)",
    padding: "28px 0 24px", "min-height": "0", "flex-wrap": "wrap",
  });
  assertProperties(baseStyle(".page-header-eyebrow"), {
    display: "block", "font-size": "12px", "line-height": "18px", "font-weight": "500",
    color: "#60717a", "letter-spacing": "0", margin: "0 0 8px",
  });
  assertProperties(baseStyle(".page-header-title"), {
    "font-size": "28px", "line-height": "36px", "font-weight": "600",
    color: "var(--ink)", "letter-spacing": "0", margin: "0", "overflow-wrap": "anywhere",
  });
  assertProperties(baseStyle(".page-header-description"), {
    "font-size": "14px", "line-height": "24px", "font-weight": "400",
    color: "#60717a", "letter-spacing": "0", margin: "8px 0 0", "overflow-wrap": "anywhere",
  });
  assertProperties(baseStyle(".main-area:has(.page-header)"), { "padding-inline": "32px", background: "var(--canvas)" });
});

test("narrow screens share one 24px title rule without page-specific overrides", () => {
  const titleRules = rulesFor(".page-header-title");
  assert.equal(titleRules.length, 2);
  const narrow = titleRules.find((rule) => rule.parent.type === "atrule");
  assert.equal(narrow.parent.name, "media");
  assert.equal(narrow.parent.params, "(max-width: 540px)");
  assertProperties(declarations(narrow), { "font-size": "24px", "line-height": "32px" });
  for (const [selector, expected] of [
    [".main-area:has(.page-header)", { "padding-inline": "20px" }],
    [".page-header", { "padding-block": "24px" }],
  ]) {
    const rule = rulesFor(selector).find((item) => item.parent.type === "atrule" && item.parent.params === "(max-width: 540px)");
    assert.ok(rule, `${selector} must adapt at the shared breakpoint`);
    assertProperties(declarations(rule), expected);
  }
  assert.equal(rulesFor(".page-header-eyebrow").length, 1);
  assert.equal(rulesFor(".page-header-description").length, 1);
  stylesheet.walkRules((rule) => {
    if (rule.selector.includes("data-section")) {
      assert.doesNotMatch(rule.selector, /page-header-(?:title|eyebrow|description|copy)/);
    }
  });
  assert.doesNotMatch(styles, /\.topbar|overview-automotive-lines\.webp|selling-points-hero-bg\.png/);
});

test("headers and their first content blocks no longer overlap", () => {
  assertProperties(baseStyle(".page-stack:has(> .page-header)"), { "padding-top": "0", "row-gap": "24px" });
  assertProperties(baseStyle(".main-area > .page-header ~ .page-stack"), { "margin-top": "0", "padding-top": "24px" });
  for (const selector of [".selling-points-page", ".spu-audience-page"]) {
    assert.equal(baseStyle(selector)["margin-top"], "0");
    for (const rule of rulesFor(selector)) assert.doesNotMatch(declarations(rule)["margin-top"] ?? "", /^-/);
  }
  assertProperties(baseStyle(".page-header-copy"), { "min-width": "0" });
  assertProperties(baseStyle(".page-header-actions"), { "flex-wrap": "wrap", "max-width": "100%" });
  // Long selling-point activation notices must wrap; only controls stay on one line.
  assert.equal(baseStyle(".page-header-actions > *")["white-space"], undefined);
  assert.equal(baseStyle(".page-header-actions > :is(button, a, label)")["white-space"], "nowrap");
});

test("the three shell headers share the same markup and retain their original copy", async () => {
  const shell = await sourceFile("components/AppShell.tsx");
  const headers = classElements(shell, "page-header");
  assert.equal(headers.length, 1);
  for (const role of ["copy", "eyebrow", "title", "description", "actions"]) {
    assert.equal(classElements(headers[0], `page-header-${role}`).length, 1, role);
  }
  const copyDeclaration = collect(shell, (node) => ts.isVariableDeclaration(node) && node.name.getText(shell) === "pageCopy")[0];
  const copies = new Map(copyDeclaration.initializer.properties.map((property) => [property.name.text, property.initializer]));
  for (const [page, title, eyebrow, description] of [
    ["overview", "数据概览", "全渠道运营", "多渠道内容运营核心指标总览与场景分析"],
    ["selling-points", "卖点标准", "评估标准基线", "围绕 E、X、M 三个业务场景，提供清晰的标签定义与分级规则，为内容评估与运营复核提供统一规范。"],
    ["spu-audience", "SPU人群（未生效）", "车型 × 人群 × 场景", "维护车型、人群与场景的识别规则，并按统计窗口查看三者的数据表现。"],
    ["users", "用户权限", "用户管理&质检", "查看工作台用户的注册信息与权限等级，修改资料或删除用户。"],
  ]) {
    const copy = Object.fromEntries(copies.get(page).properties.map((property) => [property.name.text, property.initializer.text]));
    assert.deepEqual(copy, { eyebrow, title, description });
  }
});

test("the three custom shell headers keep their title, description and existing actions", async () => {
  for (const [path, eyebrow, title, description, actions] of [
    ["contents/ContentsPage.tsx", "内容资料库", "发布内容明细", "更新数据时会同步更新详情、指标以及已保存的视频和图片；重复提醒会指向最早发布的内容。", ["下载内容表格"]],
    ["accounts/AccountsPage.tsx", ["系统托管名单", "矩阵通名册"], "账号信息", "一个平台账号一行，手机号仅作运营信息；未采集的粉丝和平台作品总量显示“—”。", ["新增系统账号", "批量上传账号", "下载账号表格"]],
    ["tasks/TasksPage.tsx", "每次生成都会保留", "日报、周报与自定义报告", "报告包含开始和结束当天；重新生成会新增一个版本，旧版本仍会保留。", ["新建任务", "generatingCount"]],
  ]) {
    const source = await sourceFile(path);
    const headers = classElements(source, "page-header");
    assert.equal(headers.length, 1, path);
    const header = headers[0];
    for (const [role, expected] of [["eyebrow", eyebrow], ["title", title], ["description", description]]) {
      const elements = classElements(header, `page-header-${role}`);
      assert.equal(elements.length, 1, `${path}: ${role}`);
      if (Array.isArray(expected)) {
        const expressions = elements[0].children.filter(ts.isJsxExpression);
        assert.equal(expressions.length, 1, `${path}: ${role} mode expression`);
        const conditional = expressions[0].expression;
        assert.ok(conditional && ts.isConditionalExpression(conditional), `${path}: ${role} must switch by mode`);
        assert.equal(conditional.condition.getText(source), "managedMode");
        assert.equal(conditional.whenTrue.text, expected[0], `${path}: ${role} managed title`);
        assert.equal(conditional.whenFalse.text, expected[1], `${path}: ${role} matrix title`);
      } else {
        assert.equal(textOf(elements[0]), expected);
      }
    }
    const controls = classElements(header, "page-header-actions");
    assert.equal(controls.length, 1);
    for (const action of actions) assert.ok(controls[0].getText(source).includes(action), `${path}: ${action}`);
  }
});

test("card, modal and secondary-page typography is outside the new page-header scope", async () => {
  assertProperties(baseStyle(".eyebrow"), { "font-size": "10px", "font-weight": "850", color: "#7b8b90" });
  assert.equal(baseStyle(".detail-toolbar h2")["font-size"], "21px");
  for (const path of ["accounts/douyin-authorization/DouyinAuthorizationPage.tsx", "tasks/[id]/TaskDetailPage.tsx"]) {
    const source = await sourceFile(path);
    assert.equal(classElements(source, "page-header").length, 0, path);
    assert.ok(classElements(source, "detail-toolbar").length > 0, path);
  }
});
