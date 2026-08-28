import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";
import ts from "typescript";

const source = await readFile(new URL("../app/selling-points/SellingPointsPage.tsx", import.meta.url), "utf8");
const taxonomy = JSON.parse(await readFile(new URL("../../../config/business_selling_points_v5_3.json", import.meta.url), "utf8"));
const sourceFile = ts.createSourceFile("SellingPointsPage.tsx", source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
const functions = new Map();
function collectFunctions(node) {
  if (ts.isFunctionDeclaration(node) && node.name) functions.set(node.name.text, node.getText(sourceFile));
  ts.forEachChild(node, collectFunctions);
}
collectFunctions(sourceFile);
const editorCode = ts.transpileModule(
  ["withV53PreviewCopy", "ensureDraft", "beginPoint", "open"].map((name) => {
    assert.ok(functions.has(name), `missing actual editor function: ${name}`);
    return functions.get(name);
  }).join("\n"),
  { compilerOptions: { target: ts.ScriptTarget.ES2022 } },
).outputText;

function isolatedEditor({ draftMode = false, items = [], loadError } = {}) {
  const state = { form: null, editingCode: null, error: "", saving: false, created: 0, fetched: 0, previewMode: true };
  const draft = { taxonomy: { version: "existing-draft" }, items };
  const emptyPoint = { code: "", tier: "other", label: "", definition: "", matcherRuleJson: "{}" };
  const context = vm.createContext({
    draftMode,
    emptyPoint,
    v53PreviewByCode: new Map(taxonomy.labels.map((point) => [point.id, point])),
    queryKeys: { draftSellingPoints: ["selling-points", "draft"] },
    draftSellingPointsQueryOptions: () => ({ queryKey: ["selling-points", "draft"] }),
    readJson: async (path, init) => {
      assert.equal(path, "/api/v8/selling-points/draft");
      assert.equal(init.method, "POST");
      state.created += 1;
    },
    queryClient: {
      invalidateQueries: async () => {},
      fetchQuery: async () => {
        state.fetched += 1;
        if (loadError) throw loadError;
        return draft;
      },
    },
    Error,
    setDraftMode: (value) => { context.draftMode = value; },
    setPreviewMode: (value) => { state.previewMode = value; },
    setMessage: () => {},
    setSaving: (value) => { state.saving = value; },
    setError: (value) => { state.error = value; },
    setEditingCode: (value) => { state.editingCode = value; },
    setForm: (value) => { state.form = value; },
  });
  vm.runInContext(editorCode, context);
  return { state, context, emptyPoint };
}

const savedPoint = {
  code: "E1", tier: "core", label: "已保存的草稿名称", definition: "已保存的草稿定义",
  matcher_rule: { type: "existing-draft-rule" },
};

test("editing a preview row uses the fetched draft instead of promoting preview copy", async () => {
  const editor = isolatedEditor({ items: [savedPoint] });
  const activePoint = { ...savedPoint, label: "正式生效名称", definition: "正式生效定义" };
  const previewPoint = editor.context.withV53PreviewCopy({ items: [activePoint] }).items[0];
  assert.notEqual(previewPoint.label, savedPoint.label);
  assert.equal(activePoint.label, "正式生效名称");
  await editor.context.beginPoint(previewPoint);
  assert.equal(editor.state.form.label, savedPoint.label);
  assert.equal(editor.state.form.definition, savedPoint.definition);
  assert.equal(editor.state.form.matcherRuleJson, JSON.stringify(savedPoint.matcher_rule, null, 2));
  assert.equal(editor.state.editingCode, "E1");
  assert.equal(editor.state.previewMode, false);
  assert.equal(editor.state.created, 1);
  assert.equal(editor.state.fetched, 1);
  assert.equal(editor.state.saving, false);
});

test("editing an existing draft reloads its record without creating another draft", async () => {
  const editor = isolatedEditor({ draftMode: true, items: [savedPoint] });
  await editor.context.beginPoint({ ...savedPoint, label: "旧页面名称" });
  assert.equal(editor.state.form.label, savedPoint.label);
  assert.equal(editor.state.created, 0);
  assert.equal(editor.state.fetched, 1);
});

test("a point removed from the draft cannot fall back to preview data", async () => {
  const editor = isolatedEditor();
  await editor.context.beginPoint(savedPoint);
  assert.equal(editor.state.form, null);
  assert.match(editor.state.error, /不在草稿中/);
  assert.equal(editor.state.saving, false);
});

test("a failed draft read never opens the preview as an editable fallback", async () => {
  const editor = isolatedEditor({ loadError: new Error("草稿读取失败") });
  await editor.context.beginPoint(savedPoint);
  assert.equal(editor.state.form, null);
  assert.equal(editor.state.error, "草稿读取失败");
  assert.equal(editor.state.saving, false);
});

test("adding a selling point still opens an empty form after preparing the draft", async () => {
  const editor = isolatedEditor({ items: [savedPoint] });
  await editor.context.beginPoint();
  assert.equal(editor.state.form.code, editor.emptyPoint.code);
  assert.equal(editor.state.form.label, editor.emptyPoint.label);
  assert.equal(editor.state.editingCode, null);
  assert.equal(editor.state.created, 1);
});
