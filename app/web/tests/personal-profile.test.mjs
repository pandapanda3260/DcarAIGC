import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import React from "react";
import * as jsxRuntime from "react/jsx-runtime";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";
import { isAbortError, markedJsonRequest } from "../app/lib/api.ts";

function harness({ initialName, username = "账号 @example.com", fail = false, hold = false } = {}) {
  const values = [], requests = [], saved = [];
  let cursor = 0, closed = 0;
  const session = { authenticated: true, username, display_name: initialName, role: "operator" };
  let release;
  const responseReady = hold ? new Promise((resolve) => { release = resolve; }) : Promise.resolve();
  const react = { ...React,
    useEffect() {},
    useState(initial) {
      const index = cursor++;
      if (!(index in values)) values[index] = initial;
      return [values[index], (value) => { values[index] = value; }];
    },
    useRef(initial) {
      const index = cursor++;
      return values[index] ??= { current: initial };
    },
  };
  const dependencies = {
    react, "react/jsx-runtime": jsxRuntime,
    "./PersonalProfile.module.css": { default: new Proxy({}, { get: (_, key) => key }) },
    "./QuickActionTooltip.module.css": { default: new Proxy({}, { get: (_, key) => key }) },
    "../lib/api": { isAbortError, markedJsonRequest, requireApprovedSession: (value) => value,
      readQueryJson: async (url, init, timeoutMs) => {
        requests.push({ url, init, timeoutMs, body: JSON.parse(init.body) });
        await responseReady;
        if (fail) throw new Error("无法连接服务，请重试。");
        return { ...session, display_name: JSON.parse(init.body).display_name };
      },
    },
  };
  const source = readFileSync(new URL("../app/components/InlineNicknameEditor.tsx", import.meta.url), "utf8");
  const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX } }).outputText;
  const compiledModule = { exports: {} };
  new Function("require", "module", "exports", compiled)((name) => {
    assert.ok(name in dependencies, `Unexpected dependency ${name}`);
    return dependencies[name];
  }, compiledModule, compiledModule.exports);
  return {
    requests, saved, release, get closed() { return closed; },
    refreshSession(next) { Object.assign(session, next); },
    render() { cursor = 0; return compiledModule.exports.default({ session, onCancel: () => { closed++; }, onSaved: (value) => saved.push(value) }); },
  };
}

function find(element, predicate) {
  if (!React.isValidElement(element)) return undefined;
  if (predicate(element)) return element;
  for (const child of React.Children.toArray(element.props.children)) {
    const result = find(child, predicate);
    if (result) return result;
  }
}
const input = (tree) => find(tree, (element) => element.type === "textarea");
async function submit(tree) {
  find(tree, (element) => element.type === "form").props.onSubmit({ preventDefault() {} });
  await new Promise(setImmediate);
}

test("editing starts from the displayed name and only saves a changed draft", async () => {
  for (const initialName of [undefined, "", "已有昵称"]) {
    const view = harness({ username: "panyang", initialName });
    const visibleName = initialName || "panyang";
    let tree = view.render();
    assert.equal(input(tree).props.value, visibleName);
    assert.equal(input(tree).props.placeholder, "输入昵称");
    assert.equal(input(tree).props.disabled, false);
    const saveButton = (element) => find(element, (child) => child.type === "button" && child.props.type === "submit");
    assert.equal(saveButton(tree).props.disabled, true);
    input(tree).props.onKeyDown(keyEvent("Enter"));
    await submit(tree);
    assert.equal(view.requests.length, 0, "an unchanged name never writes a profile");

    input(tree).props.onChange({ target: { value: `${visibleName}01` } });
    tree = view.render();
    assert.equal(saveButton(tree).props.disabled, false);
    input(tree).props.onChange({ target: { value: visibleName } });
    tree = view.render();
    assert.equal(saveButton(tree).props.disabled, true);
    await submit(tree);
    assert.equal(view.requests.length, 0, "reverting the edit also prevents a write");

    // A session refresh must not change the baseline for an open editor.
    view.refreshSession({ display_name: "另一个窗口更新的昵称" });
    tree = view.render();
    assert.equal(input(tree).props.value, visibleName);
    assert.equal(saveButton(tree).props.disabled, true);
    input(tree).props.onChange({ target: { value: `${visibleName}01` } });
    await submit(view.render());
    assert.deepEqual(view.requests.map((request) => request.body), [{ display_name: `${visibleName}01` }]);
    assert.equal(view.saved[0].username, "panyang");
  }
});

test("clearing the name explains the account fallback and saves an empty nickname", async () => {
  const view = harness({ username: "panyang", initialName: "已有昵称" });
  let tree = view.render();
  assert.equal(find(tree, (element) => element.props.id === "nickname-fallback-help"), undefined);
  input(tree).props.onChange({ target: { value: "" } });
  tree = view.render();
  assert.equal(find(tree, (element) => element.props.id === "nickname-fallback-help").props.children, "保存后将显示登录账号");
  assert.match(input(tree).props["aria-describedby"], /nickname-fallback-help/);
  await submit(tree);
  assert.deepEqual(view.requests[0].body, { display_name: "" });
  input(tree).props.onChange({ target: { value: "新昵称" } });
  assert.equal(find(view.render(), (element) => element.props.id === "nickname-fallback-help"), undefined);
});

test("profile accepts special characters, long names and an empty fallback without altering the login account", async () => {
  for (const name of ["", "  中文 🚗 first\nsecond @+&.?  ", '<script>alert("昵称")</script>', "🚗".repeat(1000)]) {
    const view = harness();
    let tree = view.render();
    assert.equal(input(tree).props.value, "账号 @example.com");
    assert.equal(input(tree).props.maxLength, undefined);
    assert.equal(input(tree).props.required, undefined);
    assert.equal(input(tree).props.wrap, "off");
    assert.doesNotMatch(renderToStaticMarkup(tree), /modal|dialog/);
    input(tree).props.onChange({ target: { value: name } });
    tree = view.render();
    assert.ok(!renderToStaticMarkup(tree).includes("<script>"));
    await submit(tree);
    assert.equal(view.requests.length, 1);
    assert.equal(view.requests[0].url, "/auth/profile");
    assert.equal(view.requests[0].init.headers["X-Dcar-Request"], "profile-update");
    assert.equal(view.requests[0].timeoutMs, 15_000);
    assert.deepEqual(view.requests[0].body, { display_name: name });
    assert.equal(view.saved[0].display_name, name);
    assert.equal(view.saved[0].username, "账号 @example.com");
    assert.equal(view.saved[0].role, "operator");
  }
});

test("a failed save retains the draft and allows retry; cancellation performs no write", async () => {
  const view = harness({ initialName: "旧昵称", fail: true });
  input(view.render()).props.onChange({ target: { value: "草稿 @🚗" } });
  await submit(view.render());
  let tree = view.render();
  assert.equal(input(tree).props.value, "草稿 @🚗");
  assert.equal(input(tree).props.disabled, false);
  assert.match(renderToStaticMarkup(tree), /无法连接服务，请重试/);
  assert.equal(view.saved.length, 0);
  await submit(tree);
  assert.equal(view.requests.length, 2);
  tree = view.render();
  find(tree, (element) => element.props["aria-label"] === "取消编辑昵称").props.onClick();
  assert.equal(view.closed, 1);
  assert.equal(view.requests.length, 2);
});

function keyEvent(key, overrides = {}) {
  return { key, shiftKey: false, prevented: false, preventDefault() { this.prevented = true; }, nativeEvent: { isComposing: false, keyCode: key === "Enter" ? 13 : 27 }, ...overrides };
}

test("IME confirmation and Shift+Enter never save; a deliberate Enter saves once", async () => {
  const view = harness({ initialName: "草稿" });
  let field = input(view.render());
  field.props.onCompositionStart();
  field.props.onKeyDown(keyEvent("Enter"));
  field.props.onKeyDown(keyEvent("Escape"));
  field.props.onCompositionEnd();
  field.props.onKeyDown(keyEvent("Enter", { nativeEvent: { isComposing: true, keyCode: 13 } }));
  field.props.onKeyDown(keyEvent("Enter", { nativeEvent: { isComposing: false, keyCode: 229 } }));
  const newline = keyEvent("Enter", { shiftKey: true });
  field.props.onKeyDown(newline);
  assert.equal(newline.prevented, false);
  assert.equal(view.requests.length, 0);
  assert.equal(view.closed, 0);
  field.props.onChange({ target: { value: "草稿\n第二行" } });
  field = input(view.render());
  const enter = keyEvent("Enter");
  field.props.onKeyDown(enter);
  await new Promise(setImmediate);
  assert.equal(enter.prevented, true);
  assert.equal(view.requests.length, 1);
  assert.equal(view.requests[0].body.display_name, "草稿\n第二行");
});

test("blur and background session refresh preserve the draft; Escape cancels without a request", () => {
  const view = harness({ initialName: "已保存" });
  input(view.render()).props.onChange({ target: { value: "我的草稿🚗" } });
  input(view.render()).props.onBlur?.({});
  view.refreshSession({ display_name: "其他标签的昵称" });
  const field = input(view.render());
  assert.equal(field.props.value, "我的草稿🚗");
  assert.equal(view.requests.length, 0);
  const escape = keyEvent("Escape");
  field.props.onKeyDown(escape);
  assert.equal(escape.prevented, true);
  assert.equal(view.closed, 1);
  assert.equal(view.requests.length, 0);
});

test("an in-flight save locks repeated Enter, submit and cancellation until the response", async () => {
  const view = harness({ initialName: "草稿", hold: true });
  input(view.render()).props.onChange({ target: { value: "修改后的草稿" } });
  await submit(view.render());
  let tree = view.render();
  assert.equal(input(tree).props.disabled, true);
  input(tree).props.onKeyDown(keyEvent("Enter"));
  input(tree).props.onKeyDown(keyEvent("Escape"));
  await submit(tree);
  assert.equal(view.requests.length, 1);
  assert.equal(view.closed, 0);
  view.release();
  await new Promise(setImmediate);
  tree = view.render();
  assert.equal(input(tree).props.disabled, false);
  assert.equal(view.saved.length, 1);
});
