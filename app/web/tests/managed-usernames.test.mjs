import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import React from "react";
import * as jsxRuntime from "react/jsx-runtime";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";
import { markedJsonRequest } from "../app/lib/api.ts";

function usersHarness(username, display_name, options = {}) {
  const values = [], requests = [];
  let cursor = 0;
  const user = { username, display_name, phone: "13800138000", role: "new_user", status: "active", created_at: "2026-09-12T00:00:00Z", ...options.user };
  const actor = options.actor ?? { username: "admin", role: "admin" };
  const react = {
    ...React,
    useEffect() {},
    useRef: () => ({ current: null }),
    useState(initial) {
      const index = cursor++;
      if (!(index in values)) values[index] = initial;
      return [values[index], (value) => { values[index] = typeof value === "function" ? value(values[index]) : value; }];
    },
  };
  const dependencies = {
    react,
    "react/jsx-runtime": jsxRuntime,
    "@tanstack/react-query": {
      useQuery: () => ({ data: { actor, items: [user] } }),
      useQueryClient: () => ({ invalidateQueries: async () => {} }),
    },
    "../components/AppShell": { default: ({ children }) => React.createElement("main", null, children) },
    "../components/Feedback": { Feedback: () => null, Loading: () => null, Notice: () => null },
    "../components/useDialogFocus": { useDialogFocus() {} },
    "../lib/api": {
      ApiRequestError: class extends Error {},
      markedJsonRequest,
      readJson: async (url, requestOptions) => {
        requests.push({ url, body: JSON.parse(requestOptions.body) });
        if (url === "/auth/users/update" && options.saveError) throw new Error(options.saveError);
        return {};
      },
    },
    "../lib/paths": { publicAssetPath: (path) => path },
    "../lib/format": { formatDateTime: () => "2026-09-12 08:00" },
    "../lib/queries": { queryKeys: { users: ["auth", "users"], session: ["auth", "session"] }, usersQueryOptions: () => ({}) },
    "./UsersPage.module.css": { default: new Proxy({}, { get: (_target, key) => key }) },
  };
  const source = readFileSync(new URL("../app/users/UsersPage.tsx", import.meta.url), "utf8");
  const compiled = ts.transpileModule(source, { compilerOptions: {
    target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX,
  } }).outputText;
  const compiledModule = { exports: {} };
  new Function("require", "module", "exports", compiled)((name) => {
    assert.ok(name in dependencies, `Unexpected dependency: ${name}`);
    return dependencies[name];
  }, compiledModule, compiledModule.exports);
  return {
    requests,
    render() { cursor = 0; return compiledModule.exports.default(); },
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

function content(element) {
  if (element == null || typeof element === "boolean") return "";
  if (typeof element === "string" || typeof element === "number") return String(element);
  if (Array.isArray(element)) return element.map(content).join("");
  return React.isValidElement(element) ? content(element.props.children) : "";
}

function button(tree, label) {
  const result = find(tree, (item) => item.type === "button" && content(item) === label);
  assert.ok(result, `Button not found: ${label}`);
  return result;
}

function field(tree, id) {
  const result = find(tree, (item) => item.props.id === id);
  assert.ok(result, `Field not found: ${id}`);
  return result;
}

function editDialog(tree) {
  return find(tree, (item) => item.props.role === "dialog" && item.props["aria-label"] === "修改用户");
}

function openEdit(harness) {
  button(harness.render(), "修改").props.onClick();
  return harness.render();
}

async function saveEdit(harness, tree) {
  button(tree, "保存修改").props.onClick();
  await new Promise(setImmediate);
  return harness.render();
}

test("management renders special names safely and preserves the exact identity through update and delete", async () => {
  for (const username of [
    "  中文🚗 first\nsecond@example.com +&=?  ",
    "🚗".repeat(150),
    '<script>alert("account")</script>',
    "temporary-bypass",
  ]) {
    const harness = usersHarness(username);
    let tree = harness.render();
    const html = renderToStaticMarkup(tree);
    assert.ok(!html.includes("<script>"));
    assert.equal(find(tree, (item) => item.type === "tr" && item.props["data-username"] === username).props["data-username"], username);
    tree = openEdit(harness);
    const identity = field(tree, "edit-user-username");
    assert.equal(content(identity), username);
    assert.equal(identity.props["aria-label"], "登录账号");
    assert.ok(!find(identity, (item) => item.type === "textarea" || item.type === "input"));
    assert.ok(!renderToStaticMarkup(editDialog(tree)).includes("<script>"));
    tree = await saveEdit(harness, tree);
    assert.equal(harness.requests[0].url, "/auth/users/update");
    assert.equal(harness.requests[0].body.username, username);

    find(tree, (item) => item.type === "button" && item.props.children === "删除").props.onClick();
    tree = harness.render();
    assert.ok(!renderToStaticMarkup(tree).includes("<script>"));
    find(tree, (item) => item.type === "button" && item.props.children === "确认删除").props.onClick();
    await new Promise(setImmediate);
    assert.equal(harness.requests[1].url, "/auth/users/delete");
    assert.equal(harness.requests[1].body.username, username);
  }
});

test("management shows a nickname alongside the original login identity", () => {
  const username = "login@example.com";
  const name = '<昵称🚗 & "团队">';
  const harness = usersHarness(username, name);
  const tree = harness.render();
  assert.equal(find(tree, (item) => item.props.className === "username").props.children, name);
  assert.match(renderToStaticMarkup(tree), /登录账号：login@example.com/);
  assert.equal(content(field(openEdit(harness), "edit-user-username")), username);
});

test("phone is immediately editable with the full stored value and saving another field retains it", async () => {
  const harness = usersHarness("managed-user");
  let tree = openEdit(harness);
  const phone = field(tree, "edit-user-phone");
  assert.equal(phone.type, "input");
  assert.equal(phone.props.value, "13800138000");
  assert.ok(!phone.props.readOnly);
  assert.ok(!phone.props.disabled);
  assert.ok(!find(editDialog(tree), (item) => item.type === "button" && content(item) === "更改"));
  field(tree, "edit-user-role").props.onChange({ target: { value: "operator" } });
  tree = await saveEdit(harness, harness.render());
  assert.deepEqual(harness.requests[0], {
    url: "/auth/users/update",
    body: { username: "managed-user", phone: "13800138000", role: "operator", password: "" },
  });
  assert.ok(!editDialog(tree));
});

test("phone can be cleared directly and empty phone records always render an editable input", async () => {
  const harness = usersHarness("managed-user");
  const tree = openEdit(harness);
  assert.equal(field(tree, "edit-user-phone").props.value, "13800138000");
  field(tree, "edit-user-phone").props.onChange({ target: { value: "" } });
  await saveEdit(harness, harness.render());
  assert.equal(harness.requests[0].body.phone, "");

  for (const storedPhone of ["", null, undefined]) {
    const emptyPhoneHarness = usersHarness("empty-phone-user", undefined, { user: { phone: storedPhone } });
    const phone = field(openEdit(emptyPhoneHarness), "edit-user-phone");
    assert.equal(phone.type, "input");
    assert.equal(phone.props.value, "");
    assert.ok(!phone.props.readOnly);
    assert.ok(!phone.props.disabled);
  }
});

test("editing oneself keeps role locked and password absent, and roles remain limited to the actor rank", () => {
  const harness = usersHarness("admin", undefined, { user: { role: "admin" } });
  const tree = openEdit(harness);
  assert.equal(field(tree, "edit-user-role").props.disabled, true);
  assert.ok(!find(editDialog(tree), (item) => item.props.id === "edit-user-password"));
  assert.ok(!find(field(tree, "edit-user-role"), (item) => item.type === "option" && item.props.value === "superadmin"));
});

test("failed saves keep the draft and show the error inside the dialog; reopening clears both", async () => {
  const harness = usersHarness("managed-user", undefined, { saveError: "服务暂时不可用，请重试" });
  let tree = openEdit(harness);
  field(tree, "edit-user-phone").props.onChange({ target: { value: "13900139000" } });
  tree = harness.render();
  field(tree, "edit-user-role").props.onChange({ target: { value: "operator" } });
  tree = harness.render();
  field(tree, "edit-user-password").props.onChange({ target: { value: "DraftPass123" } });
  tree = await saveEdit(harness, harness.render());
  assert.ok(editDialog(tree));
  assert.equal(field(tree, "edit-user-phone").props.value, "13900139000");
  assert.equal(field(tree, "edit-user-role").props.value, "operator");
  assert.equal(field(tree, "edit-user-password").props.value, "DraftPass123");
  assert.match(content(find(editDialog(tree), (item) => item.props.role === "alert")), /服务暂时不可用，请重试/);
  assert.equal(button(tree, "保存修改").props.disabled, false);
  assert.deepEqual(harness.requests[0].body, {
    username: "managed-user", phone: "13900139000", role: "operator", password: "DraftPass123",
  });

  find(editDialog(tree), (item) => item.type === "button" && item.props["aria-label"] === "关闭").props.onClick();
  assert.ok(!editDialog(harness.render()));
  tree = openEdit(harness);
  assert.ok(!find(editDialog(tree), (item) => item.props.role === "alert"));
  assert.equal(field(tree, "edit-user-role").props.value, "new_user");
  assert.equal(field(tree, "edit-user-password").props.value, "");
  assert.equal(field(tree, "edit-user-phone").props.value, "13800138000");
});
