import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";
import test from "node:test";
import React from "react";
import * as jsxRuntime from "react/jsx-runtime";
import ts from "typescript";

const source = readFileSync(new URL("../app/accounts/RepairAccountIdentityDialog.tsx", import.meta.url), "utf8");
const account = { id: -7, directory_row_id: 7, directory_platform: "douyin", directory_uid: null,
  locator_sha256: "a".repeat(64), locator_revision: 0, platforms: [{ platform: "douyin", uid: null }],
  directory_locator: { platform: "douyin", uid: "", display_account_id: "", references: {} } };

function harness(target = account) {
  const state = [], calls = [], saved = [];
  let cursor = 0, uuid = 0, fail = false;
  const hooks = { ...React,
    useState(initial) { const index = cursor++; if (!(index in state)) state[index] = initial;
      return [state[index], (value) => { state[index] = typeof value === "function" ? value(state[index]) : value; }]; },
    useRef(initial) { const index = cursor++; if (!(index in state)) state[index] = { current: initial }; return state[index]; },
  };
  const compiledModule = { exports: {} };
  const javascript = ts.transpileModule(source, { compilerOptions: { jsx: ts.JsxEmit.ReactJSX,
    target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
  vm.runInNewContext(javascript, { module: compiledModule, exports: compiledModule.exports, Object, crypto: { randomUUID: () => `fixture-${++uuid}` },
    require(name) {
      if (name === "react") return hooks;
      if (name === "react/jsx-runtime") return jsxRuntime;
      if (name.endsWith("accounts.module.css")) return { default: {} };
      if (name.endsWith("/lib/api")) return {
        jsonRequest: (value) => ({ body: JSON.stringify(value) }),
        readJson: async (url, request) => { calls.push({ url, body: JSON.parse(request.body) });
          if (fail) throw new Error("账号定位已被修改，请刷新后重新补充。");
          return { status: "accepted", message: "资料已接收", intake_id: 42, uid: "123456789" }; },
      };
      throw new Error(`unexpected dependency ${name}`);
    },
  });
  const render = () => { cursor = 0; return compiledModule.exports.default({ account: target, onClose() {}, onSaved(value) { saved.push(value); } }); };
  function find(node, predicate) {
    if (node == null || typeof node !== "object") return null;
    if (Array.isArray(node)) { for (const item of node) { const found = find(item, predicate); if (found) return found; } return null; }
    if (predicate(node)) return node;
    return find(node.props?.children, predicate);
  }
  return { calls, saved, fail(value) { fail = value; },
    input(name, value) { find(render(), (node) => node.type === "input" && node.props.name === name).props.onChange({ target: { value } }); },
    async submit() { await find(render(), (node) => node.type === "form").props.onSubmit({ preventDefault() {} });
      // The component deliberately discards the promise in its event wrapper.
      await new Promise((resolve) => setImmediate(resolve)); },
    alert() { return find(render(), (node) => node.props?.role === "alert")?.props.children; },
  };
}

test("identity repair targets the original row with server CAS and typed UID", async () => {
  const h = harness(); h.input("uid", "00123456789");
  await h.submit();
  assert.equal(h.calls.length, 1);
  assert.equal(h.calls[0].url, "/api/v8/account-directory/7/identity");
  assert.equal(h.calls[0].body.expected_locator_sha256, account.locator_sha256);
  assert.equal(h.calls[0].body.uid, "00123456789");
  assert.equal(h.saved[0].intake_id, 42);
  assert.equal(account.directory_uid, null);
});

test("failed identical repair reuses its request id and retains server conflict", async () => {
  const h = harness(); h.input("uid", "123456789"); h.fail(true);
  await h.submit(); await h.submit();
  assert.equal(h.calls.length, 2);
  assert.equal(h.calls[0].body.request_id, h.calls[1].body.request_id);
  assert.match(h.alert(), /刷新/);
  assert.equal(h.saved.length, 0);
});

test("missing locator or CAS cannot submit a blind mutation", async () => {
  const empty = harness(); await empty.submit(); assert.equal(empty.calls.length, 0);
  const stale = harness({ ...account, locator_sha256: undefined }); stale.input("uid", "123456789");
  await stale.submit(); assert.equal(stale.calls.length, 0); assert.match(stale.alert(), /刷新/);
});
