import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import ts from "typescript";
import { QueryClient } from "@tanstack/react-query";

const require = createRequire(import.meta.url);
const source = readFileSync(new URL("../app/lib/queryClient.ts", import.meta.url), "utf8");
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS } }).outputText;
const loaded = { exports: {} };
new Function("require", "module", "exports", compiled)((name) => {
  if (name === "./api") return { setSessionDataClearer() {}, shouldRetryQuery() {} };
  return require(name);
}, loaded, loaded.exports);

test("navigation wrappers are invalidated on identity, role and session removal, not initial hydration or ordinary refresh", () => {
  const client = new QueryClient();
  let cleared = 0;
  const unsubscribe = loaded.exports.bindNavigationSession(client, () => cleared++);
  const key = ["auth", "session"];
  client.setQueryData(key, { username: "first", role: "admin" });
  client.setQueryData(key, { username: "first", role: "admin" });
  client.setQueryData(["contents"], { items: [] });
  client.setQueryData(key, { username: "first", role: "admin", display_name: "姓名 @🚗" });
  assert.equal(cleared, 0);
  assert.deepEqual(client.getQueryData(["contents"]), { items: [] }, "nickname changes keep the business cache");
  client.setQueryData(key, { username: "first", role: "operator" });
  assert.equal(cleared, 1);
  assert.equal(client.getQueryData(["contents"]), undefined, "role changes discard data read with earlier permissions");
  assert.deepEqual(client.getQueryData(key), { username: "first", role: "operator" }, "the new session remains available");
  client.setQueryData(key, { username: "second", role: "operator" });
  assert.equal(cleared, 2);
  client.removeQueries({ queryKey: key, exact: true });
  assert.equal(cleared, 3);
  client.setQueryData(key, { username: "third", role: "admin" });
  assert.equal(cleared, 3);
  unsubscribe();
  client.clear();
});

test("permission changes cannot be repopulated by a late response from the old scope", async () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const unsubscribe = loaded.exports.bindNavigationSession(client, () => {});
  const key = ["auth", "session"];
  client.setQueryData(key, { username: "first", role: "admin" });
  let complete;
  const pending = client.fetchQuery({ queryKey: ["accounts", "private"], queryFn: () => new Promise((resolve) => { complete = resolve; }) }).catch(() => {});
  client.setQueryData(key, { username: "first", role: "operator" });
  complete({ private: "old role data" });
  await pending;
  assert.equal(client.getQueryData(["accounts", "private"]), undefined);
  unsubscribe(); client.clear();
});

test("content search keeps the installed reader regardless of old date environment flags", async () => {
  const before = process.env.NEXT_PUBLIC_DCAR_CONTENT_DATE_FILTER;
  try {
    for (const value of ["1", "0", ""]) {
      process.env.NEXT_PUBLIC_DCAR_CONTENT_DATE_FILTER = value;
      const feature = await import(`../app/lib/features.ts?flag=${value}`);
      assert.equal(feature.CONTENT_SEARCH_PATH, "/api/v8/contents/search");
    }
  } finally {
    if (before === undefined) delete process.env.NEXT_PUBLIC_DCAR_CONTENT_DATE_FILTER;
    else process.env.NEXT_PUBLIC_DCAR_CONTENT_DATE_FILTER = before;
  }
});

test("a literal temporary-bypass account remains distinct from a bypass session", () => {
  const client = new QueryClient();
  let cleared = 0;
  const unsubscribe = loaded.exports.bindNavigationSession(client, () => cleared++);
  const key = ["auth", "session"];
  const session = { username: "temporary-bypass", role: "admin" };
  client.setQueryData(key, session);
  client.setQueryData(key, { ...session, bypass: false });
  assert.equal(cleared, 0, "an omitted bypass field represents an ordinary session");
  client.setQueryData(key, { ...session, bypass: true });
  assert.equal(cleared, 1);
  client.setQueryData(key, { ...session, bypass: false });
  assert.equal(cleared, 2);
  unsubscribe();
  client.clear();
});
