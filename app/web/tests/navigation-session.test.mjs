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
  assert.equal(cleared, 0);
  client.setQueryData(key, { username: "first", role: "operator" });
  assert.equal(cleared, 1);
  client.setQueryData(key, { username: "second", role: "operator" });
  assert.equal(cleared, 2);
  client.removeQueries({ queryKey: key, exact: true });
  assert.equal(cleared, 3);
  client.setQueryData(key, { username: "third", role: "admin" });
  assert.equal(cleared, 3);
  unsubscribe();
  client.clear();
});

test("date search capability explicitly selects the matching backend in each environment", async () => {
  const before = process.env.NEXT_PUBLIC_DCAR_CONTENT_DATE_FILTER;
  try {
    for (const [value, enabled, path] of [
      ["1", true, "/workbench-api/content-search"],
      ["0", false, "/api/v8/contents/search"],
      ["", false, "/api/v8/contents/search"],
    ]) {
      process.env.NEXT_PUBLIC_DCAR_CONTENT_DATE_FILTER = value;
      const feature = await import(`../app/lib/features.ts?flag=${value}`);
      assert.equal(feature.CONTENT_DATE_FILTER_ENABLED, enabled);
      assert.equal(feature.CONTENT_SEARCH_PATH, path);
    }
  } finally {
    if (before === undefined) delete process.env.NEXT_PUBLIC_DCAR_CONTENT_DATE_FILTER;
    else process.env.NEXT_PUBLIC_DCAR_CONTENT_DATE_FILTER = before;
  }
});
