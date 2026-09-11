import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import ts from "typescript";
import { QueryClient, QueryObserver } from "@tanstack/react-query";
import { ApiRequestError } from "../app/lib/api.ts";

const source = readFileSync(new URL("../app/lib/serviceRecovery.ts", import.meta.url), "utf8");
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS } }).outputText;
const loaded = { exports: {} };
new Function("require", "module", "exports", compiled)((name) => {
  assert.equal(name, "./api");
  return { ApiRequestError };
}, loaded, loaded.exports);
const { createServiceRecovery } = loaded.exports;

function fixture(t) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: Infinity } } });
  const subscriptions = [];
  t.after(() => { subscriptions.forEach((unsubscribe) => unsubscribe()); client.clear(); });
  async function failed(key, error, { active = true, alwaysFails = false } = {}) {
    let calls = 0;
    const options = { queryKey: key, retry: false, queryFn: async () => {
      calls++;
      if (calls === 1 || alwaysFails) throw error;
      return { items: [{ id: 7 }], total: 1 };
    } };
    const observer = new QueryObserver(client, options);
    if (active) subscriptions.push(observer.subscribe(() => {}));
    await observer.refetch();
    assert.equal(observer.getCurrentResult().status, "error");
    return { observer, calls: () => calls };
  }
  return { client, failed, recover: createServiceRecovery(client) };
}

test("service recovery reloads the visible failed account list without page navigation", async (t) => {
  const { failed, recover } = fixture(t);
  const account = await failed(["accounts", "search", { page: 1 }], new ApiRequestError("unavailable", { status: 503, code: "upstream_unavailable" }));
  await recover(false);
  assert.equal(account.calls(), 1);
  await recover(true);
  assert.equal(account.calls(), 2);
  assert.equal(account.observer.getCurrentResult().status, "success");
  assert.equal(account.observer.getCurrentResult().data.total, 1);
  await recover(true);
  assert.equal(account.calls(), 2);
});

test("first successful health check recovers an earlier failed list, while loading alone does not", async (t) => {
  const { failed, recover } = fixture(t);
  const contents = await failed(["contents", "search"], new ApiRequestError("network", { retryable: true }));
  await recover(null);
  assert.equal(contents.calls(), 1);
  await recover(true);
  assert.equal(contents.observer.getCurrentResult().status, "success");
  assert.equal(contents.calls(), 2);
});

test("stable successful health polls do not loop failed reads; a new outage permits one new recovery", async (t) => {
  const { failed, recover } = fixture(t);
  const account = await failed(["accounts", "search"], new ApiRequestError("unavailable", { status: 502 }), { alwaysFails: true });
  await recover(true);
  await recover(true);
  await recover(null);
  await recover(true);
  assert.equal(account.calls(), 2);
  await recover(false);
  await recover(true);
  assert.equal(account.calls(), 3);
});

test("recovery leaves permission failures, explicit gates, inactive queries, sessions and writes alone", async (t) => {
  const { client, failed, recover } = fixture(t);
  const failure = new ApiRequestError("unavailable", { status: 503 });
  const cases = [
    [["accounts", "unauthorized"], new ApiRequestError("expired", { status: 401 })],
    [["accounts", "forbidden"], new ApiRequestError("denied", { status: 403 })],
    [["contents", "gate"], new ApiRequestError("gate", { status: 503, code: "source_invalid" })],
    [["contents", "aborted"], new DOMException("aborted", "AbortError")],
    [["auth", "session"], failure],
    [["content-update-jobs", "operator"], failure],
  ];
  const untouched = await Promise.all(cases.map(([key, error]) => failed(key, error)));
  untouched.push(await failed(["tasks", "inactive"], failure, { active: false }));
  let writes = 0;
  const mutation = client.getMutationCache().build(client, { mutationFn: async () => { writes++; throw failure; } });
  await mutation.execute().catch(() => {});
  await recover(true);
  untouched.forEach((query) => assert.equal(query.calls(), 1));
  assert.equal(writes, 1);
});

test("recovery preserves successful cached data and an in-flight manual retry", async (t) => {
  const { client, failed, recover } = fixture(t);
  let successfulCalls = 0;
  const successful = new QueryObserver(client, { queryKey: ["overview"], queryFn: async () => ++successfulCalls });
  const unsubscribe = successful.subscribe(() => {});
  t.after(unsubscribe);
  await successful.refetch();
  const before = successfulCalls;
  const account = await failed(["accounts", "search"], new ApiRequestError("down", { status: 503 }));
  let resolve;
  let manualCalls = 0;
  account.observer.setOptions({ queryKey: ["accounts", "search"], retry: false, queryFn: () => {
    manualCalls++;
    return new Promise((done) => { resolve = done; });
  } });
  const manual = account.observer.refetch();
  await recover(true);
  assert.equal(manualCalls, 1);
  assert.equal(successfulCalls, before);
  resolve({ items: [{ id: 8 }], total: 1 });
  await manual;
  assert.equal(account.observer.getCurrentResult().data.items[0].id, 8);
});
