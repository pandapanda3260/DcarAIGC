import assert from "node:assert/strict";
import test from "node:test";
import { contentSearchResponse } from "../app/lib/contentSearchServer.ts";

const request = (body, authenticated = true) => new Request("http://localhost/workbench-api/content-search", {
  method: "POST", headers: { "content-type": "application/json", ...(authenticated ? { "x-dcar-authenticated-user": "test" } : {}) }, body,
});

test("content search requires gateway identity and bounded object JSON before reading", async () => {
  let calls = 0;
  const reader = async () => { calls++; return { items: [], total: 0 }; };
  assert.equal((await contentSearchResponse(request("{}", false), reader)).status, 401);
  for (const body of ["null", "[]", "1", "broken"]) assert.equal((await contentSearchResponse(request(body), reader)).status, 422);
  assert.equal((await contentSearchResponse(request(JSON.stringify({ query: "x".repeat(8192) })), reader)).status, 413);
  assert.equal(calls, 0);
});

test("content search forwards date, filters and pagination without filtering a page in memory", async () => {
  const payload = { published_from: "2026-09-01", published_to: "2026-09-07", platform: "douyin", page: 2, page_size: 50 };
  const response = await contentSearchResponse(request(JSON.stringify(payload)), async (received) => {
    assert.deepEqual(received, payload); return { items: [{ id: 3 }], total: 51, page: 2, page_size: 50 };
  });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("cache-control"), "private, no-store");
  assert.equal((await response.json()).total, 51);
});

test("content search failures do not leak database or internal paths", async () => {
  const response = await contentSearchResponse(request("{}"), async () => { throw new Error("private database path"); });
  assert.equal(response.status, 503);
  assert.doesNotMatch(await response.text(), /private database/);
});
