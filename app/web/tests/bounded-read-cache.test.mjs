import assert from "node:assert/strict";
import test from "node:test";
import { BoundedReadCache } from "../app/lib/boundedReadCache.mjs";

const defaults = { ttlMs: 100, maxEntries: 2, maxBytes: 1000, concurrency: 1, maxQueued: 2, queueTimeoutMs: 1000 };
const deferred = () => { let resolve; const promise = new Promise((done) => { resolve = done; }); return { promise, resolve }; };

test("identical reads share a load; other reads queue without exceeding process capacity", async () => {
  const cache = new BoundedReadCache(defaults);
  const gate = deferred(); let calls = 0; let active = 0; let peak = 0;
  const load = async () => { calls++; active++; peak = Math.max(active, peak); await gate.promise; active--; return { ok: true }; };
  const reads = [cache.get("a", load), cache.get("a", load), cache.get("b", load), cache.get("c", load)];
  await assert.rejects(cache.get("d", load), /queue full/);
  assert.equal(calls, 1);
  gate.resolve();
  await Promise.all(reads);
  assert.equal(calls, 3); assert.equal(peak, 1);
});

test("expiry, eviction and failures cannot return an invalid cached success", async () => {
  let time = 0; let calls = 0;
  const cache = new BoundedReadCache({ ...defaults, clock: () => time });
  const load = async () => ({ revision: ++calls });
  assert.deepEqual(await cache.get("a", load), { revision: 1 });
  assert.deepEqual(await cache.get("a", load), { revision: 1 });
  time = 101;
  assert.deepEqual(await cache.get("a", load), { revision: 2 });
  await cache.get("b", load); await cache.get("c", load);
  assert.deepEqual(await cache.get("a", load), { revision: 5 });
  await assert.rejects(cache.get("failed", async () => { throw new Error("unavailable"); }), /unavailable/);
  assert.deepEqual(await cache.get("failed", load), { revision: 6 });
});

test("an obsolete in-flight read cannot refill a cleared generation", async () => {
  const cache = new BoundedReadCache(defaults); const gate = deferred();
  const old = cache.get("a", () => gate.promise);
  cache.clear();
  const current = cache.get("a", async () => ({ revision: 2 }));
  gate.resolve({ revision: 1 });
  assert.deepEqual(await old, { revision: 1 });
  assert.deepEqual(await current, { revision: 2 });
  assert.deepEqual(await cache.get("a", async () => { throw new Error("must be cached"); }), { revision: 2 });
});

test("queue timeouts and byte limits keep helper work bounded", async () => {
  const cache = new BoundedReadCache({ ...defaults, queueTimeoutMs: 15, maxBytes: 5 });
  const gate = deferred(); let ran = false;
  const first = cache.get("a", () => gate.promise);
  await assert.rejects(cache.get("b", async () => { ran = true; return {}; }), /queue timeout/);
  gate.resolve({ tooLarge: true }); await first;
  assert.equal(ran, false);
  assert.deepEqual(await cache.get("a", async () => ({ newValue: true })), { newValue: true });
});
