import assert from "node:assert/strict";
import test from "node:test";
import { chmod, mkdtemp, rm, writeFile, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { thumbnailMissingHint, thumbnailSource, thumbnailSources } from "../app/lib/contentThumbnails.ts";
import { readThumbnailMetadata, thumbnailResponse } from "../app/lib/contentThumbnailServer.ts";

test("thumbnails use only online covers and reject unsafe URLs", () => {
  const local = "/api/v8/contents/7/evidence/files/12/0";
  const remote = "https://example.com/cover.jpg";
  assert.equal(thumbnailSource({ local_url: local, remote_url: remote }), remote);
  assert.equal(thumbnailSource({ local_url: local, remote_url: null }), null);
  assert.equal(thumbnailSource(), null);
  for (const url of ["javascript:alert(1)", "http://example.com/x", "https://user:secret@example.com/x", "/other", "bad url"]) {
    assert.equal(thumbnailSource({ local_url: local, remote_url: url }), null);
  }
});

test("candidate covers retain exact signed URLs, remove duplicates, and cap valid sources at three", () => {
  const signed = "HTTPS://Other-Provider.example:443/a/../cover.webp?signature=a%2Fb%2bc&x=1#still-signed";
  const second = "https://new-cdn.example/two.webp";
  const third = "https://another.example/three.jpg";
  const fourth = "https://ignored.example/four.png";
  assert.deepEqual(thumbnailSources({ remote_url: signed, remote_urls: [signed, signed, "http://unsafe.example/x", second, third, fourth] }), [signed, second, third]);
  assert.deepEqual(thumbnailSources({ remote_url: second, remote_urls: [signed] }), [signed, second]);
  assert.deepEqual(thumbnailSources({ remote_url: signed }), [signed], "old metadata still works");
  assert.deepEqual(thumbnailSources({ remote_url: signed, remote_urls: null }), [signed]);
  for (const remote_url of [null, 7, {}, "https://", "https:example.com/x", "https://@example.com/x", "https://example.com:/x", "https://example.com:444/x", "https://bad_host.example/x", "https://-bad.example/x", "https://example.com/\ncover", "https://example.com/ cover", "https://example.com\\cover", "https://example.com/" + "x".repeat(4096), "https://example.com/" + "封".repeat(1400)]) {
    assert.deepEqual(thumbnailSources({ remote_url }), [], String(remote_url));
  }
});

test("missing cover hints distinguish metadata outcomes and stay quiet before metadata arrives", () => {
  assert.equal(thumbnailMissingHint(), null);
  assert.equal(thumbnailMissingHint({ remote_url: null }), "未取得封面");
  assert.equal(thumbnailMissingHint({ remote_url: null, reason: "not_found" }), "未取得封面");
  assert.equal(thumbnailMissingHint({ remote_url: null, reason: "unsupported_format" }), "封面格式暂不支持");
  assert.equal(thumbnailMissingHint({ remote_url: null, reason: "source_unavailable" }), "封面资料暂不可用");
});

test("metadata reader requires gateway identity and a bounded id list before any file reads", async () => {
  let reads = 0;
  const reader = async (ids) => { reads += 1; assert.deepEqual(ids, [7, 8]); return { items: {} }; };
  const request = (ids, authenticated = true) => new Request(`http://localhost/workbench-api/content-thumbnails?ids=${encodeURIComponent(ids)}`, { headers: authenticated ? { "x-dcar-authenticated-user": "test" } : {} });
  assert.equal((await thumbnailResponse(request("7,8", false), reader)).status, 401);
  for (const ids of ["", "0", "-1", "1.2", "../x", "9007199254740992", Array(101).fill("7").join(",")]) {
    assert.equal((await thumbnailResponse(request(ids), reader)).status, 422);
  }
  assert.equal(reads, 0);
  const response = await thumbnailResponse(request("7,8,7"), reader);
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("cache-control"), "private, no-store");
  assert.deepEqual(await response.json(), { items: {} });
  assert.equal(reads, 1);
});

test("missing thumbnail sources fail quietly without leaking internal paths", async () => {
  const request = new Request("http://localhost/workbench-api/content-thumbnails?ids=7", { headers: { "x-dcar-authenticated-user": "test" } });
  const response = await thumbnailResponse(request, async () => { throw new Error("private database path"); });
  assert.equal(response.status, 503);
  assert.doesNotMatch(await response.text(), /private database/);
});

test("thumbnail loads merge identical requests and queue different pages with one child process", async () => {
  const directory = await mkdtemp(join(tmpdir(), "dcar-thumbnail-concurrency-"));
  const previous = Object.fromEntries(["DCAR_THUMBNAIL_PYTHON", "DCAR_THUMBNAIL_HELPER", "DCAR_THUMBNAIL_DB", "DCAR_THUMBNAIL_PROJECT_ROOT"].map((key) => [key, process.env[key]]));
  try {
    const executable = join(directory, "reader.mjs");
    const lock = join(directory, "active");
    const calls = join(directory, "calls");
    await writeFile(executable, `#!/usr/bin/env node
import { mkdirSync, rmdirSync, appendFileSync } from 'node:fs';
mkdirSync(${JSON.stringify(lock)});
appendFileSync(${JSON.stringify(calls)}, process.argv.at(-1) + '\\n');
setTimeout(() => { rmdirSync(${JSON.stringify(lock)}); process.stdout.write(JSON.stringify({items:{}})); }, 100);
`);
    await chmod(executable, 0o700);
    const database = join(directory, "test.sqlite3");
    await writeFile(database, "test fixture");
    Object.assign(process.env, { DCAR_THUMBNAIL_PYTHON: executable, DCAR_THUMBNAIL_HELPER: "unused", DCAR_THUMBNAIL_DB: database, DCAR_THUMBNAIL_PROJECT_ROOT: directory });
    const first = readThumbnailMetadata([1]);
    const same = readThumbnailMetadata([1]);
    const second = readThumbnailMetadata([2]);
    assert.deepEqual(await Promise.all([first, same, second]), [{ items: {} }, { items: {} }, { items: {} }]);
    assert.deepEqual(await readThumbnailMetadata([1]), { items: {} });
    assert.deepEqual((await readFile(calls, "utf8")).trim().split("\n"), ["1", "2"]);
    assert.deepEqual(await readThumbnailMetadata([3]), { items: {} });
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    await rm(directory, { recursive: true, force: true });
  }
});

test("a full page of signed fallback covers fits the metadata helper response buffer", async () => {
  const directory = await mkdtemp(join(tmpdir(), "dcar-thumbnail-large-page-"));
  const previous = Object.fromEntries(["DCAR_THUMBNAIL_PYTHON", "DCAR_THUMBNAIL_HELPER", "DCAR_THUMBNAIL_DB", "DCAR_THUMBNAIL_PROJECT_ROOT"].map((key) => [key, process.env[key]]));
  try {
    const ids = Array.from({ length: 100 }, (_, index) => 10_000 + index);
    const covers = Array.from({ length: 3 }, (_, index) => `https://source${index}.example/cover.webp?sig=` + "a".repeat(3900));
    const payload = { items: Object.fromEntries(ids.map((id) => [String(id), { remote_url: covers[0], remote_urls: covers, reason: null, local_url: null }])) };
    const serialized = JSON.stringify(payload);
    assert.ok(Buffer.byteLength(serialized) > 1024 * 1024, "fixture exceeds the previous one-MiB limit");
    const executable = join(directory, "reader.mjs");
    await writeFile(executable, `#!/usr/bin/env node\nimport { readFileSync } from 'node:fs';\nprocess.stdout.write(readFileSync(${JSON.stringify(join(directory, "payload.json"))}));\n`);
    await chmod(executable, 0o700);
    await writeFile(join(directory, "payload.json"), serialized);
    const database = join(directory, "test.sqlite3");
    await writeFile(database, "test fixture");
    Object.assign(process.env, { DCAR_THUMBNAIL_PYTHON: executable, DCAR_THUMBNAIL_HELPER: "unused", DCAR_THUMBNAIL_DB: database, DCAR_THUMBNAIL_PROJECT_ROOT: directory });
    const response = await readThumbnailMetadata(ids);
    assert.equal(Object.keys(response.items).length, 100);
    assert.deepEqual(response.items[ids.at(-1)], payload.items[ids.at(-1)]);
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    await rm(directory, { recursive: true, force: true });
  }
});
