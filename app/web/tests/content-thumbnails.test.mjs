import assert from "node:assert/strict";
import test from "node:test";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { thumbnailSources } from "../app/lib/contentThumbnails.ts";
import { readThumbnailMetadata, thumbnailResponse } from "../app/lib/contentThumbnailServer.ts";

test("thumbnail sources prefer reusable local images and reject cross-content or executable URLs", () => {
  const local = "/api/v8/contents/7/evidence/files/12/0";
  const remote = "https://example.com/cover.jpg";
  assert.deepEqual(thumbnailSources(7, { local_url: local, remote_url: remote }), [local, remote]);
  assert.deepEqual(thumbnailSources(8, { local_url: local, remote_url: remote }), [remote]);
  for (const url of ["javascript:alert(1)", "http://example.com/x", "https://user:secret@example.com/x", "/other", "bad url"]) {
    assert.deepEqual(thumbnailSources(7, { local_url: null, remote_url: url }), []);
  }
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

test("large snapshot projection permits one child process and releases capacity after completion", async () => {
  const directory = await mkdtemp(join(tmpdir(), "dcar-thumbnail-concurrency-"));
  const previous = Object.fromEntries(["DCAR_THUMBNAIL_PYTHON", "DCAR_THUMBNAIL_HELPER", "DCAR_THUMBNAIL_DB", "DCAR_THUMBNAIL_PROJECT_ROOT"].map((key) => [key, process.env[key]]));
  try {
    const executable = join(directory, "reader.mjs");
    await writeFile(executable, '#!/usr/bin/env node\nsetTimeout(() => process.stdout.write(JSON.stringify({items:{}})), 100);\n');
    await chmod(executable, 0o700);
    Object.assign(process.env, { DCAR_THUMBNAIL_PYTHON: executable, DCAR_THUMBNAIL_HELPER: "unused", DCAR_THUMBNAIL_DB: "unused", DCAR_THUMBNAIL_PROJECT_ROOT: directory });
    const first = readThumbnailMetadata([1]);
    await assert.rejects(readThumbnailMetadata([2]), /Thumbnail reader unavailable/);
    assert.deepEqual(await first, { items: {} });
    assert.deepEqual(await readThumbnailMetadata([3]), { items: {} });
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    await rm(directory, { recursive: true, force: true });
  }
});
