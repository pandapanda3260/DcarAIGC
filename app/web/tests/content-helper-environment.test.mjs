import assert from "node:assert/strict";
import { mkdtemp, writeFile, chmod, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { runExportCommand } from "../app/lib/contentExportServer.ts";
import { readContentSearch } from "../app/lib/contentSearchServer.ts";

test("real helper processes receive snapshot configuration without provider or auth secrets", async () => {
  const root = await mkdtemp(join(tmpdir(), "dcar-helper-env-"));
  const executable = join(root, "fixture-python");
  const values = {
    DCAR_CONTENT_SEARCH_PYTHON: executable, DCAR_CONTENT_EXPORT_HELPER: join(root, "exports.py"),
    DCAR_CONTENT_SEARCH_HELPER: join(root, "search.py"), DCAR_CONTENT_SEARCH_DB: join(root, "snapshot.db"),
    DCAR_CONTENT_SEARCH_BACKEND: root, DCAR_CONTENT_SEARCH_PROJECT_ROOT: root, DCAR_CONTENT_EXPORT_ROOT: root,
    DCAR_CONTENT_DATA_MODE: "snapshot", DCAR_ACTIVE_SNAPSHOT: join(root, "active-snapshot.json"),
    TIKHUB_API_KEY: "fixture-provider-secret", DCAR_AUTH_SECRET: "fixture-auth-secret",
  };
  const original = Object.fromEntries(Object.keys(values).map((key) => [key, process.env[key]]));
  try {
    await writeFile(executable, `#!${process.execPath}\nprocess.stdin.resume(); process.stdin.on("end", () => {
      const proof = { mode: process.env.DCAR_CONTENT_DATA_MODE, receipt: process.env.DCAR_ACTIVE_SNAPSHOT,
        readonly: process.env.DCAR_READ_ONLY, scheduler: process.env.DCAR_SCHEDULER_ENABLED,
        provider: process.env.TIKHUB_API_KEY, auth: process.env.DCAR_AUTH_SECRET };
      process.stdout.write(JSON.stringify({ items: [proof], total: 1, jobs: [proof] }));
    });\n`);
    await chmod(executable, 0o755);
    Object.assign(process.env, values);
    const search = await readContentSearch({ page: 1, page_size: 1 });
    const exported = await runExportCommand({ action: "list", owner: "a".repeat(64) });
    for (const proof of [search.items[0], exported.jobs[0]]) {
      assert.deepEqual(proof, { mode: "snapshot", receipt: values.DCAR_ACTIVE_SNAPSHOT,
        readonly: "1", scheduler: "0" });
    }
  } finally {
    for (const [key, value] of Object.entries(original)) {
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
    }
    await rm(root, { recursive: true, force: true });
  }
});
