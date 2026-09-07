// Run with Node >=22.13.0: node scripts/build-release.mjs local|production /absolute/new/output
// Build both environments from a committed, identical frontend source snapshot.
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { copyFileSync, existsSync, mkdirSync, readFileSync, readdirSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { verifyVinextPackage, VINEXT_PATCH_FILES, VINEXT_PATCH_VERSION } from "./vinext-navigation-patch.mjs";

const [environment, destination] = process.argv.slice(2);
if (!["local", "production"].includes(environment) || !destination?.startsWith("/")) {
  throw new Error("usage: node scripts/build-release.mjs local|production /absolute/new/output");
}
const source = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repo = execFileSync("git", ["rev-parse", "--show-toplevel"], { cwd: source, encoding: "utf8" }).trim();
const git = (...args) => execFileSync("git", args, { cwd: repo, encoding: "utf8" }).trim();
const scopes = ["app/web", "src/dcar_eval/dcar_auth/gateway.py"];
if (git("status", "--porcelain", "--", ...scopes)) throw new Error("Commit the reviewed frontend/auth scope before building; runtime-only or uncommitted versions cannot be released");
if (existsSync(destination)) throw new Error("Output already exists; create a new immutable release directory");
const revision = git("rev-parse", "HEAD");
verifyVinextPackage(source);
const files = git("ls-files", "-z", "--", ...scopes).split("\0").filter(Boolean).sort();
const sha = (bytes) => createHash("sha256").update(bytes).digest("hex");
const sourceFiles = {};
mkdirSync(destination, { recursive: true });
for (const file of files) {
  const bytes = readFileSync(join(repo, file));
  sourceFiles[file] = sha(bytes);
  if (!file.startsWith("app/web/")) continue;
  const target = join(destination, file.slice("app/web/".length));
  mkdirSync(dirname(target), { recursive: true });
  copyFileSync(join(repo, file), target);
}
symlinkSync(join(source, "node_modules"), join(destination, "node_modules"), "dir");
const buildEnvironment = {
  DCAR_WEB_BASE_PATH: environment === "production" ? "/dcar" : "",
  NEXT_PUBLIC_DCAR_API_BASE: environment === "production" ? "/dcar" : "",
  NEXT_PUBLIC_DCAR_CONTENT_DATE_FILTER: environment === "local" ? "1" : "0",
};
execFileSync(process.execPath, [join(source, "node_modules/vinext/dist/cli.js"), "build"], {
  cwd: destination,
  env: { ...process.env, ...buildEnvironment, WRANGLER_LOG_PATH: ".wrangler/build.log" },
  stdio: "inherit",
});
// Refuse a source race while the two build environments run.
for (const [file, expected] of Object.entries(sourceFiles)) {
  if (sha(readFileSync(join(repo, file))) !== expected) throw new Error(`Source changed during build: ${file}`);
}
if (git("rev-parse", "HEAD") !== revision) throw new Error("HEAD changed during build");
const artifacts = {};
function scan(directory, prefix) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const file = join(directory, entry.name), relative = `${prefix}/${entry.name}`;
    if (entry.isDirectory()) scan(file, relative);
    else if (entry.isFile()) artifacts[relative] = { sha256: sha(readFileSync(file)), bytes: statSync(file).size };
  }
}
scan(join(destination, "dist"), "dist");
for (const file of ["dist/client/.vite/manifest.json", "dist/server/index.js"]) {
  if (!artifacts[file]) throw new Error(`Missing required build artifact: ${file}`);
}
const manifest = {
  schema_version: 1, environment, source_revision: revision,
  source_tree_sha256: sha(JSON.stringify(sourceFiles)), source_files: sourceFiles,
  built_at: new Date().toISOString(), node_version: process.version,
  vinext_patch: { version: VINEXT_PATCH_VERSION, upstream_files: VINEXT_PATCH_FILES },
  build_environment: buildEnvironment,
  capabilities: ["immediate-destination-shell", "persistent-navigation", "guarded-route-cache-v1", "lazy-pinyin-search", "read-only-thumbnail-stream", "account-permissions", "snapshot-sync-status", ...(environment === "local" ? ["content-date-filter"] : [])],
  gateway_sha256: sourceFiles["src/dcar_eval/dcar_auth/gateway.py"],
  artifacts,
};
writeFileSync(join(destination, "release-manifest.json"), JSON.stringify(manifest, null, 2) + "\n");
console.log(JSON.stringify({ environment, destination, revision, source_tree_sha256: manifest.source_tree_sha256, artifacts: Object.keys(artifacts).length }));
