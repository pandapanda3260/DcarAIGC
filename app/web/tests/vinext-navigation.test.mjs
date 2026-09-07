import assert from "node:assert/strict";
import { cp, mkdtemp, readFile, realpath, rm, symlink, writeFile, mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { after, before, beforeEach, test } from "node:test";
import vm from "node:vm";
import ts from "typescript";
import { transformVinextModule, verifyVinextPackage, VINEXT_PATCH_FILES, vinextNavigationPatch } from "../scripts/vinext-navigation-patch.mjs";
import { createStaticRouteCache } from "../scripts/vinext-route-cache.mjs";
import { STATIC_NAVIGATION_ROUTES, verifyStaticNavigationRoutes } from "../scripts/vinext-static-route-guard.mjs";

const webRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const originalFetch = globalThis.fetch;
const originalWindow = globalThis.window;
let temporaryRoot;
let nav;
let wire;
let rsc;
let elementsHelpers;
let prefetchLink;
let patchedEntry;
const tick = () => new Promise((resolve) => setImmediate(resolve));
async function until(predicate) { for (let i = 0; i < 100; i++) { if (predicate()) return; await new Promise((resolve) => setTimeout(resolve, 1)); } assert.fail("condition did not settle"); }
function deferred() { let resolve; let reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; }
function elementPayload(path) { return { __route: `route:${path}`, __interceptionContext: null, __layoutIds: ["layout:/"], __rootLayout: "/", [`route:${path}`]: "client-wrapper" }; }
function snapshot(value = "payload") { return { buffer: new TextEncoder().encode(value).buffer, contentType: "text/x-component", mountedSlotsHeader: null, paramsHeader: null, url: "https://audit.invalid/overview.rsc" }; }
function cache() { return createStaticRouteCache({ routes: STATIC_NAVIGATION_ROUTES, origin: "https://audit.invalid", readMetadata: wire.AppElementsWire.readMetadata }); }

before(async () => {
  temporaryRoot = await mkdtemp(join(tmpdir(), "dcar-vinext-navigation-"));
  const copiedPackage = join(temporaryRoot, "vinext");
  await cp(join(webRoot, "node_modules/vinext"), copiedPackage, { recursive: true });
  await symlink(join(webRoot, "node_modules"), join(copiedPackage, "node_modules"), "dir");
  for (const file of Object.keys(VINEXT_PATCH_FILES)) {
    const original = await readFile(join(copiedPackage, "dist", file), "utf8");
    let transformed = transformVinextModule(original, file, { routes: STATIC_NAVIGATION_ROUTES });
    assert.equal(transformVinextModule(transformed, file), transformed, "patch is idempotent");
    // Expose the actual Link intent function only inside the isolated test copy.
    if (file === "shims/link.js") transformed += "\nexport { prefetchUrl as __prefetchUrlForTest };\n";
    await writeFile(join(copiedPackage, "dist", file), transformed);
    if (file === "server/app-browser-entry.js") patchedEntry = transformed;
  }
  globalThis.window = { location: { href: "https://audit.invalid/overview", origin: "https://audit.invalid", pathname: "/overview", search: "" }, history: { pushState() {}, replaceState() {} }, addEventListener() {} };
  nav = await import(pathToFileURL(join(copiedPackage, "dist/shims/navigation.js")));
  wire = await import(pathToFileURL(join(copiedPackage, "dist/server/app-elements-wire.js")));
  rsc = await import(pathToFileURL(join(copiedPackage, "dist/server/app-rsc-cache-busting.js")));
  elementsHelpers = await import(pathToFileURL(join(copiedPackage, "dist/server/app-elements.js")));
  prefetchLink = (await import(pathToFileURL(join(copiedPackage, "dist/shims/link.js")))).__prefetchUrlForTest;
});
beforeEach(() => {
  globalThis.fetch = originalFetch;
  nav.getPrefetchCache().clear(); nav.getPrefetchedUrls().clear(); nav.setMountedSlotsHeader(null);
  delete window.__DCAR_HAS_STATIC_ROUTE_CACHE__;
  window.__VINEXT_RSC_NAVIGATE__ = async () => {};
  window.requestIdleCallback = (callback) => callback();
  Object.assign(window.location, { href: "https://audit.invalid/overview", pathname: "/overview" });
});
after(async () => { globalThis.fetch = originalFetch; globalThis.window = originalWindow; await rm(temporaryRoot, { recursive: true, force: true }); });

test("pending prefetch and immediate click share one request through body completion", async () => {
  const body = deferred();
  let requests = 0;
  globalThis.fetch = () => { requests += 1; return body.promise; };
  nav.appRouterInstance.prefetch("/contents");
  await until(() => requests === 1);
  const href = await rsc.createRscRequestUrl("/contents", rsc.createRscRequestHeaders({ interceptionContext: "/overview" }));
  let settled = false;
  const navigation = nav.consumePrefetchResponse(href, "/overview").then((result) => { settled = true; return result; });
  await tick();
  assert.equal(settled, false);
  body.resolve(new Response("shared-body", { headers: { "content-type": "text/x-component", "x-vinext-params": "%7B%7D" } }));
  const cached = await navigation;
  assert.equal(new TextDecoder().decode(cached.buffer), "shared-body");
  assert.equal(cached.paramsHeader, "%7B%7D");
  assert.equal(requests, 1);
  assert.equal(nav.getPrefetchCache().size, 0);
});

test("expired prefetch is fetched again even while cache is below capacity", async () => {
  let requests = 0;
  globalThis.fetch = async () => { requests += 1; return new Response("body"); };
  nav.appRouterInstance.prefetch("/contents");
  await until(() => nav.getPrefetchCache().size === 1 && ![...nav.getPrefetchCache().values()][0].pending);
  [...nav.getPrefetchCache().values()][0].timestamp = Date.now() - 31_000;
  nav.appRouterInstance.prefetch("/contents");
  await until(() => requests === 2);
  await Promise.all([...nav.getPrefetchCache().values()].map((entry) => entry.pending));
  assert.equal(nav.getPrefetchedUrls().size, 1);
});

test("failed prefetch permits navigation fallback and later retries", async () => {
  for (const result of [Promise.resolve(new Response("bad", { status: 503 })), Promise.reject(new Error("offline"))]) {
    // Attach immediately; no network request is made by this direct lifecycle test.
    nav.prefetchRscResponse("/tasks.rsc", result, "/overview");
    assert.equal(await nav.consumePrefetchResponse("/tasks.rsc", "/overview"), null);
    assert.equal(nav.getPrefetchCache().size, 0);
  }
});

test("prefetch timeout covers a response body that never finishes", async () => {
  const realTimeout = AbortSignal.timeout;
  const controller = new AbortController();
  AbortSignal.timeout = (duration) => { assert.equal(duration, 10_000); return controller.signal; };
  globalThis.fetch = async (_url, options) => new Response(new ReadableStream({ start(stream) { options.signal.addEventListener("abort", () => stream.error(new Error("timeout"))); } }));
  try {
    nav.appRouterInstance.prefetch("/contents");
    await until(() => nav.getPrefetchCache().size === 1);
    const href = await rsc.createRscRequestUrl("/contents", rsc.createRscRequestHeaders({ interceptionContext: "/overview" }));
    const pending = nav.consumePrefetchResponse(href, "/overview");
    controller.abort();
    assert.equal(await pending, null);
    assert.equal(nav.getPrefetchedUrls().size, 0);
  } finally { AbortSignal.timeout = realTimeout; }
});

test("cleared pending response cannot delete a newer prefetch or reappear", async () => {
  const old = deferred();
  nav.prefetchRscResponse("/tasks.rsc", old.promise, "/overview");
  const waiting = nav.consumePrefetchResponse("/tasks.rsc", "/overview");
  nav.getPrefetchCache().clear(); nav.getPrefetchedUrls().clear();
  nav.prefetchRscResponse("/tasks.rsc", Promise.resolve(new Response("new")), "/overview");
  await Promise.all([...nav.getPrefetchCache().values()].map((entry) => entry.pending));
  old.reject(new Error("old failure"));
  assert.equal(await waiting, null);
  assert.equal(new TextDecoder().decode((await nav.consumePrefetchResponse("/tasks.rsc", "/overview")).buffer), "new");
});

test("prefetch keeps source/slot separation and original response headers", async () => {
  nav.prefetchRscResponse("/tasks.rsc", Promise.resolve(new Response("body", { headers: { "x-vinext-mounted-slots": "slot:modal:/" } })), "/overview", "request-slot");
  assert.equal(await nav.consumePrefetchResponse("/tasks.rsc", "/contents", "request-slot"), null);
  assert.equal(await nav.consumePrefetchResponse("/tasks.rsc", "/overview", "other-slot"), null);
  const cached = await nav.consumePrefetchResponse("/tasks.rsc", "/overview", "request-slot");
  assert.equal(cached.mountedSlotsHeader, "slot:modal:/");
  assert.equal(nav.restoreRscResponse(cached).headers.get("x-vinext-mounted-slots"), "slot:modal:/");
});

const prefetchEntrypoints = [
  ["router", (href) => nav.appRouterInstance.prefetch(href)],
  ["Link", (href) => prefetchLink(href, "full")],
];

test("router and Link skip network for valid static payloads across source routes", async () => {
  const target = cache();
  assert.equal(target.store("/contents", snapshot(), {}, elementPayload("/contents"), target.generation), true);
  window.__DCAR_HAS_STATIC_ROUTE_CACHE__ = (href, slots) => target.get(href, slots, "navigate") !== null;
  const requests = [];
  globalThis.fetch = async (href) => { requests.push(href); return new Response("unexpected"); };
  for (const source of ["/overview", "/accounts", "/tasks"]) {
    Object.assign(window.location, { href: `https://audit.invalid${source}`, pathname: source });
    for (const [, prefetch] of prefetchEntrypoints) prefetch("/contents");
    await tick();
  }
  assert.deepEqual(requests, []);
  assert.equal(nav.getPrefetchCache().size, 0, "skip must not create or consume a prefetch entry");
  assert.ok(target.get("/contents", null, "navigate"), "peek keeps the payload available to navigation");
});

for (const [name, prefetch] of prefetchEntrypoints) {
  test(`${name} resumes prefetch after static TTL or authentication invalidation`, async () => {
    let now = 0;
    const target = createStaticRouteCache({ routes: STATIC_NAVIGATION_ROUTES, origin: "https://audit.invalid", readMetadata: wire.AppElementsWire.readMetadata, now: () => now });
    window.__DCAR_HAS_STATIC_ROUTE_CACHE__ = (href, slots) => target.get(href, slots, "navigate") !== null;
    let requests = 0;
    globalThis.fetch = async () => { requests += 1; return new Response("body"); };
    for (const invalidation of ["ttl", "auth-clear"]) {
      nav.getPrefetchCache().clear(); nav.getPrefetchedUrls().clear();
      assert.equal(target.store("/contents", snapshot(), {}, elementPayload("/contents"), target.generation), true);
      prefetch("/contents"); await tick();
      const before = requests;
      if (invalidation === "ttl") now += 300_000;
      else target.clear();
      prefetch("/contents");
      await until(() => requests === before + 1);
      await Promise.all([...nav.getPrefetchCache().values()].map((entry) => entry.pending));
    }
    assert.equal(requests, 2);
  });

  test(`${name} preserves mounted-slot request context instead of using static skip`, async () => {
    const target = cache();
    assert.equal(target.store("/contents", snapshot(), {}, elementPayload("/contents"), target.generation), true);
    window.__DCAR_HAS_STATIC_ROUTE_CACHE__ = (href, slots) => target.get(href, slots, "navigate") !== null;
    nav.setMountedSlotsHeader("slot:modal:/");
    let headers;
    globalThis.fetch = async (_url, options) => { headers = options.headers; return new Response("body"); };
    prefetch("/contents");
    await until(() => headers !== undefined);
    assert.equal(headers.get("x-vinext-mounted-slots"), "slot:modal:/");
    await Promise.all([...nav.getPrefetchCache().values()].map((entry) => entry.pending));
  });
}

test("cross-source reuse keeps original metadata and requires declared routes", async () => {
  const target = cache();
  const payload = elementPayload("/overview");
  const response = snapshot(JSON.stringify(payload));
  const fromContents = await rsc.createRscRequestUrl("/overview", rsc.createRscRequestHeaders({ interceptionContext: "/contents" }));
  const fromAccounts = await rsc.createRscRequestUrl("/overview", rsc.createRscRequestHeaders({ interceptionContext: "/accounts" }));
  assert.notEqual(fromContents, fromAccounts, "request protocol still varies by source");
  assert.equal(target.store(fromContents, response, {}, payload, target.generation), true);
  assert.equal(target.get(fromAccounts, null, "navigate").response, response);
  assert.equal(target.get(fromAccounts, null, "refresh"), null);
  assert.equal(target.get(fromAccounts, "slot:modal:/", "navigate"), null);
  assert.equal(target.get("/overview.rsc?filter=private", null, "navigate"), null);
  assert.equal(target.get("https://other.invalid/overview.rsc", null, "navigate"), null);
});

test("runtime rejects intercepted, slotted, param-bearing and mismatched payloads", () => {
  const target = cache();
  const inputs = [
    [snapshot(), {}, { ...elementPayload("/overview"), __interceptionContext: "/contents" }],
    [snapshot(), {}, { ...elementPayload("/overview"), "slot:modal:/": null }],
    [snapshot(), { id: "1" }, elementPayload("/overview")],
    [snapshot(), {}, elementPayload("/tasks")],
    [{ ...snapshot(), paramsHeader: "%7B%22id%22%3A%221%22%7D" }, {}, elementPayload("/overview")],
    [{ ...snapshot(), contentType: "text/html" }, {}, elementPayload("/overview")],
  ];
  for (const input of inputs) assert.equal(target.store("/overview.rsc", ...input, target.generation), false);
});

test("auth/refresh clear rejects late completions; TTL and base path are bounded", () => {
  let now = 0;
  const target = createStaticRouteCache({ routes: STATIC_NAVIGATION_ROUTES, basePath: "/workbench", origin: "https://audit.invalid", readMetadata: wire.AppElementsWire.readMetadata, now: () => now });
  const generation = target.generation;
  assert.equal(target.store("/workbench/overview.rsc", snapshot(), {}, elementPayload("/overview"), generation), true);
  assert.ok(target.get("/workbench/overview.rsc?_rsc=different-source", null, "navigate"));
  assert.equal(target.get("/overview.rsc", null, "navigate"), null);
  now = 300_000;
  assert.equal(target.get("/workbench/overview.rsc", null, "traverse"), null);
  target.clear();
  assert.equal(target.store("/workbench/overview.rsc", snapshot(), {}, elementPayload("/overview"), generation), false);
});

test("patched bootstrap seeds hydration, real navigation reuses it across sources, and clear empties it", async () => {
  // Execute the actual transformed entry with transport/React at the boundary
  // replaced by minimal test doubles. Its bootstrap and cache wiring are real.
  const ast = ts.createSourceFile("entry.js", patchedEntry, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
  let executable = patchedEntry;
  for (const statement of [...ast.statements].reverse()) if (ts.isImportDeclaration(statement) || ts.isExportDeclaration(statement)) executable = executable.slice(0, statement.pos) + executable.slice(statement.end);
  executable = executable.replaceAll("import.meta.env.DEV", "false").replaceAll("import.meta.hot", "undefined");
  let navigationId = 0;
  let commits = 0;
  let requests = 0;
  const context = vm.createContext({
    ...nav, ...elementsHelpers, ...rsc, AppElementsWire: wire.AppElementsWire,
    createStaticRouteCache, Response, Promise, URL, Headers, console, performance,
    VINEXT_MOUNTED_SLOTS_HEADER: "x-vinext-mounted-slots", VINEXT_PARAMS_HEADER: "x-vinext-params",
    window: { ...globalThis.window, history: { state: null } }, history: {},
    createAppBrowserNavigationController: () => ({
      beginNavigation: () => ++navigationId,
      isCurrentNavigation: (id) => id === navigationId,
      waitForBrowserRouterStateReady: async () => {},
      getBrowserRouterState: () => ({ elements: elementPayload("/contents") }),
      finalizeNavigation() {},
    }),
    getCurrentInterceptionContext: () => "/contents", getCurrentNextUrl: () => "/contents",
    createFromFetch: async (response) => JSON.parse(await (await response).text()),
    fetch: async () => { requests += 1; throw new Error("unexpected network request"); },
    createFromReadableStream: async (stream) => JSON.parse(await new Response(stream).text()),
    createHistoryStateWithPreviousNextUrl: (state) => state,
    replaceHistoryStateWithoutNotify() {}, createOnUncaughtError: () => () => {},
    consumeInitialFormState: () => undefined, getVinextBrowserGlobal: () => ({}),
    createVinextHydrateRootOptions: (value) => value, createElement: () => ({}), hydrateRoot() {},
  });
  vm.runInContext(executable, context);
  context.document = {};
  context.stream = new Response(JSON.stringify(elementPayload("/overview"))).body;
  vm.runInContext("bootstrapHydration(stream)", context);
  await until(() => vm.runInContext('staticRouteCache.get("/overview.rsc?_rsc=another-source", null, "navigate") !== null', context));
  assert.equal(vm.runInContext('window.__DCAR_HAS_STATIC_ROUTE_CACHE__("/overview", null)', context), true);
  assert.equal(vm.runInContext('window.__DCAR_HAS_STATIC_ROUTE_CACHE__("/overview", "slot:modal:/")', context), false);
  context.recordCommit = async (payload) => { assert.equal((await payload).__route, "route:/overview"); commits += 1; return "committed"; };
  vm.runInContext("renderNavigationPayload = recordCommit", context);
  await vm.runInContext('window.__VINEXT_RSC_NAVIGATE__("/overview")', context);
  assert.equal(commits, 1);
  assert.equal(requests, 0, "initial route from another source must avoid RSC network");
  vm.runInContext("window.__VINEXT_CLEAR_NAV_CACHES__()", context);
  assert.equal(vm.runInContext('staticRouteCache.get("/overview.rsc", null, "navigate")', context), null);
  assert.equal(vm.runInContext('window.__DCAR_HAS_STATIC_ROUTE_CACHE__("/overview", null)', context), false);
  context.fetch = async (url) => {
    requests += 1;
    const response = new Response(JSON.stringify(elementPayload("/overview")), { headers: { "content-type": "text/x-component" } });
    Object.defineProperty(response, "url", { value: new URL(url, "https://audit.invalid").href });
    return response;
  };
  context.recordCommit = async (payload) => {
    await payload;
    context.window.__VINEXT_CLEAR_NAV_CACHES__();
    return "committed";
  };
  vm.runInContext("renderNavigationPayload = recordCommit", context);
  await vm.runInContext('window.__VINEXT_RSC_NAVIGATE__("/overview")', context);
  assert.equal(requests, 1);
  assert.equal(vm.runInContext("visitedResponseCache.size", context), 0, "late request cannot refill original visited cache after auth/refresh clear");
  assert.equal(vm.runInContext('staticRouteCache.get("/overview.rsc", null, "navigate")', context), null);
});

test("build plugin is mandatory, version locked and does not mutate dependencies", async () => {
  verifyVinextPackage(webRoot);
  const plugin = vinextNavigationPatch();
  plugin.configResolved({ root: webRoot });
  plugin.buildStart();
  const modules = {};
  const metadata = {};
  for (const file of Object.keys(VINEXT_PATCH_FILES)) {
    const id = join(webRoot, "node_modules/vinext/dist", file);
    const original = await readFile(id, "utf8");
    const result = plugin.transform(original, id);
    modules[id] = {};
    metadata[id] = { meta: result.meta };
    assert.ok(result.code.includes("dcar-vinext-navigation-0.0.50-v2"));
    assert.equal(await readFile(id, "utf8"), original);
    assert.throws(() => transformVinextModule(original + "\n", file), /unrecognized transform input/);
  }
  const context = { environment: { name: "client" }, getModuleInfo: (id) => metadata[id], error: (message) => { throw new Error(message); } };
  plugin.generateBundle.call(context, {}, { main: { type: "chunk", modules } });
  assert.throws(() => plugin.generateBundle.call(context, {}, {}), /missing patched modules/);
  assert.doesNotThrow(() => plugin.generateBundle.call({ environment: { name: "rsc" } }, {}, {}));
  const linkedRoot = join(temporaryRoot, "linked-build");
  await mkdir(linkedRoot);
  await symlink(join(webRoot, "node_modules"), join(linkedRoot, "node_modules"), "dir");
  await symlink(join(webRoot, "app"), join(linkedRoot, "app"), "dir");
  const linkedPlugin = vinextNavigationPatch();
  linkedPlugin.configResolved({ root: linkedRoot });
  const navigationPath = await realpath(join(webRoot, "node_modules/vinext/dist/shims/navigation.js"));
  assert.ok(linkedPlugin.transform(await readFile(navigationPath, "utf8"), navigationPath)?.code, "Vite real paths must match a symlinked candidate install");
  assert.match(await readFile(join(webRoot, "vite.config.ts"), "utf8"), /vinextNavigationPatch\(\)/);
});

test("build guard accepts current wrappers and rejects unknown server behavior", async () => {
  assert.deepEqual(verifyStaticNavigationRoutes(webRoot), STATIC_NAVIGATION_ROUTES);
  const fixture = join(temporaryRoot, "guard");
  await mkdir(fixture);
  await cp(join(webRoot, "app"), join(fixture, "app"), { recursive: true });
  const page = join(fixture, "app/overview/page.tsx");
  const original = await readFile(page, "utf8");
  for (const changed of [
    original.replace("function Page()", "async function Page()"),
    original.replace("function Page()", "function Page({ params })"),
    'import { cookies } from "next/headers";\n' + original,
    original + '\nexport const revalidate = 0;\n',
  ]) {
    await writeFile(page, changed);
    assert.throws(() => verifyStaticNavigationRoutes(fixture), /navigation guard/);
  }
  await writeFile(page, original);
  await mkdir(join(fixture, "app/@modal"));
  assert.throws(() => verifyStaticNavigationRoutes(fixture), /parallel\/intercepted/);
});
