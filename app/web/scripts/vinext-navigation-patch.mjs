import { createHash } from "node:crypto";
import { readFileSync, realpathSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { verifyStaticNavigationRoutes } from "./vinext-static-route-guard.mjs";

export const VINEXT_PATCH_VERSION = "0.0.50";
export const VINEXT_PATCH_FILES = Object.freeze({
  "shims/navigation.js": "511ebbfccf4eb47bb1ab8259fbfe3696998168124f8f7f9425bd949ed88176a5",
  "shims/link.js": "666aa5fee4ae34cd19624023a438adaf8c162ac9f0341b1091f4bb1dab960d6d",
  "server/app-browser-entry.js": "b60da30d4f73847327e10574947caf1c8b0c4d47d134a3b5e82aca295753134e",
});
const scriptRoot = dirname(fileURLToPath(import.meta.url));
const defaultRoot = resolve(scriptRoot, "..");
const marker = "/* dcar-vinext-navigation-0.0.50-v2 */";

function replaceOnce(source, before, after) {
  if (source.split(before).length !== 2) throw new Error(`[dcar vinext patch] expected one source anchor: ${before.slice(0, 100)}`);
  return source.replace(before, after);
}

function replaceFunction(source, name, nextName, replacement) {
  const start = source.indexOf(`function ${name}(`);
  const end = source.indexOf(`function ${nextName}(`, start);
  if (start < 0 || end < 0) throw new Error(`[dcar vinext patch] missing function ${name}`);
  return source.slice(0, start) + replacement + "\n" + source.slice(end);
}

export function verifyVinextPackage(root = defaultRoot) {
  // Candidate builds can symlink node_modules to the shared pristine install.
  // Vite resolves module IDs to real paths unless preserveSymlinks is enabled.
  const packageRoot = realpathSync(join(root, "node_modules/vinext"));
  const manifest = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8"));
  if (manifest.version !== VINEXT_PATCH_VERSION) throw new Error(`[dcar vinext patch] expected ${VINEXT_PATCH_VERSION}, found ${manifest.version}; review the patch before upgrading`);
  for (const [file, expected] of Object.entries(VINEXT_PATCH_FILES)) {
    const actual = createHash("sha256").update(readFileSync(join(packageRoot, "dist", file))).digest("hex");
    if (actual !== expected) throw new Error(`[dcar vinext patch] unrecognized source ${file}; dependency files must remain pristine`);
  }
  return packageRoot;
}

export function transformVinextModule(source, file, { routes = [], runtimeModule = pathToFileURL(join(scriptRoot, "vinext-route-cache.mjs")).href } = {}) {
  if (!(file in VINEXT_PATCH_FILES)) return null;
  if (source.startsWith(marker)) return source;
  if (createHash("sha256").update(source).digest("hex") !== VINEXT_PATCH_FILES[file]) throw new Error(`[dcar vinext patch] unrecognized transform input ${file}`);
  if (file === "shims/navigation.js") {
    source = replaceOnce(source, "\t\t\tconst mountedSlotsHeader = getMountedSlotsHeader();", `\t\t\tconst mountedSlotsHeader = getMountedSlotsHeader();
\t\t\tif (window.__DCAR_HAS_STATIC_ROUTE_CACHE__?.(fullHref, mountedSlotsHeader) === true) return;`);
    source = replaceOnce(source, "\treturn window.__VINEXT_RSC_PREFETCHED_URLS__;", `\tconst prefetched = window.__VINEXT_RSC_PREFETCHED_URLS__;
\tfor (const [key, entry] of getPrefetchCache()) if (Date.now() - entry.timestamp >= PREFETCH_CACHE_TTL) {
\t\tgetPrefetchCache().delete(key);
\t\tprefetched.delete(key);
\t}
\treturn prefetched;`);
    source = replaceFunction(source, "prefetchRscResponse", "consumePrefetchResponse", `function prefetchRscResponse(rscUrl, fetchPromise, interceptionContext = null, mountedSlotsHeader = null) {
\tconst cacheKey = AppElementsWire.encodeCacheKey(rscUrl, interceptionContext);
\tconst cache = getPrefetchCache();
\tconst prefetched = getPrefetchedUrls();
\tevictPrefetchCacheIfNeeded();
\tconst entry = { outcome: "pending", timestamp: Date.now(), requestMountedSlotsHeader: mountedSlotsHeader };
\tconst discard = () => {
\t\tif (cache.get(cacheKey) !== entry) return;
\t\tcache.delete(cacheKey);
\t\tprefetched.delete(cacheKey);
\t};
\tentry.pending = fetchPromise.then(async (response) => {
\t\tif (!response.ok) { discard(); return; }
\t\tconst snapshot = await snapshotRscResponse(response);
\t\tif (cache.get(cacheKey) === entry) entry.snapshot = snapshot;
\t}).catch(discard).finally(() => {
\t\tentry.pending = void 0;
\t\tif (entry.snapshot) entry.outcome = "cache-seeded";
\t});
\tcache.set(cacheKey, entry);
}
/** Await an existing prefetch, including its body, without issuing a duplicate. */`);
    const consumeStart = source.indexOf("function consumePrefetchResponse(");
    const consumeEnd = source.indexOf("const _CLIENT_NAV_STATE_KEY", consumeStart);
    source = source.slice(0, consumeStart) + `async function consumePrefetchResponse(rscUrl, interceptionContext = null, mountedSlotsHeader = null) {
\tconst cacheKey = AppElementsWire.encodeCacheKey(rscUrl, interceptionContext);
\tconst prefetched = getPrefetchedUrls();
\tconst cache = getPrefetchCache();
\tconst entry = cache.get(cacheKey);
\tif (!entry) return null;
\tif (entry.requestMountedSlotsHeader !== void 0 && entry.requestMountedSlotsHeader !== mountedSlotsHeader) return null;
\tif (entry.pending) await entry.pending;
\tif (cache.get(cacheKey) !== entry) return null;
\tcache.delete(cacheKey);
\tprefetched.delete(cacheKey);
\tif (entry.outcome !== "cache-seeded" || !entry.snapshot || Date.now() - entry.timestamp >= PREFETCH_CACHE_TTL) return null;
\tif (entry.requestMountedSlotsHeader === void 0 && (entry.snapshot.mountedSlotsHeader ?? null) !== mountedSlotsHeader) return null;
\treturn entry.snapshot;
}
` + source.slice(consumeEnd);
    source = replaceOnce(source, "\t\t\tif (prefetched.has(cacheKey)) return;", "\t\t\tif (prefetched.has(cacheKey) && getPrefetchCache().has(cacheKey)) return;");
    source = replaceOnce(source, '\t\t\t\tpriority: "low"', '\t\t\t\tpriority: "low",\n\t\t\t\tsignal: AbortSignal.timeout(10_000)');
  }
  if (file === "shims/link.js") {
    source = replaceOnce(source, "\t\t\t\tconst mountedSlotsHeader = getMountedSlotsHeader();", `\t\t\t\tconst mountedSlotsHeader = getMountedSlotsHeader();
\t\t\t\tif (window.__DCAR_HAS_STATIC_ROUTE_CACHE__?.(fullHref, mountedSlotsHeader) === true) return;`);
    source = replaceOnce(source, "getMountedSlotsHeader, getPrefetchedUrls,", "getMountedSlotsHeader, getPrefetchCache, getPrefetchedUrls,");
    source = replaceOnce(source, "if (prefetched.has(cacheKey)) return;", "if (prefetched.has(cacheKey) && getPrefetchCache().has(cacheKey)) return;");
    source = replaceOnce(source, '\t\t\t\t\tpurpose: "prefetch"', '\t\t\t\t\tpurpose: "prefetch",\n\t\t\t\t\tsignal: AbortSignal.timeout(10_000)');
  }
  if (file === "server/app-browser-entry.js") {
    source = replaceOnce(source, "\twindow.__VINEXT_CLEAR_NAV_CACHES__ = clearClientNavigationCaches;", `\twindow.__VINEXT_CLEAR_NAV_CACHES__ = clearClientNavigationCaches;
\t// Peek only: navigation still consumes and validates the original payload.
\t// Sharing this validity check stops pointer/hover prefetch from refetching a
\t// route already covered by the guarded cache (including initial hydration).
\twindow.__DCAR_HAS_STATIC_ROUTE_CACHE__ = (href, mountedSlotsHeader) =>
\t\tstaticRouteCache.get(href, mountedSlotsHeader, "navigate") !== null;`);
    source = replaceOnce(source, "const prefetchedResponse = consumePrefetchResponse(rscUrl, requestInterceptionContext, mountedSlotsHeader);", "const prefetchedResponse = await consumePrefetchResponse(rscUrl, requestInterceptionContext, mountedSlotsHeader);\n\t\t\t\t\tif (!browserNavigationController.isCurrentNavigation(navId)) return;");
    source = `import { createStaticRouteCache } from ${JSON.stringify(runtimeModule)};\n` + source;
    source = replaceOnce(source, "const visitedResponseCache = /* @__PURE__ */ new Map();", `const visitedResponseCache = /* @__PURE__ */ new Map();
const staticRouteCache = createStaticRouteCache({
\troutes: ${JSON.stringify(routes)}, basePath: __basePath,
\torigin: typeof window === "undefined" ? "http://localhost" : window.location.origin,
\treadMetadata: AppElementsWire.readMetadata
});`);
    source = replaceOnce(source, "function clearVisitedResponseCache() {\n\tvisitedResponseCache.clear();", "function clearVisitedResponseCache() {\n\tvisitedResponseCache.clear();\n\tstaticRouteCache.clear();");
    source = replaceOnce(source, "\tconst root = decodeAppElementsPromise(createFromReadableStream(rscStream));", `\tconst initialHref = window.location.href;
\tconst initialParams = { ...latestClientParams };
\tconst initialCacheGeneration = staticRouteCache.generation;
\tconst [hydrationStream, initialCacheStream] = rscStream.tee();
\tconst root = decodeAppElementsPromise(createFromReadableStream(hydrationStream));
\tPromise.all([root, new Response(initialCacheStream).arrayBuffer()]).then(([elements, buffer]) => {
\t\tstaticRouteCache.store(initialHref, {
\t\t\tbuffer, contentType: "text/x-component", mountedSlotsHeader: getMountedSlotIdsHeader(elements),
\t\t\tparamsHeader: null, url: initialHref
\t\t}, initialParams, elements, initialCacheGeneration);
\t}).catch(() => { /* Hydration error handling remains with vinext. */ });`);
    source = replaceOnce(source, "\t\tconst navId = browserNavigationController.beginNavigation();", "\t\tconst navId = browserNavigationController.beginNavigation();\n\t\tconst staticCacheGeneration = staticRouteCache.generation;");
    source = replaceOnce(source, "const cachedRoute = getVisitedResponse(rscUrl, requestInterceptionContext, mountedSlotsHeader, navigationKind);", "const cachedRoute = getVisitedResponse(rscUrl, requestInterceptionContext, mountedSlotsHeader, navigationKind) ?? staticRouteCache.get(rscUrl, mountedSlotsHeader, navigationKind);");
    source = replaceOnce(source, "\t\t\t\tstoreVisitedResponseSnapshot(rscUrl, resolveVisitedResponseInterceptionContext(requestInterceptionContext, metadata.interceptionContext), {", "\t\t\t\tconst cachedSnapshot = {");
    source = replaceOnce(source, "\t\t\t\t\turl: navResponse.url\n\t\t\t\t}, navParams);", `\t\t\t\t\turl: navResponseUrl ?? navResponse.url
\t\t\t\t};
\t\t\t\tif (staticCacheGeneration !== staticRouteCache.generation) return;
\t\t\t\tstoreVisitedResponseSnapshot(rscUrl, resolveVisitedResponseInterceptionContext(requestInterceptionContext, metadata.interceptionContext), cachedSnapshot, navParams);
\t\t\t\tif (navigationKind !== "refresh") staticRouteCache.store(rscUrl, cachedSnapshot, navParams, resolvedElements, staticCacheGeneration);`);
  }
  return marker + "\n" + source;
}

// Vite runs this even when developers invoke the vinext CLI directly. Applying
// the patch in memory also prevents builds from modifying a live shared install.
export function vinextNavigationPatch() {
  let root = defaultRoot;
  let packageRoot;
  let routes;
  function matchedFile(id) {
    const cleanId = id.split("?")[0].replaceAll("\\", "/");
    for (const packagePath of [packageRoot, join(root, "node_modules/vinext")]) {
      const prefix = `${packagePath.replaceAll("\\", "/")}/dist/`;
      if (cleanId.startsWith(prefix)) return cleanId.slice(prefix.length);
    }
    return null;
  }
  return {
    name: "dcar-vinext-navigation-patch",
    enforce: "pre",
    configResolved(config) {
      root = config.root;
      packageRoot = verifyVinextPackage(root);
      routes = verifyStaticNavigationRoutes(root);
    },
    buildStart() {
      verifyVinextPackage(root);
      routes = verifyStaticNavigationRoutes(root);
    },
    transform(code, id) {
      const file = matchedFile(id);
      if (file === null) return null;
      const patched = transformVinextModule(code, file, { routes, runtimeModule: join(scriptRoot, "vinext-route-cache.mjs") });
      return patched === null ? null : { code: patched, map: null, meta: { dcarVinextNavigationPatch: VINEXT_PATCH_VERSION } };
    },
    generateBundle(_options, bundle) {
      // SSR/RSC legitimately do not load the browser entry. Verify the actual
      // client output, including module metadata retained by incremental builds.
      if (this.environment?.name !== "client") return;
      const applied = new Set();
      for (const chunk of Object.values(bundle)) {
        if (chunk.type !== "chunk") continue;
        for (const id of Object.keys(chunk.modules)) {
          const file = matchedFile(id);
          if (file !== null && file in VINEXT_PATCH_FILES && this.getModuleInfo(id)?.meta.dcarVinextNavigationPatch === VINEXT_PATCH_VERSION) applied.add(file);
        }
      }
      const missing = Object.keys(VINEXT_PATCH_FILES).filter((file) => !applied.has(file));
      if (missing.length) this.error(`[dcar vinext patch] client output is missing patched modules: ${missing.join(", ")}`);
      console.log(`[dcar vinext patch] client output verified: ${applied.size} patched modules, ${routes.length} guarded routes`);
    },
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  verifyVinextPackage(process.argv[2] ? resolve(process.argv[2]) : defaultRoot);
  console.log(`[dcar vinext patch] verified pristine vinext ${VINEXT_PATCH_VERSION}; Vite applies the patch in memory`);
}
