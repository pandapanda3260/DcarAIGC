// This cache is enabled only after the build-time static-wrapper guard passes.
// It never changes vinext request headers, URL hashes, payloads or commit checks.
export function createStaticRouteCache({ routes, basePath = "", origin, readMetadata, now = Date.now }) {
  const allowed = new Set(routes);
  const entries = new Map();
  const ttl = 5 * 60_000;
  let generation = 0;

  function routeKey(href) {
    const url = new URL(href, origin);
    if (url.origin !== origin) return null;
    let pathname = url.pathname.replace(/\.rsc$/, "");
    if (basePath) {
      if (!pathname.startsWith(basePath + "/")) return null;
      pathname = pathname.slice(basePath.length);
    }
    url.searchParams.delete("_rsc");
    if (url.search || !allowed.has(pathname)) return null;
    return pathname;
  }

  function emptyParams(params) {
    return params != null && typeof params === "object" && !Array.isArray(params) && Object.keys(params).length === 0;
  }

  return {
    get generation() { return generation; },
    clear() {
      generation += 1;
      entries.clear();
    },
    get(href, mountedSlotsHeader, navigationKind) {
      if (navigationKind === "refresh" || mountedSlotsHeader != null) return null;
      const key = routeKey(href);
      const cached = key == null ? null : entries.get(key);
      if (!cached) return null;
      if (cached.expiresAt <= now()) {
        entries.delete(key);
        return null;
      }
      return cached;
    },
    store(href, response, params, elements, startedGeneration) {
      // An old request must not repopulate caches after refresh/auth invalidation.
      if (startedGeneration !== generation) return false;
      const key = routeKey(href);
      if (key == null || !emptyParams(params)) return false;
      const metadata = readMetadata(elements);
      if (metadata.interceptionContext != null || metadata.routeId !== `route:${key}`) return false;
      if (Object.keys(elements).some((name) => name.startsWith("slot:"))) return false;
      if (response.mountedSlotsHeader != null || !response.contentType.startsWith("text/x-component")) return false;
      if (response.paramsHeader != null) {
        try {
          if (!emptyParams(JSON.parse(decodeURIComponent(response.paramsHeader)))) return false;
        } catch { return false; }
      }
      entries.set(key, { response, params, expiresAt: now() + ttl });
      return true;
    },
  };
}
