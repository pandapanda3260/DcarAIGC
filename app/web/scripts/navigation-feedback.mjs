const primaryRoute = /^\/(overview|contents|accounts|selling-points|spu-audience|tasks|users)\/?$/;

// Only public route metadata lives here. The router remains responsible for
// authorization, history and committing the actual destination tree.
export function createNavigationFeedbackStore() {
  let snapshot = null;
  const listeners = new Set();
  function publish(next) {
    if (snapshot === next) return;
    snapshot = next;
    for (const listener of listeners) listener();
  }
  return {
    start(id, href, basePath = "", navigationKind = "push") {
      let next = null;
      if (typeof window !== "undefined" && navigationKind !== "refresh") {
        try {
          const url = new URL(href, window.location.href);
          const base = basePath.replace(/\/+$/, "");
          const path = base && url.pathname.startsWith(`${base}/`) ? url.pathname.slice(base.length)
            : base ? "" : url.pathname;
          const route = primaryRoute.exec(path);
          const unchanged = navigationKind === "navigate" && snapshot === null && url.href === window.location.href;
          if (!unchanged && url.origin === window.location.origin && !url.search && !url.hash && route) {
            next = Object.freeze({ id, href: url.pathname, section: route[1] });
          }
        } catch { /* Invalid or non-workbench destinations cancel old feedback. */ }
      }
      publish(next);
    },
    finish(id) { if (snapshot?.id === id) publish(null); },
    subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); },
    getSnapshot: () => snapshot,
    getServerSnapshot: () => null,
  };
}

export const navigationFeedbackStore = createNavigationFeedbackStore();
