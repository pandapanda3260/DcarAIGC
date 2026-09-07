import { isServer, QueryClient } from "@tanstack/react-query";
import { setSessionDataClearer, shouldRetryQuery } from "./api";

function makeQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 60_000,
        gcTime: isServer ? Infinity : 10 * 60_000,
        refetchOnWindowFocus: false,
        refetchOnReconnect: true,
        retry: shouldRetryQuery,
        retryDelay: 250,
      },
    },
  });
}

let browserQueryClient: QueryClient | undefined;

function clearNavigationCache() {
  if (typeof window !== "undefined") {
    (window as Window & { __VINEXT_CLEAR_NAV_CACHES__?: () => void }).__VINEXT_CLEAR_NAV_CACHES__?.();
  }
}

// The RSC cache contains page wrappers, but must still obey session changes.
// Observe the shared session query so role updates from any observer invalidate it.
export function bindNavigationSession(client: QueryClient, clear = clearNavigationCache) {
  let identity: string | undefined;
  return client.getQueryCache().subscribe((event) => {
    if (event.query.queryKey.length !== 2 || event.query.queryKey[0] !== "auth"
      || event.query.queryKey[1] !== "session") return;
    if (event.type === "removed") {
      identity = undefined;
      clear();
      return;
    }
    if (event.type !== "updated" || event.action.type !== "success") return;
    const session = event.query.state.data as { username?: string; role?: string } | undefined;
    if (!session) return;
    const next = JSON.stringify([session.username, session.role]);
    if (identity !== undefined && identity !== next) clear();
    identity = next;
  });
}

export function getQueryClient() {
  if (isServer) return makeQueryClient();
  if (!browserQueryClient) {
    browserQueryClient = makeQueryClient();
    bindNavigationSession(browserQueryClient);
    setSessionDataClearer(() => {
      clearNavigationCache();
      browserQueryClient?.clear();
    });
  }
  return browserQueryClient;
}
