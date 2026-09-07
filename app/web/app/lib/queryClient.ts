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

export function getQueryClient() {
  if (isServer) return makeQueryClient();
  if (!browserQueryClient) {
    browserQueryClient = makeQueryClient();
    setSessionDataClearer(() => browserQueryClient?.clear());
  }
  return browserQueryClient;
}
