import { isServer, QueryClient } from "@tanstack/react-query";
import { shouldRetryQuery } from "./api";

function makeQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 60_000,
        gcTime: isServer ? Infinity : 10 * 60_000,
        refetchOnWindowFocus: false,
        refetchOnReconnect: true,
        retry: shouldRetryQuery,
      },
    },
  });
}

let browserQueryClient: QueryClient | undefined;

export function getQueryClient() {
  if (isServer) return makeQueryClient();
  if (!browserQueryClient) browserQueryClient = makeQueryClient();
  return browserQueryClient;
}
