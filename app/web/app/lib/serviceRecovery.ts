import type { QueryClient } from "@tanstack/react-query";
import { ApiRequestError } from "./api";

const businessQueryRoots = new Set(["accounts", "contents", "overview", "selling-points", "spu", "tasks", "media"]);

function isTemporaryReadFailure(error: unknown) {
  if (!(error instanceof ApiRequestError)) return false;
  if (error.status === null) return error.retryable;
  return error.status >= 500 && error.status < 600
    && (error.code === null || error.code === "upstream_unavailable");
}

/** A recovered service gets one retry of failed visible reads, never writes or permission errors. */
export function createServiceRecovery(client: QueryClient) {
  let wasAvailable = false;
  return (available: boolean | null): Promise<void> => {
    if (available === null) return Promise.resolve();
    const recovered = available && !wasAvailable;
    wasAvailable = available;
    if (!recovered) return Promise.resolve();
    return client.refetchQueries({
      type: "active",
      predicate: (query) => businessQueryRoots.has(String(query.queryKey[0]))
        && query.state.status === "error"
        && query.state.fetchStatus === "idle"
        && isTemporaryReadFailure(query.state.error),
    }, { cancelRefetch: false });
  };
}
