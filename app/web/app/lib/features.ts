// Explicit per-environment capability: the local date-filter reader has its own
// read-only backend. Production keeps its existing API until that backend ships.
export const CONTENT_DATE_FILTER_ENABLED = process.env.NEXT_PUBLIC_DCAR_CONTENT_DATE_FILTER === "1";
export const CONTENT_SEARCH_PATH = CONTENT_DATE_FILTER_ENABLED
  ? "/workbench-api/content-search"
  : "/api/v8/contents/search";
