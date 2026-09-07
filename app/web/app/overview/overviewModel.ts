import type { ConclusionMetricKey, Metric, OverviewChannel } from "../lib/types";

export const overviewMetrics = [
  ["selling_point_count_share", "卖点条数占比", "blue"],
  ["core_selling_point_count_share", "核心卖点条数占比", "amber"],
  ["selling_point_exposure_share", "卖点曝光占比", "purple"],
  ["core_selling_point_exposure_share", "核心卖点曝光占比", "teal"],
] as const satisfies ReadonlyArray<readonly [ConclusionMetricKey, string, string]>;

export type OverviewSellingPoint = {
  code: string;
  code_missing?: boolean;
  label: string;
  tier: "core" | "other";
  publication_count: number;
  count_share: Metric;
  view_count: Metric;
  exposure_share: Metric;
  provided_view_items: number;
  missing_view_items: number;
  stale_view_items: number;
};

export type ReportChannel = OverviewChannel & {
  selling_points?: OverviewSellingPoint[];
};

export function visibleMetricNumber(metric: Metric | undefined): number | null {
  if (!metric || !["available", "sample_only"].includes(metric.status)) return null;
  const value = metric.kind === "ratio" ? metric.percentage : metric.value;
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}

export function visibleNumerator(metric: Metric | undefined): number | null {
  if (visibleMetricNumber(metric) == null) return null;
  const value = metric?.numerator;
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}

export function numberText(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "—" : value.toLocaleString("zh-CN");
}

export function percentageNumber(value: number): string {
  if (value > 0 && value < 0.1) return "<0.1";
  return value.toLocaleString("zh-CN", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

export function percentageText(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "—" : `${percentageNumber(value)}%`;
}

export function progressWidth(value: number | null): string {
  return `${value == null || !Number.isFinite(value) ? 0 : Math.min(100, Math.max(0, value))}%`;
}

function compactNumber(value: number): string {
  if (value >= 100_000_000) return `${(value / 100_000_000).toLocaleString("zh-CN", { maximumFractionDigits: 1 })} 亿`;
  if (value >= 10_000) return `${(value / 10_000).toLocaleString("zh-CN", { maximumFractionDigits: 1 })} 万`;
  return numberText(value);
}

export function ratioEvidence(metric: Metric, exposure: boolean): string | null {
  if (visibleMetricNumber(metric) == null || metric.numerator == null || metric.denominator == null) return null;
  return exposure
    ? `${compactNumber(metric.numerator)} / ${compactNumber(metric.denominator)} VV`
    : `${numberText(metric.numerator)} / ${numberText(metric.denominator)} 条`;
}

export function contentStructure(channel: OverviewChannel) {
  const total = channel.publication_count;
  const sellingMetric = channel.summary.metrics.selling_point_count_share;
  const coreMetric = channel.summary.metrics.core_selling_point_count_share;
  const selling = visibleNumerator(sellingMetric);
  const core = visibleNumerator(coreMetric);
  if (!Number.isInteger(total) || total <= 0 || selling == null || core == null
    || !Number.isInteger(selling) || !Number.isInteger(core)
    || selling > total || core > selling
    || sellingMetric.denominator !== total || coreMetric.denominator !== total) return null;
  return { total, selling, core, other: selling - core, rest: total - selling,
    sellingPercentage: selling * 100 / total, corePercentage: core * 100 / total };
}

export function sortedSellingPoints(items: OverviewSellingPoint[]) {
  const currentView = (item: OverviewSellingPoint) => item.view_count.status === "available"
    ? visibleMetricNumber(item.view_count) : null;
  const availableCount = items.filter((item) => currentView(item) != null).length;
  const sorted = [...items].sort((a, b) => {
    const av = currentView(a), bv = currentView(b);
    if (av != null && bv == null) return -1;
    if (av == null && bv != null) return 1;
    return (av != null && bv != null ? bv - av : 0)
      || b.publication_count - a.publication_count || a.code.localeCompare(b.code);
  });
  return { items: sorted, ordering: availableCount === 0 ? "按内容数排序"
    : availableCount === items.length ? "按累计 VV 排序" : "先看曝光可用项" };
}
