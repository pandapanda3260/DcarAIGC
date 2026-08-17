import type { Metric } from "./types";

export const platformKeys = ["douyin", "xiaohongshu", "wechat_channels", "kuaishou"];

const enumLabels: Record<string, string> = {
  douyin: "抖音", xiaohongshu: "小红书", wechat_channels: "视频号", kuaishou: "快手",
  boutique_ip: "精品 IP", original: "原创", mixed_edit: "混剪", unknown: "未知",
  new_car: "新车", used_car: "二手车", media: "媒体", other: "其他",
  content_explicit: "内容显式", rule_prior: "规则推断", llm: "大模型补充", series: "车系", trim: "款型",
  yes: "是", no: "否", succeeded: "已完成", partial: "部分完成", failed: "失败",
  interrupted: "已中断", queued: "排队中", running: "运行中",
  cancel_requested: "取消中", cancelled: "已取消", daily: "日报", weekly: "周报", custom: "自定义",
};

export function label(value: string | null | undefined) {
  if (!value) return "—";
  return enumLabels[value] ?? value;
}

export function formatDate(value: string | null | undefined) {
  if (!value) return "缺失";
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit",
  }).format(new Date(value));
}

export function formatDateTime(value: string | null | undefined) {
  if (!value) return "缺失";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "缺失";
  const parts = new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
  }).formatToParts(date);
  const get = (type: Intl.DateTimeFormatPartTypes) => parts.find((part) => part.type === type)?.value ?? "";
  const day = `${get("year")}/${get("month")}/${get("day")}`;
  const hour = get("hour") === "24" ? "00" : get("hour");
  const minute = get("minute");
  const second = get("second");
  if (`${hour}${minute}${second}` === "000000") return day;
  if (second === "00") return `${day} ${hour}:${minute}`;
  return `${day} ${hour}:${minute}:${second}`;
}

const publishingStatuses = new Set<Metric["status"]>(["available", "sample_only"]);
const numberFormat = new Intl.NumberFormat("zh-CN");

const unavailableReasonLabels: ReadonlyArray<readonly [string, string]> = [
  ["未提供阅读数", "平台未提供阅读数"],
  ["没有 view_count > 0", "暂无有效曝光"],
  ["没有一级评论互动", "无评论互动"],
  ["评论尚未采集", "评论未采集"],
  ["无可识别的用户身份", "无可识别用户"],
  ["候选评论用户均为排除项", "暂无有效互动用户"],
  ["用户身份覆盖率", "身份数据待补齐"],
  ["用户分类覆盖率", "用户分类未完成"],
  ["兴趣分类尚未运行", "待运行分类器"],
  ["去重有效用户", "互动用户少于30人"],
  ["分类器定标未通过", "分类器未通过定标"],
  ["用户级汽车兴趣占比尚未接入用户聚合", "用户聚合未接入"],
  ["可归类有效曝光", "曝光归类待补齐"],
  ["感知指纹尚未完成定标", "重复识别待补齐"],
  ["评分证据门槛", "暂无可评分内容"],
];

const unavailableStatusLabels: Record<Metric["status"], string> = {
  available: "暂不可计算",
  below_threshold: "暂不发布",
  sample_only: "暂不可计算",
  not_applicable: "无适用内容",
  not_calculable: "暂无模型",
  missing: "无数据",
  stale: "数据已过期",
};

const metricStatusLabels: Record<Metric["status"], string> = {
  available: "可用",
  below_threshold: "暂不发布",
  sample_only: "仅样本",
  not_applicable: "窗口无适用内容",
  not_calculable: "暂无模型",
  missing: "缺失",
  stale: "已过期",
};

function publishedNumber(metric: Metric): number | null {
  if (metric.kind === "ratio") return metric.percentage ?? null;
  return metric.value ?? null;
}

function formattedPublishedValue(metric: Metric): string | null {
  if (!publishingStatuses.has(metric.status)) return null;
  const value = publishedNumber(metric);
  if (value == null) return null;
  return metric.kind === "quantity" ? numberFormat.format(value) : `${value}%`;
}

export function metricPublishesValue(metric: Metric | undefined): boolean {
  return metric != null && formattedPublishedValue(metric) != null;
}

export function metricUnavailableLabel(metric: Metric): string {
  const reason = metric.reason ?? "";
  for (const [needle, label] of unavailableReasonLabels) {
    if (reason.includes(needle)) return label;
  }
  return unavailableStatusLabels[metric.status];
}

export function metricValue(metric: Metric | undefined) {
  if (!metric) return "—";
  return formattedPublishedValue(metric) ?? metricUnavailableLabel(metric);
}

export function metricCompactValue(metric: Metric | undefined) {
  if (!metric) return "—";
  const value = formattedPublishedValue(metric);
  if (value == null) return metricUnavailableLabel(metric);
  return metric.status === "sample_only" ? `${value}（仅样本）` : value;
}

export function metricStatus(metric: Metric | undefined) {
  if (!metric) return "等待读取";
  return metricStatusLabels[metric.status];
}

export function metricEvidence(metric: Metric): string {
  if (
    metricPublishesValue(metric)
    && metric.kind === "ratio"
    && metric.numerator != null
    && metric.denominator != null
  ) {
    const ratio = `${numberFormat.format(metric.numerator)}/${numberFormat.format(metric.denominator)}`;
    return metric.reason ? `${ratio} · ${metric.reason}` : ratio;
  }
  if (metricPublishesValue(metric) && metric.kind === "score" && metric.total_items != null) {
    const coverage = `可评分 ${metric.scorable_items ?? 0}/${metric.total_items} 条`;
    return metric.reason ? `${coverage} · ${metric.reason}` : coverage;
  }
  return metric.reason || "当前窗口没有可发布的结论";
}

export function formatBytes(size: number): string {
  if (!Number.isFinite(size) || size < 0) return "—";
  if (size < 1024) return `${numberFormat.format(size)} 字节`;
  const kb = size / 1024;
  if (kb < 1024) return `${kb >= 100 ? Math.round(kb) : kb.toFixed(1)} KB`;
  return `${(kb / 1024).toFixed(1)} MB`;
}

// 老版本任务里存过带 revision 的英文事件文案；展示层统一翻成中文（新写入的事件已直接是中文）。
const legacyMessageRules: ReadonlyArray<readonly [RegExp, string]> = [
  [/revision (\d+) 已生成/g, "第 $1 版报告已生成"],
  [/已请求生成新 revision/g, "已请求重新生成报告"],
  [/等待生成新 revision/g, "等待重新生成报告"],
];

export function humanizeTaskMessage(value: string | null | undefined): string {
  if (!value) return "";
  return legacyMessageRules.reduce((text, [pattern, replacement]) => text.replace(pattern, replacement), value);
}
