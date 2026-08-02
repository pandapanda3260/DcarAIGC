import type { Metric } from "./types";

export const platformKeys = ["douyin", "xiaohongshu", "wechat_channels", "kuaishou"];

const enumLabels: Record<string, string> = {
  douyin: "抖音", xiaohongshu: "小红书", wechat_channels: "视频号", kuaishou: "快手",
  boutique_ip: "精品 IP", original: "原创", mixed_edit: "混剪", unknown: "未知",
  new_car: "新车", used_car: "二手车", media: "媒体", other: "其他",
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

export function metricValue(metric: Metric | undefined) {
  if (!metric) return "—";
  if (metric.kind === "ratio") {
    return metric.percentage == null ? "暂不可计算" : `${metric.percentage}%`;
  }
  return metric.value == null ? "暂不可计算" : new Intl.NumberFormat("zh-CN").format(metric.value);
}

export function metricStatus(metric: Metric | undefined) {
  if (!metric) return "等待读取";
  const labels: Record<string, string> = {
    available: "可用", below_threshold: "覆盖不足", sample_only: "仅样本",
    not_applicable: "窗口无适用内容", not_calculable: "暂无模型", missing: "缺失", stale: "已过期",
  };
  return labels[metric.status] ?? metric.status;
}
