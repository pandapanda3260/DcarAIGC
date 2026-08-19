import type { Metric } from "./types";

export const platformKeys = ["douyin", "xiaohongshu", "wechat_channels", "kuaishou"];

const enumLabels: Record<string, string> = {
  douyin: "抖音", xiaohongshu: "小红书", wechat_channels: "视频号", kuaishou: "快手",
  boutique_ip: "精品 IP", original: "原创", mixed_edit: "混剪", unknown: "未知",
  new_car: "新车", used_car: "二手车", media: "媒体", other: "其他",
  content_explicit: "内容中直接提到", rule_prior: "系统按规则判断", llm: "系统智能判断", series: "车系", trim: "款型",
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
  ["用户身份覆盖率", "部分用户身份信息不足"],
  ["用户分类覆盖率", "用户分类未完成"],
  ["兴趣分类尚未运行", "系统还没完成用户分类"],
  ["去重有效用户", "互动用户少于 30 人"],
  ["分类器定标未通过", "用户分类结果还没完成校验"],
  ["用户级汽车兴趣占比尚未接入用户聚合", "暂时无法按用户汇总"],
  ["可归类有效曝光", "部分曝光还没完成分类"],
  ["感知指纹尚未完成定标", "重复内容识别规则还没完成校验"],
  ["评分证据门槛", "暂无可评分内容"],
];

const unavailableStatusLabels: Record<Metric["status"], string> = {
  available: "暂不可计算",
  below_threshold: "暂不显示",
  sample_only: "暂不可计算",
  not_applicable: "无适用内容",
  not_calculable: "暂时无法计算",
  missing: "暂无数据",
  stale: "数据需要更新",
};

const metricStatusLabels: Record<Metric["status"], string> = {
  available: "可用",
  below_threshold: "暂不显示",
  sample_only: "仅供参考",
  not_applicable: "所选时间内无相关内容",
  not_calculable: "暂时无法计算",
  missing: "暂无数据",
  stale: "需要更新",
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
  return metric.status === "sample_only" ? `${value}（仅供参考）` : value;
}

export function metricStatus(metric: Metric | undefined) {
  if (!metric) return "等待读取";
  return metricStatusLabels[metric.status];
}

export function plainMetricReason(value: string | null | undefined): string {
  let text = value?.trim() ?? "";
  if (!text) return "";
  const rules: ReadonlyArray<readonly [RegExp, string]> = [
    [/正式评估覆盖率为 ([\d.]+)%，低于 ([\d.]+)% 发布阈值/g, "卖点评估完成率为 $1%，低于至少 $2% 的要求"],
    [/曝光量快照覆盖率为 ([\d.]+)%，低于 ([\d.]+)% 发布阈值/g, "有曝光量的数据占 $1%，低于至少 $2% 的要求"],
    [/评论量快照覆盖率为 ([\d.]+)%，低于 ([\d.]+)% 发布阈值/g, "有评论数的数据占 $1%，低于至少 $2% 的要求"],
    [/固定采集截止点前 (\d+) 小时内的新鲜指标覆盖率为 ([\d.]+)%，低于 ([\d.]+)% 发布门槛/g, "截止统计前 $1 小时内有更新的数据占 $2%，低于至少 $3% 的要求"],
    [/报告窗口发现覆盖率为 ([\d.]+)%，低于 ([\d.]+)% 发布门槛/g, "计划采集完成率为 $1%，低于至少 $2% 的要求"],
    [/感知指纹覆盖率为 ([\d.]+)%，低于 ([\d.]+)% 发布阈值/g, "完成重复内容识别的数据占 $1%，低于至少 $2% 的要求"],
    [/分类器定标/g, "用户分类规则校验"],
    [/感知指纹/g, "重复内容识别"],
    [/用户级汽车兴趣占比尚未接入用户聚合/g, "暂时无法按用户汇总汽车兴趣占比"],
    [/可归类有效曝光/g, "已完成分类的曝光"],
    [/评分证据门槛/g, "可评估资料要求"],
    [/报告窗口/g, "报告期内"],
    [/发现覆盖/g, "计划采集完成率"],
    [/指标新鲜度/g, "播放和互动数据更新率"],
    [/详情覆盖/g, "内容详情完整率"],
    [/正式评估覆盖/g, "卖点评估完成率"],
    [/媒体处理终态覆盖/g, "视频和图片处理完成率"],
    [/重复指纹覆盖/g, "重复内容识别完成率"],
    [/周评论证据覆盖/g, "评论采集完成率"],
    [/统计口径/g, "计算方式"],
    [/分母/g, "统计总数"],
    [/发布阈值|发布门槛/g, "显示要求"],
  ];
  for (const [pattern, replacement] of rules) text = text.replace(pattern, replacement);
  return /[A-Za-z_]{3,}|定标|感知指纹|分类器|用户聚合|发布阈值|发布门槛|终态|快照/.test(text)
    ? "数据还不够完整，暂时无法显示结果。"
    : text;
}

export function metricEvidence(metric: Metric): string {
  const reason = plainMetricReason(metric.reason);
  if (
    metricPublishesValue(metric)
    && metric.kind === "ratio"
    && metric.numerator != null
    && metric.denominator != null
  ) {
    const ratio = `${numberFormat.format(metric.numerator)}/${numberFormat.format(metric.denominator)}`;
    return reason ? `${ratio} · ${reason}` : ratio;
  }
  if (metricPublishesValue(metric) && metric.kind === "score" && metric.total_items != null) {
    const coverage = `可评分 ${metric.scorable_items ?? 0}/${metric.total_items} 条`;
    return reason ? `${coverage} · ${reason}` : coverage;
  }
  return reason || "所选时间内还无法得出结果。";
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
  [/revision (\d+) invalidated from freeze manifest/g, "第 $1 版报告已作废。"],
  [/已由发现补跑后的新任务替代；revision (\d+) 仅供审计/g, "这份旧任务已被更新后的报告替代；第 $1 版仅用于保留记录。"],
  [/旧报告已由发现补跑后的新报告替代/g, "这份旧报告已被更新后的报告替代。"],
  [/历史泛化质量消息已回溯为具体门槛/g, "旧任务的数据说明已更新。"],
  [/正式报告已阻断：仍有 (\d+) 条灰区内容(?:或人工结论冲突)?未完成人工复核/g, "这份旧任务未生成报告，有 $1 条内容按旧规则无法判断。请重新生成报告。"],
  [/报告已生成；未达发布门槛：/g, "报告已生成，但以下数据未达到要求："],
  [/周评论证据覆盖/g, "评论采集完成率"],
  [/发现覆盖/g, "计划采集完成率"],
  [/指标新鲜度/g, "播放和互动数据更新率"],
  [/详情覆盖/g, "内容详情完整率"],
  [/正式评估覆盖/g, "卖点评估完成率"],
  [/媒体处理终态覆盖/g, "视频和图片处理完成率"],
  [/重复指纹覆盖/g, "重复内容识别完成率"],
  [/（门槛 ([\d.]+)%）/g, "（要求至少 $1%）"],
  [/已请求生成新 revision/g, "已请求重新生成报告"],
  [/等待生成新 revision/g, "等待重新生成报告"],
  [/已请求取消，等待当前安全点/g, "正在取消，当前步骤完成后会停止"],
  [/任务已在安全点取消/g, "任务已取消"],
  [/正在快照报告窗口内容/g, "正在整理报告日期内的内容"],
  [/正在汇总窗口指标与结论/g, "正在统计数据并生成结论"],
  [/正在写出报告文件/g, "正在生成报告文件"],
  [/正在校验并归档 revision/g, "正在检查并保存报告版本"],
];

export function humanizeTaskMessage(value: string | null | undefined): string {
  if (!value) return "";
  const text = legacyMessageRules.reduce((current, [pattern, replacement]) => current.replace(pattern, replacement), value);
  return /[A-Za-z_]{3,}/.test(text)
    ? "任务没有完成，请重新生成；如果仍然失败，请联系管理员。"
    : text;
}

export function taskWasSuperseded(message: string | null | undefined): boolean {
  return Boolean(message?.includes("已由发现补跑后的新任务替代"));
}

export function humanizeTaskStatus(status: string, message: string | null | undefined): string {
  if (taskWasSuperseded(message)) return "已被替代";
  return label(status);
}
