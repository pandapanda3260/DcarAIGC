"use client";

import Image from "next/image";
import { useEffect, useState } from "react";
import {
  CarIcon,
  ClockIcon,
  EyeIcon,
  RobotIcon,
  ShieldCheckIcon,
  SparkleIcon,
  StackIcon,
  StarIcon,
  TagIcon,
  TrendUpIcon,
  UserFocusIcon,
} from "@phosphor-icons/react";
import AppShell from "../components/AppShell";
import { Loading, Notice } from "../components/Feedback";
import { readJson } from "../lib/api";
import {
  formatDate,
  metricEvidence,
  metricPublishesValue,
  metricStatus,
  metricValue,
} from "../lib/format";
import { publicAssetPath } from "../lib/paths";
import type {
  BusinessSceneKey,
  ConclusionMetricKey,
  Metric,
  Overview,
  OverviewChannel,
  OverviewChannelKey,
  WindowKey,
} from "../lib/types";

const windowLabels: Record<WindowKey, string> = {
  yesterday: "昨天", this_week: "本周", last_week: "上周",
};
const channelOrder: OverviewChannelKey[] = ["douyin", "xiaohongshu"];
const sceneOrder: BusinessSceneKey[] = ["used_car", "new_car", "media"];
const conclusionMetrics: Array<[ConclusionMetricKey, string]> = [
  ["selling_point_count_share", "卖点条数占比"],
  ["core_selling_point_count_share", "核心卖点条数占比"],
  ["selling_point_exposure_share", "卖点曝光占比"],
  ["core_selling_point_exposure_share", "核心卖点曝光占比"],
  ["content_verticality", "内容垂直度"],
  ["automotive_user_rate", "互动用户汽车兴趣占比"],
  ["acquisition_potential", "内容拉新效果预估"],
];
const channelBrandAssets: Record<OverviewChannelKey, { src: string; label: string }> = {
  douyin: { src: publicAssetPath("/brand-douyin-tiktok.svg"), label: "抖音" },
  xiaohongshu: { src: publicAssetPath("/brand-xiaohongshu.svg"), label: "小红书" },
};
const metricIcons = {
  selling_point_count_share: TagIcon,
  core_selling_point_count_share: StarIcon,
  selling_point_exposure_share: EyeIcon,
  core_selling_point_exposure_share: SparkleIcon,
  content_verticality: StackIcon,
  automotive_user_rate: UserFocusIcon,
  acquisition_potential: TrendUpIcon,
} satisfies Record<ConclusionMetricKey, typeof TagIcon>;
const sceneIcons = {
  used_car: CarIcon,
  new_car: CarIcon,
  media: RobotIcon,
} satisfies Record<BusinessSceneKey, typeof CarIcon>;

function MetricIcon({ metricKey }: { metricKey: ConclusionMetricKey }) {
  const Icon = metricIcons[metricKey];
  return <Icon className="conclusion-metric-icon" size={16} weight="regular" aria-hidden="true" />;
}

function SceneIcon({ sceneKey }: { sceneKey: BusinessSceneKey }) {
  const Icon = sceneIcons[sceneKey];
  return <Icon className="scene-title-icon" size={18} weight="regular" aria-hidden="true" />;
}

function SummaryMetric({ metricKey, metric, label, reasonId }: { metricKey: ConclusionMetricKey; metric: Metric; label: string; reasonId: string }) {
  const evidence = metricEvidence(metric);
  const publishesValue = metricPublishesValue(metric);
  return <article className="conclusion-metric-card" aria-label={label} aria-describedby={publishesValue ? undefined : reasonId} title={evidence}>
    <div><span className="conclusion-metric-label"><MetricIcon metricKey={metricKey} /><span>{label}</span></span><em className={`metric-status ${metric.status === "available" ? "available" : "limited"}`}>{metricStatus(metric)}</em></div>
    <strong>{metricValue(metric)}</strong>
    {publishesValue ? <p>{evidence}</p> : <span id={reasonId} className="visually-hidden">{evidence}</span>}
  </article>;
}

function SceneConclusion({ channel, sceneKey }: { channel: OverviewChannel; sceneKey: BusinessSceneKey }) {
  const scene = channel.scenes[sceneKey];
  return <article className="scene-conclusion-card" data-scene={sceneKey}>
    <header><SceneIcon sceneKey={sceneKey} /><h5>{scene.label}</h5><span className="visually-hidden">{scene.publication_count} 条发布</span></header>
    <dl>
      {conclusionMetrics.map(([key, label]) => {
        const metric = scene.metrics[key];
        const evidence = metricEvidence(metric);
        const publishesValue = metricPublishesValue(metric);
        const reasonId = `metric-reason-${channel.platform}-${sceneKey}-${key}`;
        return <div className="scene-metric-row" key={key} aria-describedby={publishesValue ? undefined : reasonId} title={evidence}>
          <dt><MetricIcon metricKey={key} /><span>{label}</span></dt>
          <dd><strong>{metricValue(metric)}</strong><em className={`metric-status ${metric.status === "available" ? "available" : "limited"}`}>{metricStatus(metric)}</em>{!publishesValue && <span id={reasonId} className="visually-hidden">{evidence}</span>}</dd>
        </div>;
      })}
    </dl>
  </article>;
}

function ChannelConclusion({ channel, index }: { channel: OverviewChannel; index: number }) {
  const channelNumber = String(index + 1).padStart(2, "0");
  const brand = channelBrandAssets[channel.platform];
  return <section className="panel channel-conclusion" data-channel={channel.platform}>
    <div className="channel-conclusion-head">
      <span className="channel-number" aria-hidden="true">{channelNumber}</span>
      <div className="channel-heading-copy"><span className="eyebrow">CHANNEL {channelNumber}</span><div className="channel-title-row"><h3>{channel.label}渠道</h3><span className={`channel-platform-mark ${channel.platform}`} title={brand.label}><Image src={brand.src} alt="" width={17} height={17} unoptimized /></span></div><p>窗口发布 {channel.publication_count} 条 · 证据覆盖 {channel.evidence_coverage_percentage ?? "—"}% · 有效曝光内容 {channel.valid_exposure_items} 条 · 可归类曝光覆盖 {channel.exposure_coverage_percentage ?? "—"}%</p></div>
    </div>
    <div className="conclusion-subhead"><b>1</b><div><h4>汇总</h4><p>条数指标以该渠道窗口全部发布为分母；曝光分母仅含 view_count&gt;0 的有效曝光，未取得有效曝光不入分母。</p></div></div>
    <div className="conclusion-summary-grid">
      {conclusionMetrics.map(([key, label]) => <SummaryMetric key={key} metricKey={key} label={label} metric={channel.summary.metrics[key]} reasonId={`metric-reason-${channel.platform}-summary-${key}`} />)}
    </div>
    <div className="conclusion-subhead scene-subhead"><b>2</b><div><h4>三个业务场景</h4><p>固定顺序为二手车、新车、媒体-AI小懂；其他和未知内容只进入渠道汇总分母。</p></div></div>
    <div className="scene-conclusion-grid">
      {sceneOrder.map((sceneKey) => <SceneConclusion key={sceneKey} channel={channel} sceneKey={sceneKey} />)}
    </div>
  </section>;
}

export default function OverviewPage() {
  const [overview, setOverview] = useState<Overview | null>(null);
  const [windowKey, setWindowKey] = useState<WindowKey>("yesterday");
  const [error, setError] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    readJson<Overview>("/api/v8/overview", { signal: controller.signal })
      .then(setOverview)
      .catch((reason) => { if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : "概览读取失败"); });
    return () => controller.abort();
  }, []);

  const activeWindow = overview?.windows[windowKey];
  const windowSwitch = <div className="channel-switch" role="group" aria-label="统计窗口">
    {(Object.keys(windowLabels) as WindowKey[]).map((key) => <button key={key} type="button" aria-pressed={windowKey === key} className={windowKey === key ? "active" : ""} onClick={() => setWindowKey(key)}>{windowLabels[key]}</button>)}
  </div>;
  return (
    <AppShell active="overview" actions={windowSwitch}>
      {error && <Notice tone="error">读取失败：{error}</Notice>}
      {!overview && !error ? <Loading label="正在读取 v8 运营数据" /> : (
        <section className="page-stack overview-dashboard">
          <h2 className="visually-hidden">渠道结论</h2>
          <p className="visually-hidden" aria-live="polite">已切换到{windowLabels[windowKey]}，数据已更新</p>
          {activeWindow && channelOrder.map((key, index) => <ChannelConclusion key={key} channel={activeWindow.channels[key]} index={index} />)}
          <div className="overview-support-grid">
            <article className="panel overview-support-card boundary-card">
              <div className="support-card-title"><ClockIcon size={19} weight="regular" aria-hidden="true" /><h3>{windowLabels[windowKey]}统计边界</h3></div>
              <dl className="definition-list">
                <div><dt>开始</dt><dd>{activeWindow ? formatDate(activeWindow.period_start) : "—"}</dd></div>
                <div><dt>结束（不含）</dt><dd>{activeWindow ? formatDate(activeWindow.period_end) : "—"}</dd></div>
                <div><dt>窗口发布内容</dt><dd>{activeWindow?.metrics.publication_count?.value ?? "—"} 条</dd></div>
                <div><dt>V2 / V3 可评分内容</dt><dd>{activeWindow?.eligible_count ?? "—"} 条</dd></div>
                <div><dt>未关联账号内容</dt><dd>{activeWindow?.unassociated_content_count ?? "—"} 条</dd></div>
              </dl>
            </article>
            <article className="panel overview-support-card quality-card">
              <div className="support-card-title"><ShieldCheckIcon size={19} weight="regular" aria-hidden="true" /><div><h3>数据质量状态</h3><p>缺日期内容不进入任何日期窗口，复核与终态记录单独保留。</p></div></div>
              <div className="quality-grid">
                <div><strong>{overview?.data_quality.missing_published_at ?? "—"}</strong><span>缺失发布日期</span></div>
                <div><strong>{overview?.data_quality.pending_reviews ?? "—"}</strong><span>待复核 / 补证</span></div>
                <div><strong>{overview?.data_quality.terminal_reviews ?? "—"}</strong><span>终态不可用</span></div>
                <div><strong>{overview?.data_quality.duplicate_fingerprint_coverage ?? "—"}%</strong><span>重复指纹覆盖</span></div>
                <div><strong>{overview?.data_quality.confirmed_duplicate_count ?? "—"}</strong><span>确认重复内容</span></div>
                <div><strong>{overview?.data_quality.duplicate_calibration_ready ? "已通过" : "未通过"}</strong><span>150 对定标</span></div>
              </div>
            </article>
          </div>
        </section>
      )}
    </AppShell>
  );
}
