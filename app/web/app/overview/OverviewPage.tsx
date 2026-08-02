"use client";

import { useEffect, useState } from "react";
import AppShell from "../components/AppShell";
import { Loading, Notice } from "../components/Feedback";
import { readJson } from "../lib/api";
import { formatDate, metricStatus, metricValue } from "../lib/format";
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
  ["audience_verticality", "互动用户垂直度"],
  ["acquisition_potential", "内容拉新效果预估"],
];
const operationalMetrics: Array<[string, string]> = [
  ["publication_count", "发布内容"], ["active_account_count", "发布账号"],
  ["view_count", "阅读 / 播放"], ["comment_count", "评论数"],
  ["duplicate_rate", "重复内容率"],
  ["estimated_new_users", "预估拉新量"],
  ["estimated_reactivated_users", "预估拉活量"],
  ["estimated_leads", "预估线索量"],
];
const numberFormat = new Intl.NumberFormat("zh-CN");

function conclusionValue(metric: Metric) {
  if (metric.kind === "score") {
    return metric.value == null ? "暂不可计算" : `${metric.value}%`;
  }
  return metricValue(metric);
}

function metricEvidence(metric: Metric) {
  if (metric.kind === "ratio" && metric.numerator != null && metric.denominator != null) {
    return `${numberFormat.format(metric.numerator)}/${numberFormat.format(metric.denominator)} · ${metric.reason}`;
  }
  if (metric.kind === "score" && metric.total_items != null) {
    const coverage = `可评分 ${metric.scorable_items ?? 0}/${metric.total_items} 条`;
    return metric.reason ? `${coverage} · ${metric.reason}` : coverage;
  }
  return metric.reason || "当前窗口没有可发布的结论";
}

function SummaryMetric({ metric, label }: { metric: Metric; label: string }) {
  return <article className="conclusion-metric-card">
    <div><span>{label}</span><em className={`metric-status ${metric.status === "available" ? "available" : "limited"}`}>{metricStatus(metric)}</em></div>
    <strong>{conclusionValue(metric)}</strong>
    <p>{metricEvidence(metric)}</p>
  </article>;
}

function SceneConclusion({ channel, sceneKey }: { channel: OverviewChannel; sceneKey: BusinessSceneKey }) {
  const scene = channel.scenes[sceneKey];
  return <article className="scene-conclusion-card">
    <header><div><span>BUSINESS SCENE</span><h5>{scene.label}</h5></div><strong>{scene.publication_count}<small>条发布</small></strong></header>
    <dl>
      {conclusionMetrics.map(([key, label]) => {
        const metric = scene.metrics[key];
        return <div key={key}>
          <dt><span>{label}</span><em className={`metric-status ${metric.status === "available" ? "available" : "limited"}`}>{metricStatus(metric)}</em></dt>
          <dd><strong>{conclusionValue(metric)}</strong><small>{metricEvidence(metric)}</small></dd>
        </div>;
      })}
    </dl>
  </article>;
}

function ChannelConclusion({ channel, index }: { channel: OverviewChannel; index: number }) {
  return <section className="panel channel-conclusion">
    <div className="channel-conclusion-head">
      <div><span className="eyebrow">CHANNEL {String(index + 1).padStart(2, "0")}</span><h3>【{channel.label}渠道】</h3><p>窗口发布 {channel.publication_count} 条 · 证据覆盖 {channel.evidence_coverage_percentage ?? "—"}% · 有效曝光内容 {channel.valid_exposure_items} 条 · 曝光交叉覆盖 {channel.exposure_coverage_percentage ?? "—"}%</p></div>
    </div>
    <div className="conclusion-subhead"><b>1</b><div><h4>汇总</h4><p>条数指标以该渠道窗口全部发布为分母；曝光指标以该渠道有效总曝光为分母。</p></div></div>
    <div className="conclusion-summary-grid">
      {conclusionMetrics.map(([key, label]) => <SummaryMetric key={key} label={label} metric={channel.summary.metrics[key]} />)}
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
  return (
    <AppShell active="overview">
      {error && <Notice tone="error">读取失败：{error}</Notice>}
      {!overview && !error ? <Loading label="正在读取 v8 运营数据" /> : (
        <section className="page-stack">
          <div className="section-heading dashboard-heading">
            <div><span className="eyebrow">CHANNEL CONCLUSIONS BY WINDOW</span><h2>时间窗口筛选，渠道结构保持不变</h2><p>每个窗口固定输出抖音与小红书，并依次展示渠道汇总和三个业务场景的七项结论。</p></div>
            <div className="channel-switch" aria-label="统计窗口">
              {(Object.keys(windowLabels) as WindowKey[]).map((key) => <button key={key} className={windowKey === key ? "active" : ""} onClick={() => setWindowKey(key)}>{windowLabels[key]}</button>)}
            </div>
          </div>
          {activeWindow?.empty_explanation && <p className="empty-explanation window-empty-explanation">{activeWindow.empty_explanation}以下仍保留完整渠道与场景结构。</p>}
          {activeWindow && channelOrder.map((key, index) => <ChannelConclusion key={key} channel={activeWindow.channels[key]} index={index} />)}
          <details className="panel operational-details">
            <summary>运营补充指标（不属于上述七项结论）</summary>
            <div className="metric-grid insight-metrics">
              {operationalMetrics.map(([key, name]) => {
                const metric = activeWindow?.metrics[key];
                return <article className="metric-card compact insight-metric" key={key}>
                  <div className="metric-card-head"><span>{name}</span><span className={`metric-status ${metric?.status === "available" ? "available" : "limited"}`}>{metricStatus(metric)}</span></div>
                  <strong className="insight-metric-value">{metricValue(metric)}</strong>
                  <p>{metric?.reason || (metric?.coverage_percentage != null ? `数据覆盖 ${metric.coverage_percentage}%` : "按当前窗口独立统计")}</p>
                </article>;
              })}
            </div>
          </details>
          <div className="two-column">
            <article className="panel">
              <div className="panel-head"><div><span className="eyebrow">WINDOW BOUNDARY</span><h3>{windowLabels[windowKey]}统计边界</h3></div></div>
              <dl className="definition-list">
                <div><dt>开始</dt><dd>{activeWindow ? formatDate(activeWindow.period_start) : "—"}</dd></div>
                <div><dt>结束（不含）</dt><dd>{activeWindow ? formatDate(activeWindow.period_end) : "—"}</dd></div>
                <div><dt>窗口发布内容</dt><dd>{activeWindow?.metrics.publication_count?.value ?? "—"} 条</dd></div>
                <div><dt>V2 / V3 可评分内容</dt><dd>{activeWindow?.eligible_count ?? "—"} 条</dd></div>
                <div><dt>未关联账号内容</dt><dd>{activeWindow?.unassociated_content_count ?? "—"} 条</dd></div>
              </dl>
            </article>
            <article className="panel">
              <div className="panel-head"><div><span className="eyebrow">DATA QUALITY</span><h3>数据质量状态</h3><p>缺日期内容不进入任何日期窗口，复核与终态记录单独保留。</p></div></div>
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
