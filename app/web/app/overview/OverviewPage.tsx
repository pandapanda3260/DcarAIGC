"use client";

import { useEffect, useState } from "react";
import AppShell from "../components/AppShell";
import { Loading, Notice } from "../components/Feedback";
import { readJson } from "../lib/api";
import { formatDate, metricStatus, metricValue } from "../lib/format";
import type { Overview, WindowKey } from "../lib/types";

const windowLabels: Record<WindowKey, string> = { yesterday: "昨天", this_week: "本周", last_week: "上周" };
const metricLabels: Array<[string, string]> = [
  ["publication_count", "发布内容"], ["active_account_count", "发布账号"],
  ["view_count", "阅读 / 播放"], ["comment_count", "评论数"],
  ["verticality_rate", "内容垂直度"], ["selling_point_coverage_rate", "卖点覆盖率"],
  ["estimated_new_users", "预估拉新量"], ["estimated_reactivated_users", "预估拉活量"],
  ["estimated_leads", "预估线索量"],
];

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
            <div><span className="eyebrow">LIVE DATABASE WINDOWS</span><h2>按发布日期统计，不混用抓取时间</h2><p>窗口采用 Asia/Shanghai；无适用内容时明确解释，不用 0 冒充结果。</p></div>
            <div className="channel-switch" aria-label="统计窗口">
              {(Object.keys(windowLabels) as WindowKey[]).map((key) => <button key={key} className={windowKey === key ? "active" : ""} onClick={() => setWindowKey(key)}>{windowLabels[key]}</button>)}
            </div>
          </div>
          <div className="metric-grid insight-metrics">
            {metricLabels.map(([key, name]) => {
              const metric = activeWindow?.metrics[key];
              return <article className="metric-card compact insight-metric" key={key}>
                <div className="metric-card-head"><span>{name}</span><span className={`metric-status ${metric?.status === "available" ? "available" : "limited"}`}>{metricStatus(metric)}</span></div>
                <strong className="insight-metric-value">{metricValue(metric)}</strong>
                <p>{metric?.reason || (metric?.coverage_percentage != null ? `数据覆盖 ${metric.coverage_percentage}%` : "以本窗口全部发布内容为分母")}</p>
              </article>;
            })}
          </div>
          <div className="two-column">
            <article className="panel">
              <div className="panel-head"><div><span className="eyebrow">WINDOW BOUNDARY</span><h3>{windowLabels[windowKey]}统计边界</h3></div></div>
              <dl className="definition-list">
                <div><dt>开始</dt><dd>{activeWindow ? formatDate(activeWindow.period_start) : "—"}</dd></div>
                <div><dt>结束（不含）</dt><dd>{activeWindow ? formatDate(activeWindow.period_end) : "—"}</dd></div>
                <div><dt>适用内容</dt><dd>{activeWindow?.eligible_count ?? "—"} 条</dd></div>
                <div><dt>未关联账号内容</dt><dd>{activeWindow?.unassociated_content_count ?? "—"} 条</dd></div>
              </dl>
              {activeWindow?.empty_explanation && <p className="empty-explanation">{activeWindow.empty_explanation}</p>}
            </article>
            <article className="panel">
              <div className="panel-head"><div><span className="eyebrow">DATA QUALITY</span><h3>数据质量状态</h3><p>缺日期内容不进入任何日期窗口，复核与终态记录单独保留。</p></div></div>
              <div className="quality-grid">
                <div><strong>{overview?.data_quality.missing_published_at ?? "—"}</strong><span>缺失发布日期</span></div>
                <div><strong>{overview?.data_quality.pending_reviews ?? "—"}</strong><span>待复核 / 补证</span></div>
                <div><strong>{overview?.data_quality.terminal_reviews ?? "—"}</strong><span>终态不可用</span></div>
              </div>
            </article>
          </div>
        </section>
      )}
    </AppShell>
  );
}
