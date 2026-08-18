"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import AppShell from "../../components/AppShell";
import { Feedback, Loading } from "../../components/Feedback";
import { API_BASE, readJson } from "../../lib/api";
import {
  formatBytes,
  formatDate,
  formatDateTime,
  humanizeTaskMessage,
  label,
  metricCompactValue,
  metricEvidence,
  metricPublishesValue,
  metricValue,
} from "../../lib/format";
import type { BusinessSceneKey, ConclusionMetricKey, Metric, OverviewChannelKey, ReportView, TaskDetail } from "../../lib/types";

const tabs = [["summary", "报告概览"], ["platforms", "平台维度"], ["dimensions", "账号 / 方向"], ["contents", "内容明细"], ["files", "文件与日志"]] as const;
// 重新生成同样跑在后台：详情页轮询任务读模型，进度与阶段文案跟任务列表卡片一致。
const GENERATING_STATUSES = new Set(["queued", "running", "cancel_requested"]);
const POLL_INTERVAL_MS = 1500;
const taskActionPaths = { retry: "/retry", cancel: "/cancel", resume: "/resume" } as const;
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

// 数据质量检查的中文名与白话说明；名称与后端发布门槛文案（reports.py 的 _QUALITY_GATE_LABELS）保持一致，
// 接口字段名只留在代码里，不再直接摆到界面上。
const qualityCheckCopy: Record<string, { name: string; hint: string }> = {
  discovery_coverage: { name: "发现覆盖", hint: "报告期内计划的账号内容采集是否真的执行了" },
  detail_coverage: { name: "详情覆盖", hint: "窗口内容成功抓到标题、互动数等详情数据的比例" },
  metrics_freshness: { name: "指标新鲜度", hint: "播放、互动等指标在采集截止前更新过的比例" },
  evaluation_coverage: { name: "正式评估覆盖", hint: "窗口内容完成卖点评估的比例" },
  core_artifact_coverage: { name: "核心产物覆盖", hint: "视频转写、图片文字识别等核心证据的完成比例" },
  media_terminal_coverage: { name: "媒体处理终态覆盖", hint: "视频、图文素材处理全部有明确结果（成功或失败）的比例" },
  duplicate_fingerprint_coverage: { name: "重复指纹覆盖", hint: "用于识别重复内容的特征指纹计算完成比例" },
  duplicate_calibration_ready: { name: "重复识别定标", hint: "重复内容判定阈值是否已通过人工校验" },
  weekly_comment_coverage: { name: "周评论证据覆盖", hint: "周报窗口内容近一周评论证据的采集比例" },
};

// 报告产物与任务日志同样只给人看中文；接口里的 file_kind / event_type 保持英文不动。
const fileKindLabels: Record<string, string> = {
  "report-markdown": "报告正文 Markdown",
  "report-json": "报告数据 JSON",
  "content-csv": "内容明细表 CSV",
  "channel-csv": "渠道结论表 CSV",
  "summary-svg": "核心摘要图 SVG",
  "summary-png": "核心摘要图 PNG",
};
// 证据等级文案与内容页一致（ContentsPage 的 EVIDENCE_LEVEL_LABELS）。
const evidenceLevelLabels: Record<string, string> = {
  V3: "V3-信息完整",
  V2: "V2-媒体存在",
  V1: "V1-只有文字",
  V0: "V0-不可用",
};
const taskEventLabels: Record<string, string> = {
  created: "任务创建",
  started: "开始生成",
  completed: "生成完成",
  failed: "生成失败",
  retry_requested: "请求重新生成",
  cancel_requested: "请求取消",
  cancelled: "任务已取消",
  resumed: "任务恢复",
  interrupted: "生成中断",
};

function finiteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function formatPercentage(value: unknown): string | null {
  const percentage = finiteNumber(value);
  return percentage === null
    ? null
    : `${new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 2 }).format(percentage)}%`;
}

type QualityTone = "ok" | "warn" | "muted";
type QualityRow = { key: string; name: string; hint: string; tone: QualityTone; value: string; note?: string };

function qualityStatusTone(status: "available" | "below_threshold" | "not_applicable"): QualityTone {
  return status === "available" ? "ok" : status === "below_threshold" ? "warn" : "muted";
}

function genericQualityRow(key: string, value: unknown): QualityRow {
  const copy = qualityCheckCopy[key] ?? { name: key, hint: "" };
  if (typeof value === "boolean") return { key, ...copy, tone: value ? "ok" : "warn", value: value ? "已通过" : "未通过" };
  const percentage = formatPercentage(value);
  if (percentage !== null) {
    const number = finiteNumber(value);
    return { key, ...copy, tone: number !== null && number >= 100 ? "ok" : "warn", value: percentage };
  }
  return { key, ...copy, tone: "muted", value: "无适用内容" };
}

function discoveryCoverageRow(report: ReportView): QualityRow {
  const copy = qualityCheckCopy.discovery_coverage;
  const detail = report.data_quality_details?.discovery_coverage;
  if (!detail || typeof detail !== "object") {
    return genericQualityRow("discovery_coverage", report.data_quality.discovery_coverage);
  }
  const covered = finiteNumber(detail.covered_identity_occurrence_count);
  const eligible = finiteNumber(detail.eligible_identity_occurrence_count);
  const reason = typeof detail.reason === "string" && detail.reason.trim() ? detail.reason.trim() : "";
  const note = reason
    || (covered !== null && eligible !== null ? `计划内的账号采集执行 ${eligible} 次，实际覆盖 ${covered} 次` : undefined);
  return {
    key: "discovery_coverage",
    ...copy,
    tone: qualityStatusTone(detail.status),
    value: detail.status === "not_applicable" ? "无适用内容" : formatPercentage(detail.percentage) ?? "暂不可计算",
    note,
  };
}

function metricsFreshnessRow(report: ReportView): QualityRow {
  const copy = qualityCheckCopy.metrics_freshness;
  const detail = report.data_quality_details?.metrics_freshness;
  const cutoff = report.metadata?.collection_cutoff_at;
  const cutoffText = typeof cutoff === "string" && cutoff.trim() ? `采集截止 ${formatDateTime(cutoff)}` : "";
  if (!detail || typeof detail !== "object") {
    const row = genericQualityRow("metrics_freshness", report.data_quality.metrics_freshness);
    return cutoffText ? { ...row, note: cutoffText } : row;
  }
  const freshCount = finiteNumber(detail.fresh_count);
  const eligibleCount = finiteNumber(detail.eligible_count);
  const notApplicable = detail.status === "not_applicable" || eligibleCount === 0;
  const parts: string[] = [];
  if (!notApplicable && freshCount !== null && eligibleCount !== null) parts.push(`${freshCount}/${eligibleCount} 条内容在截止前有新数据`);
  if (!notApplicable && typeof detail.reason === "string" && detail.reason.trim()) parts.push(detail.reason.trim());
  if (cutoffText) parts.push(cutoffText);
  return {
    key: "metrics_freshness",
    ...copy,
    tone: notApplicable ? "muted" : qualityStatusTone(detail.status),
    value: notApplicable ? "无适用内容" : formatPercentage(detail.percentage) ?? "暂不可计算",
    note: parts.length ? parts.join(" · ") : undefined,
  };
}

function MetricText({ metric, compact = false }: { metric?: Metric; compact?: boolean }) {
  const text = compact ? metricCompactValue(metric) : metricValue(metric);
  return <>{text}{metric && !metricPublishesValue(metric) && <span className="visually-hidden">。完整原因：{metricEvidence(metric)}</span>}</>;
}

export default function TaskDetailPage({ taskId }: { taskId: string }) {
  const [detail, setDetail] = useState<TaskDetail | null>(null);
  const [report, setReport] = useState<ReportView | null>(null);
  const [tab, setTab] = useState<(typeof tabs)[number][0]>("summary");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const load = useCallback(async () => {
    const next = await readJson<TaskDetail>(`/api/v8/tasks/${taskId}`);
    setDetail(next);
    const display = next.display_effective_revision;
    setReport(display ? await readJson<ReportView>(`/api/v8/tasks/${taskId}/revisions/${display.revision}/report`) : null);
  }, [taskId]);
  useEffect(() => {
    let stopped = false;
    Promise.resolve().then(load).catch((reason) => { if (!stopped) setError(reason instanceof Error ? reason.message : "任务详情读取失败"); });
    return () => { stopped = true; };
  }, [load]);

  async function action(kind: "retry" | "cancel" | "resume") {
    setSaving(true); setError(""); setMessage("");
    try {
      await readJson(`/api/v8/tasks/${taskId}${taskActionPaths[kind]}`, { method: "POST" });
      await load();
      setMessage(kind === "cancel" ? "取消请求已记录" : kind === "resume" ? "任务已恢复，正在后台生成新版本" : "已开始在后台生成新版本");
    } catch (reason) { setError(reason instanceof Error ? reason.message : "任务操作失败"); }
    finally { setSaving(false); }
  }

  const generating = Boolean(detail && GENERATING_STATUSES.has(detail.task_status));
  const progress = detail ? Math.max(0, Math.min(100, Math.round(detail.progress))) : 0;
  useEffect(() => {
    if (!generating) return;
    const timer = setInterval(() => { void load().catch(() => { /* 后台仍在生成：保留上一次快照，等下一次轮询 */ }); }, POLL_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [generating, load]);
  const cancellable = detail && ["queued", "running", "partial", "failed", "interrupted", "cancel_requested"].includes(detail.task_status);
  const retryable = detail && ["succeeded", "partial", "failed", "interrupted"].includes(detail.task_status);
  const hasDiscoveryCoverage = Boolean(report && (
    Object.prototype.hasOwnProperty.call(report.data_quality, "discovery_coverage")
    || report.data_quality_details?.discovery_coverage
  ));
  const hasMetricsFreshness = Boolean(report && (
    Object.prototype.hasOwnProperty.call(report.data_quality, "metrics_freshness")
    || report.data_quality_details?.metrics_freshness
  ));
  const qualityRows: QualityRow[] = report ? [
    ...(hasDiscoveryCoverage ? [discoveryCoverageRow(report)] : []),
    ...(hasMetricsFreshness ? [metricsFreshnessRow(report)] : []),
    ...Object.entries(report.data_quality)
      .filter(([key]) => key !== "discovery_coverage" && key !== "metrics_freshness")
      // 周评论检查只对周报有实际意义：其他任务后端恒填 100%，这行占位不值得展示。
      .filter(([key]) => key !== "weekly_comment_coverage" || detail?.task_type === "weekly")
      .map(([key, value]) => genericQualityRow(key, value)),
  ] : [];
  return <AppShell active="tasks" actions={<Link className="secondary button-link" href="/tasks">返回任务</Link>}>
    <Feedback error={error} message={message} onClose={() => { setError(""); setMessage(""); }} />
    {!detail ? <Loading label="正在读取任务详情" /> : <section className="page-stack wide-stack">
      <div className="detail-toolbar"><div><span className="eyebrow">{detail.id}</span><h2>{detail.name}</h2><p>{formatDate(detail.period_start)} — {formatDate(detail.period_end)} · {label(detail.task_status)} · {humanizeTaskMessage(detail.message) || "无附加说明"}</p></div><div>
        {detail.display_effective_revision && <a className="primary button-link report-download-button" href={`${API_BASE}/api/v8/tasks/${encodeURIComponent(detail.id)}/revisions/${detail.display_effective_revision.revision}/download`} download title={`下载当前展示的第 ${detail.display_effective_revision.revision} 版报告图片和 Excel 明细`}>下载报告</a>}
        {retryable && <button className="secondary" disabled={saving} onClick={() => void action("retry")}>重新生成报告</button>}
        {cancellable && <button className="secondary danger-button" disabled={saving || detail.task_status === "cancel_requested"} onClick={() => void action("cancel")}>{detail.task_status === "cancel_requested" ? "取消中" : "取消任务"}</button>}
        {detail.task_status === "cancelled" && <button className="primary" disabled={saving} onClick={() => void action("resume")}>恢复任务</button>}
      </div></div>
      {generating && <article className="panel task-progress-panel"><div className="task-progress">
        <div><span>{humanizeTaskMessage(detail.message) || "正在生成报告"}</span><em>{progress}%</em></div>
        <div className="progress-track" role="progressbar" aria-label="报告生成进度" aria-valuenow={progress} aria-valuemin={0} aria-valuemax={100}><i style={{ width: `${progress}%` }} /></div>
      </div></article>}
      <article className="panel">
        <div className="task-tabs" role="tablist">{tabs.map(([id, name]) => <button key={id} className={tab === id ? "active" : ""} onClick={() => setTab(id)}>{name}</button>)}</div>
        {tab === "summary" && <div className="task-tab-body">{detail.display_effective_revision?.revision_state === "stale" && <p className="status-badge stale-evaluation">当前展示的是历史规则报告，已过时；请重新生成报告。</p>}<div className="quality-grid"><div><strong><MetricText metric={report?.summary_metrics.publication_count} /></strong><span>发布内容</span></div><div><strong><MetricText metric={report?.summary_metrics.verticality_rate} /></strong><span>内容垂直度</span></div><div><strong><MetricText metric={report?.summary_metrics.selling_point_coverage_rate} /></strong><span>卖点覆盖率</span></div><div><strong><MetricText metric={report?.summary_metrics.duplicate_rate} /></strong><span>重复内容率</span></div></div>
        {report && qualityRows.length > 0 && <>
          <h4 className="quality-check-title">数据质量检查</h4>
          <p className="quality-check-intro">发布报告前会核对以下数据是否齐全；有未达标项时，任务标记为“部分完成”，原因同时写在页面顶部的任务说明里。</p>
          <ul className="quality-check-list">{qualityRows.map((row) => <li key={row.key}>
            <div className="quality-check-copy"><strong>{row.name}</strong><span>{row.hint}</span></div>
            <div className="quality-check-result"><em className={`quality-check-value ${row.tone}`}>{row.value}</em>{row.note && <span>{row.note}</span>}</div>
          </li>)}</ul>
        </>}
        {!report && <p className="empty-explanation">当前没有可读取的有效报告版本。</p>}</div>}
        {tab === "platforms" && <div className="task-tab-body">
          {report?.channels ? <>
            {channelOrder.map((channelKey) => {
              const channel = report.channels?.[channelKey];
              if (!channel) return null;
              return <div key={channelKey} className="table-scroll channel-conclusion-table" data-channel={channelKey}>
                <h4>{channel.label}渠道 · 窗口发布 {channel.publication_count} 条</h4>
                <table>
                  <thead><tr><th scope="col">指标</th><th scope="col">汇总</th>{sceneOrder.map((sceneKey) => <th scope="col" key={sceneKey}>{channel.scenes[sceneKey]?.label ?? sceneKey}</th>)}</tr></thead>
                  <tbody>{conclusionMetrics.map(([metricKey, name]) => <tr key={metricKey}><th scope="row">{name}</th><td><MetricText metric={channel.summary.metrics[metricKey]} compact /></td>{sceneOrder.map((sceneKey) => <td key={sceneKey}><MetricText metric={channel.scenes[sceneKey]?.metrics[metricKey]} compact /></td>)}</tr>)}</tbody>
                </table>
              </div>;
            })}
            <p className="empty-explanation">互动用户汽车兴趣占比为用户级去重口径（汽车兴趣用户数 / 去重互动用户数），未达发布门槛的切片不显示比例；各切片用户量与覆盖率见「文件与日志」中的渠道结论表（channel_conclusions.csv）。</p>
          </> : <p className="empty-explanation">该版本报告生成于渠道结论上线前，没有渠道/场景结论；重新生成报告后可见。</p>}
          <h4>平台发布分布</h4>
          <div className="dimension-list">{report?.platform_dimensions.map((item) => <div key={String(item.key)}><strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span></div>)}</div>
        </div>}
        {tab === "dimensions" && <div className="task-tab-body"><h4>账号类型</h4><div className="dimension-list">{report?.account_type_dimensions.map((item) => <div key={String(item.key)}><strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span></div>)}</div><h4>内容方向</h4><div className="dimension-list">{report?.content_direction_dimensions.map((item) => <div key={String(item.key)}><strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span></div>)}</div></div>}
        {tab === "contents" && <div className="task-tab-body table-scroll"><table><thead><tr><th>链接 ID</th><th>平台</th><th>标题</th><th>证据等级</th><th>垂直度</th><th>重复提醒</th></tr></thead><tbody>{report?.content_details.map((item) => <tr key={String(item.content_id)}><td>{item.link_id}</td><td>{label(String(item.platform))}</td><td>{item.title || "标题缺失"}</td><td>{item.evidence_level ? evidenceLevelLabels[String(item.evidence_level)] ?? item.evidence_level : "—"}</td><td>{item.content_automotive_score == null ? "暂不可计算" : `${item.content_automotive_score}%`}</td><td>{item.duplicate_original_link_id || "—"}</td></tr>)}</tbody></table></div>}
        {tab === "files" && <div className="task-tab-body"><h4>报告文件</h4><div className="download-list">{detail.revisions.map((revision) => <article key={revision.revision} className={revision.invalidated_at ? "invalidated-revision" : ""}><strong>第 {revision.revision} 版{revision.invalidated_at ? "（已作废）" : revision.revision_state === "current" ? "（当前）" : revision.revision_state === "stale" ? "（历史规则，已过时）" : "（历史）"}</strong><span>{formatDateTime(revision.created_at)}</span><div>{revision.files.map((file) => <a key={file.file_kind} title={file.file_kind} href={`${API_BASE}/api/v8/tasks/${detail.id}/revisions/${revision.revision}/files/${file.file_kind}`} target="_blank" rel="noreferrer">{fileKindLabels[file.file_kind] ?? file.file_kind} · {formatBytes(file.byte_size)}</a>)}</div></article>)}</div><h4>任务日志</h4><ol className="event-list">{detail.events.map((event) => <li key={event.id}><strong>{taskEventLabels[event.event_type] ?? event.event_type}</strong><span>{humanizeTaskMessage(event.message)} · {formatDateTime(event.created_at)}</span></li>)}</ol></div>}
      </article>
    </section>}
  </AppShell>;
}
