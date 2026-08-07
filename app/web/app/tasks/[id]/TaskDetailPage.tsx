"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import AppShell from "../../components/AppShell";
import { Feedback, Loading } from "../../components/Feedback";
import { API_BASE, readJson } from "../../lib/api";
import {
  formatDate,
  label,
  metricCompactValue,
  metricEvidence,
  metricPublishesValue,
  metricValue,
} from "../../lib/format";
import type { BusinessSceneKey, ConclusionMetricKey, Metric, OverviewChannelKey, ReportView, TaskDetail } from "../../lib/types";

const tabs = [["summary", "报告概览"], ["platforms", "平台维度"], ["dimensions", "账号 / 方向"], ["contents", "内容明细"], ["files", "文件与日志"]] as const;
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

  async function load() {
    const next = await readJson<TaskDetail>(`/api/v8/tasks/${taskId}`);
    setDetail(next);
    const display = next.display_effective_revision;
    setReport(display ? await readJson<ReportView>(`/api/v8/tasks/${taskId}/revisions/${display.revision}/report`) : null);
  }
  useEffect(() => {
    readJson<TaskDetail>(`/api/v8/tasks/${taskId}`)
      .then(async (next) => {
        setDetail(next);
        const display = next.display_effective_revision;
        setReport(display ? await readJson<ReportView>(`/api/v8/tasks/${taskId}/revisions/${display.revision}/report`) : null);
      })
      .catch((reason) => setError(reason instanceof Error ? reason.message : "任务详情读取失败"));
  }, [taskId]);

  async function action(kind: "retry" | "cancel" | "resume") {
    setSaving(true); setError(""); setMessage("");
    try {
      await readJson(`/api/v8/tasks/${taskId}${taskActionPaths[kind]}`, { method: "POST" });
      await load();
      setMessage(kind === "cancel" ? "取消请求已记录" : kind === "resume" ? "任务已恢复并生成新 revision" : "任务已生成新 revision");
    } catch (reason) { setError(reason instanceof Error ? reason.message : "任务操作失败"); }
    finally { setSaving(false); }
  }

  const cancellable = detail && ["queued", "running", "partial", "failed", "interrupted", "cancel_requested"].includes(detail.task_status);
  const retryable = detail && ["succeeded", "partial", "failed", "interrupted"].includes(detail.task_status);
  return <AppShell active="tasks" actions={<Link className="secondary button-link" href="/tasks">返回任务</Link>}>
    <Feedback error={error} message={message} onClose={() => { setError(""); setMessage(""); }} />
    {!detail ? <Loading label="正在读取任务详情" /> : <section className="page-stack wide-stack">
      <div className="detail-toolbar"><div><span className="eyebrow">{detail.id}</span><h2>{detail.name}</h2><p>{formatDate(detail.period_start)} — {formatDate(detail.period_end)} · {label(detail.task_status)} · {detail.message || "无附加说明"}</p></div><div>
        {retryable && <button className="secondary" disabled={saving} onClick={() => void action("retry")}>生成新 revision</button>}
        {cancellable && <button className="secondary danger-button" disabled={saving || detail.task_status === "cancel_requested"} onClick={() => void action("cancel")}>{detail.task_status === "cancel_requested" ? "取消中" : "取消任务"}</button>}
        {detail.task_status === "cancelled" && <button className="primary" disabled={saving} onClick={() => void action("resume")}>恢复任务</button>}
      </div></div>
      <article className="panel">
        <div className="task-tabs" role="tablist">{tabs.map(([id, name]) => <button key={id} className={tab === id ? "active" : ""} onClick={() => setTab(id)}>{name}</button>)}</div>
        {tab === "summary" && <div className="task-tab-body">{detail.display_effective_revision?.revision_state === "stale" && <p className="status-badge stale-evaluation">当前展示的是历史规则报告，已过时；请生成新 revision。</p>}<div className="quality-grid"><div><strong><MetricText metric={report?.summary_metrics.publication_count} /></strong><span>发布内容</span></div><div><strong><MetricText metric={report?.summary_metrics.verticality_rate} /></strong><span>内容垂直度</span></div><div><strong><MetricText metric={report?.summary_metrics.selling_point_coverage_rate} /></strong><span>卖点覆盖率</span></div><div><strong><MetricText metric={report?.summary_metrics.duplicate_rate} /></strong><span>重复内容率</span></div></div><dl className="definition-list">{Object.entries(report?.data_quality ?? {}).map(([key, value]) => <div key={key}><dt>{key}</dt><dd>{value}</dd></div>)}</dl>{!report && <p className="empty-explanation">当前没有可读取的有效 revision。</p>}</div>}
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
            <p className="empty-explanation">互动用户汽车兴趣占比为用户级去重口径（汽车兴趣用户数 / 去重互动用户数），未达发布门槛的切片不显示比例；各切片用户量与覆盖率见 channel_conclusions.csv。</p>
          </> : <p className="empty-explanation">该 revision 生成于渠道结论上线前，没有渠道/场景结论；生成新 revision 后可见。</p>}
          <h4>平台发布分布</h4>
          <div className="dimension-list">{report?.platform_dimensions.map((item) => <div key={String(item.key)}><strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span></div>)}</div>
        </div>}
        {tab === "dimensions" && <div className="task-tab-body"><h4>账号类型</h4><div className="dimension-list">{report?.account_type_dimensions.map((item) => <div key={String(item.key)}><strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span></div>)}</div><h4>内容方向</h4><div className="dimension-list">{report?.content_direction_dimensions.map((item) => <div key={String(item.key)}><strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span></div>)}</div></div>}
        {tab === "contents" && <div className="task-tab-body table-scroll"><table><thead><tr><th>链接 ID</th><th>平台</th><th>标题</th><th>证据</th><th>垂直度</th><th>重复提醒</th></tr></thead><tbody>{report?.content_details.map((item) => <tr key={String(item.content_id)}><td>{item.link_id}</td><td>{label(String(item.platform))}</td><td>{item.title || "标题缺失"}</td><td>{item.evidence_level || "—"}</td><td>{item.content_automotive_score == null ? "暂不可计算" : `${item.content_automotive_score}%`}</td><td>{item.duplicate_original_link_id || "—"}</td></tr>)}</tbody></table></div>}
        {tab === "files" && <div className="task-tab-body"><div className="download-list">{detail.revisions.map((revision) => <article key={revision.revision} className={revision.invalidated_at ? "invalidated-revision" : ""}><strong>Revision {revision.revision}{revision.invalidated_at ? "（已作废）" : revision.revision_state === "current" ? "（当前）" : revision.revision_state === "stale" ? "（历史规则，已过时）" : "（历史）"}</strong><span>{formatDate(revision.created_at)}</span><div>{revision.files.map((file) => <a key={file.file_kind} href={`${API_BASE}/api/v8/tasks/${detail.id}/revisions/${revision.revision}/files/${file.file_kind}`} target="_blank" rel="noreferrer">{file.file_kind} · {new Intl.NumberFormat("zh-CN").format(file.byte_size)} B</a>)}</div></article>)}</div><h4>任务日志</h4><ol className="event-list">{detail.events.map((event) => <li key={event.id}><strong>{event.event_type}</strong><span>{event.message} · {formatDate(event.created_at)}</span></li>)}</ol></div>}
      </article>
    </section>}
  </AppShell>;
}
