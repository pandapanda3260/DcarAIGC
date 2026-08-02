"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import AppShell from "../../components/AppShell";
import { Feedback, Loading } from "../../components/Feedback";
import { API_BASE, readJson } from "../../lib/api";
import { formatDate, label, metricValue } from "../../lib/format";
import type { ReportView, TaskDetail } from "../../lib/types";

const tabs = [["summary", "报告概览"], ["platforms", "平台维度"], ["dimensions", "账号 / 方向"], ["contents", "内容明细"], ["files", "文件与日志"]] as const;
const taskActionPaths = { retry: "/retry", cancel: "/cancel", resume: "/resume" } as const;

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
    const valid = next.revisions.find((item) => !item.invalidated_at);
    setReport(valid ? await readJson<ReportView>(`/api/v8/tasks/${taskId}/revisions/${valid.revision}/report`) : null);
  }
  useEffect(() => {
    readJson<TaskDetail>(`/api/v8/tasks/${taskId}`)
      .then(async (next) => {
        setDetail(next);
        const valid = next.revisions.find((item) => !item.invalidated_at);
        setReport(valid ? await readJson<ReportView>(`/api/v8/tasks/${taskId}/revisions/${valid.revision}/report`) : null);
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
  const retryable = detail && ["partial", "failed", "interrupted"].includes(detail.task_status);
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
        {tab === "summary" && <div className="task-tab-body"><div className="quality-grid"><div><strong>{report ? metricValue(report.summary_metrics.publication_count) : "—"}</strong><span>发布内容</span></div><div><strong>{report ? metricValue(report.summary_metrics.verticality_rate) : "—"}</strong><span>内容垂直度</span></div><div><strong>{report ? metricValue(report.summary_metrics.selling_point_coverage_rate) : "—"}</strong><span>卖点覆盖率</span></div></div><dl className="definition-list">{Object.entries(report?.data_quality ?? {}).map(([key, value]) => <div key={key}><dt>{key}</dt><dd>{value}</dd></div>)}</dl>{!report && <p className="empty-explanation">当前没有可读取的有效 revision。</p>}</div>}
        {tab === "platforms" && <div className="task-tab-body dimension-list">{report?.platform_dimensions.map((item) => <div key={String(item.key)}><strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span></div>)}</div>}
        {tab === "dimensions" && <div className="task-tab-body"><h4>账号类型</h4><div className="dimension-list">{report?.account_type_dimensions.map((item) => <div key={String(item.key)}><strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span></div>)}</div><h4>内容方向</h4><div className="dimension-list">{report?.content_direction_dimensions.map((item) => <div key={String(item.key)}><strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span></div>)}</div></div>}
        {tab === "contents" && <div className="task-tab-body table-scroll"><table><thead><tr><th>链接 ID</th><th>平台</th><th>标题</th><th>证据</th><th>垂直度</th></tr></thead><tbody>{report?.content_details.map((item) => <tr key={String(item.content_id)}><td>{item.link_id}</td><td>{label(String(item.platform))}</td><td>{item.title || "标题缺失"}</td><td>{item.evidence_level || "—"}</td><td>{item.content_automotive_score == null ? "暂不可计算" : `${item.content_automotive_score}%`}</td></tr>)}</tbody></table></div>}
        {tab === "files" && <div className="task-tab-body"><div className="download-list">{detail.revisions.map((revision) => <article key={revision.revision} className={revision.invalidated_at ? "invalidated-revision" : ""}><strong>Revision {revision.revision}{revision.invalidated_at ? "（已作废）" : ""}</strong><span>{formatDate(revision.created_at)}</span><div>{revision.files.map((file) => <a key={file.file_kind} href={`${API_BASE}/api/v8/tasks/${detail.id}/revisions/${revision.revision}/files/${file.file_kind}`} target="_blank" rel="noreferrer">{file.file_kind} · {new Intl.NumberFormat("zh-CN").format(file.byte_size)} B</a>)}</div></article>)}</div><h4>任务日志</h4><ol className="event-list">{detail.events.map((event) => <li key={event.id}><strong>{event.event_type}</strong><span>{event.message} · {formatDate(event.created_at)}</span></li>)}</ol></div>}
      </article>
    </section>}
  </AppShell>;
}
