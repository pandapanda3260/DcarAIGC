"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import AppShell from "../components/AppShell";
import { Loading, Notice } from "../components/Feedback";
import { jsonRequest, readJson } from "../lib/api";
import { formatDate, label } from "../lib/format";
import type { Task, TaskDetail } from "../lib/types";

function yesterday() { return new Date(Date.now() - 86_400_000).toLocaleDateString("en-CA", { timeZone: "Asia/Shanghai" }); }

export default function TasksPage() {
  const router = useRouter();
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [modal, setModal] = useState(false);
  const [error, setError] = useState("");
  const [form, setForm] = useState({ periodStart: yesterday(), periodEnd: yesterday(), name: "" });

  useEffect(() => {
    readJson<{ items: Task[] }>("/api/v8/tasks")
      .then((result) => setTasks(result.items))
      .catch((reason) => setError(reason instanceof Error ? reason.message : "任务读取失败"))
      .finally(() => setLoading(false));
  }, []);

  async function createTask() {
    setSaving(true); setError("");
    try {
      const task = await readJson<TaskDetail>("/api/v8/tasks", jsonRequest({ period_start: form.periodStart, period_end: form.periodEnd, name: form.name || null }));
      setModal(false); router.push(`/tasks/${task.id}`);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "报告任务创建失败"); }
    finally { setSaving(false); }
  }

  return <AppShell active="tasks" actions={<button className="primary small" onClick={() => setModal(true)}>新建任务</button>}>
    {error && <Notice tone="error">{error}</Notice>}
    {loading ? <Loading label="正在读取报告任务" /> : <section className="page-stack wide-stack">
      <div className="detail-toolbar"><div><span className="eyebrow">IMMUTABLE REVISIONS</span><h2>日报、周报与自定义报告</h2><p>报告按发布日期闭区间生成；每次重试新增 revision，不覆盖历史产物。</p></div><span className="rule-chip">共 {tasks.length} 个任务</span></div>
      <article className="panel table-panel"><div className="table-scroll"><table><thead><tr><th>任务</th><th>类型</th><th>日期区间</th><th>状态</th><th>进度</th><th>内容数</th><th>Revision</th></tr></thead><tbody>
        {tasks.map((task) => <tr key={task.id}><td><Link href={`/tasks/${task.id}`}>{task.name}</Link><span>{task.id}</span></td><td>{label(task.task_type)}</td><td>{formatDate(task.period_start)} — {formatDate(task.period_end)}</td><td><span className={`status-badge ${task.task_status === "succeeded" ? "" : "pending"}`}>{label(task.task_status)}</span></td><td>{task.progress}%</td><td>{task.content_count}</td><td>{task.revision_count}</td></tr>)}
      </tbody></table></div>{tasks.length === 0 && <div className="empty-state"><strong>还没有报告任务</strong><span>新建任务后会立即基于已落库数据生成。</span></div>}</article>
    </section>}
    {modal && <div className="modal-backdrop" role="presentation"><section className="review-modal compact-modal" role="dialog" aria-modal="true" aria-label="新建报告任务">
      <div className="panel-head"><div><span className="eyebrow">CUSTOM REPORT</span><h3>新建自定义报告</h3><p>生成报告不会隐式触发付费抓取。</p></div><button className="modal-close" onClick={() => setModal(false)} aria-label="关闭">×</button></div>
      <div className="review-fields"><label>开始日期<input type="date" value={form.periodStart} onChange={(event) => setForm({ ...form, periodStart: event.target.value })} /></label><label>结束日期<input type="date" value={form.periodEnd} onChange={(event) => setForm({ ...form, periodEnd: event.target.value })} /></label><label className="span-two">任务名称（可不填）<input value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} /></label></div>
      <div className="modal-actions"><button className="secondary" onClick={() => setModal(false)}>取消</button><button className="primary" disabled={saving} onClick={() => void createTask()}>{saving ? "生成中" : "生成报告"}</button></div>
    </section></div>}
  </AppShell>;
}
