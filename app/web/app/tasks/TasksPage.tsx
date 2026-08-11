"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import AppShell from "../components/AppShell";
import DateRangePicker, { shiftDays, todayInShanghai } from "../components/DateRangePicker";
import { Loading, Notice } from "../components/Feedback";
import { jsonRequest, readJson } from "../lib/api";
import { formatDate, label } from "../lib/format";
import type { Task, TaskDetail } from "../lib/types";

function yesterday() { return shiftDays(todayInShanghai(), -1); }

export default function TasksPage() {
  const router = useRouter();
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [modal, setModal] = useState(false);
  const [error, setError] = useState("");
  const [createError, setCreateError] = useState("");
  const [form, setForm] = useState({ periodStart: yesterday(), periodEnd: yesterday(), name: "" });

  useEffect(() => {
    readJson<{ items: Task[] }>("/api/v8/tasks")
      .then((result) => setTasks(result.items))
      .catch((reason) => setError(reason instanceof Error ? reason.message : "任务读取失败"))
      .finally(() => setLoading(false));
  }, []);

  function openModal() { setCreateError(""); setModal(true); }
  function closeModal() { if (!saving) { setCreateError(""); setModal(false); } }

  async function createTask() {
    if (saving) return;
    setSaving(true); setCreateError("");
    try {
      const task = await readJson<TaskDetail>("/api/v8/tasks", jsonRequest({ period_start: form.periodStart, period_end: form.periodEnd, name: form.name || null }));
      setModal(false); router.push(`/tasks/${task.id}`);
    } catch (reason) { setCreateError(reason instanceof Error ? reason.message : "报告任务创建失败"); }
    finally { setSaving(false); }
  }

  return <AppShell active="tasks">
    {error && <Notice tone="error">{error}</Notice>}
    {loading ? <Loading label="正在读取报告任务" /> : <section className="page-stack wide-stack">
      <div className="detail-toolbar"><div><span className="eyebrow">IMMUTABLE REVISIONS</span><h2>日报、周报与自定义报告</h2><p>报告按发布日期闭区间生成；每次重试新增 revision，不覆盖历史产物。</p></div><div className="placeholder-actions"><button className="primary small" onClick={openModal}>新建任务</button><span className="rule-chip">共 {tasks.length} 个任务</span></div></div>
      <article className="panel table-panel"><div className="table-scroll"><table><thead><tr><th>任务</th><th>类型</th><th>日期区间</th><th>状态</th><th>进度</th><th>内容数</th><th>Revision</th></tr></thead><tbody>
        {tasks.map((task) => <tr key={task.id}><td><Link href={`/tasks/${task.id}`}>{task.name}</Link><span>{task.id}</span></td><td>{label(task.task_type)}</td><td>{formatDate(task.period_start)} — {formatDate(task.period_end)}</td><td><span className={`status-badge ${task.task_status === "succeeded" ? "" : "pending"}`}>{label(task.task_status)}</span></td><td>{task.progress}%</td><td>{task.content_count}</td><td>{task.current_valid_revision ? `R${task.current_valid_revision.revision} 当前` : task.stale_display_revision ? `R${task.stale_display_revision.revision} 已过时` : "—"}<span>{task.historical_revision_count} 个历史 revision</span></td></tr>)}
      </tbody></table></div>{tasks.length === 0 && <div className="empty-state"><strong>还没有报告任务</strong><span>新建任务后会立即基于已落库数据生成。</span></div>}</article>
    </section>}
    {modal && <div className="modal-backdrop" role="presentation"><section className="review-modal range-modal" role="dialog" aria-modal="true" aria-label="新建报告任务">
      <div className="panel-head"><div><span className="eyebrow">CUSTOM REPORT</span><h3>新建自定义报告</h3><p>生成报告不会隐式触发付费抓取。</p></div><button className="modal-close" onClick={closeModal} disabled={saving} aria-label="关闭">×</button></div>
      <div className="review-fields">
        <div className="span-two drp-block">
          <span>报告区间（按发布日期，闭区间）</span>
          <DateRangePicker start={form.periodStart} end={form.periodEnd} disabled={saving} onChange={(periodStart, periodEnd) => setForm((current) => ({ ...current, periodStart, periodEnd }))} />
        </div>
        <label className="span-two">任务名称（可不填）<input value={form.name} disabled={saving} placeholder={`例如：${form.periodStart.slice(0, 7).replace("-", "年")}月报`} onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))} /></label>
      </div>
      {createError && <div className="modal-error" role="alert"><span>!</span>{createError}</div>}
      <div className="modal-actions">
        {saving && <span className="modal-progress"><span className="drp-spinner" aria-hidden="true" />正在生成报告，可能需要一段时间，请保持窗口打开</span>}
        <button className="secondary" onClick={closeModal} disabled={saving}>取消</button>
        <button className="primary" disabled={saving} onClick={() => void createTask()}>{saving ? "生成中…" : "生成报告"}</button>
      </div>
    </section></div>}
  </AppShell>;
}
