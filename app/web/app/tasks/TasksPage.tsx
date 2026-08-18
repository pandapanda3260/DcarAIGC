"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import AppShell from "../components/AppShell";
import DateRangePicker, { shiftDays, todayInShanghai } from "../components/DateRangePicker";
import { Loading, Notice } from "../components/Feedback";
import { jsonRequest, readJson } from "../lib/api";
import { formatDate, humanizeTaskMessage, label } from "../lib/format";
import type { Task, TaskDetail } from "../lib/types";

// 报告生成跑在请求之外的后台线程里：创建接口立即返回排队中的任务，
// 列表卡片靠轮询任务读模型显示真实进度，用户不必停在某个页面等待。
const GENERATING_STATUSES = new Set(["queued", "running", "cancel_requested"]);
const POLL_INTERVAL_MS = 1500;

function yesterday() { return shiftDays(todayInShanghai(), -1); }

function statusTone(status: string) {
  if (status === "succeeded") return "done";
  if (status === "failed" || status === "interrupted" || status === "cancelled") return "terminal";
  return "pending";
}

function revisionText(task: Task) {
  if (task.current_valid_revision) return `第 ${task.current_valid_revision.revision} 版 · 当前`;
  if (task.stale_display_revision) return `第 ${task.stale_display_revision.revision} 版 · 已过时`;
  return "暂无有效版本";
}

function TaskCard({ task }: { task: Task }) {
  const generating = GENERATING_STATUSES.has(task.task_status);
  const progress = Math.max(0, Math.min(100, Math.round(task.progress)));
  return <article className={generating ? "task-card generating" : "task-card"}>
    <div className="task-card-body">
      <div className="task-card-head"><h3>{task.name}</h3><span className={`status-badge ${statusTone(task.task_status)}`}>{label(task.task_status)}</span><span className="task-card-type">{label(task.task_type)}</span></div>
      <p className="task-card-meta">{formatDate(task.period_start)} — {formatDate(task.period_end)}<span>{task.id}</span></p>
      {generating ? <div className="task-progress">
        <div><span>{humanizeTaskMessage(task.message) || "正在生成报告"}</span><em>{progress}%</em></div>
        <div className="progress-track" role="progressbar" aria-label="报告生成进度" aria-valuenow={progress} aria-valuemin={0} aria-valuemax={100}><i style={{ width: `${progress}%` }} /></div>
      </div> : <>
        <p className="task-card-facts"><span>内容 {task.content_count} 条</span><span>{revisionText(task)}</span><span>{task.historical_revision_count} 个历史版本</span></p>
        {task.message && <p className="task-card-note">{humanizeTaskMessage(task.message)}</p>}
      </>}
    </div>
    <Link className="secondary button-link" href={`/tasks/${task.id}`}>查看详情</Link>
  </article>;
}

export default function TasksPage() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [modal, setModal] = useState(false);
  const [error, setError] = useState("");
  const [createError, setCreateError] = useState("");
  const [form, setForm] = useState({ periodStart: yesterday(), periodEnd: yesterday(), name: "" });
  const generatingCount = tasks.filter((task) => GENERATING_STATUSES.has(task.task_status)).length;

  useEffect(() => {
    readJson<{ items: Task[] }>("/api/v8/tasks")
      .then((result) => setTasks(result.items))
      .catch((reason) => setError(reason instanceof Error ? reason.message : "任务读取失败"))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!generatingCount) return;
    let stopped = false;
    const timer = setInterval(() => {
      readJson<{ items: Task[] }>("/api/v8/tasks")
        .then((result) => { if (!stopped) setTasks(result.items); })
        .catch(() => { /* 生成仍在后台进行：保留上一次快照，等下一次轮询恢复 */ });
    }, POLL_INTERVAL_MS);
    return () => { stopped = true; clearInterval(timer); };
  }, [generatingCount]);

  function openModal() { setCreateError(""); setModal(true); }
  function closeModal() { if (!saving) { setCreateError(""); setModal(false); } }

  async function createTask() {
    if (saving) return;
    setSaving(true); setCreateError("");
    try {
      const task = await readJson<TaskDetail>("/api/v8/tasks", jsonRequest({ period_start: form.periodStart, period_end: form.periodEnd, name: form.name || null }));
      setTasks((current) => [task, ...current.filter((item) => item.id !== task.id)]);
      setModal(false);
    } catch (reason) { setCreateError(reason instanceof Error ? reason.message : "报告任务创建失败"); }
    finally { setSaving(false); }
  }

  return <AppShell active="tasks">
    {error && <Notice tone="error">{error}</Notice>}
    {loading ? <Loading label="正在读取报告任务" /> : <section className="page-stack wide-stack">
      <div className="detail-toolbar"><div><span className="eyebrow">版本留档 · 不可覆盖</span><h2>日报、周报与自定义报告</h2><p>报告按发布日期闭区间生成；每次重试新增一版报告，不覆盖历史产物。</p></div><div className="placeholder-actions"><button className="primary small" onClick={openModal}>新建任务</button><span className="rule-chip">共 {tasks.length} 个任务{generatingCount > 0 && ` · ${generatingCount} 个生成中`}</span></div></div>
      <div className="task-card-list">{tasks.map((task) => <TaskCard key={task.id} task={task} />)}</div>
      {tasks.length === 0 && <article className="panel"><div className="empty-state"><strong>还没有报告任务</strong><span>新建任务后会立即基于已落库数据生成。</span></div></article>}
    </section>}
    {modal && <div className="modal-backdrop" role="presentation"><section className="modal-panel range-modal" role="dialog" aria-modal="true" aria-label="新建报告任务">
      <div className="panel-head"><div><span className="eyebrow">自定义报告</span><h3>新建自定义报告</h3><p>生成报告不会隐式触发付费抓取。</p></div><button className="modal-close" onClick={closeModal} disabled={saving} aria-label="关闭">×</button></div>
      <div className="modal-fields">
        <div className="span-two drp-block">
          <span>报告区间（按发布日期，闭区间）</span>
          <DateRangePicker start={form.periodStart} end={form.periodEnd} disabled={saving} onChange={(periodStart, periodEnd) => setForm((current) => ({ ...current, periodStart, periodEnd }))} />
        </div>
        <label className="span-two">任务名称（可不填）<input value={form.name} disabled={saving} placeholder={`例如：${form.periodStart.slice(0, 7).replace("-", "年")}月报`} onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))} /></label>
      </div>
      {createError && <div className="modal-error" role="alert"><span>!</span>{createError}</div>}
      <div className="modal-actions">
        <span className="modal-progress">{saving ? <><span className="drp-spinner" aria-hidden="true" />正在创建任务</> : "任务创建后在列表卡片上显示生成进度，可以关掉弹窗做别的事"}</span>
        <button className="secondary" onClick={closeModal} disabled={saving}>取消</button>
        <button className="primary" disabled={saving} onClick={() => void createTask()}>{saving ? "创建中…" : "生成报告"}</button>
      </div>
    </section></div>}
  </AppShell>;
}
