"use client";

import Link from "next/link";
import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import AppShell from "../components/AppShell";
import DateRangePicker, { shiftDays, todayInShanghai } from "../components/DateRangePicker";
import { Loading, Notice } from "../components/Feedback";
import { jsonRequest, readJson } from "../lib/api";
import { formatDate, humanizeTaskMessage, humanizeTaskStatus, label, taskWasSuperseded } from "../lib/format";
import { isGeneratingTaskStatus } from "../lib/queryContracts";
import { queryKeys, tasksListQueryOptions } from "../lib/queries";
import type { Task, TaskDetail } from "../lib/types";

// 报告生成跑在请求之外的后台线程里：创建接口立即返回排队中的任务，
// 列表卡片靠轮询任务读模型显示真实进度，用户不必停在某个页面等待。

function yesterday() { return shiftDays(todayInShanghai(), -1); }

function statusTone(status: string, message?: string | null) {
  if (taskWasSuperseded(message)) return "done";
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
  const generating = isGeneratingTaskStatus(task.task_status);
  const progress = Math.max(0, Math.min(100, Math.round(task.progress)));
  return <article className={generating ? "task-card generating" : "task-card"}>
    <div className="task-card-body">
      <div className="task-card-head"><h3>{task.name}</h3><span className={`status-badge ${statusTone(task.task_status, task.message)}`}>{humanizeTaskStatus(task.task_status, task.message)}</span><span className="task-card-type">{label(task.task_type)}</span></div>
      <p className="task-card-meta">{formatDate(task.period_start)} — {formatDate(task.period_end)}<span>任务编号：{task.id}</span></p>
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
  const [saving, setSaving] = useState(false);
  const [modal, setModal] = useState(false);
  const [createError, setCreateError] = useState("");
  const [retrying, setRetrying] = useState(false);
  const [form, setForm] = useState({ periodStart: yesterday(), periodEnd: yesterday(), name: "" });
  const queryClient = useQueryClient();
  const tasksQuery = useQuery(tasksListQueryOptions());
  const tasks = tasksQuery.data?.items ?? [];
  const generatingCount = tasks.filter((task) => isGeneratingTaskStatus(task.task_status)).length;
  const tasksReadFailed = tasksQuery.isLoadingError || retrying;

  function retryTasksRead() {
    if (retrying) return;
    setRetrying(true);
    void tasksQuery.refetch().finally(() => setRetrying(false));
  }

  function openModal() { setCreateError(""); setModal(true); }
  function closeModal() { if (!saving) { setCreateError(""); setModal(false); } }

  async function createTask() {
    if (saving) return;
    setSaving(true); setCreateError("");
    try {
      const task = await readJson<TaskDetail>("/api/v8/tasks", jsonRequest({ period_start: form.periodStart, period_end: form.periodEnd, name: form.name || null }));
      queryClient.setQueryData<{ items: Task[] }>(queryKeys.tasksList, (current) => ({
        items: [task, ...(current?.items ?? []).filter((item) => item.id !== task.id)],
      }));
      await queryClient.invalidateQueries({ queryKey: queryKeys.tasksList, exact: true });
      setModal(false);
    } catch (reason) { setCreateError(reason instanceof Error ? reason.message : "报告创建失败，请检查日期后重试。"); }
    finally { setSaving(false); }
  }

  return <AppShell active="tasks">
    {tasksQuery.isError && <Notice tone="error">{tasksQuery.data ? `数据刷新失败，当前显示上次数据。${tasksQuery.error instanceof Error ? tasksQuery.error.message : ""}` : tasksQuery.error instanceof Error ? tasksQuery.error.message : "任务读取失败"}</Notice>}
    {tasksQuery.isPending && !tasksQuery.data && !tasksReadFailed ? <Loading label="正在读取报告任务" /> : <section className="page-stack wide-stack">
      <div className="detail-toolbar"><div><span className="eyebrow">每次生成都会保留</span><h2>日报、周报与自定义报告</h2><p>报告包含开始和结束当天；重新生成会新增一个版本，旧版本仍会保留。</p></div><div className="placeholder-actions"><button className="primary small" onClick={openModal}>新建任务</button><span className="rule-chip">{tasksReadFailed ? "读取失败" : `共 ${tasks.length} 个任务${generatingCount > 0 ? ` · ${generatingCount} 个生成中` : ""}`}</span></div></div>
      <div className="task-card-list">{tasks.map((task) => <TaskCard key={task.id} task={task} />)}</div>
      {tasksReadFailed ? <article className="panel"><div className="empty-state"><strong>任务读取失败</strong><span>请检查网络后重新加载。</span><button type="button" className="secondary read-error-retry" disabled={retrying} onClick={retryTasksRead}>{retrying ? "正在重新加载…" : "重新加载"}</button></div></article> : tasksQuery.data && tasks.length === 0 && <article className="panel"><div className="empty-state"><strong>还没有报告任务</strong><span>新建后，系统会使用已经保存的数据生成报告。</span></div></article>}
    </section>}
    {modal && <div className="modal-backdrop" role="presentation"><section className="modal-panel range-modal" role="dialog" aria-modal="true" aria-label="新建报告任务">
      <div className="panel-head"><div><span className="eyebrow">自定义报告</span><h3>新建自定义报告</h3><p>只使用已有数据，不会调用收费的数据服务。</p></div><button className="modal-close" onClick={closeModal} disabled={saving} aria-label="关闭">×</button></div>
      <div className="modal-fields">
        <div className="span-two drp-block">
          <span>报告日期（包含开始和结束当天）</span>
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
