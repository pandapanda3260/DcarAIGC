"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import ContentDialog from "../contents/ContentDialog";
import { contentUpdateFeedback } from "../contents/contentUpdate";
import { formatJobElapsed, isActiveContentUpdateJob, type ContentUpdateJob, type PendingContentUpdate } from "../contents/contentUpdateJobs";
import styles from "./ContentUpdateRecords.module.css";

const activeLabels = { queued: "等待更新", running: "正在更新" };

function resultStatus(job: ContentUpdateJob, feedbackError?: string) {
  if (job.error_code === "result_uncertain") return { label: "结果待确认", state: "uncertain" };
  if (job.status === "queued" || job.status === "running") return { label: activeLabels[job.status], state: job.status };
  if (job.status === "failed" || job.result?.status === "failed") return { label: "未完成", state: "failed" };
  if (!job.result || feedbackError) return { label: "结果待确认", state: "uncertain" };
  const result = job.result;
  const partial = result.status === "partial" || result.metrics?.status === "partial"
    || Boolean(result.metrics?.missing_fields?.length) || result.stages?.some((stage) => stage.status === "failed")
    || (result.media && result.media.status !== "evidence_ready");
  return partial ? { label: "部分更新", state: "partial" } : { label: "已完成", state: "succeeded" };
}

export default function ContentUpdateRecords({ jobs, pending, reading, readError, onRefresh, onRetrySubmission, onClose, onViewContent, closeLabel = "关闭" }: {
  jobs: ContentUpdateJob[];
  pending: PendingContentUpdate[];
  reading: boolean;
  readError: string;
  onRefresh: () => void;
  onRetrySubmission: (contentId: number) => void;
  onClose: () => void;
  onViewContent: () => void;
  closeLabel?: string;
}) {
  const [now, setNow] = useState(Date.now);
  const hasActiveRequests = jobs.some(isActiveContentUpdateJob) || pending.some((request) => request.status === "submitting");
  useEffect(() => {
    if (!hasActiveRequests) return;
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [hasActiveRequests]);

  return <ContentDialog title="更新记录" subtitle="进度自动更新，关闭弹窗后更新仍会继续。" onClose={onClose} footer={<>
    <button type="button" className="secondary" disabled={reading} onClick={onRefresh}>{reading ? "正在读取…" : "刷新状态"}</button>
    <button type="button" className="primary" onClick={onClose}>{closeLabel}</button>
  </>}>
    <div id="content-update-records" className={styles.records}>
      {readError && <p className={styles.readError} role="alert">状态暂时无法读取。已有任务可能仍在执行，读取重试不会重新提交更新。{readError}</p>}
      {!jobs.length && !pending.length && <p className={styles.empty}>{reading ? "正在读取更新记录…" : "暂无更新记录"}</p>}
      {pending.map((request) => <article key={request.requestId} className={styles.record}>
        <div className={styles.content}>
          <h3>{request.title || `内容 ${request.contentId}`}</h3>
          <p className={request.status === "submitting" ? styles.muted : styles.warning}>{request.status === "submitting"
            ? "正在等待任务回执，可以继续浏览其他内容。"
            : request.status === "uncertain"
              ? "等待超时不代表未入队。请先刷新状态；确认原请求会沿用同一个提交标识。"
              : request.error || "提交未成功，可重试原请求。"}</p>
          {request.status === "uncertain" && request.error && <p className={styles.muted}>{request.error}</p>}
        </div>
        <div className={styles.status}><span className={styles.badge} data-state={request.status}>{request.status === "submitting" ? "正在提交" : request.status === "uncertain" ? "提交结果待确认" : "提交未成功"}</span></div>
        <div className={styles.elapsed}>{request.status === "submitting" ? <time>{formatJobElapsed({ created_at: request.createdAt, completed_at: null }, now)}</time> : <span>{request.status === "uncertain" ? "回执待确认" : "尚未开始"}</span>}</div>
        <div className={styles.actions}>{request.status !== "submitting" && <button type="button" className="secondary" onClick={() => onRetrySubmission(request.contentId)}>{request.status === "uncertain" ? "确认原请求" : "重试原请求"}</button>}</div>
      </article>)}
      {jobs.map((job) => {
        const active = isActiveContentUpdateJob(job);
        const feedback = job.result ? contentUpdateFeedback(job.result) : null;
        const status = resultStatus(job, feedback?.error);
        const resultText = job.status === "failed" ? job.error || feedback?.error || feedback?.message : feedback?.error || feedback?.message || job.error;
        return <article key={job.id} className={styles.record}>
          <div className={styles.content}>
            <h3>{job.title || `内容 ${job.content_id}`}</h3>
            {active
              ? <p className={styles.muted}>可以继续浏览其他内容，结果会保留在这里。</p>
              : <p className={status.state === "failed" || status.state === "uncertain" || status.state === "partial" ? styles.warning : styles.result}>{resultText || "任务已结束，结果详情暂未返回，请刷新状态。"}{job.error_code === "result_uncertain" && " 此状态不会自动重试付费更新。"}</p>}
          </div>
          <div className={styles.status}>
            <span className={styles.badge} data-state={status.state}>{status.label}</span>
            {active && job.stage_label && job.stage_label !== status.label && <span className={styles.stage}>{job.stage_label}</span>}
          </div>
          <div className={styles.elapsed}><time>{formatJobElapsed(job, now)}</time><small>任务 #{job.id}</small></div>
          <div className={styles.actions}><Link href={`/contents?content_id=${job.content_id}`} className="secondary button-link" onClick={onViewContent}>{active ? "查看内容" : "查看结果内容"}</Link></div>
        </article>;
      })}
    </div>
  </ContentDialog>;
}
