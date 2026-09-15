"use client";

import { useEffect, useId, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiRequestError, jsonRequest, readDownload, readQueryJson, saveDownload } from "../lib/api";
import { sessionQueryOptions } from "../lib/queries";
import type { ContentSearchRequest } from "../lib/queryContracts";
import { accountGroupLabel, businessDirectionLabel } from "../lib/accountClassification";
import { formatDateTime, label } from "../lib/format";
import { useDialogFocus } from "../components/useDialogFocus";
import ContentDateFilter from "./ContentDateFilter";
import {
  CONTENT_EXPORT_PATH, exportFilterKey, exportFilters, exportGenerationBlock, exportStatusText, hasExportFilters,
  isActiveExport, isContentExportJob, matchingActiveExport, readPendingExport,
  type ContentExportJob, type PendingContentExport,
} from "./contentExport";
import styles from "./ContentExportControl.module.css";

type Props = {
  filters: ContentSearchRequest;
  total: number | null;
  queryPending: boolean;
  queryError: boolean;
  unappliedQuery: boolean;
  onApplyQuery: () => void;
  onApplyDates: (start: string, end: string) => void;
  sellingPoints: Array<{ code: string; label: string }>;
};

function FilterSummary({ filters, sellingPoints }: Pick<Props, "filters" | "sellingPoints">) {
  const parts = [
    filters.query && `搜索：${filters.query}`,
    filters.platform && `平台：${label(filters.platform)}`,
    filters.account_group && `账号分组：${accountGroupLabel(filters.account_group)}`,
    filters.business_direction && `业务方向：${businessDirectionLabel(filters.business_direction)}`,
    filters.content_direction && `作品内容方向：${label(filters.content_direction)}`,
    filters.selling_point && `卖点：${filters.selling_point === "__none__" ? "资料不足" : sellingPoints.find((point) => point.code === filters.selling_point)?.label ?? filters.selling_point}`,
    filters.spu_series && `车系：${filters.spu_series}`,
    filters.audience && `人群：${label(filters.audience)}`,
    filters.scene && `场景：${label(filters.scene)}`,
    (filters.published_from || filters.published_to) && `发布时间：${filters.published_from || "不限开始"} 至 ${filters.published_to || "不限结束"}`,
  ].filter(Boolean);
  return <div className={styles.scope}>{parts.map((part) => <span className={styles.tag} key={String(part)}>{part}</span>)}</div>;
}

export default function ContentExportControl(props: Props) {
  const session = useQuery(sessionQueryOptions());
  const owner = session.data && !session.isError
    ? JSON.stringify([session.data.username, session.data.role ?? "", session.data.bypass === true]) : "";
  // Remount all dialog/submission state before rendering another identity's jobs.
  return owner ? <OwnedContentExportControl key={owner} {...props} owner={owner} />
    : <button type="button" className="secondary" disabled title="正在确认登录状态">导出筛选结果</button>;
}

export function OwnedContentExportControl({ owner, ...props }: Props & { owner: string }) {
  const [mode, setMode] = useState<"confirm" | "history" | null>(null);
  const [pending, setPending] = useState<PendingContentExport | null>(null);
  const [hydrated, setHydrated] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [downloading, setDownloading] = useState<string | null>(null);
  const [feedback, setFeedback] = useState({ error: "", message: "" });
  const pendingRef = useRef<PendingContentExport | null>(null);
  const operation = useRef(false);
  const mounted = useRef(true);
  const panelRef = useRef<HTMLElement | null>(null);
  const headingId = useId();
  const queryClient = useQueryClient();
  const queryKey = ["content-exports", owner] as const;
  const storageKey = `dcar-content-export-pending-v1:${encodeURIComponent(owner)}`;
  useDialogFocus(Boolean(mode), panelRef, { onClose: () => {
    // The date picker is a portal with its own Escape/focus handling. Its open
    // trigger stays here, so the outer dialog must let that picker close first.
    if (!panelRef.current?.querySelector('[aria-haspopup="dialog"][aria-expanded="true"]')) setMode(null);
  }, busy: false });
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);

  function persistPending(value: PendingContentExport | null) {
    if (value) localStorage.setItem(storageKey, JSON.stringify(value));
    else { try { localStorage.removeItem(storageKey); } catch { /* An acknowledged id can safely be replayed. */ } }
    pendingRef.current = value;
    setPending(value);
  }

  useEffect(() => {
    let restored: PendingContentExport | null = null;
    try { restored = readPendingExport(localStorage.getItem(storageKey)); } catch { /* Saving is required before a new request. */ }
    pendingRef.current = restored;
    // Only restore the pending request belonging to this authenticated identity.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setPending(restored); setHydrated(true);
  }, [storageKey]);

  const jobQuery = useQuery({
    queryKey,
    queryFn: async ({ signal }) => {
      const response = await readQueryJson<{ jobs: ContentExportJob[] }>(CONTENT_EXPORT_PATH, { signal });
      if (!Array.isArray(response.jobs) || !response.jobs.every(isContentExportJob)) throw new Error("导出记录读取不完整，请重新读取。");
      return response.jobs.slice(0, 20);
    },
    staleTime: 0, retry: false, refetchOnMount: "always", refetchOnWindowFocus: "always",
    refetchInterval: (query) => pending || query.state.data?.some(isActiveExport) ? 2_000 : 15_000,
    refetchIntervalInBackground: false,
  });
  const jobs = jobQuery.data ?? [];
  const block = exportGenerationBlock(props);
  const activeMatch = matchingActiveExport(jobs, props.filters);
  const readableCount = !props.queryPending && !props.queryError && props.total !== null && Number.isSafeInteger(props.total) && props.total >= 0;

  useEffect(() => {
    if (!pending || !jobQuery.data?.some((job) => job.request_id === pending.request_id)) return;
    try { localStorage.removeItem(storageKey); } catch { /* Reusing this id remains safe. */ }
    pendingRef.current = null;
    // A recovered server receipt resolves an uncertain submission.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setPending(null);
  }, [pending, jobQuery.data, storageKey]);

  function showHistory() { setFeedback({ error: "", message: "" }); setMode("history"); }
  function showConfirmation() { setFeedback({ error: "", message: "" }); setMode("confirm"); }

  async function submit(filters: ContentSearchRequest, resume = false) {
    if (!hydrated || operation.current) return;
    if (pendingRef.current && !resume) { setMode("history"); setFeedback({ error: "", message: "请先确认上一次提交的结果。" }); return; }
    const existing = matchingActiveExport(jobs, filters);
    if (existing && !resume) { setMode("history"); setFeedback({ error: "", message: "相同筛选条件正在生成，可在下方查看进度。" }); return; }
    const request = resume && pendingRef.current ? pendingRef.current
      : { request_id: crypto.randomUUID(), filters: exportFilters(filters) };
    try { persistPending(request); }
    catch { setFeedback({ error: "无法保存本次提交标识，请允许浏览器存储后再试。", message: "" }); return; }
    operation.current = true;
    setSubmitting(true); setMode("history"); setFeedback({ error: "", message: "正在提交导出请求…" });
    try {
      const response = await readQueryJson<{ job: ContentExportJob; reused: boolean }>(CONTENT_EXPORT_PATH, jsonRequest(request));
      if (!isContentExportJob(response.job) || exportFilterKey(response.job.filters) !== exportFilterKey(request.filters)
          || (!response.reused && response.job.request_id !== request.request_id)) throw new Error("导出提交结果尚未确认，请重新确认结果。");
      if (!mounted.current) return;
      // An older list request must not replace the newly acknowledged job.
      await queryClient.cancelQueries({ queryKey, exact: true });
      if (!mounted.current) return;
      queryClient.setQueryData<ContentExportJob[]>(queryKey, (previous = []) => [response.job, ...previous.filter((job) => job.id !== response.job.id)].slice(0, 20));
      persistPending(null);
      setFeedback({ error: "", message: response.reused ? "已找到相同导出任务，可在下方查看。" : "已提交，文件在后台生成。可以关闭窗口，稍后从最近导出中下载。" });
      void jobQuery.refetch();
    } catch (reason) {
      if (!mounted.current) return;
      if (reason instanceof ApiRequestError && reason.status != null && reason.status >= 400 && reason.status < 500 && reason.status !== 408) persistPending(null);
      setFeedback({ error: reason instanceof Error ? reason.message : "提交结果尚未确认，请重新确认结果。", message: "" });
    } finally {
      operation.current = false;
      if (mounted.current) setSubmitting(false);
    }
  }

  async function download(job: ContentExportJob) {
    if (downloading) return;
    setDownloading(job.id); setFeedback({ error: "", message: "正在准备下载…" });
    try {
      const file = await readDownload(`${CONTENT_EXPORT_PATH}/${encodeURIComponent(job.id)}/download`, {
        contentTypes: ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"],
        fallbackFilename: job.filename || "内容筛选结果.xlsx",
      });
      if (!mounted.current) return;
      saveDownload(file); setFeedback({ error: "", message: "文件已开始下载。" });
    } catch (reason) {
      if (mounted.current) setFeedback({ error: reason instanceof Error ? reason.message : "下载失败，请重试。", message: "" });
    } finally { if (mounted.current) setDownloading(null); }
  }

  return <div className={styles.control}>
    <button type="button" className="secondary" aria-haspopup="dialog" onClick={showConfirmation}>导出筛选结果</button>
    {mode && <div className={styles.backdrop} onClick={(event) => { if (event.target === event.currentTarget) setMode(null); }}>
      <section ref={panelRef} className={styles.panel} role="dialog" aria-modal="true" aria-labelledby={headingId} tabIndex={-1}>
        <header className={styles.header}><h2 id={headingId}>{mode === "confirm" ? "导出筛选结果" : "最近导出"}</h2><button type="button" className={styles.close} aria-label="关闭导出窗口" onClick={() => setMode(null)}>×</button></header>
        <div className={styles.body}>
          {mode === "confirm" ? <>
            <p className={styles.muted}>按列表中已生效的条件导出全部匹配内容，包含所有分页。</p>
            <FilterSummary filters={props.filters} sellingPoints={props.sellingPoints} />
            {!hasExportFilters(props.filters) && <div className={styles.dates}><span className={styles.muted}>先选择导出时间范围</span><ContentDateFilter start={props.filters.published_from ?? ""} end={props.filters.published_to ?? ""} onChange={props.onApplyDates} /></div>}
            <p className={styles.count}>{readableCount ? <>预计 <strong>{props.total!.toLocaleString("zh-CN")}</strong> 条内容</> : props.queryError ? "当前条数读取失败" : "正在确认条数…"}</p>
            <p className={styles.muted}>Excel 文件 · 时间为北京时间 · 指标使用生成文件时保存的最新累计值。</p>
            {block && <p className={styles.feedback} data-error={props.queryError || props.unappliedQuery} role="status">{block}</p>}
            {(props.unappliedQuery || props.queryError) && <div className={styles.jobActions}><button type="button" className="secondary" onClick={props.onApplyQuery}>{props.unappliedQuery ? "应用搜索词" : "重新搜索"}</button></div>}
          </> : <>
            {pending && <div className={`${styles.job} ${styles.pending}`}><strong className={styles.muted}>{submitting ? "正在提交…" : "提交结果待确认"}</strong><FilterSummary filters={pending.filters} sellingPoints={props.sellingPoints} /><p className={styles.muted}>确认结果会沿用本次提交标识。</p><div className={styles.jobActions}><button type="button" className="secondary" disabled={submitting || !hydrated} onClick={() => void submit(pending.filters, true)}>{submitting ? "提交中…" : "重新确认结果"}</button></div></div>}
            {jobQuery.isPending && <p className={styles.muted} role="status">正在读取导出记录…</p>}
            {jobQuery.isError && <p className={styles.feedback} data-error="true" role="alert">{jobQuery.error instanceof Error ? jobQuery.error.message : "导出记录读取失败。"} 已显示的状态可能尚未更新。</p>}
            {jobQuery.isSuccess && jobs.length === 0 && !pending && <p className={styles.muted}>暂无导出记录。生成后可在这里下载。</p>}
            <ul className={styles.history}>{jobs.map((job) => <li className={styles.job} key={job.id}>
              <div className={styles.jobHead}><strong>{exportStatusText(job)}</strong><time>{formatDateTime(job.created_at)}</time></div>
              <FilterSummary filters={job.filters} sellingPoints={props.sellingPoints} />
              {job.status === "failed" && <p className={styles.feedback} data-error="true">{job.error || "生成失败，请重试。"}</p>}
              <div className={styles.jobActions}>
                {job.status === "succeeded" && <button type="button" className="secondary" disabled={Boolean(downloading)} onClick={() => void download(job)}>{downloading === job.id ? "正在下载…" : "下载 Excel"}</button>}
                {job.status === "failed" && <button type="button" className="secondary" disabled={submitting || Boolean(pending) || !hydrated} onClick={() => void submit(job.filters)}>重新生成</button>}
              </div>
            </li>)}</ul>
          </>}
          {(feedback.error || feedback.message) && <p className={styles.feedback} data-error={Boolean(feedback.error)} role={feedback.error ? "alert" : "status"}>{feedback.error || feedback.message}</p>}
        </div>
        <footer className={styles.footer}>
          {mode === "confirm" ? <><button type="button" className="secondary" onClick={showHistory}>最近导出</button><button type="button" className="primary" disabled={Boolean(block) || !hydrated || submitting} onClick={() => { if (!exportGenerationBlock(props)) void submit(props.filters); }}>{activeMatch ? "查看生成进度" : "生成 Excel"}</button></>
            : <><button type="button" className={`secondary ${styles.back}`} onClick={showConfirmation}>返回</button><button type="button" className="secondary" disabled={jobQuery.isFetching} onClick={() => void jobQuery.refetch()}>{jobQuery.isFetching ? "正在刷新…" : "刷新记录"}</button><button type="button" className="primary" onClick={() => setMode(null)}>关闭</button></>}
        </footer>
      </section>
    </div>}
  </div>;
}
