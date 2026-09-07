"use client";

import { createContext, useContext, useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { usePathname, useRouter } from "next/navigation";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { readQueryJson } from "../lib/api";
import { queryKeys, sessionQueryOptions } from "../lib/queries";
import type { ServiceHealth } from "../lib/serviceStatus";
import { contentUpdateFeedback } from "../contents/contentUpdate";
import { CONTENT_UPDATE_API_BASE, contentUpdateAvailability, contentUpdateJobBlocksWrites, isActiveContentUpdateJob, isContentUpdateJob, mergeContentUpdateJobs, readPendingContentUpdates,
  reconcilePendingContentUpdates, submissionFailureStatus, submitContentUpdateJob,
  type ContentUpdateJob, type PendingContentUpdate } from "../contents/contentUpdateJobs";
import { showToast } from "./Feedback";

type UpdateJobsContext = {
  available: boolean; readOnly: boolean; ready: boolean; jobs: ContentUpdateJob[]; pending: PendingContentUpdate[];
  readError: string; reading: boolean; activeCount: number; openRequest: number; openTasks: () => void;
  refresh: () => void; retrySubmission: (contentId: number) => void;
  submit: (item: { id: number; title: string }) => Promise<void>;
  locked: (contentId: number) => boolean;
};
const emptyContext: UpdateJobsContext = { available: false, readOnly: false, ready: false, jobs: [], pending: [], readError: "", reading: false,
  activeCount: 0, openRequest: 0, openTasks: () => {}, refresh: () => {}, retrySubmission: () => {}, submit: async () => {}, locked: () => true };
const Context = createContext<UpdateJobsContext>(emptyContext);
const noJobs: ContentUpdateJob[] = [];
export function useContentUpdateJobs() { return useContext(Context); }

export default function ContentUpdateJobsProvider({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const [openRequest, setOpenRequest] = useState(0);
  const openTasks = useCallback(() => {
    setOpenRequest((current) => current + 1);
    router.push("/contents?updates=open", { scroll: false });
  }, [router]);
  const activePage = /^\/(overview|contents|accounts|selling-points|spu-audience|tasks|users)(\/|$)/.test(pathname ?? "");
  const session = useQuery({ ...sessionQueryOptions(), enabled: activePage });
  const owner = activePage && session.data?.username ? session.data.username : "";
  const health = useQuery({
    queryKey: ["system", "health"], enabled: Boolean(owner),
    queryFn: () => readQueryJson<ServiceHealth>("/api/v8/health", undefined, 5_000),
    staleTime: 15_000, refetchInterval: 30_000, refetchOnWindowFocus: "always", retry: false,
  });
  const availability = contentUpdateAvailability(health.data, health.isError);
  const readOnly = availability === "read_only";
  const canPoll = Boolean(owner) && availability === "available";
  const ownerRef = useRef(owner);
  useEffect(() => { ownerRef.current = owner; }, [owner]);
  const [hydratedOwner, setHydratedOwner] = useState<string | null>(null);
  const [pending, setPending] = useState<PendingContentUpdate[]>([]);
  const pendingRef = useRef(pending);
  const previousStatuses = useRef(new Map<number, ContentUpdateJob["status"]>());
  const queryClient = useQueryClient();
  const storageKey = `dcar-content-update-pending-v1:${encodeURIComponent(owner)}`;
  const jobQuery = useQuery({
    queryKey: ["content-update-jobs", owner], enabled: canPoll,
    queryFn: async () => {
      const response = await readQueryJson<{ jobs: ContentUpdateJob[] }>(`${CONTENT_UPDATE_API_BASE}/content-update-jobs`);
      if (!Array.isArray(response.jobs) || !response.jobs.every(isContentUpdateJob)) throw new Error("更新记录状态不完整，请重新读取。");
      if (ownerRef.current === owner) {
        const remaining = reconcilePendingContentUpdates(pendingRef.current, response.jobs);
        if (remaining.length !== pendingRef.current.length) savePending(remaining);
      }
      return mergeContentUpdateJobs(queryClient.getQueryData<ContentUpdateJob[]>(["content-update-jobs", owner]) ?? [], response.jobs);
    },
    staleTime: 0, refetchInterval: (query) => query.state.data?.some(isActiveContentUpdateJob) || pending.length ? 3_000 : 15_000,
    refetchIntervalInBackground: true, refetchOnWindowFocus: "always", retry: false,
  });
  const jobs = readOnly ? noJobs : jobQuery.data ?? noJobs;
  const ready = canPoll && hydratedOwner === owner && jobQuery.isSuccess;
  const readError = jobQuery.isError ? jobQuery.error instanceof Error ? jobQuery.error.message : "更新记录读取失败。" : "";

  function savePending(next: PendingContentUpdate[], required = false) {
    try { localStorage.setItem(storageKey, JSON.stringify(next)); }
    catch {
      if (required) throw new Error("无法保存本次提交标识，请允许浏览器存储后再更新。");
    }
    pendingRef.current = next; setPending(next);
  }

  useEffect(() => {
    let restored: PendingContentUpdate[] = [];
    try { if (owner) restored = readPendingContentUpdates(localStorage.getItem(storageKey)); } catch { /* Submission checks storage before sending. */ }
    pendingRef.current = restored;
    // Restore externally persisted submissions only when the authenticated owner changes.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setPending(restored);
    previousStatuses.current.clear(); setHydratedOwner(owner);
  }, [owner, storageKey]);

  useEffect(() => {
    for (const job of jobs) {
      const previous = previousStatuses.current.get(job.id);
      previousStatuses.current.set(job.id, job.status);
      if (!previous || !["queued", "running"].includes(previous) || isActiveContentUpdateJob(job)) continue;
      const summary = job.result ? contentUpdateFeedback(job.result) : { error: job.error || "更新失败，请查看任务结果。", message: "" };
      const failed = job.status === "failed" || Boolean(summary.error);
      const partial = !failed && summary.message.includes("未全部完成");
      showToast(failed ? "error" : "success", <><strong>{job.title || `内容 ${job.content_id}`}</strong><div>{failed ? "更新未完成" : partial ? "数据已部分更新" : "数据已更新"} · <button type="button" onClick={openTasks}>查看结果</button></div></>, { dedupeKey: `content-update-job:${job.id}:${job.status}` });
      // Completion feedback never waits for unrelated, possibly slow cached reads.
      for (const key of [queryKeys.contents, queryKeys.accounts, queryKeys.overview, queryKeys.activeSellingPoints, queryKeys.spu]) {
        void queryClient.invalidateQueries({ queryKey: key }).catch(() => {});
      }
    }
  }, [jobs, queryClient, openTasks]);

  function locked(contentId: number) {
    return !ready || jobs.some((job) => job.content_id === contentId && contentUpdateJobBlocksWrites(job))
      || pending.some((request) => request.contentId === contentId && request.status !== "rejected");
  }

  async function send(request: PendingContentUpdate) {
    if (!canPoll) return;
    const submittingOwner = owner;
    savePending([...pendingRef.current.filter((entry) => entry.contentId !== request.contentId), { ...request, status: "submitting", error: "" }], true);
    try {
      const job = await submitContentUpdateJob(request.contentId, request.requestId, request.title);
      if (ownerRef.current !== submittingOwner) return;
      previousStatuses.current.set(job.id, "queued");
      queryClient.setQueryData<ContentUpdateJob[]>(["content-update-jobs", owner], (current) => mergeContentUpdateJobs(current ?? [], [job]));
      savePending(pendingRef.current.filter((entry) => entry.requestId !== request.requestId));
      showToast("success", "更新已提交，关闭详情后仍会继续。可在内容列表的“更新记录”中查看进度。");
      void jobQuery.refetch();
    } catch (reason) {
      if (ownerRef.current !== submittingOwner) return;
      const status = submissionFailureStatus(reason);
      const error = reason instanceof Error ? reason.message : "提交结果暂时无法确认。";
      savePending(pendingRef.current.map((entry) => entry.requestId === request.requestId ? { ...entry, status, error } : entry));
      showToast("error", status === "uncertain" ? "提交结果待确认，等待超时不代表任务未入队。请在更新记录中查询，避免重复提交。" : error);
      void jobQuery.refetch();
    }
  }

  async function submit(item: { id: number; title: string }) {
    if (!canPoll) return;
    if (!ready) { openTasks(); return; }
    if (queryClient.getQueryData<ContentUpdateJob[]>(["content-update-jobs", owner])?.some((job) => job.content_id === item.id && contentUpdateJobBlocksWrites(job))) { openTasks(); return; }
    const existing = pendingRef.current.find((request) => request.contentId === item.id);
    if (existing) { openTasks(); return; }
    try { await send({ contentId: item.id, title: item.title, requestId: crypto.randomUUID(), createdAt: new Date().toISOString(), status: "submitting", error: "" }); }
    catch (reason) { showToast("error", reason instanceof Error ? reason.message : "无法提交更新。"); }
  }
  function retrySubmission(contentId: number) {
    if (!canPoll) return;
    const request = pendingRef.current.find((entry) => entry.contentId === contentId);
    if (!request || request.status === "submitting") return;
    void send(request).catch((reason) => showToast("error", reason instanceof Error ? reason.message : "无法确认原请求。"));
  }

  const activeContentIds = new Set(jobs.filter(isActiveContentUpdateJob).map((job) => job.content_id));
  for (const request of pending) if (request.status === "submitting") activeContentIds.add(request.contentId);
  return <Context.Provider value={{ available: canPoll, readOnly, ready, jobs, pending: readOnly ? [] : pending, readError,
    reading: jobQuery.isFetching, refresh: () => { if (canPoll) void jobQuery.refetch(); }, retrySubmission,
    activeCount: activeContentIds.size, openRequest, openTasks, submit, locked }}>
    {children}
  </Context.Provider>;
}
