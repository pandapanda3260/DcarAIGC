"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { ArrowSquareOutIcon, CaretRightIcon } from "@phosphor-icons/react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useRouter, useSearchParams } from "next/navigation";
import AppShell from "../components/AppShell";
import { Feedback, Loading, Notice, ReadErrorState } from "../components/Feedback";
import { Pagination } from "../components/Pagination";
import { API_BASE, jsonRequest, readJson, readQueryJson } from "../lib/api";
import { resolveMediaAction } from "../lib/contentMedia";
import type { ContentThumbnails } from "../lib/contentThumbnails";
import { formatDateTime, label, platformKeys } from "../lib/format";
import { buildContentSearchRequest, lastPageFor } from "../lib/queryContracts";
import { activeSellingPointsQueryOptions, contentSearchQueryOptions, defaultContentSearchRequest, queryKeys } from "../lib/queries";
import type { ContentItem, ContentTagSpu, EvidenceBundle } from "../lib/types";
import ContentMediaBox, { ContentMediaMark } from "./ContentMediaBox";
import ContentMediaModal from "./ContentMediaModal";
import EvidenceModal from "./EvidenceModal";
import ContentDialog from "./ContentDialog";
import ContentTitle from "./ContentTitle";
import ContentDateFilter from "./ContentDateFilter";
import { CONTENT_DATE_FILTER_ENABLED } from "../lib/features";
import { useContentUpdateJobs } from "../components/ContentUpdateJobsProvider";
import ContentUpdateRecords from "../components/ContentUpdateRecords";
import { contentUpdateJobBlocksWrites } from "./contentUpdateJobs";
import { contentUpdateRowState } from "./contentUpdatePresentation";
import { buildContentSaveOperation, toShanghaiDateTimeLocal, type ContentForm } from "./contentForm";
import styles from "./ContentsPage.module.css";

const EVIDENCE_LEVEL_HINTS: Record<string, string> = {
  V3: "V3：信息完整，视频、语音和画面文字均可用，可以自动评估",
  V2: "V2：信息较完整，至少一种媒体资料能说明主要内容，可以自动评估",
  V1: "V1：信息不足，只有标题、正文或话题，暂时无法得出评估结果",
  V0: "V0：无法评估，没有可用的正文、语音或画面文字",
};

const EVIDENCE_LEVEL_LABELS: Record<string, string> = {
  V3: "信息完整（V3）",
  V2: "有媒体资料（V2）",
  V1: "只有文字（V1）",
  V0: "资料不可用（V0）",
};

function spuDisplayName(spu: ContentTagSpu) {
  return spu.series.startsWith(spu.brand) ? spu.series : `${spu.brand} ${spu.series}`;
}

function spuSubline(item: ContentItem) {
  const spu = item.spu;
  if (!spu) return "";
  const parts = [spu.resolved_level === "trim" && spu.trim_label ? spu.trim_label : "未细化"];
  const alias = spu.matched_aliases[0] ?? "";
  if (alias && !spu.series.toLowerCase().includes(alias.toLowerCase()) && !alias.toLowerCase().includes(spu.series.toLowerCase())) {
    parts.push(`命中「${alias}」`);
  }
  if (item.spu_secondary_count > 0) parts.push(`另提及 ${item.spu_secondary_count} 车系`);
  return parts.join(" · ");
}

function spuCellTitle(spu: ContentTagSpu) {
  const name = `${spu.brand} ${spu.series}${spu.trim_label ? ` · ${spu.trim_label}` : "（未细化）"}`;
  const aliases = spu.matched_aliases.length > 0 ? `｜命中：${spu.matched_aliases.join("、")}` : "";
  return `${name}｜识别评分 ${spu.score}${aliases}`;
}

function contentDateTime(value: string | null) {
  return formatDateTime(value).replaceAll("/", "-");
}

function contentMetric(value: number | null) {
  return value == null ? "—" : value.toLocaleString("zh-CN");
}

function ContentDetails({ item, sellingPoint, saving, status, updateLocked, updateLabel, recordsAvailable, onTasks, onClose, onEvidence, onEdit, onUpdate }: {
  item: ContentItem;
  sellingPoint: string;
  saving: boolean;
  status: { error: string; message: string };
  updateLocked: boolean;
  updateLabel: string;
  recordsAvailable: boolean;
  onTasks: () => void;
  onClose: () => void;
  onEvidence: () => void;
  onEdit: () => void;
  onUpdate: () => void;
}) {
  return <ContentDialog title="内容详情" busy={saving} onClose={onClose} status={status} footer={<>
    <button type="button" className="primary" disabled={saving} onClick={onEvidence}>查看依据</button>
    {updateLocked && recordsAvailable && <button type="button" className="secondary" onClick={onTasks}>查看更新记录</button>}
    <button type="button" className="secondary" disabled={saving || updateLocked} onClick={onUpdate}>{updateLabel}</button>
    <button type="button" className="secondary" disabled={saving || updateLocked} title={updateLocked ? updateLabel : undefined} onClick={onEdit}>修改</button>
  </>}>
        <a className={styles.detailTitle} href={item.canonical_url} target="_blank" rel="noreferrer">{item.title || "标题缺失"}<ArrowSquareOutIcon size={16} aria-label="打开原帖" /></a>
        <div className={styles.metadata}><ContentMediaMark platform={item.platform} /><span>{item.raw_account_name || "昵称缺失"}</span><time dateTime={item.published_at ?? undefined}>{contentDateTime(item.published_at)}</time></div>
        <div className={styles.detailMetrics}><div>阅读 <strong>{contentMetric(item.view_count)}</strong></div><div>评论 <strong>{contentMetric(item.comment_count)}</strong></div><div>点赞 <strong>{contentMetric(item.like_count)}</strong></div></div>
        <section className={styles.detailSection}><h3>卖点</h3><p>{sellingPoint}</p>{Boolean(item.evaluation_is_stale) && <span className="status-badge stale-evaluation">结果需更新</span>}</section>
        <section className={styles.detailSection}><h3>分类与评估</h3><dl className={styles.detailGrid}>
          <div><dt>账号类型</dt><dd>{label(item.account_type)}</dd></div>
          <div><dt>内容方向</dt><dd>{label(item.content_direction)}</dd></div>
          <div className={styles.detailWide}><dt>车型</dt><dd>{item.spu ? <><span title={spuCellTitle(item.spu)}>{spuDisplayName(item.spu)}</span><small className="spu-tag-subline">{spuSubline(item)}</small></> : item.spu_gray_count > 0 ? "车型不确定" : "未识别"}</dd></div>
          <div><dt>人群</dt><dd>{item.audience ? <>{item.audience.label}<small>{label(item.audience.source)}</small></> : "—"}</dd></div>
          <div><dt>场景</dt><dd className="content-scene-cell">{item.scenes?.length ? item.scenes.map((scene) => scene.label).join("、") : "—"}</dd></div>
          <div><dt>资料完整度</dt><dd>{item.evidence_level ? <span className="evidence-level-tag" title={EVIDENCE_LEVEL_HINTS[item.evidence_level]}>{EVIDENCE_LEVEL_LABELS[item.evidence_level] ?? item.evidence_level}</span> : "—"}</dd></div>
          <div><dt>垂直度</dt><dd>{item.content_automotive_score == null ? "暂不可计算" : `${item.content_automotive_score}%`}</dd></div>
          <div><dt>拉新</dt><dd>暂不可计算</dd></div><div><dt>拉活</dt><dd>暂不可计算</dd></div><div><dt>线索</dt><dd>暂不可计算</dd></div>
          <div><dt>重复提醒</dt><dd>{item.duplicate_original_link_id || "—"}</dd></div>
        </dl></section>
        <section className={styles.detailSection}><h3>内容资料</h3><dl className={styles.identityGrid}>
          <div><dt>内容编号</dt><dd>{item.link_id}</dd></div>
          <div><dt>平台作品编号</dt><dd><a href={item.canonical_url} target="_blank" rel="noreferrer">{item.platform_content_id || "平台作品编号缺失"}</a></dd></div>
          <div><dt>平台账号编号</dt><dd>{item.raw_account_uid || "平台账号编号缺失"}</dd></div>
          <div><dt>发布时间</dt><dd>{contentDateTime(item.published_at)}</dd></div>
          <div><dt>指标更新时间</dt><dd>{contentDateTime(item.metrics_captured_at)}</dd></div>
        </dl></section>
        {item.body && <section className={styles.detailSection}><h3>正文</h3><p className={styles.bodyText}>{item.body}</p></section>}
  </ContentDialog>;
}

export default function ContentsPage() {
  const [query, setQuery] = useState("");
  const [platform, setPlatform] = useState("");
  const [accountType, setAccountType] = useState("");
  const [direction, setDirection] = useState("");
  const [sellingPoint, setSellingPoint] = useState("");
  const [appliedRequest, setAppliedRequest] = useState(() => ({ ...defaultContentSearchRequest }));
  const [form, setForm] = useState<ContentForm | null>(null);
  const [originalForm, setOriginalForm] = useState<ContentForm | null>(null);
  const [evidenceItem, setEvidenceItem] = useState<ContentItem | null>(null);
  const [mediaItem, setMediaItem] = useState<ContentItem | null>(null);
  const [detailSelection, setDetailSelection] = useState<ContentItem | null>(null);
  const [saving, setSaving] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [dialogStatus, setDialogStatus] = useState({ error: "", message: "" });
  const operationRunning = useRef(false);
  const detailTriggerRef = useRef<HTMLButtonElement | null>(null);
  const queryClient = useQueryClient();
  const contentUpdates = useContentUpdateJobs();
  const router = useRouter();
  const searchParams = useSearchParams();
  const recordsOpen = searchParams.get("updates") === "open";
  const selectedContentId = Number(searchParams.get("content_id"));
  const selectedContentQuery = useQuery({
    queryKey: [...queryKeys.contents, "selected", selectedContentId],
    enabled: Number.isSafeInteger(selectedContentId) && selectedContentId > 0,
    queryFn: async () => {
      const bundle = await readQueryJson<EvidenceBundle>(`/api/v8/contents/${selectedContentId}/evidence`);
      const result = await readQueryJson<{ items: ContentItem[] }>("/api/v8/contents/search", jsonRequest({ ...defaultContentSearchRequest, query: bundle.content.link_id }));
      const selected = result.items.find((item) => item.id === selectedContentId);
      if (!selected) throw new Error("暂时无法读取该内容，请稍后从更新记录中再次打开。");
      return selected;
    }, retry: false,
  });
  useEffect(() => {
    if (!selectedContentQuery.data) return;
    // A task-result URL is an external navigation event, including same-page navigation.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setDetailSelection(selectedContentQuery.data); setEvidenceItem(null); setForm(null);
  }, [selectedContentQuery.data]);
  const contentsQuery = useQuery(contentSearchQueryOptions(appliedRequest));
  const sellingPointsQuery = useQuery(activeSellingPointsQueryOptions());
  const items = contentsQuery.data?.items ?? [];
  const thumbnailIds = items.map((item) => item.id).join(",");
  const thumbnailsQuery = useQuery({
    queryKey: [...queryKeys.contents, "thumbnails", thumbnailIds],
    queryFn: () => readQueryJson<ContentThumbnails>(`/workbench-api/content-thumbnails?ids=${thumbnailIds}`),
    enabled: Boolean(thumbnailIds), retry: false, staleTime: 60_000, gcTime: 60_000,
    refetchOnWindowFocus: false,
  });
  const total = contentsQuery.data?.total ?? 0;
  const contentsReadFailed = contentsQuery.isLoadingError || retrying;
  const sellingPoints = sellingPointsQuery.data?.items ?? [];
  const listLoading = contentsQuery.isPending && !contentsQuery.data && !contentsReadFailed;
  const formOpen = Boolean(form);
  const recordsVisible = recordsOpen && contentUpdates.available && !formOpen;
  useEffect(() => {
    if (!recordsOpen || !formOpen) return;
    // A records request must not discard another content item's unsaved edit.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setDialogStatus({ error: "", message: "完成或关闭当前编辑后，将打开更新记录。" });
  }, [recordsOpen, contentUpdates.openRequest, formOpen]);

  const feedback = useCallback((nextError: string, nextMessage = "") => { setError(nextError); setMessage(nextMessage); }, []);
  function operationFeedback(nextError: string, nextMessage = "") {
    setDialogStatus({ error: nextError, message: nextMessage });
    feedback(nextError, nextMessage);
  }
  useEffect(() => {
    if (detailSelection || !detailTriggerRef.current) return;
    const frame = requestAnimationFrame(() => {
      if (detailTriggerRef.current?.isConnected) detailTriggerRef.current.focus();
      detailTriggerRef.current = null;
    });
    return () => cancelAnimationFrame(frame);
  }, [detailSelection]);
  function retryContentsRead() {
    if (retrying) return;
    setRetrying(true);
    void contentsQuery.refetch().finally(() => setRetrying(false));
  }
  function applySearch(overrides: Partial<{ query: string; platform: string; accountType: string; direction: string; sellingPoint: string; publishedFrom: string; publishedTo: string; page: number; pageSize: number }> = {}) {
    const filters = { query, platform, accountType, direction, sellingPoint, spuSeries: "", audience: "", scene: "", publishedFrom: appliedRequest.published_from ?? "", publishedTo: appliedRequest.published_to ?? "", ...overrides };
    const nextRequest = buildContentSearchRequest(
      filters,
      overrides.page ?? appliedRequest.page,
      overrides.pageSize ?? appliedRequest.page_size,
    );
    if (JSON.stringify(nextRequest) === JSON.stringify(appliedRequest)) {
      if (contentsReadFailed) retryContentsRead();
      else void contentsQuery.refetch();
      return;
    }
    setAppliedRequest(nextRequest);
  }

  useEffect(() => {
    if (!contentsQuery.data || contentsQuery.isPlaceholderData) return;
    const lastPage = lastPageFor(contentsQuery.data.total, appliedRequest.page_size);
    if (appliedRequest.page > lastPage) {
      const timer = window.setTimeout(() => {
        setAppliedRequest((current) => ({ ...current, page: lastPage }));
      }, 0);
      return () => window.clearTimeout(timer);
    }
  }, [appliedRequest.page, appliedRequest.page_size, contentsQuery.data, contentsQuery.isPlaceholderData]);

  async function invalidateContentData() {
    void Promise.all([
      queryClient.invalidateQueries({ queryKey: queryKeys.contents }),
      queryClient.invalidateQueries({ queryKey: queryKeys.accounts }),
      queryClient.invalidateQueries({ queryKey: queryKeys.overview }),
      queryClient.invalidateQueries({ queryKey: queryKeys.activeSellingPoints, exact: true }),
      queryClient.invalidateQueries({ queryKey: queryKeys.spu }),
    ]).catch(() => {});
  }
  const sellingPointSelectRef = useRef<HTMLSelectElement | null>(null);
  const sellingPointLabel = sellingPoint === "" ? "全部卖点" : sellingPoint === "__none__" ? "资料不足" : (() => { const point = sellingPoints.find((item) => item.code === sellingPoint); return point ? `${point.code} · ${point.label}` : sellingPoint; })();
  useEffect(() => {
    const node = sellingPointSelectRef.current;
    if (!node) return;
    const context = document.createElement("canvas").getContext("2d");
    if (!context) return;
    const style = window.getComputedStyle(node as unknown as Element);
    context.font = `${style.fontStyle} ${style.fontWeight} ${style.fontSize} ${style.fontFamily}`;
    node.style.width = `${Math.ceil(context.measureText(sellingPointLabel).width) + 46}px`;
  }, [sellingPointLabel, contentsQuery.isPending]);
  const sellingPointLabelByCode = new Map<string, string>(sellingPoints.map((point) => [point.code, point.label]));
  const contentGroups = new Map<string, ContentItem[]>();
  for (const item of items) {
    const date = contentDateTime(item.published_at).split(" ")[0];
    const day = date === "缺失" ? "发布时间缺失" : date;
    const group = contentGroups.get(day);
    if (group) group.push(item);
    else contentGroups.set(day, [item]);
  }
  const detailItem = items.find((item) => item.id === detailSelection?.id) ?? detailSelection;
  function sellingPointText(item: ContentItem) {
    return item.primary_selling_point_code
      ? sellingPointLabelByCode.get(item.primary_selling_point_code) ?? item.primary_selling_point_code
      : item.evidence_level ? "暂不可计算" : "资料不足";
  }

  function edit(item: ContentItem) {
    if (contentUpdates.locked(item.id)) { contentUpdates.openTasks(); return; }
    setDialogStatus({ error: "", message: "" });
    const next: ContentForm = { id: item.id, platform: item.platform, platformContentId: item.platform_content_id ?? "", canonicalUrl: item.canonical_url, publishedAt: toShanghaiDateTimeLocal(item.published_at), title: item.title, body: item.body ?? "", contentType: item.content_type ?? "unknown", accountUid: item.raw_account_uid ?? "", accountName: item.raw_account_name ?? "", accountType: item.account_type, contentDirection: item.content_direction };
    setForm(next);
    setOriginalForm({ ...next });
  }
  function closeForm() {
    if (saving) return;
    setForm(null);
    setOriginalForm(null);
  }
  async function save() {
    if (!form || operationRunning.current) return;
    if (contentUpdates.locked(form.id)) { operationFeedback("该内容的更新状态尚未完成确认，请先查看更新记录。"); return; }
    operationRunning.current = true;
    setSaving(true); operationFeedback("");
    try {
      const operation = buildContentSaveOperation(form, originalForm);
      if (operation.unchanged) { setForm(null); setOriginalForm(null); operationFeedback("", "内容未发生修改"); return; }
      const saved = await readJson<{ id: number }>(operation.path, jsonRequest(operation.body, operation.method));
      setForm(null); setOriginalForm(null);
      await invalidateContentData();
      if (detailSelection) {
        try {
          const result = await readQueryJson<{ items: ContentItem[] }>("/api/v8/contents/search", jsonRequest({ ...defaultContentSearchRequest, query: saved.id === detailSelection.id ? detailSelection.link_id : form.canonicalUrl }));
          const selected = result.items.find((item) => item.id === saved.id);
          if (!selected) throw new Error("未找到保存后的内容");
          setDetailSelection(selected);
        } catch {
          operationFeedback("内容已保存，但最新详情读取失败，当前仍显示上次数据。请关闭后重新打开详情，无需重复保存。");
          return;
        }
      }
      operationFeedback("", "内容已更新");
    } catch (reason) { operationFeedback(reason instanceof Error ? reason.message : "内容保存失败"); }
    finally { operationRunning.current = false; setSaving(false); }
  }
  async function updateData(item: ContentItem) {
    setDialogStatus({ error: "", message: "" });
    await contentUpdates.submit(item);
  }
  function openUpdateTasks() {
    contentUpdates.openTasks();
  }
  function closeUpdateRecords() {
    const params = new URLSearchParams(searchParams.toString());
    params.delete("updates");
    router.replace(`/contents${params.size ? `?${params}` : ""}`, { scroll: false });
  }
  const detailJob = contentUpdates.jobs.find((job) => job.content_id === detailItem?.id && contentUpdateJobBlocksWrites(job));
  const detailPending = contentUpdates.pending.find((request) => request.contentId === detailItem?.id && request.status !== "rejected");
  const detailUpdateLocked = detailItem ? contentUpdates.locked(detailItem.id) : false;
  const detailUpdateLabel = contentUpdates.readOnly ? "只读快照" : detailPending ? detailPending.status === "submitting" ? "正在提交…" : "提交待确认" : detailJob ? detailJob.error_code === "result_uncertain" ? "结果待确认" : detailJob.status === "queued" ? "等待更新" : "后台更新中" : !contentUpdates.ready ? "正在确认任务" : "更新数据";
  const detailStatus = contentUpdates.readOnly ? { error: "", message: "当前为只读数据快照，内容更新和修改请在本地工作台完成。" } : detailJob || detailPending ? { error: "", message: detailJob?.error_code === "result_uncertain" ? "更新结果待确认，不会自动重试付费更新。请在更新记录中查看处理说明。" : `${detailJob?.stage_label || detailUpdateLabel}。关闭详情后仍会继续，可在更新记录中查看进度。` } : dialogStatus;

  return <AppShell active="contents" header={contentsQuery.isPending && !contentsQuery.data && !contentsReadFailed ? undefined :
    <header className="page-header"><div className="page-header-copy"><span className="page-header-eyebrow">内容资料库</span><h1 className="page-header-title">发布内容明细</h1><p className="page-header-description">更新数据时会同步更新详情、指标以及已保存的视频和图片；重复提醒会指向最早发布的内容。</p></div><div className="page-header-actions"><a className="secondary button-link" href={`${API_BASE}/api/v8/contents/export`} title="导出内容库中的全部可见内容，不受当前筛选条件限制">{appliedRequest.published_from || appliedRequest.published_to ? "下载全部内容表格" : "下载内容表格"}</a></div></header>
  }>
    <Feedback error={error} message={message} onClose={() => feedback("")} />
    {selectedContentQuery.isError && <Notice tone="error">{selectedContentQuery.error instanceof Error ? selectedContentQuery.error.message : "内容详情读取失败。"}</Notice>}
    {contentsQuery.isError && <Notice tone="error">{contentsQuery.data ? `数据刷新失败，当前显示上次数据。${contentsQuery.error instanceof Error ? contentsQuery.error.message : ""}` : contentsQuery.error instanceof Error ? contentsQuery.error.message : "内容读取失败"}</Notice>}
    <section className="page-stack wide-stack">
      <div className={`filter-bar ${styles.filters}`}><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="内容编号、标题、账号编号、昵称或链接" onKeyDown={(event) => { if (event.key === "Enter") applySearch({ page: 1 }); }} /><select value={platform} onChange={(event) => { setPlatform(event.target.value); applySearch({ platform: event.target.value, page: 1 }); }}><option value="">全部平台</option>{platformKeys.map((key) => <option key={key} value={key}>{label(key)}</option>)}</select><select value={accountType} onChange={(event) => { setAccountType(event.target.value); applySearch({ accountType: event.target.value, page: 1 }); }}><option value="">全部账号类型</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option><option value="unknown">未知</option></select><select value={direction} onChange={(event) => { setDirection(event.target.value); applySearch({ direction: event.target.value, page: 1 }); }}><option value="">全部内容方向</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option><option value="unknown">未知</option></select><select className="selling-point-filter" ref={sellingPointSelectRef} value={sellingPoint} onChange={(event) => { setSellingPoint(event.target.value); applySearch({ sellingPoint: event.target.value, page: 1 }); }}><option value="">全部卖点</option><option value="__none__">资料不足</option>{sellingPoints.map((point) => <option key={point.code} value={point.code} title={point.label}>{point.code} · {point.label}</option>)}</select>{CONTENT_DATE_FILTER_ENABLED && <ContentDateFilter start={appliedRequest.published_from ?? ""} end={appliedRequest.published_to ?? ""} onChange={(start, end) => applySearch({ publishedFrom: start, publishedTo: end, page: 1 })} />}<button className="secondary" onClick={() => applySearch({ page: 1 })}>搜索</button><span role="status" aria-live="polite">{contentsReadFailed ? "读取失败" : listLoading ? "正在读取…" : contentsQuery.isFetching && contentsQuery.isPlaceholderData ? "正在筛选…" : `${total} 条内容`}</span></div>
      <article aria-busy={contentsQuery.isFetching} className={`panel table-panel ${styles.listPanel}${contentsReadFailed ? " has-read-error" : ""}`}>
        <header className={styles.listHeader}><h2>内容列表</h2>
          {contentUpdates.available && <button type="button" className={styles.recordsButton} aria-haspopup="dialog" onClick={openUpdateTasks}>更新记录{contentUpdates.activeCount > 0 && <span className={styles.activeUpdates}>更新中 {contentUpdates.activeCount}</span>}</button>}
        </header>
        {listLoading && <Loading label="正在读取内容库" />}
        {!contentsReadFailed && !listLoading && <div className={styles.listBody}>
          {Array.from(contentGroups, ([day, group]) => <section className={styles.dateGroup} key={day} aria-label={day}>
            <h3 className={styles.dateHeading}>{day}</h3>
            <table className={styles.table} aria-label={`${day}发布内容`}>
              <colgroup><col className={styles.contentColumn} /><col className={styles.sellingColumn} /><col className={styles.readColumn} /><col className={styles.commentColumn} /><col className={styles.likeColumn} /><col className={styles.classificationColumn} /><col className={styles.actionColumn} /></colgroup>
              <thead><tr><th scope="col">内容</th><th scope="col">卖点</th><th scope="col" className={styles.numberCell}>阅读</th><th scope="col" className={styles.numberCell}>评论</th><th scope="col" className={styles.numberCell}>点赞</th><th scope="col" className={styles.classificationCell}>分类</th><th scope="col"><span className="visually-hidden">操作</span></th></tr></thead>
              <tbody>{group.map((item) => {
                const updateState = contentUpdateRowState(item.id, contentUpdates.jobs, contentUpdates.pending);
                return <tr key={item.id}>
                <td className={styles.contentCell}>
                  <div className={styles.contentMain}>
                    <ContentMediaBox item={item} thumbnail={thumbnailsQuery.data?.items[item.id]} onOpen={setMediaItem} showPlatformMark={false} />
                    <div className={styles.contentCopy}>
                      <div className={styles.titleLine}>
                        <ContentTitle text={item.title || "标题缺失"} href={item.canonical_url} />
                        {updateState && <button type="button" className={styles.rowUpdateStatus} data-tone={updateState.tone} title={updateState.description} aria-label={`${updateState.label}，查看更新记录：${item.title || "标题缺失"}`} onClick={openUpdateTasks}>{updateState.tone === "active" && <span className={styles.updateSpinner} aria-hidden="true" />}{updateState.label}</button>}
                      </div>
                      <div className={styles.metadata}>
                        <ContentMediaMark platform={item.platform} />
                        <span className={styles.accountName} title={item.raw_account_name || "昵称缺失"}>{item.raw_account_name || "昵称缺失"}</span>
                        <time dateTime={item.published_at ?? undefined}>{contentDateTime(item.published_at)}</time>
                      </div>
                    </div>
                  </div>
                </td>
                <td className={styles.sellingCell}>
                  <div className={styles.sellingContent}>
                  <div className={styles.sellingPoint} data-matched={item.primary_selling_point_code ? "true" : undefined}>
                    <span title={item.primary_selling_point_code ? `${item.primary_selling_point_code} · ${sellingPointText(item)}` : undefined}>{sellingPointText(item)}</span>
                  </div>
                  <button type="button" className={styles.evidenceAction} disabled={saving} onClick={() => setEvidenceItem(item)}>查看依据</button>
                  </div>
                  {Boolean(item.evaluation_is_stale) && <span className="status-badge stale-evaluation">结果需更新</span>}
                </td>
                <td className={styles.numberCell} data-label="阅读" title={item.view_count == null ? "暂无可用数据" : undefined}>{contentMetric(item.view_count)}</td>
                <td className={styles.numberCell} data-label="评论" title={item.comment_count == null ? "暂无可用数据" : undefined}>{contentMetric(item.comment_count)}</td>
                <td className={styles.numberCell} data-label="点赞" title={item.like_count == null ? "暂无可用数据" : undefined}>{contentMetric(item.like_count)}</td>
                <td className={styles.classificationCell}><dl className={styles.classification}>
                  <div><dt>账号类型</dt><dd data-known={item.account_type && item.account_type !== "unknown" ? "true" : undefined}>{label(item.account_type)}</dd></div>
                  <div><dt>内容方向</dt><dd data-known={item.content_direction && item.content_direction !== "unknown" ? "true" : undefined}>{label(item.content_direction)}</dd></div>
                </dl></td>
                <td className={styles.actionCell}><button type="button" className={styles.detailButton} aria-label={`查看内容详情：${item.title || "标题缺失"}`} aria-haspopup="dialog" onClick={(event) => { detailTriggerRef.current = event.currentTarget; setDialogStatus({ error: "", message: "" }); setDetailSelection(item); }}>详情<CaretRightIcon size={16} aria-hidden="true" /></button></td>
              </tr>})}</tbody>
            </table>
          </section>)}
          {!items.length && <div className={styles.emptyState}><strong>暂无匹配内容</strong><p>试试调整搜索关键词或筛选条件。</p></div>}
          {items.length > 0 && <p className={styles.legend}>— 暂无可用数据</p>}
        </div>}
      {contentsReadFailed && <ReadErrorState title="内容读取失败" retrying={retrying} onRetry={retryContentsRead} />}
      {!contentsReadFailed && contentsQuery.data && <Pagination page={appliedRequest.page} pageSize={appliedRequest.page_size} total={total} busy={contentsQuery.isFetching || saving} ariaLabel="内容分页" onChange={(next) => applySearch({ page: next.page, pageSize: next.pageSize })} />}
      </article>
    </section>
    {recordsVisible && <ContentUpdateRecords jobs={contentUpdates.jobs} pending={contentUpdates.pending} reading={contentUpdates.reading} readError={contentUpdates.readError} onRefresh={contentUpdates.refresh} onRetrySubmission={contentUpdates.retrySubmission} onClose={closeUpdateRecords} closeLabel={evidenceItem ? "返回依据" : detailItem ? "返回详情" : mediaItem ? "返回预览" : "关闭"} onViewContent={() => { setDetailSelection(null); setEvidenceItem(null); setMediaItem(null); detailTriggerRef.current = null; }} />}
    {detailItem && !form && !evidenceItem && !recordsVisible && <ContentDetails item={detailItem} sellingPoint={sellingPointText(detailItem)} saving={saving} status={detailStatus} updateLocked={detailUpdateLocked} updateLabel={detailUpdateLabel} recordsAvailable={contentUpdates.available} onTasks={openUpdateTasks} onClose={() => { setDetailSelection(null); if (selectedContentId > 0) router.replace("/contents", { scroll: false }); }} onEvidence={() => setEvidenceItem(detailItem)} onEdit={() => edit(detailItem)} onUpdate={() => void updateData(detailItem)} />}
    {form && <ContentDialog title="修改内容" busy={saving} onClose={closeForm} status={dialogStatus} footer={<>
      <button type="button" className="secondary" disabled={saving} onClick={closeForm}>{detailSelection ? "返回详情" : "取消"}</button>
      <button type="button" className="primary" disabled={saving || contentUpdates.locked(form.id)} onClick={() => void save()}>{saving ? "保存中…" : "保存内容"}</button>
    </>}>
      <div className="modal-fields"><label>发布平台<select value={form.platform} onChange={(event) => setForm({ ...form, platform: event.target.value })}>{platformKeys.map((key) => <option key={key} value={key}>{label(key)}</option>)}</select></label><label>内容类型<select value={form.contentType} onChange={(event) => setForm({ ...form, contentType: event.target.value })}><option value="video">视频</option><option value="image">图文</option><option value="unknown">未知</option></select></label><label className="span-two">链接<input value={form.canonicalUrl} onChange={(event) => setForm({ ...form, canonicalUrl: event.target.value })} /></label><label>平台内容编号<input value={form.platformContentId} onChange={(event) => setForm({ ...form, platformContentId: event.target.value })} /></label><label>发布日期<input type="datetime-local" value={form.publishedAt} onChange={(event) => setForm({ ...form, publishedAt: event.target.value })} /></label><label className="span-two">标题<input value={form.title} onChange={(event) => setForm({ ...form, title: event.target.value })} /></label><label className="span-two">正文<textarea value={form.body} onChange={(event) => setForm({ ...form, body: event.target.value })} /></label><label>平台账号编号<input value={form.accountUid} onChange={(event) => setForm({ ...form, accountUid: event.target.value })} /></label><label>账号昵称<input value={form.accountName} onChange={(event) => setForm({ ...form, accountName: event.target.value })} /></label><label>账号类型<select value={form.accountType} onChange={(event) => setForm({ ...form, accountType: event.target.value })}><option value="unknown">未知</option><option value="boutique_ip">精品 IP</option><option value="original">原创</option><option value="mixed_edit">混剪</option></select></label><label>内容方向<select value={form.contentDirection} onChange={(event) => setForm({ ...form, contentDirection: event.target.value })}><option value="unknown">未知</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option></select></label></div>
    </ContentDialog>}

    {evidenceItem && !recordsVisible && <EvidenceModal key={evidenceItem.id} item={evidenceItem} returnToDetails={Boolean(detailSelection)} onUpdateTasks={openUpdateTasks} onClose={() => setEvidenceItem(null)} onChanged={invalidateContentData} onFeedback={feedback} />}
    {mediaItem && !recordsVisible && <ContentMediaModal key={mediaItem.id} item={mediaItem} action={resolveMediaAction(mediaItem)} onClose={() => setMediaItem(null)} />}
  </AppShell>;
}
