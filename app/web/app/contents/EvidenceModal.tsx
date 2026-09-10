"use client";

import { useEffect, useRef, useState } from "react";
import { apiUrl, jsonRequest, readJson, readQueryJson, requestMediaRestore, type MediaRestorePurpose } from "../lib/api";
import { formatDate, formatDateTime, label } from "../lib/format";
import { mediaBlockerLabel, mediaBytes, mediaDeadlineText, mediaMemberKey, mediaMemberLabel, mediaPresentation } from "../lib/mediaLifecycle";
import type { ContentItem, EvidenceBundle } from "../lib/types";
import ContentDialog from "./ContentDialog";
import { useContentUpdateJobs } from "../components/ContentUpdateJobsProvider";

function evaluationText(evaluation: Record<string, unknown> | null, key: string, fallback = "—") {
  const value = evaluation?.[key];
  if (value == null || value === "") return fallback;
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "string" || typeof value === "number") return String(value);
  return fallback;
}

function evaluationScore(evaluation: Record<string, unknown> | null, key: string) {
  const value = evaluation?.[key];
  return typeof value === "number" ? `${value}%` : "暂不可计算";
}

const processingStatusLabels: Record<string, string> = {
  queued: "等待处理",
  pending: "等待处理",
  running: "处理中",
  success: "已完成",
  succeeded: "已完成",
  available: "已完成",
  evidence_ready: "已完成",
  partial: "部分完成",
  failed: "处理失败",
  retryable_failed: "处理失败，可以重试",
  terminal_failed: "处理失败",
  missing: "没有可用资料",
  skipped: "无需处理",
};

const processorLabels: Record<string, string> = {
  asr: "语音转写",
  video_asr: "语音转写",
  ocr: "画面文字识别",
  keyframe_ocr: "画面文字识别",
};

function processingStatus(value: string) {
  return processingStatusLabels[value] ?? "状态未知";
}

function processorName(value: string) {
  return processorLabels[value] ?? "媒体资料处理";
}

function evaluationSource(value: string) {
  const labels: Record<string, string> = {
    automatic: "系统自动评估",
    manual_review: "人工复核",
    migrated_from_v5: "历史结果",
  };
  return labels[value] ?? "评估方式未知";
}

function evidenceLevel(value: string) {
  const labels: Record<string, string> = {
    V3: "信息完整（V3）",
    V2: "有媒体资料（V2）",
    V1: "只有文字（V1）",
    V0: "资料不可用（V0）",
  };
  return labels[value] ?? "资料状态未知";
}

export default function EvidenceModal({ item, onClose, onChanged, onFeedback, onUpdateTasks, returnToDetails = false }: {
  item: ContentItem; onClose: () => void; onChanged: () => Promise<void>;
  onFeedback: (error: string, message?: string) => void;
  returnToDetails?: boolean;
  onUpdateTasks?: () => void;
}) {
  const [bundle, setBundle] = useState<EvidenceBundle | null>(null);
  const [busy, setBusy] = useState(false);
  const [loadError, setLoadError] = useState("");
  const [feedback, setFeedback] = useState({ error: "", message: "" });
  const [clock, setClock] = useState(Date.now);
  const [requiresRefresh, setRequiresRefresh] = useState(false);
  const actionRunning = useRef(false);
  const presentation = bundle ? mediaPresentation(bundle, clock) : null;
  const contentUpdates = useContentUpdateJobs();
  const updateWriteBlocked = contentUpdates.available && contentUpdates.locked(item.id);

  async function reload() {
    const next = await readQueryJson<EvidenceBundle>(`/api/v8/contents/${item.id}/evidence`);
    setBundle(next); setRequiresRefresh(false); setLoadError("");
  }
  function reportFeedback(error: string, message = "") {
    setFeedback({ error, message });
    onFeedback(error, message);
  }
  useEffect(() => {
    const timer = setInterval(() => setClock(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  useEffect(() => {
    let active = true;
    async function start() {
      setBundle(null); setLoadError(""); setFeedback({ error: "", message: "" });
      try {
        const next = await readQueryJson<EvidenceBundle>(`/api/v8/contents/${item.id}/evidence`);
        if (active) { setBundle(next); setRequiresRefresh(false); }
      } catch (reason) {
        if (active) {
          const message = reason instanceof Error ? reason.message : "内容依据加载失败，请稍后重试。";
          setLoadError(message); onFeedback(message);
        }
      }
    }
    void start();
    return () => { active = false; };
  }, [item.id, onFeedback]);

  useEffect(() => {
    if (!presentation?.restoring) return;
    let active = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    async function poll() {
      try {
        const next = await readQueryJson<EvidenceBundle>(`/api/v8/contents/${item.id}/evidence`);
        if (active) {
          setBundle(next); setRequiresRefresh(false);
          timer = setTimeout(() => void poll(), 5000);
        }
      } catch {
        // A read failure never creates another restore request.
        if (active) setRequiresRefresh(true);
      }
    }
    timer = setTimeout(() => void poll(), 5000);
    return () => { active = false; if (timer !== undefined) clearTimeout(timer); };
  }, [item.id, presentation?.restoring]);

  async function retryMedia() {
    if (!bundle || updateWriteBlocked || requiresRefresh || actionRunning.current || !mediaPresentation(bundle, Date.now()).canReprocess) return;
    actionRunning.current = true;
    setBusy(true);
    setFeedback({ error: "", message: "" });
    try {
      const result = await readJson<{ status: string; provider_cost?: number }>(`/api/v8/contents/${item.id}/media/retry`, jsonRequest({ allow_paid_refresh: false }));
      await reload(); await onChanged();
      reportFeedback("", `处理结果：${processingStatus(result.status)}；本次付费服务费用 $${Number(result.provider_cost ?? 0).toFixed(3)}`);
    } catch (reason) {
      setRequiresRefresh(true);
      try { await reload(); } catch { /* A GET failure must not retry the action. */ }
      reportFeedback(reason instanceof Error ? reason.message : "媒体重新处理失败，请稍后重试。");
    }
    finally { actionRunning.current = false; setBusy(false); }
  }

  async function restoreMedia(purpose: MediaRestorePurpose) {
    if (!bundle?.media_lifecycle || updateWriteBlocked || requiresRefresh || actionRunning.current || !mediaPresentation(bundle, Date.now()).canRestore) return;
    actionRunning.current = true;
    setBusy(true);
    setFeedback({ error: "", message: "" });
    try {
      const result = await requestMediaRestore(item.id, bundle.media_lifecycle.bundle_id, purpose);
      setRequiresRefresh(true);
      try { await reload(); } catch { /* The known receipt remains accepted; only read again. */ }
      reportFeedback("", `原件恢复已入队（作业 ${result.run_id}），不代表已恢复；不会调用收费抓取服务。${purpose === "reprocess" ? "恢复后请手动继续处理。" : ""}`);
    } catch (reason) {
      setRequiresRefresh(true);
      try { await reload(); } catch { /* Never automatically resend a possibly accepted POST. */ }
      reportFeedback(reason instanceof Error ? reason.message : "恢复未能入队，请检查本地原件状态。");
    }
    finally { actionRunning.current = false; setBusy(false); }
  }

  async function refreshState() {
    if (actionRunning.current) return;
    actionRunning.current = true; setBusy(true);
    setFeedback({ error: "", message: "" });
    try { await reload(); reportFeedback("", "已重新读取最新保存状态。"); }
    catch (reason) {
      const message = reason instanceof Error ? reason.message : "状态重读失败。";
      setRequiresRefresh(true);
      if (!bundle) setLoadError(message);
      reportFeedback(message);
    }
    finally { actionRunning.current = false; setBusy(false); }
  }

  return <ContentDialog title="查看依据" busy={busy} onClose={onClose} status={feedback} subtitle={<><strong title={item.title}>{item.title || "标题缺失"}</strong><span>内容依据 · {item.link_id} · {label(item.platform)} · {formatDate(item.published_at)} · {item.raw_account_name || item.raw_account_uid || "账号未知"}</span></>} footer={<button type="button" className="secondary" disabled={busy} onClick={onClose}>{returnToDetails ? "返回详情" : "关闭"}</button>}>
    {updateWriteBlocked && <p className="evidence-meta" role="status">该内容的更新状态尚未完成确认，暂时不能恢复或重新处理媒体。您可以继续查阅。<button type="button" className="secondary" onClick={onUpdateTasks ?? contentUpdates.openTasks}>查看更新记录</button></p>}
    {!bundle || !presentation ? loadError ? <div className="empty-state" role="alert"><strong>内容依据加载失败</strong><span>{loadError}</span><button type="button" className="secondary" disabled={busy} onClick={() => void refreshState()}>{busy ? "正在重新读取…" : "重新读取资料"}</button></div> : <div className="empty-state" role="status"><strong>正在读取已保存的资料</strong><span>这一步不会产生外部服务费用。</span></div> : <div className="evidence-layout">
      <article className="evidence-section"><div className="panel-head"><div><h3>原内容与媒体</h3><p><a href={bundle.content.canonical_url} target="_blank" rel="noreferrer">打开原链接</a> · 当前显示：{bundle.display_evaluation_id == null ? "暂无评估" : `第 ${bundle.display_evaluation_id} 次评估`}{bundle.evaluation_is_stale ? "（结果需更新）" : ""}</p></div><div className="placeholder-actions"><button className="secondary" disabled={busy || updateWriteBlocked || requiresRefresh || !presentation.canReprocess} onClick={() => void retryMedia()}>重新处理已保存的媒体</button>{presentation.canRestore && <><button className="primary" disabled={busy || updateWriteBlocked || requiresRefresh} onClick={() => void restoreMedia("evidence")}>恢复原件供查阅</button><button className="secondary" disabled={busy || updateWriteBlocked || requiresRefresh} onClick={() => void restoreMedia("reprocess")}>恢复原件供重处理</button></>}<button className="secondary" disabled title="必须建立独立的新媒体获取任务并完成授权">付费重新获取暂未开放</button><button className="secondary" disabled={busy} onClick={() => void refreshState()}>重新读取状态</button></div></div>
        <div className="evidence-meta" role="status"><strong>{presentation.title}</strong><p>{presentation.message}</p>
          {requiresRefresh && <p>最新状态尚未确认，恢复和处理操作已禁用。请重新读取状态，不要重复提交。</p>}
          {bundle.media_lifecycle && <><p>原件 {bundle.media_lifecycle.original_member_count} 份 · {mediaBytes(bundle.media_lifecycle.original_bytes)} · 归档核验：{formatDateTime(bundle.media_lifecycle.archive_verified_at)} · 删除期限：{formatDateTime(bundle.media_lifecycle.delete_due_at)} · 实际删除：{formatDateTime(bundle.media_lifecycle.deleted_at)}</p>
            <p>{mediaDeadlineText(bundle.media_lifecycle.delete_due_at, clock)}。归档核验后保留 72 小时，然后直接删除，无回收区。</p>
            {Object.keys(bundle.media_lifecycle.protections ?? {}).length > 0 && <p>存在保留保护项，需本地管理员核查；保护不延长恢复期限。</p>}
            {bundle.media_lifecycle.completion_gate_aged && <p>超过 14 天仍未完成证据门，已列入人工待办；不会自动删除。</p>}</>}
        </div>
        <p className="evidence-body">{bundle.content.body || "正文缺失"}</p>
        <section aria-label="保留预览"><h4>{presentation.isPreview ? "保留预览（非原件）" : "原件预览"}</h4>
          <div className="media-gallery">{presentation.gallery.map((media) => <figure key={mediaMemberKey(media)}>
            {media.kind === "video" ? <video className="evidence-media" src={apiUrl(media.url)} controls preload="metadata" /> :
              // Evidence is served by the API, not Next image optimization.
              // eslint-disable-next-line @next/next/no-img-element
              <img className="evidence-media" src={apiUrl(media.url)} alt={mediaMemberLabel(media, presentation.isPreview)} />}
            <figcaption>{mediaMemberLabel(media, presentation.isPreview)}</figcaption>
          </figure>)}{presentation.gallery.length === 0 && <div className="empty-state"><strong>当前没有可查看的预览</strong><span>请按上方原件状态处理；不会自动重新抓取或产生费用。</span></div>}</div>
        </section>
        <section aria-label="原件成员清单"><h4>原件成员清单</h4><div className="slot-list">{presentation.originalMembers.map((media) => <div key={mediaMemberKey(media)}><strong>{mediaMemberLabel(media)}</strong>{presentation.originalsAvailable && media.available !== false ? <a href={apiUrl(media.url)} target="_blank" rel="noreferrer">查看{mediaMemberLabel(media)}</a> : <span>{media.available === false ? "该原件当前不可用" : presentation.title}</span>}</div>)}</div>{presentation.originalMembers.length === 0 && <p className="empty-explanation">当前来源尚无原件成员清单。</p>}</section>
      </article>
      <article className="evidence-section"><h3>当前评估摘要</h3><p className="evidence-meta">{bundle.evaluation_is_stale ? "这条内容还没有按最新规则完成评估，目前展示的是旧结果。" : "这里显示的是按最新规则得到的结果；资料或规则更新后，系统会自动重新评估并保留历史结果。"}</p><div className="quality-grid"><div><strong>{bundle.display_evaluation_id == null ? "无" : `第 ${bundle.display_evaluation_id} 次`}</strong><span>当前评估</span></div><div><strong>{evaluationSource(evaluationText(bundle.evaluation, "evaluation_source", "unknown"))}</strong><span>评估方式</span></div><div><strong>{evidenceLevel(evaluationText(bundle.evaluation, "evidence_level", "unknown"))}</strong><span>资料完整度</span></div><div><strong>{evaluationText(bundle.evaluation, "primary_selling_point_id", "无卖点")}</strong><span>卖点编码</span></div><div><strong>{evaluationScore(bundle.evaluation, "selling_point_score")}</strong><span>卖点分</span></div><div><strong>{evaluationText(bundle.evaluation, "selling_point_included", "未知")}</strong><span>卖点是否计入</span></div><div><strong>{label(evaluationText(bundle.evaluation, "content_direction", "unknown"))}</strong><span>内容方向</span></div><div><strong>{evaluationScore(bundle.evaluation, "content_automotive_score")}</strong><span>内容垂直度</span></div><div><strong>{evaluationScore(bundle.evaluation, "audience_automotive_score")}</strong><span>互动用户垂直度</span></div><div><strong>{evaluationScore(bundle.evaluation, "acquisition_potential")}</strong><span>内容拉新效果预估</span></div></div></article>
      <div className="two-column evidence-columns">
        <article className="evidence-section"><h3>语音转写</h3><p className="evidence-meta">{processingStatus(bundle.asr.status)}</p><pre className="evidence-text">{bundle.asr.text || "暂时没有可用的语音文字。"}</pre></article>
        <article className="evidence-section"><h3>画面文字识别</h3><p className="evidence-meta">{processingStatus(bundle.ocr.status)} · 识别到 {bundle.ocr.observation_count} 处文字</p><pre className="evidence-text">{bundle.ocr.text || "暂时没有识别到画面文字。"}</pre></article>
      </div>
      <div className="two-column evidence-columns">
        <article className="evidence-section"><h3>评论摘要</h3><p className="evidence-meta">已保存 {bundle.comments.stored_count} 条 · 平台显示 {bundle.comments.declared_count ?? "未知"} 条 · {formatDate(bundle.comments.captured_at)}</p><ol className="comment-list">{bundle.comments.top_items.map((comment, index) => <li key={index}><span>{comment.body || "空评论"}</span><small>赞 {comment.like_count ?? "—"}</small></li>)}</ol>{bundle.comments.top_items.length === 0 && <p className="empty-explanation">暂时没有可用评论。</p>}</article>
        <article className="evidence-section"><h3>媒体处理记录</h3><div className="slot-list">{bundle.processing_slots.map((slot) => <div key={slot.id}><strong>{processorName(slot.processor_type)}</strong><span>{processingStatus(slot.status)} · 已尝试 {slot.attempt_count} 次</span>{slot.error_message && <small>{mediaBlockerLabel(slot.error_message)}</small>}</div>)}</div>{bundle.processing_slots.length === 0 && <p className="empty-explanation">还没有媒体处理记录。</p>}</article>
      </div>
    </div>}
  </ContentDialog>;
}
