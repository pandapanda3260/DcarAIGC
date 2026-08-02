"use client";

import { useEffect, useState } from "react";
import { apiUrl, jsonRequest, readJson } from "../lib/api";
import { formatDate, label } from "../lib/format";
import type { ContentItem, EvidenceBundle } from "../lib/types";

type ReviewForm = {
  decision: "confirm" | "override" | "insufficient_evidence" | "terminal_unavailable";
  reason: string; reviewer: string; evidenceType: string; evidenceText: string;
  primaryCode: string; sellingScore: string; automotiveScore: string; contentDirection: string;
};

export default function EvidenceModal({ item, beginReview, onClose, onChanged, onFeedback }: {
  item: ContentItem; beginReview: boolean; onClose: () => void; onChanged: () => Promise<void>;
  onFeedback: (error: string, message?: string) => void;
}) {
  const [bundle, setBundle] = useState<EvidenceBundle | null>(null);
  const [busy, setBusy] = useState(false);
  const [reviewForm, setReviewForm] = useState<ReviewForm>({ decision: "insufficient_evidence", reason: "", reviewer: "本地复核员", evidenceType: "review_note", evidenceText: "", primaryCode: item.primary_selling_point_code ?? "", sellingScore: "", automotiveScore: item.content_automotive_score?.toString() ?? "", contentDirection: item.content_direction });

  async function reload() { setBundle(await readJson<EvidenceBundle>(`/api/v8/contents/${item.id}/evidence`)); }
  useEffect(() => {
    let active = true;
    async function start() {
      try {
        if (beginReview && item.review_queue_id && ["pending", "manual_required"].includes(item.review_status ?? "")) {
          await readJson(`/api/v8/reviews/${item.review_queue_id}/start`, { method: "POST" });
        }
        const next = await readJson<EvidenceBundle>(`/api/v8/contents/${item.id}/evidence`);
        if (active) setBundle(next);
      } catch (reason) { onFeedback(reason instanceof Error ? reason.message : "证据读取失败"); }
    }
    void start();
    return () => { active = false; };
  }, [beginReview, item.id, item.review_queue_id, item.review_status, onFeedback]);

  async function retryMedia(allowPaidRefresh: boolean) {
    if (allowPaidRefresh && !window.confirm("本次允许创建一次受幂等槽保护的付费详情刷新。确认继续？")) return;
    setBusy(true);
    try {
      const result = await readJson<{ status: string; provider_cost?: number }>(`/api/v8/contents/${item.id}/media/retry`, jsonRequest({ allow_paid_refresh: allowPaidRefresh }));
      await reload(); await onChanged();
      onFeedback("", `媒体处理状态：${result.status}；供应商费用 $${Number(result.provider_cost ?? 0).toFixed(3)}`);
    } catch (reason) { onFeedback(reason instanceof Error ? reason.message : "媒体补证失败"); }
    finally { setBusy(false); }
  }

  async function submitReview() {
    if (!item.review_queue_id || !bundle?.base_evaluation_id) return;
    setBusy(true);
    try {
      const override = reviewForm.decision === "override";
      await readJson(`/api/v8/reviews/${item.review_queue_id}/resolve`, jsonRequest({
        base_evaluation_id: bundle.base_evaluation_id,
        decision: reviewForm.decision, reason: reviewForm.reason, reviewer: reviewForm.reviewer,
        evidence_type: reviewForm.evidenceType, evidence_text: reviewForm.evidenceText,
        primary_selling_point_code: override && reviewForm.primaryCode ? reviewForm.primaryCode : null,
        selling_point_score: override && reviewForm.sellingScore ? Number(reviewForm.sellingScore) : null,
        selling_point_included: override ? Boolean(reviewForm.primaryCode) : null,
        content_automotive_score: override && reviewForm.automotiveScore ? Number(reviewForm.automotiveScore) : null,
        content_direction: override ? reviewForm.contentDirection : null,
      }));
      await onChanged(); onFeedback("", `${item.link_id} 人工复核已提交`); onClose();
    } catch (reason) {
      const text = reason instanceof Error ? reason.message : "人工复核提交失败";
      onFeedback(text.includes("刷新证据") ? `${text}；证据面板已自动刷新` : text);
      if (text.includes("刷新证据")) await reload().catch(() => undefined);
    } finally { setBusy(false); }
  }

  const reviewable = beginReview && item.review_queue_id && bundle?.review?.status === "in_review";
  return <div className="modal-backdrop task-detail-backdrop" role="presentation"><section className="review-modal evidence-modal" role="dialog" aria-modal="true" aria-label="内容证据与人工复核">
    <div className="panel-head"><div><span className="eyebrow">EVIDENCE WORKBENCH · {item.link_id}</span><h3>{item.title || "标题缺失"}</h3><p>{label(item.platform)} · {formatDate(item.published_at)} · {item.raw_account_name || item.raw_account_uid || "账号未知"}</p></div><button className="modal-close" onClick={onClose} aria-label="关闭">×</button></div>
    {!bundle ? <div className="empty-state"><strong>正在读取本地证据</strong><span>不会触发供应商调用。</span></div> : <div className="evidence-layout">
      <article className="evidence-section"><div className="panel-head"><div><h3>原内容与媒体</h3><p><a href={bundle.content.canonical_url} target="_blank" rel="noreferrer">打开原链接</a> · 当前评估 #{bundle.base_evaluation_id ?? "无"}</p></div><div className="placeholder-actions"><button className="secondary" disabled={busy} onClick={() => void retryMedia(false)}>重跑本地媒体</button><button className="secondary danger-button" disabled={busy} onClick={() => void retryMedia(true)}>付费刷新后补证</button></div></div>
        <p className="evidence-body">{bundle.content.body || "正文缺失"}</p>
        <div className="media-gallery">{bundle.media.map((media) => media.kind === "video" ? <video key={`${media.artifact_id}:${media.index}`} className="evidence-media" src={apiUrl(media.url)} controls preload="metadata" /> :
          // Evidence files are served by the local API and are not eligible for Next image optimization.
          // eslint-disable-next-line @next/next/no-img-element
          <img key={`${media.artifact_id}:${media.index}`} className="evidence-media" src={apiUrl(media.url)} alt={media.name} />)}{bundle.media.length === 0 && <div className="empty-state"><strong>本地媒体缺失</strong><span>先重跑本地媒体；确无源时再显式允许付费刷新。</span></div>}</div>
      </article>
      <div className="two-column evidence-columns">
        <article className="evidence-section"><h3>ASR 全文</h3><p className="evidence-meta">{bundle.asr.status} · {bundle.asr.model || "模型未知"}</p><pre className="evidence-text">{bundle.asr.text || "ASR 证据缺失"}</pre></article>
        <article className="evidence-section"><h3>OCR 全文</h3><p className="evidence-meta">{bundle.ocr.status} · {bundle.ocr.observation_count} 条观察</p><pre className="evidence-text">{bundle.ocr.text || "OCR 证据缺失"}</pre></article>
      </div>
      <div className="two-column evidence-columns">
        <article className="evidence-section"><h3>评论摘要</h3><p className="evidence-meta">已存 {bundle.comments.stored_count} 条 · 声明 {bundle.comments.declared_count ?? "未知"} 条 · {formatDate(bundle.comments.captured_at)}</p><ol className="comment-list">{bundle.comments.top_items.map((comment, index) => <li key={index}><span>{comment.body || "空评论"}</span><small>赞 {comment.like_count ?? "—"}</small></li>)}</ol>{bundle.comments.top_items.length === 0 && <p className="empty-explanation">评论证据缺失。</p>}</article>
        <article className="evidence-section"><h3>媒体处理槽</h3><div className="slot-list">{bundle.processing_slots.map((slot) => <div key={slot.id}><strong>{slot.processor_type}</strong><span>{slot.status} · {slot.processor_version} · 尝试 {slot.attempt_count}</span>{slot.error_message && <small>{slot.error_message}</small>}</div>)}</div>{bundle.processing_slots.length === 0 && <p className="empty-explanation">尚无媒体处理槽。</p>}</article>
      </div>
      {reviewable && <article className="evidence-section review-workbench"><h3>人工复核</h3><p className="evidence-meta">提交绑定当前评估 #{bundle.base_evaluation_id}；证据变化后旧页面会被 409 拒绝。</p><div className="review-fields"><label>复核结论<select value={reviewForm.decision} onChange={(event) => setReviewForm({ ...reviewForm, decision: event.target.value as ReviewForm["decision"] })}><option value="insufficient_evidence">证据不足</option><option value="confirm">确认自动结论</option><option value="override">人工改判</option><option value="terminal_unavailable">内容终态不可用</option></select></label><label>复核人员<input value={reviewForm.reviewer} onChange={(event) => setReviewForm({ ...reviewForm, reviewer: event.target.value })} /></label><label>证据类型<select value={reviewForm.evidenceType} onChange={(event) => setReviewForm({ ...reviewForm, evidenceType: event.target.value })}><option value="review_note">复核说明</option><option value="visual_summary">画面摘要</option><option value="media_observation">媒体观察</option></select></label><label>复核原因<input value={reviewForm.reason} onChange={(event) => setReviewForm({ ...reviewForm, reason: event.target.value })} /></label></div><label className="review-reason">人工证据<textarea value={reviewForm.evidenceText} onChange={(event) => setReviewForm({ ...reviewForm, evidenceText: event.target.value })} /></label>{reviewForm.decision === "override" && <div className="review-fields override-fields"><label>卖点编码<input value={reviewForm.primaryCode} onChange={(event) => setReviewForm({ ...reviewForm, primaryCode: event.target.value.toUpperCase() })} /></label><label>卖点分<input type="number" min="0" max="100" value={reviewForm.sellingScore} onChange={(event) => setReviewForm({ ...reviewForm, sellingScore: event.target.value })} /></label><label>垂直度<input type="number" min="0" max="100" value={reviewForm.automotiveScore} onChange={(event) => setReviewForm({ ...reviewForm, automotiveScore: event.target.value })} /></label><label>内容方向<select value={reviewForm.contentDirection} onChange={(event) => setReviewForm({ ...reviewForm, contentDirection: event.target.value })}><option value="unknown">未知</option><option value="new_car">新车</option><option value="used_car">二手车</option><option value="media">媒体</option><option value="other">其他</option></select></label></div>}<div className="modal-actions"><button className="primary" disabled={busy || !reviewForm.reason.trim() || !reviewForm.evidenceText.trim()} onClick={() => void submitReview()}>{busy ? "提交中" : "提交复核"}</button></div></article>}
    </div>}
  </section></div>;
}
