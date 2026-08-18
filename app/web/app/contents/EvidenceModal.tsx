"use client";

import { useEffect, useState } from "react";
import { apiUrl, jsonRequest, readJson } from "../lib/api";
import { formatDate, label } from "../lib/format";
import type { ContentItem, EvidenceBundle } from "../lib/types";

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

export default function EvidenceModal({ item, onClose, onChanged, onFeedback }: {
  item: ContentItem; onClose: () => void; onChanged: () => Promise<void>;
  onFeedback: (error: string, message?: string) => void;
}) {
  const [bundle, setBundle] = useState<EvidenceBundle | null>(null);
  const [busy, setBusy] = useState(false);

  async function reload() { setBundle(await readJson<EvidenceBundle>(`/api/v8/contents/${item.id}/evidence`)); }
  useEffect(() => {
    let active = true;
    async function start() {
      try {
        const next = await readJson<EvidenceBundle>(`/api/v8/contents/${item.id}/evidence`);
        if (active) setBundle(next);
      } catch (reason) { onFeedback(reason instanceof Error ? reason.message : "证据读取失败"); }
    }
    void start();
    return () => { active = false; };
  }, [item.id, onFeedback]);

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

  return <div className="modal-backdrop task-detail-backdrop" role="presentation"><section className="modal-panel evidence-modal" role="dialog" aria-modal="true" aria-label="内容证据">
    <div className="panel-head"><div><span className="eyebrow">证据工作台 · {item.link_id}</span><h3>{item.title || "标题缺失"}</h3><p>{label(item.platform)} · {formatDate(item.published_at)} · {item.raw_account_name || item.raw_account_uid || "账号未知"}</p></div><button className="modal-close" onClick={onClose} aria-label="关闭">×</button></div>
    {!bundle ? <div className="empty-state"><strong>正在读取本地证据</strong><span>不会触发供应商调用。</span></div> : <div className="evidence-layout">
      <article className="evidence-section"><div className="panel-head"><div><h3>原内容与媒体</h3><p><a href={bundle.content.canonical_url} target="_blank" rel="noreferrer">打开原链接</a> · 展示评估 #{bundle.display_evaluation_id ?? "无"}{bundle.evaluation_is_stale ? "（旧规则，已过时）" : ""}</p></div><div className="placeholder-actions"><button className="secondary" disabled={busy} onClick={() => void retryMedia(false)}>重跑本地媒体</button><button className="secondary danger-button" disabled={busy} onClick={() => void retryMedia(true)}>付费刷新后补证</button></div></div>
        <p className="evidence-body">{bundle.content.body || "正文缺失"}</p>
        <div className="media-gallery">{bundle.media.map((media) => media.kind === "video" ? <video key={`${media.artifact_id}:${media.index}`} className="evidence-media" src={apiUrl(media.url)} controls preload="metadata" /> :
          // Evidence files are served by the local API and are not eligible for Next image optimization.
          // eslint-disable-next-line @next/next/no-img-element
          <img key={`${media.artifact_id}:${media.index}`} className="evidence-media" src={apiUrl(media.url)} alt={media.name} />)}{bundle.media.length === 0 && <div className="empty-state"><strong>{bundle.media_availability.status === "omitted" ? "线上大媒体未发布" : "本地媒体缺失"}</strong><span>{bundle.media_availability.reason || "先重跑本地媒体；确无源时再显式允许付费刷新。"}</span></div>}</div>
      </article>
      <article className="evidence-section"><h3>当前评估摘要</h3><p className="evidence-meta">{bundle.evaluation_is_stale ? "当前 release 尚无评估，暂时展示旧规则结论并明确标记为已过时。" : "只读展示当前 release 的有效评估；证据或规则变化后系统会自动重新评估并追加新版本。"}</p><div className="quality-grid"><div><strong>#{bundle.display_evaluation_id ?? "无"}</strong><span>展示评估版本</span></div><div><strong>{evaluationText(bundle.evaluation, "evaluation_source", "来源缺失")}</strong><span>评估来源</span></div><div><strong>{evaluationText(bundle.evaluation, "evidence_level", "证据缺失")}</strong><span>证据等级</span></div><div><strong>{evaluationText(bundle.evaluation, "primary_selling_point_id", "无卖点")}</strong><span>卖点编码</span></div><div><strong>{evaluationScore(bundle.evaluation, "selling_point_score")}</strong><span>卖点分</span></div><div><strong>{evaluationText(bundle.evaluation, "selling_point_included", "未知")}</strong><span>卖点是否计入</span></div><div><strong>{label(evaluationText(bundle.evaluation, "content_direction", "unknown"))}</strong><span>内容方向</span></div><div><strong>{evaluationScore(bundle.evaluation, "content_automotive_score")}</strong><span>内容垂直度</span></div><div><strong>{evaluationScore(bundle.evaluation, "audience_automotive_score")}</strong><span>互动用户垂直度</span></div><div><strong>{evaluationScore(bundle.evaluation, "acquisition_potential")}</strong><span>内容拉新效果预估</span></div></div></article>
      <div className="two-column evidence-columns">
        <article className="evidence-section"><h3>ASR 全文</h3><p className="evidence-meta">{bundle.asr.status} · {bundle.asr.model || "模型未知"}</p><pre className="evidence-text">{bundle.asr.text || "ASR 证据缺失"}</pre></article>
        <article className="evidence-section"><h3>OCR 全文</h3><p className="evidence-meta">{bundle.ocr.status} · {bundle.ocr.observation_count} 条观察</p><pre className="evidence-text">{bundle.ocr.text || "OCR 证据缺失"}</pre></article>
      </div>
      <div className="two-column evidence-columns">
        <article className="evidence-section"><h3>评论摘要</h3><p className="evidence-meta">已存 {bundle.comments.stored_count} 条 · 声明 {bundle.comments.declared_count ?? "未知"} 条 · {formatDate(bundle.comments.captured_at)}</p><ol className="comment-list">{bundle.comments.top_items.map((comment, index) => <li key={index}><span>{comment.body || "空评论"}</span><small>赞 {comment.like_count ?? "—"}</small></li>)}</ol>{bundle.comments.top_items.length === 0 && <p className="empty-explanation">评论证据缺失。</p>}</article>
        <article className="evidence-section"><h3>媒体处理槽</h3><div className="slot-list">{bundle.processing_slots.map((slot) => <div key={slot.id}><strong>{slot.processor_type}</strong><span>{slot.status} · {slot.processor_version} · 尝试 {slot.attempt_count}</span>{slot.error_message && <small>{slot.error_message}</small>}</div>)}</div>{bundle.processing_slots.length === 0 && <p className="empty-explanation">尚无媒体处理槽。</p>}</article>
      </div>
    </div>}
  </section></div>;
}
