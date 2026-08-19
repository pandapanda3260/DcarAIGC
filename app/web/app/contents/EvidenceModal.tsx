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

const processingStatusLabels: Record<string, string> = {
  queued: "等待处理",
  pending: "等待处理",
  running: "处理中",
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
      } catch (reason) { onFeedback(reason instanceof Error ? reason.message : "内容依据加载失败，请稍后重试。"); }
    }
    void start();
    return () => { active = false; };
  }, [item.id, onFeedback]);

  async function retryMedia(allowPaidRefresh: boolean) {
    if (allowPaidRefresh && !window.confirm("这次操作会调用付费服务并产生费用，同一内容只会提交一次。确定继续吗？")) return;
    setBusy(true);
    try {
      const result = await readJson<{ status: string; provider_cost?: number }>(`/api/v8/contents/${item.id}/media/retry`, jsonRequest({ allow_paid_refresh: allowPaidRefresh }));
      await reload(); await onChanged();
      onFeedback("", `处理结果：${processingStatus(result.status)}；本次付费服务费用 $${Number(result.provider_cost ?? 0).toFixed(3)}`);
    } catch (reason) { onFeedback(reason instanceof Error ? reason.message : "媒体重新处理失败，请稍后重试。"); }
    finally { setBusy(false); }
  }

  return <div className="modal-backdrop task-detail-backdrop" role="presentation"><section className="modal-panel evidence-modal" role="dialog" aria-modal="true" aria-label="内容证据">
    <div className="panel-head"><div><span className="eyebrow">内容依据 · {item.link_id}</span><h3>{item.title || "标题缺失"}</h3><p>{label(item.platform)} · {formatDate(item.published_at)} · {item.raw_account_name || item.raw_account_uid || "账号未知"}</p></div><button className="modal-close" onClick={onClose} aria-label="关闭">×</button></div>
    {!bundle ? <div className="empty-state"><strong>正在读取已保存的资料</strong><span>这一步不会产生外部服务费用。</span></div> : <div className="evidence-layout">
      <article className="evidence-section"><div className="panel-head"><div><h3>原内容与媒体</h3><p><a href={bundle.content.canonical_url} target="_blank" rel="noreferrer">打开原链接</a> · 当前显示：{bundle.display_evaluation_id == null ? "暂无评估" : `第 ${bundle.display_evaluation_id} 次评估`}{bundle.evaluation_is_stale ? "（结果需更新）" : ""}</p></div><div className="placeholder-actions"><button className="secondary" disabled={busy} onClick={() => void retryMedia(false)}>重新处理已保存的媒体</button><button className="secondary danger-button" disabled={busy} onClick={() => void retryMedia(true)}>付费重新获取媒体资料</button></div></div>
        <p className="evidence-body">{bundle.content.body || "正文缺失"}</p>
        <div className="media-gallery">{bundle.media.map((media) => media.kind === "video" ? <video key={`${media.artifact_id}:${media.index}`} className="evidence-media" src={apiUrl(media.url)} controls preload="metadata" /> :
          // Evidence files are served by the local API and are not eligible for Next image optimization.
          // eslint-disable-next-line @next/next/no-img-element
          <img key={`${media.artifact_id}:${media.index}`} className="evidence-media" src={apiUrl(media.url)} alt={media.name} />)}{bundle.media.length === 0 && <div className="empty-state"><strong>{bundle.media_availability.status === "omitted" ? "视频或图片暂未提供" : "没有找到已保存的视频或图片"}</strong><span>请先尝试重新处理已保存的媒体；如果仍然缺失，再使用付费重新获取。</span></div>}</div>
      </article>
      <article className="evidence-section"><h3>当前评估摘要</h3><p className="evidence-meta">{bundle.evaluation_is_stale ? "这条内容还没有按最新规则完成评估，目前展示的是旧结果。" : "这里显示的是按最新规则得到的结果；资料或规则更新后，系统会自动重新评估并保留历史结果。"}</p><div className="quality-grid"><div><strong>{bundle.display_evaluation_id == null ? "无" : `第 ${bundle.display_evaluation_id} 次`}</strong><span>当前评估</span></div><div><strong>{evaluationSource(evaluationText(bundle.evaluation, "evaluation_source", "unknown"))}</strong><span>评估方式</span></div><div><strong>{evidenceLevel(evaluationText(bundle.evaluation, "evidence_level", "unknown"))}</strong><span>资料完整度</span></div><div><strong>{evaluationText(bundle.evaluation, "primary_selling_point_id", "无卖点")}</strong><span>卖点编码</span></div><div><strong>{evaluationScore(bundle.evaluation, "selling_point_score")}</strong><span>卖点分</span></div><div><strong>{evaluationText(bundle.evaluation, "selling_point_included", "未知")}</strong><span>卖点是否计入</span></div><div><strong>{label(evaluationText(bundle.evaluation, "content_direction", "unknown"))}</strong><span>内容方向</span></div><div><strong>{evaluationScore(bundle.evaluation, "content_automotive_score")}</strong><span>内容垂直度</span></div><div><strong>{evaluationScore(bundle.evaluation, "audience_automotive_score")}</strong><span>互动用户垂直度</span></div><div><strong>{evaluationScore(bundle.evaluation, "acquisition_potential")}</strong><span>内容拉新效果预估</span></div></div></article>
      <div className="two-column evidence-columns">
        <article className="evidence-section"><h3>语音转写</h3><p className="evidence-meta">{processingStatus(bundle.asr.status)}</p><pre className="evidence-text">{bundle.asr.text || "暂时没有可用的语音文字。"}</pre></article>
        <article className="evidence-section"><h3>画面文字识别</h3><p className="evidence-meta">{processingStatus(bundle.ocr.status)} · 识别到 {bundle.ocr.observation_count} 处文字</p><pre className="evidence-text">{bundle.ocr.text || "暂时没有识别到画面文字。"}</pre></article>
      </div>
      <div className="two-column evidence-columns">
        <article className="evidence-section"><h3>评论摘要</h3><p className="evidence-meta">已保存 {bundle.comments.stored_count} 条 · 平台显示 {bundle.comments.declared_count ?? "未知"} 条 · {formatDate(bundle.comments.captured_at)}</p><ol className="comment-list">{bundle.comments.top_items.map((comment, index) => <li key={index}><span>{comment.body || "空评论"}</span><small>赞 {comment.like_count ?? "—"}</small></li>)}</ol>{bundle.comments.top_items.length === 0 && <p className="empty-explanation">暂时没有可用评论。</p>}</article>
        <article className="evidence-section"><h3>媒体处理记录</h3><div className="slot-list">{bundle.processing_slots.map((slot) => <div key={slot.id}><strong>{processorName(slot.processor_type)}</strong><span>{processingStatus(slot.status)} · 已尝试 {slot.attempt_count} 次</span>{slot.error_message && <small>处理没有完成，请重新尝试；如果一直失败，请联系管理员。</small>}</div>)}</div>{bundle.processing_slots.length === 0 && <p className="empty-explanation">还没有媒体处理记录。</p>}</article>
      </div>
    </div>}
  </section></div>;
}
