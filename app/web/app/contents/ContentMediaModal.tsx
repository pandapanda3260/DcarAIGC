"use client";

import { useEffect, useRef, useState, type CSSProperties } from "react";
import { CaretLeftIcon, CaretRightIcon } from "@phosphor-icons/react";
import { apiUrl, readQueryJson } from "../lib/api";
import { douyinPlayerEligible, douyinPlayerUrl, type MediaAction } from "../lib/contentMedia";
import { formatDate, label } from "../lib/format";
import { mediaMemberKey, mediaMemberLabel, mediaPlaybackPresentation } from "../lib/mediaLifecycle";
import type { ContentItem, EvidenceBundle, EvidenceMedia } from "../lib/types";
import { useDialogFocus } from "../lib/useDialogFocus";

// 只读的媒体弹窗：Evidence 返回后优先播放可读原件，不可读时显示绑定到同一来源的保留预览；
// 没有本地媒体的抖音视频直接内嵌官方播放器（0 个本站请求）。这里没有恢复、重处理、付费等写操作，
// 全部状态提示都是弹窗内的行内提示，不发 toast。
// 弹窗尺寸跟内容走：本地媒体在 loadedmetadata / load 后把宽高比写进 --media-ratio，面板宽度随之变化
// （加载前默认 9:16，库里绝大多数是竖屏）；抖音播放器固定竖版，尺寸规则在 globals.css 的 [data-stage="player"]。
// DOM 顺序是 标题 → 舞台 → 工具栏 → 操作区（打开原帖、关闭），操作区靠 CSS grid 钉在右上角：
// 跨源 iframe 里的键盘事件到不了父文档，Tab 从播放器出来必须先落到父文档自己的控件上。

const DEFAULT_RATIO = 9 / 16;

type Stage =
  | { kind: "loading" }
  | { kind: "local"; gallery: EvidenceMedia[]; isPreview: boolean }
  | { kind: "player"; url: string }
  | { kind: "unavailable"; title: string; message: string }
  | { kind: "error"; message: string };

function mediaRatio(width: number, height: number) {
  return width > 0 && height > 0 ? width / height : DEFAULT_RATIO;
}

function MediaFailure() {
  return <div className="empty-state"><strong>文件无法读取</strong><span>请打开原帖查看。</span></div>;
}

// 视频成员按 key 挂载/卸载：切换或关闭时先暂停并卸掉 src，确保停声、停止网络读取。
function VideoMember({ src, alt, onError, onMeasure }: { src: string; alt: string; onError: () => void; onMeasure: (ratio: number) => void }) {
  const videoRef = useRef<HTMLVideoElement | null>(null);
  useEffect(() => {
    const video = videoRef.current;
    return () => {
      if (!video) return;
      video.pause();
      video.removeAttribute("src");
      video.load();
    };
  }, []);
  return <video ref={videoRef} className="content-media-video" src={src} controls autoPlay playsInline preload="metadata" aria-label={alt} onError={onError}
    onLoadedMetadata={(event) => onMeasure(mediaRatio(event.currentTarget.videoWidth, event.currentTarget.videoHeight))} />;
}

export default function ContentMediaModal({ item, action, onClose }: { item: ContentItem; action: MediaAction; onClose: () => void }) {
  const panelRef = useRef<HTMLElement | null>(null);
  // 结果带上请求序号：点"重新读取"只需递增序号，旧结果自然失效，效果里不做同步 setState。
  const [result, setResult] = useState<{ token: number; bundle: EvidenceBundle | null; error: string } | null>(null);
  const [reloadToken, setReloadToken] = useState(0);
  const [index, setIndex] = useState(0);
  const [failedMembers, setFailedMembers] = useState<string[]>([]);
  const [ratios, setRatios] = useState<Record<string, number>>({});
  useDialogFocus(true, panelRef, onClose);

  const needsEvidence = action.kind === "local";
  useEffect(() => {
    if (!needsEvidence) return;
    let active = true;
    const token = reloadToken;
    readQueryJson<EvidenceBundle>(`/api/v8/contents/${item.id}/evidence`)
      .then((next) => { if (active) setResult({ token, bundle: next, error: "" }); })
      .catch((reason) => { if (active) setResult({ token, bundle: null, error: reason instanceof Error ? reason.message : "内容资料读取失败，请稍后重试。" }); });
    return () => { active = false; };
  }, [item.id, needsEvidence, reloadToken]);
  const bundle = result?.token === reloadToken ? result.bundle : null;
  const error = result?.token === reloadToken ? result.error : "";

  // 弹窗里可以退到抖音播放器的条件与列表判定一致（没有本地媒体时的第二级）。
  const playerUrl = action.playerUrl ?? (douyinPlayerEligible(item) ? douyinPlayerUrl(item.platform_content_id) : null);
  let stage: Stage;
  if (!needsEvidence) {
    stage = playerUrl ? { kind: "player", url: playerUrl } : { kind: "unavailable", title: "暂无可查看的媒体", message: "可打开原帖查看。" };
  } else if (error) {
    stage = { kind: "error", message: error };
  } else if (!bundle) {
    stage = { kind: "loading" };
  } else {
    const presentation = mediaPlaybackPresentation(bundle);
    if (presentation.gallery.length > 0) stage = { kind: "local", gallery: presentation.gallery, isPreview: presentation.isPreview };
    else if (playerUrl) stage = { kind: "player", url: playerUrl };
    else stage = { kind: "unavailable", title: presentation.title, message: presentation.message || "可打开原帖查看。" };
  }

  const gallery = stage.kind === "local" ? stage.gallery : [];
  const currentIndex = Math.min(index, Math.max(0, gallery.length - 1));
  const current = gallery[currentIndex];
  const canNavigate = gallery.length > 1;

  useEffect(() => {
    if (!canNavigate) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.target instanceof HTMLElement && /^(INPUT|TEXTAREA|SELECT)$/.test(event.target.tagName)) return;
      if (event.key === "ArrowLeft") { event.preventDefault(); setIndex((value) => Math.max(0, value - 1)); }
      else if (event.key === "ArrowRight") { event.preventDefault(); setIndex((value) => Math.min(gallery.length - 1, value + 1)); }
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [canNavigate, gallery.length]);

  const title = item.title || "标题缺失";
  const markFailed = (media: EvidenceMedia) => setFailedMembers((value) => value.includes(mediaMemberKey(media)) ? value : [...value, mediaMemberKey(media)]);
  const measure = (media: EvidenceMedia, ratio: number) => setRatios((value) => value[mediaMemberKey(media)] === ratio ? value : { ...value, [mediaMemberKey(media)]: ratio });
  const ratio = current ? ratios[mediaMemberKey(current)] ?? DEFAULT_RATIO : DEFAULT_RATIO;
  const panelStyle = { "--media-ratio": ratio.toFixed(4) } as CSSProperties;
  // 工具栏只在有话可说时出现：多成员轮播（切换 + 计数）或正在显示保留预览；单个原件、播放器态都没有工具栏。
  const localStage = stage.kind === "local" ? stage : null;

  return <div className="modal-backdrop content-media-backdrop" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget) onClose(); }}>
    <section ref={panelRef} className="modal-panel content-media-modal" role="dialog" aria-modal="true" aria-labelledby="content-media-title" tabIndex={-1} data-stage={stage.kind} style={panelStyle}>
      <div className="content-media-head">
        <span className="eyebrow">内容媒体 · {item.link_id}</span>
        <h3 id="content-media-title" title={title}>{title}</h3>
        <p>{label(item.platform)} · {formatDate(item.published_at)} · {item.raw_account_name || item.raw_account_uid || "账号未知"}</p>
      </div>
      <div className="content-media-stage" data-stage={stage.kind}>
        {stage.kind === "loading" && <div className="empty-state"><strong>正在读取已保存的资料</strong><span>这一步不会产生外部服务费用。</span></div>}
        {stage.kind === "error" && <div className="empty-state"><strong>内容资料读取失败</strong><span>{stage.message}</span><button type="button" className="secondary read-error-retry" onClick={() => setReloadToken((value) => value + 1)}>重新读取</button></div>}
        {stage.kind === "unavailable" && <div className="empty-state"><strong>{stage.title}</strong><span>{stage.message}</span><span>可打开原帖查看。</span></div>}
        {stage.kind === "player" && <iframe className="content-media-player" src={stage.url} title="抖音官方播放器" aria-describedby="content-media-player-help" allow="autoplay; fullscreen" referrerPolicy="unsafe-url" allowFullScreen />}
        {stage.kind === "local" && current && (failedMembers.includes(mediaMemberKey(current)) ? <MediaFailure /> : current.kind === "video"
          ? <VideoMember key={mediaMemberKey(current)} src={apiUrl(current.url)} alt={mediaMemberLabel(current, stage.isPreview)} onError={() => markFailed(current)} onMeasure={(value) => measure(current, value)} />
          // Evidence is served by the API, not Next image optimization.
          // eslint-disable-next-line @next/next/no-img-element
          : <img key={mediaMemberKey(current)} className="content-media-image" src={apiUrl(current.url)} alt={mediaMemberLabel(current, stage.isPreview)} onError={() => markFailed(current)}
            onLoad={(event) => measure(current, mediaRatio(event.currentTarget.naturalWidth, event.currentTarget.naturalHeight))} />)}
      </div>
      {stage.kind === "player" && <p id="content-media-player-help" className="content-media-player-help">抖音可能限制站外播放。若播放器空白或无法播放，请<a href={item.canonical_url} target="_blank" rel="noreferrer">打开原帖观看</a>。<span className="visually-hidden">播放器内的键盘操作由抖音播放器处理；按 Tab 回到弹窗后，可按 Esc 关闭。</span></p>}
      {localStage && current && (canNavigate || localStage.isPreview) && <div className="content-media-toolbar">
        <span aria-live="polite">{mediaMemberLabel(current, localStage.isPreview)}{canNavigate && ` · 第 ${currentIndex + 1} 个 / 共 ${gallery.length} 个`}{localStage.isPreview && " · 原件已归档或不可用，显示保留预览"}</span>
        {canNavigate && <span className="content-media-nav">
          <button type="button" className="secondary" onClick={() => setIndex(Math.max(0, currentIndex - 1))} disabled={currentIndex === 0}><CaretLeftIcon aria-hidden="true" /> 上一个</button>
          <button type="button" className="secondary" onClick={() => setIndex(Math.min(gallery.length - 1, currentIndex + 1))} disabled={currentIndex >= gallery.length - 1}>下一个 <CaretRightIcon aria-hidden="true" /></button>
        </span>}
      </div>}
      <div className="content-media-actions">
        <a className="secondary button-link" href={item.canonical_url} target="_blank" rel="noreferrer">打开原帖</a>
        <button type="button" className="modal-close" onClick={onClose} aria-label="关闭">×</button>
      </div>
    </section>
  </div>;
}
