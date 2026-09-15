"use client";

import Image from "next/image";
import { useRef, useState } from "react";
import { ArrowSquareOutIcon, ImagesIcon, PlayIcon, VideoCameraIcon } from "@phosphor-icons/react";
import { PLATFORM_LOGO_PATHS, originalPostAction, originalPostHint, resolveMediaAction } from "../lib/contentMedia";
import { label } from "../lib/format";
import { publicAssetPath } from "../lib/paths";
import { thumbnailMissingHint, thumbnailSources, type ContentThumbnail } from "../lib/contentThumbnails";
import type { ContentItem } from "../lib/types";

// 列表只加载线上封面；加载前或失败时使用纯色底。
// 封面来源独立于播放路径，不触发外部采集或产生新的磁盘图片。
// 第三级（原帖）直接渲染成新标签链接而不是按钮：语义正确、不被弹窗拦截、0 个本站请求。
// 它的角标只放一个外链箭头（与详情弹窗标题旁的同款），不写「原帖」这类词；去哪、为什么跳出去由 title 悬停提示说明。

export function ContentMediaMark({ platform }: { platform: string }) {
  const logoPath = PLATFORM_LOGO_PATHS[platform];
  return (
    <span className="content-media-mark" data-official-logo={logoPath ? "true" : undefined} role="img" aria-label={`${label(platform)}平台`} title={label(platform)}>
      {logoPath ? <Image src={publicAssetPath(logoPath)} alt="" width={14} height={14} unoptimized /> : <VideoCameraIcon weight="fill" aria-hidden="true" />}
    </span>
  );
}

type ContentMediaBoxProps = { item: ContentItem; thumbnail?: ContentThumbnail; onOpen: (item: ContentItem) => void; showPlatformMark?: boolean };

function MediaBoxWithThumbnail({ item, thumbnail, onOpen, showPlatformMark = true, sources }: ContentMediaBoxProps & { sources: string[] }) {
  const [attempt, setAttempt] = useState(0);
  const activeAttempt = useRef(0);
  const [loaded, setLoaded] = useState(false);
  const action = resolveMediaAction(item);
  const title = item.title || "标题缺失";
  const source = sources[attempt];
  const missingHint = sources.length ? (source ? null : "封面加载失败") : thumbnailMissingHint(thumbnail);
  const hint = (actionHint: string) => missingHint ? `${actionHint}；${missingHint}` : actionHint;
  function handleError() {
    if (activeAttempt.current !== attempt) return;
    // Advance synchronously so repeated or late events cannot skip a candidate.
    activeAttempt.current = attempt + 1;
    setLoaded(false);
    setAttempt(attempt + 1);
  }
  const body = (
    <>
      {/* External covers are loaded directly by the browser, never by Next image optimization. */}
      {/* eslint-disable-next-line @next/next/no-img-element */}
      {source && <img key={source} className="content-thumbnail content-thumbnail-image" src={source} width={240} height={150} alt="" loading="lazy" decoding="async" referrerPolicy="no-referrer" data-loaded={loaded} onLoad={() => { if (activeAttempt.current === attempt) setLoaded(true); }} onError={handleError} />}
      {showPlatformMark && <ContentMediaMark platform={item.platform} />}
      {action.kind !== "unavailable" && <span className="content-media-glyph" aria-hidden="true">{action.label === "播放" ? <PlayIcon weight="fill" /> : <ImagesIcon weight="fill" />}</span>}
      <span className="content-media-label">{action.label}</span>
      {action.kind === "original" && <span className="content-media-badge" aria-hidden="true"><ArrowSquareOutIcon weight="bold" /></span>}
    </>
  );
  if (action.kind === "unavailable") {
    return <div className="content-media-box" data-action="unavailable" title={hint(action.label)} aria-label={`${title}：${action.label}`}>{body}</div>;
  }
  if (action.kind === "original") {
    const platformName = label(item.platform);
    return (
      <a className="content-media-box" data-action={action.kind} href={action.href!} target="_blank" rel="noreferrer" title={hint(originalPostHint(platformName))} aria-label={`${originalPostAction(platformName)}：${title}`}>
        {body}
      </a>
    );
  }
  return (
    <button type="button" className="content-media-box" data-action={action.kind} onClick={() => onOpen(item)} title={hint(action.label)} aria-label={`${action.label}：${title}`}>
      {body}
    </button>
  );
}

export default function ContentMediaBox(props: ContentMediaBoxProps) {
  const sources = thumbnailSources(props.thumbnail);
  // New content or any changed candidate resets attempts, even if the first URL
  // stays unchanged. Events retained by an old image cannot affect the new box.
  return <MediaBoxWithThumbnail key={JSON.stringify([props.item.id, sources])} {...props} sources={sources} />;
}
