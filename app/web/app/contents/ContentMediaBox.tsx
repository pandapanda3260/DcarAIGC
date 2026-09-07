"use client";

import Image from "next/image";
import { useState } from "react";
import { ArrowSquareOutIcon, ImagesIcon, PlayIcon, VideoCameraIcon } from "@phosphor-icons/react";
import { PLATFORM_LOGO_PATHS, originalPostAction, originalPostHint, resolveMediaAction } from "../lib/contentMedia";
import { label } from "../lib/format";
import { publicAssetPath } from "../lib/paths";
import { apiUrl } from "../lib/api";
import { thumbnailSources, type ContentThumbnail } from "../lib/contentThumbnails";
import type { ContentItem } from "../lib/types";

// 列表只加载现成图片，不加载视频或 Evidence；加载前或失败时使用纯色底。
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

function Thumbnail({ sources }: { sources: string[] }) {
  const [failed, setFailed] = useState<string[]>([]);
  const [loadedSource, setLoadedSource] = useState<string | null>(null);
  const source = sources.find((value) => !failed.includes(value));
  return <>
    {/* External covers are loaded directly by the browser, never by Next image optimization. */}
    {/* eslint-disable-next-line @next/next/no-img-element */}
    {source && <img key={source} className="content-thumbnail content-thumbnail-image" src={source.startsWith("/") ? apiUrl(source) : source} width={240} height={150} alt="" loading="lazy" decoding="async" referrerPolicy="no-referrer" data-loaded={loadedSource === source} onLoad={() => setLoadedSource(source)} onError={() => setFailed((values) => [...values, source])} />}
  </>;
}

export default function ContentMediaBox({ item, thumbnail, onOpen, showPlatformMark = true }: { item: ContentItem; thumbnail?: ContentThumbnail; onOpen: (item: ContentItem) => void; showPlatformMark?: boolean }) {
  const action = resolveMediaAction(item);
  const title = item.title || "标题缺失";
  const sources = thumbnailSources(item.id, thumbnail);
  const body = (
    <>
      <Thumbnail key={`${item.id}:${sources.join("|")}`} sources={sources} />
      {showPlatformMark && <ContentMediaMark platform={item.platform} />}
      <span className="content-media-glyph" aria-hidden="true">{action.label === "播放" ? <PlayIcon weight="fill" /> : <ImagesIcon weight="fill" />}</span>
      <span className="content-media-label">{action.label}</span>
      {action.kind === "original" && <span className="content-media-badge" aria-hidden="true"><ArrowSquareOutIcon weight="bold" /></span>}
    </>
  );
  if (action.kind === "original") {
    const platformName = label(item.platform);
    return (
      <a className="content-media-box" data-action={action.kind} href={action.href ?? item.canonical_url} target="_blank" rel="noreferrer" title={originalPostHint(platformName)} aria-label={`${originalPostAction(platformName)}：${title}`}>
        {body}
      </a>
    );
  }
  return (
    <button type="button" className="content-media-box" data-action={action.kind} onClick={() => onOpen(item)} aria-label={`${action.label}：${title}`}>
      {body}
    </button>
  );
}
