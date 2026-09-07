import type { ContentItem } from "./types";

// 内容列表媒体框的三级路径判定（纯函数，列表与弹窗共用）：
//   1. 本地有媒体 → 站内只读弹窗（请求一次 Evidence）
//   2. 没有本地媒体但是抖音视频 → 弹窗内嵌抖音官方播放器（0 个本站请求）
//   3. 其余 → 直接打开原帖（媒体框只带一个外链箭头角标，含义靠悬停提示与无障碍名称说清）
// 抖音官方播放器地址的 vid 就是数字作品 ID（实测 get_iframe_by_video 返回同一个值），
// 所以不需要任何后端接口，也不向 open.douyin.com 发服务端请求。
// 播放器只读这几个查询参数（2026-09-05 读 open.douyin.com 播放器脚本实测）：mode=mobile 固定竖版布局；
// width/height 支持 vw/vh，给 100vw/100vh 才会精确填满 iframe（不给时固定 324×672+48px 底条，iframe 更小就被裁）；
// autoplay 播放器根本不读，按官方嵌入模板保留 autoplay=0，视频需要点播放键。

export type MediaActionKind = "local" | "douyin_player" | "original";
export type MediaLabel = "播放" | "查看";

export type MediaAction = {
  kind: MediaActionKind;
  label: MediaLabel;
  href: string | null;
  playerUrl: string | null;
};

export type MediaActionInput = Pick<ContentItem, "platform" | "content_type" | "platform_content_id" | "canonical_url"> & {
  local_media_available?: boolean | null;
};

export const DOUYIN_PLAYER_ORIGIN = "https://open.douyin.com";
export const DOUYIN_ITEM_ID_PATTERN = /^\d{5,25}$/;

// 平台 Logo（public/ 下的官方 PNG），媒体框左上角使用；缺失的平台退回通用图标。
export const PLATFORM_LOGO_PATHS: Partial<Record<string, `/${string}`>> = {
  douyin: "/brand-douyin-official.png",
  xiaohongshu: "/brand-xiaohongshu-official.png",
  wechat_channels: "/brand-wechat-channels-official.png",
  kuaishou: "/brand-kuaishou-official.png",
};

export function douyinPlayerUrl(platformContentId: string | null | undefined): string | null {
  if (typeof platformContentId !== "string" || !DOUYIN_ITEM_ID_PATTERN.test(platformContentId)) return null;
  return `${DOUYIN_PLAYER_ORIGIN}/player/video?vid=${platformContentId}&autoplay=0&mode=mobile&width=100vw&height=100vh`;
}

export function mediaLabelFor(contentType: string | null | undefined): MediaLabel {
  return contentType === "video" ? "播放" : "查看";
}

// 第三级媒体框的文案：无障碍名称用短句，悬停提示补上"为什么要跳出去"。
// 入参是平台的中文名（调用方用 format.label 换好再传），本模块保持零依赖，方便 node 直接单测。
export function originalPostAction(platformName: string): string {
  return `去${platformName}查看原作品`;
}

export function originalPostHint(platformName: string): string {
  return `站内未保存这条内容的视频或图片，点击${originalPostAction(platformName)}`;
}

export function douyinPlayerEligible(item: Pick<MediaActionInput, "platform" | "content_type" | "platform_content_id">) {
  return item.platform === "douyin"
    && (item.content_type === "video" || item.content_type === "unknown")
    && douyinPlayerUrl(item.platform_content_id) !== null;
}

export function resolveMediaAction(item: MediaActionInput): MediaAction {
  // Older running APIs omit this field. Unknown is not proof that media is
  // absent: inspect Evidence only after a click, without preloading each row.
  if (item.local_media_available !== false) {
    return { kind: "local", label: mediaLabelFor(item.content_type), href: null, playerUrl: null };
  }
  const playerUrl = douyinPlayerEligible(item) ? douyinPlayerUrl(item.platform_content_id) : null;
  if (playerUrl) {
    return { kind: "douyin_player", label: "播放", href: null, playerUrl };
  }
  return { kind: "original", label: mediaLabelFor(item.content_type), href: item.canonical_url, playerUrl: null };
}
