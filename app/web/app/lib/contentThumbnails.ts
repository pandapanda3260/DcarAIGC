export type ContentThumbnailReason = "not_found" | "unsupported_format" | "source_unavailable";
export type ContentThumbnail = {
  remote_url: string | null;
  remote_urls?: string[];
  reason?: ContentThumbnailReason | null;
};
export type ContentThumbnails = { items: Record<string, ContentThumbnail> };

function validSource(value: unknown): value is string {
  if (typeof value !== "string" || value.length > 4096 || !/^https:\/\//i.test(value) || /[\\\s\u0000-\u001f\u007f]/.test(value) || new TextEncoder().encode(value).length > 4096) return false;
  try {
    const authority = value.slice(value.indexOf("//") + 2).split(/[/?#]/, 1)[0];
    if (authority.includes("@") || authority.endsWith(":")) return false;
    const url = new URL(value);
    if (url.protocol !== "https:" || url.username || url.password || url.port || !url.hostname) return false;
    const host = url.hostname.replace(/\.$/, "");
    // URL validates IPv6 literals; DNS names also need valid, bounded labels.
    return host.startsWith("[") || (host.length <= 253 && host.split(".").every((part) => /^[a-z\d](?:[a-z\d-]{0,61}[a-z\d])?$/i.test(part)));
  } catch { return false; }
}

// Keep the original string: serializing URL can alter signed paths or queries.
// Thumbnail failure never changes where the media button opens the content.
export function thumbnailSources(thumbnail?: ContentThumbnail): string[] {
  const candidates = Array.isArray(thumbnail?.remote_urls) ? thumbnail.remote_urls : [];
  return [...new Set([...candidates, thumbnail?.remote_url].filter(validSource))].slice(0, 3);
}

export function thumbnailSource(thumbnail?: ContentThumbnail): string | null {
  return thumbnailSources(thumbnail)[0] ?? null;
}

export function thumbnailMissingHint(thumbnail?: ContentThumbnail): string | null {
  if (!thumbnail) return null; // Metadata is still loading.
  if (thumbnail.reason === "unsupported_format") return "封面格式暂不支持";
  if (thumbnail.reason === "source_unavailable") return "封面资料暂不可用";
  return "未取得封面";
}
