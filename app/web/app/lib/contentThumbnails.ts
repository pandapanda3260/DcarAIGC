export type ContentThumbnail = { local_url: string | null; remote_url: string | null };
export type ContentThumbnails = { items: Record<string, ContentThumbnail> };

// Thumbnail failure never changes where the media button opens the content.
export function thumbnailSources(contentId: number, thumbnail?: ContentThumbnail): string[] {
  const sources: string[] = [];
  if (thumbnail?.local_url && new RegExp(`^/api/v8/contents/${contentId}/evidence/(?:files|previews)/[1-9]\\d*/\\d+$`).test(thumbnail.local_url)) {
    sources.push(thumbnail.local_url);
  }
  if (thumbnail?.remote_url && thumbnail.remote_url.length <= 4096) {
    try {
      const url = new URL(thumbnail.remote_url);
      if (url.protocol === "https:" && !url.username && !url.password) sources.push(url.href);
    } catch { /* Missing or invalid covers use the solid background. */ }
  }
  return sources;
}
