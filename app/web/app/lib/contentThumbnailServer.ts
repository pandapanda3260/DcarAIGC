import { execFile } from "node:child_process";
import { stat } from "node:fs/promises";
import type { ContentThumbnails } from "./contentThumbnails";
import { BoundedReadCache } from "./boundedReadCache.mjs";

type Reader = (ids: number[]) => Promise<ContentThumbnails>;
const metadataCache = new BoundedReadCache<ContentThumbnails>({
  ttlMs: 10_000, maxEntries: 16, maxBytes: 8 * 1024 * 1024,
  concurrency: 1, maxQueued: 4, queueTimeoutMs: 2_000,
});

// The existing loopback page service reads metadata only. No network fetching,
// image generation, database writes, media cache, or writer restart is needed.
export async function readThumbnailMetadata(ids: number[]): Promise<ContentThumbnails> {
  const python = process.env.DCAR_THUMBNAIL_PYTHON;
  const helper = process.env.DCAR_THUMBNAIL_HELPER;
  const db = process.env.DCAR_THUMBNAIL_DB;
  const projectRoot = process.env.DCAR_THUMBNAIL_PROJECT_ROOT;
  if (!python || !helper || !db || !projectRoot) throw new Error("Thumbnail reader unavailable");
  const file = await stat(db);
  // Snapshot replacement changes inode; unrelated WAL writes do not evict covers.
  // Changes inside the same database are bounded by the short ten-second TTL.
  const normalized = [...new Set(ids)].sort((a, b) => a - b);
  const key = JSON.stringify([python, helper, projectRoot, db, file.dev, file.ino, normalized]);
  return metadataCache.get(key, () => new Promise((resolve, reject) => {
      execFile(python, ["-B", "-I", helper, "--db", db, "--project-root", projectRoot, "--ids", normalized.join(",")], {
        // Up to 100 items, each with three bounded cover URLs plus the legacy
        // first URL. Leave room for JSON escaping without truncating a page.
        timeout: 8000, maxBuffer: 4 * 1024 * 1024, windowsHide: true,
      }, (error, stdout) => {
        if (error) { reject(error); return; }
        try {
          const result = JSON.parse(stdout) as ContentThumbnails;
          if (!result?.items || typeof result.items !== "object" || Array.isArray(result.items)) throw new Error("Invalid thumbnail metadata");
          resolve(result);
        } catch (reason) { reject(reason); }
      });
    }));
}

export async function thumbnailResponse(request: Request, reader: Reader = readThumbnailMetadata): Promise<Response> {
  const headers = { "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff" };
  // Auth gateway strips client-supplied identities and injects the current user.
  if (!request.headers.get("x-dcar-authenticated-user")) {
    return Response.json({ detail: "登录已过期，请重新登录。" }, { status: 401, headers });
  }
  const raw = new URL(request.url).searchParams.get("ids") ?? "";
  if (!/^[1-9]\d*(?:,[1-9]\d*){0,99}$/.test(raw) || raw.length > 1800) {
    return Response.json({ detail: "内容编号无效。" }, { status: 422, headers });
  }
  const ids = [...new Set(raw.split(",").map(Number))];
  if (ids.some((id) => !Number.isSafeInteger(id))) {
    return Response.json({ detail: "内容编号无效。" }, { status: 422, headers });
  }
  try {
    return Response.json(await reader(ids), { headers });
  } catch {
    // Optional decoration: callers retain their shared placeholder, without retrying capture.
    return Response.json({ detail: "缩略图暂不可用。" }, { status: 503, headers });
  }
}
