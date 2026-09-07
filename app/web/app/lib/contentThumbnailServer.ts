import { execFile } from "node:child_process";
import type { ContentThumbnails } from "./contentThumbnails";

type Reader = (ids: number[]) => Promise<ContentThumbnails>;
let activeReads = 0;

// The existing loopback page service reads metadata only. No network fetching,
// image generation, database writes, media cache, or writer restart is needed.
export async function readThumbnailMetadata(ids: number[]): Promise<ContentThumbnails> {
  const python = process.env.DCAR_THUMBNAIL_PYTHON;
  const helper = process.env.DCAR_THUMBNAIL_HELPER;
  const db = process.env.DCAR_THUMBNAIL_DB;
  const projectRoot = process.env.DCAR_THUMBNAIL_PROJECT_ROOT;
  if (!python || !helper || !db || !projectRoot || activeReads >= 1) throw new Error("Thumbnail reader unavailable");
  activeReads += 1;
  try {
    return await new Promise((resolve, reject) => {
      execFile(python, ["-B", "-I", helper, "--db", db, "--project-root", projectRoot, "--ids", ids.join(",")], {
        timeout: 8000, maxBuffer: 1024 * 1024, windowsHide: true,
      }, (error, stdout) => {
        if (error) { reject(error); return; }
        try {
          const result = JSON.parse(stdout) as ContentThumbnails;
          if (!result?.items || typeof result.items !== "object" || Array.isArray(result.items)) throw new Error("Invalid thumbnail metadata");
          resolve(result);
        } catch (reason) { reject(reason); }
      });
    });
  } finally { activeReads -= 1; }
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
