import { thumbnailResponse } from "../../lib/contentThumbnailServer";

export async function GET(request: Request) {
  return thumbnailResponse(request);
}
