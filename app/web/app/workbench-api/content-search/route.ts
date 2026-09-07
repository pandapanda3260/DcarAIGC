import { contentSearchResponse } from "../../lib/contentSearchServer";

export async function POST(request: Request) {
  return contentSearchResponse(request);
}
