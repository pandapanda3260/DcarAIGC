import { contentExportsResponse } from "../../../lib/contentExportServer";

type Context = { params: Promise<{ id: string }> };
export async function GET(request: Request, context: Context) {
  return contentExportsResponse(request, (await context.params).id);
}
