import { proxyContentUpdates } from "../../lib/contentUpdateProxy";

type Context = { params: Promise<{ path: string[] }> };
export async function GET(request: Request, context: Context) {
  return proxyContentUpdates(request, (await context.params).path);
}
export async function POST(request: Request, context: Context) {
  return proxyContentUpdates(request, (await context.params).path);
}
