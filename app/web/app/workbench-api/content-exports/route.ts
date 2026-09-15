import { contentExportsResponse } from "../../lib/contentExportServer";

export async function GET(request: Request) { return contentExportsResponse(request); }
export async function POST(request: Request) { return contentExportsResponse(request); }
