import type { NextRequest } from "next/server";

import { readLimitedRequestBody } from "../../lib/request-body";

type RouteContext = { params: Promise<{ path: string[] }> };
const MAX_REQUEST_BYTES = 8 * 1024 * 1024;

async function proxy(request: NextRequest, { params }: RouteContext) {
  const token = process.env.READER_API_TOKEN?.trim();
  if (!token) return Response.json({ detail: "API 服务令牌尚未配置" }, { status: 503 });

  const path = (await params).path.map(encodeURIComponent).join("/");
  const target = new URL(`/` + path, process.env.API_INTERNAL_URL || "http://api:8000");
  target.search = request.nextUrl.search;
  const headers = new Headers();
  for (const name of ["accept", "content-type"]) {
    const value = request.headers.get(name);
    if (value) headers.set(name, value);
  }
  headers.set("X-Reader-API-Token", token);

  try {
    const body =
      request.method === "GET" || request.method === "HEAD"
        ? undefined
        : await readLimitedRequestBody(request, MAX_REQUEST_BYTES);
    if (body === null) {
      return Response.json({ detail: "请求体超过 8MB 限制" }, { status: 413 });
    }
    const response = await fetch(target, {
      body,
      cache: "no-store",
      headers,
      method: request.method
    });
    const responseHeaders = new Headers();
    for (const name of ["cache-control", "content-disposition", "content-type", "x-content-type-options"]) {
      const value = response.headers.get(name);
      if (value) responseHeaders.set(name, value);
    }
    return new Response(response.body, {
      headers: responseHeaders,
      status: response.status,
      statusText: response.statusText
    });
  } catch {
    return Response.json({ detail: "API 服务不可用" }, { status: 502 });
  }
}

export { proxy as DELETE, proxy as GET, proxy as PATCH, proxy as POST, proxy as PUT };
