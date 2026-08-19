import { NextRequest, NextResponse } from "next/server.js";

import { apiFetch } from "../../lib/api";
import { readLimitedRequestBody } from "../../lib/request-body";
import { actionErrorUrl, backToSettings } from "../shared";

const MAX_OPML_FILE_BYTES = 2 * 1024 * 1024;
const MAX_OPML_REQUEST_BYTES = MAX_OPML_FILE_BYTES + 64 * 1024;

export async function POST(request: NextRequest) {
  const target = backToSettings(request);
  try {
    const body = await readLimitedRequestBody(request, MAX_OPML_REQUEST_BYTES);
    if (body === null) throw new Error("OPML 文件超过 2MB 限制");
    const contentType = request.headers.get("content-type");
    if (!contentType) throw new Error("OPML 表单格式无效");
    const form = await new Response(body, {
      headers: { "Content-Type": contentType }
    }).formData();
    const file = form.get("file");
    if (file instanceof File && file.size > 0) {
      if (file.size > MAX_OPML_FILE_BYTES) {
        throw new Error("OPML 文件超过 2MB 限制");
      }
      const body = new FormData();
      body.set("file", file);
      await apiFetch("/imports/opml", { method: "POST", body });
    }
  } catch (error) {
    return NextResponse.redirect(actionErrorUrl(request, target, error, "OPML 导入失败"), 303);
  }
  return NextResponse.redirect(target, 303);
}
