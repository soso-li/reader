import { NextRequest, NextResponse } from "next/server";

import { apiFetch } from "../../lib/api";
import { actionErrorUrl, cleanActionUrl } from "../shared";

export async function POST(request: NextRequest) {
  const referer = request.headers.get("referer");
  const target = cleanActionUrl(request, referer || "/");
  target.searchParams.delete("item_id");
  target.searchParams.delete("assistant");
  target.searchParams.delete("assistant_ask");
  try {
    await apiFetch("/jobs/fetch", { method: "POST" });
  } catch (error) {
    return NextResponse.redirect(actionErrorUrl(request, target, error, "刷新 RSS 失败"), 303);
  }
  return NextResponse.redirect(target, 303);
}
