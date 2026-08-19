import { NextRequest, NextResponse } from "next/server";

import { apiFetch } from "../../lib/api";
import { actionErrorUrl, backToSettings } from "../shared";

export async function POST(request: NextRequest) {
  const target = backToSettings(request);
  try {
    await apiFetch("/jobs/embeddings", { method: "POST" });
  } catch (error) {
    return NextResponse.redirect(actionErrorUrl(request, target, error, "Embedding 任务启动失败"), 303);
  }
  return NextResponse.redirect(target, 303);
}
