import { NextRequest, NextResponse } from "next/server";

import { apiFetch } from "../../lib/api";
import { actionErrorUrl, backToSettings } from "../shared";

export async function POST(request: NextRequest) {
  const form = await request.formData();
  const target = backToSettings(request);
  const payload: Record<string, string | number | boolean> = {};
  addTextField(payload, form, "task_provider");
  addTextField(payload, form, "base_url");
  addTextField(payload, form, "embedding_base_url");
  addTextField(payload, form, "llm_model");
  addTextField(payload, form, "embedding_model");
  if (form.has("timeout_seconds")) {
    payload.timeout_seconds = Number(form.get("timeout_seconds") || 240);
  }
  try {
    await apiFetch("/ai/settings", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
  } catch (error) {
    return NextResponse.redirect(actionErrorUrl(request, target, error, "AI 设置保存失败"), 303);
  }
  return NextResponse.redirect(target, 303);
}

function addTextField(payload: Record<string, string | number | boolean>, form: FormData, name: string) {
  if (!form.has(name)) return;
  payload[name] = String(form.get(name) ?? "").trim();
}
