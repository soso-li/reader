import { NextRequest, NextResponse } from "next/server.js";

import { apiFetch } from "../../lib/api";
import { actionErrorUrl, appUrl } from "../shared";

export async function POST(request: NextRequest) {
  const form = await request.formData();
  const topicId = Number(form.get("topic_id"));
  const action = String(form.get("action") ?? "");
  const name = String(form.get("name") ?? "").trim();
  const query = String(form.get("query") ?? "").trim();
  const description = String(form.get("description") ?? "").trim();
  const target = appUrl(request, topicId > 0 ? `/?view=topics&topic_id=${topicId}` : "/?view=topics");
  try {
    if (topicId > 0 && action === "delete") {
      await apiFetch(`/topics/${topicId}`, { method: "DELETE" });
      return NextResponse.redirect(appUrl(request, "/?view=topics"), 303);
    }
    if (!name || !query) {
      throw new Error("主题名称和关键词不能为空");
    }
    if (topicId > 0 && name && query) {
      await apiFetch(`/topics/${topicId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, query, description })
      });
      return NextResponse.redirect(target, 303);
    }
    await apiFetch("/topics", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, query, description })
    });
  } catch (error) {
    return NextResponse.redirect(actionErrorUrl(request, target, error, "议题操作失败"), 303);
  }
  return NextResponse.redirect(appUrl(request, "/?view=topics"), 303);
}
