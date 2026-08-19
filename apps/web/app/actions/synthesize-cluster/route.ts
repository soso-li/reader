import { NextRequest, NextResponse } from "next/server.js";

import { apiFetch } from "../../lib/api";
import { apiErrorMessage, appUrl } from "../shared";

export async function POST(request: NextRequest) {
  const form = await request.formData();
  const eventUid = String(form.get("event_uid") ?? "").trim();
  const redirect = String(form.get("redirect") ?? "/?view=clusters");
  if (eventUid) {
    try {
      await apiFetch(`/events/${encodeURIComponent(eventUid)}/synthesis`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({})
      });
    } catch (error) {
      const target = appUrl(request, redirect);
      target.searchParams.set("action_error", apiErrorMessage(error, "AI 合成失败"));
      return NextResponse.redirect(target, 303);
    }
  }
  return NextResponse.redirect(appUrl(request, redirect), 303);
}
