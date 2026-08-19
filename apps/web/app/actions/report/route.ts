import { NextRequest, NextResponse } from "next/server";

import { apiFetch } from "../../lib/api";
import { apiErrorMessage, appUrl } from "../shared";

export async function POST(request: NextRequest) {
  const form = await request.formData();
  const period = String(form.get("period") ?? "day");
  const date = String(form.get("date") ?? "");
  const query = new URLSearchParams({ period });
  const redirect = new URLSearchParams({ view: "reports", period });
  if (date) {
    query.set("date", date);
    redirect.set("date", date);
  }
  try {
    await apiFetch(`/reports/generate?${query}`, { method: "POST" });
  } catch (error) {
    redirect.set("report_error", apiErrorMessage(error, "报告生成失败"));
  }
  return NextResponse.redirect(appUrl(request, `/?${redirect}`), 303);
}
