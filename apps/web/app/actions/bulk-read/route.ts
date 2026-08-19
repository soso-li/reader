import { NextRequest, NextResponse } from "next/server";

import { apiErrorStatus, apiFetch } from "../../lib/api";
import {
  confirmBulkReadWithRetry,
  freezeBulkReadBatch
} from "../../bulk-read";
import { actionErrorUrl, cleanActionUrl } from "../shared";

export async function POST(request: NextRequest) {
  const form = await request.formData();
  const referer = request.headers.get("referer");
  const target = cleanActionUrl(request, String(form.get("redirect") || referer || "/"));
  target.searchParams.delete("item_id");
  target.searchParams.delete("cluster_id");
  target.searchParams.delete("assistant");
  target.searchParams.delete("assistant_ask");

  try {
    const batch = freezeBulkReadBatch(String(form.get("batch_id") || ""));
    await confirmBulkReadWithRetry(
      batch,
      (body) =>
        apiFetch("/user-state/bulk-read", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body
        }),
      (error) => apiErrorStatus(error) >= 500
    );
  } catch (error) {
    return NextResponse.redirect(actionErrorUrl(request, target, error, "批量标记已读失败"), 303);
  }
  return NextResponse.redirect(target, 303);
}
