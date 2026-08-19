import { randomUUID } from "node:crypto";

import { NextRequest, NextResponse } from "next/server";

import { apiFetch } from "../../lib/api";
import { isObjectUserStateType } from "../../object-user-state";
import { actionErrorUrl, cleanActionUrl } from "../shared";

export async function POST(request: NextRequest) {
  const form = await request.formData();
  const objectType = String(form.get("object_type") ?? "");
  const objectId = Number(form.get("object_id"));
  const operationId = form.get("operation_id");
  const payload: Record<string, string | boolean> = {
    operation_id:
      typeof operationId === "string" && operationId
        ? operationId
        : randomUUID()
  };
  const readStatus = form.get("read_status");
  const readLater = form.get("read_later");
  const starred = form.get("starred");
  if (typeof readStatus === "string") payload.read_status = readStatus;
  if (typeof readLater === "string") payload.read_later = readLater === "true";
  if (typeof starred === "string") payload.starred = starred === "true";
  const redirect = form.get("redirect");
  const referer = request.headers.get("referer");
  const base = referer || request.url;
  const target = typeof redirect === "string" && redirect.startsWith("/") ? cleanActionUrl(request, new URL(redirect, base)) : cleanActionUrl(request, referer || "/");
  if (readStatus === "unread" && ["report", "topic"].includes(objectType)) {
    target.searchParams.set("skip_seen", "1");
  } else {
    target.searchParams.delete("skip_seen");
  }
  if (isObjectUserStateType(objectType) && Number.isFinite(objectId)) {
    try {
      await apiFetch(`/user-state/${objectType}/${objectId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
    } catch (error) {
      return NextResponse.redirect(actionErrorUrl(request, target, error, "阅读状态更新失败"), 303);
    }
  }
  return NextResponse.redirect(target, 303);
}
