import { NextRequest, NextResponse } from "next/server.js";

import { apiFetch, userFacingErrorMessage } from "../../lib/api";

type FilterRuleAction = {
  action?: "preview" | "create" | "update" | "delete";
  id?: number;
  source_id?: number | null;
  match_type?: "literal" | "regex";
  pattern?: string;
  enabled?: boolean;
};

export async function POST(request: NextRequest) {
  try {
    const payload = (await request.json()) as FilterRuleAction;
    const spec = {
      source_id: payload.source_id ?? null,
      match_type: payload.match_type,
      pattern: payload.pattern
    };
    if (payload.action === "preview") {
      return NextResponse.json(await apiFetch("/filter-rules/preview", jsonRequest("POST", spec)));
    }
    if (payload.action === "create") {
      return NextResponse.json(await apiFetch("/filter-rules", jsonRequest("POST", spec)));
    }
    if (payload.action === "update" && validId(payload.id)) {
      const changes = payload.match_type || payload.pattern !== undefined || payload.source_id !== undefined
        ? { ...spec, enabled: payload.enabled }
        : { enabled: payload.enabled };
      return NextResponse.json(await apiFetch(`/filter-rules/${payload.id}`, jsonRequest("PATCH", changes)));
    }
    if (payload.action === "delete" && validId(payload.id)) {
      await apiFetch(`/filter-rules/${payload.id}`, { method: "DELETE" });
      return NextResponse.json({ ok: true });
    }
    return NextResponse.json({ error: "过滤规则操作无效" }, { status: 400 });
  } catch (error) {
    return NextResponse.json({ error: userFacingErrorMessage(error, "过滤规则操作失败") }, { status: 400 });
  }
}

function jsonRequest(method: "POST" | "PATCH", body: object): RequestInit {
  return {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

function validId(value: number | undefined): value is number {
  return Number.isInteger(value) && Number(value) > 0;
}
