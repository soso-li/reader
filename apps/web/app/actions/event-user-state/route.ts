import { NextRequest, NextResponse } from "next/server.js";

import { apiFetch, userFacingErrorMessage } from "../../lib/api";

type EventUserStatePayload = {
  event_uid?: unknown;
  observed_revision_uid?: unknown;
  operation_id?: unknown;
  action?: unknown;
  value?: unknown;
  source_id?: unknown;
};

const savedActions = new Set(["starred_set"]);
const readStatuses = new Set(["unread", "summary_seen", "original_opened"]);

export async function POST(request: NextRequest) {
  const body = (await request.json().catch(() => null)) as EventUserStatePayload | null;
  const normalizedBody = body?.action === "read_later_set"
    ? { ...body, action: "starred_set" }
    : body;
  if (
    typeof normalizedBody?.event_uid !== "string" ||
    typeof normalizedBody?.observed_revision_uid !== "string" ||
    typeof normalizedBody?.operation_id !== "string" ||
    !validOperation(normalizedBody)
  ) {
    return NextResponse.json({ error: "Event 状态操作无效" }, { status: 400 });
  }

  try {
    const result = await apiFetch<Record<string, unknown>>("/event-user-state", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(normalizedBody)
    });
    return NextResponse.json(result);
  } catch (error) {
    return NextResponse.json(
      { error: userFacingErrorMessage(error, "Event 状态更新失败") },
      { status: 502 }
    );
  }
}

function validOperation(body: EventUserStatePayload) {
  const action = String(body.action);
  if (savedActions.has(action)) {
    return typeof body.value === "boolean" && body.source_id === undefined;
  }
  if (action !== "read_status_set" || !readStatuses.has(String(body.value))) {
    return false;
  }
  if (body.value === "original_opened") {
    return Number.isInteger(body.source_id) && Number(body.source_id) > 0;
  }
  return body.source_id === undefined;
}
