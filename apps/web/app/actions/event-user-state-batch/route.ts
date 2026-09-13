import { NextRequest, NextResponse } from "next/server.js";

import { apiFetch, userFacingErrorMessage } from "../../lib/api";

// 与 schemas.EVENT_READ_BATCH_LIMIT、event-read-batch.ts 保持同步。
const BATCH_LIMIT = 50;

type EventReadBatchPayload = { marks?: unknown };

export async function POST(request: NextRequest) {
  const body = (await request.json().catch(() => null)) as EventReadBatchPayload | null;
  const marks = body?.marks;
  if (
    !Array.isArray(marks) ||
    marks.length < 1 ||
    marks.length > BATCH_LIMIT ||
    !marks.every(validMark)
  ) {
    return NextResponse.json({ error: "Event 状态操作无效" }, { status: 400 });
  }

  try {
    const result = await apiFetch<Record<string, unknown>>("/event-user-state/batch", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ marks })
    });
    return NextResponse.json(result);
  } catch (error) {
    return NextResponse.json(
      { error: userFacingErrorMessage(error, "Event 状态更新失败") },
      { status: 502 }
    );
  }
}

function validMark(value: unknown) {
  if (typeof value !== "object" || value === null) return false;
  const mark = value as Record<string, unknown>;
  return (
    Object.keys(mark).length === 3 &&
    typeof mark.event_uid === "string" &&
    mark.event_uid.length > 0 &&
    typeof mark.observed_revision_uid === "string" &&
    mark.observed_revision_uid.length > 0 &&
    typeof mark.operation_id === "string" &&
    mark.operation_id.length > 0
  );
}
