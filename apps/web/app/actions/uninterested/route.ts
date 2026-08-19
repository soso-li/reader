import { NextRequest, NextResponse } from "next/server";

import { apiFetch, userFacingErrorMessage } from "../../lib/api";

const targetTypes = new Set(["event", "item", "article"]);
const reasons = new Set(["promotion", "repetitive", "topic", "low_quality", "other"]);

export async function POST(request: NextRequest) {
  const body = await request.json().catch(() => null) as Record<string, unknown> | null;
  if (
    !body
    || typeof body.operation_id !== "string"
    || !targetTypes.has(String(body.target_type))
    || typeof body.value !== "boolean"
    || (body.reason !== undefined && !reasons.has(String(body.reason)))
    || (body.note !== undefined && typeof body.note !== "string")
  ) {
    return NextResponse.json({ error: "不感兴趣操作无效" }, { status: 400 });
  }
  const eventTarget =
    body.target_type === "event"
    && typeof body.event_uid === "string"
    && typeof body.observed_revision_uid === "string"
    && body.item_id === undefined;
  const itemTarget =
    body.target_type !== "event"
    && Number.isInteger(body.item_id)
    && Number(body.item_id) > 0
    && body.event_uid === undefined
    && body.observed_revision_uid === undefined;
  if (!eventTarget && !itemTarget) {
    return NextResponse.json({ error: "不感兴趣目标无效" }, { status: 400 });
  }

  try {
    return NextResponse.json(await apiFetch("/uninterested", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    }));
  } catch (error) {
    return NextResponse.json(
      { error: userFacingErrorMessage(error, "不感兴趣操作失败") },
      { status: 502 }
    );
  }
}
