import { randomUUID } from "node:crypto";

import { NextRequest, NextResponse } from "next/server";

import { apiErrorStatus, apiFetch, userFacingErrorMessage } from "../../lib/api";
import { isObjectUserStateType } from "../../object-user-state";

type UserStatePayload = {
  object_type?: string;
  object_id?: number;
  operation_id?: string;
  read_status?: string;
  read_later?: boolean;
  starred?: boolean;
};

async function updateUserState(request: NextRequest) {
  const body = (await request.json().catch(() => null)) as UserStatePayload | null;
  const objectType = body?.object_type;
  const objectId = Number(body?.object_id);
  if (!isObjectUserStateType(objectType) || !Number.isFinite(objectId)) {
    return NextResponse.json({ error: "阅读状态目标无效" }, { status: 400 });
  }

  const payload: Record<string, string | boolean> = {
    operation_id: body?.operation_id || randomUUID()
  };
  if (body?.read_status !== undefined) payload.read_status = body.read_status;
  if (body?.read_later !== undefined) payload.read_later = body.read_later;
  if (body?.starred !== undefined) payload.starred = body.starred;

  try {
    await apiFetch(`/user-state/${objectType}/${objectId}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    return NextResponse.json({ ok: true });
  } catch (error) {
    return NextResponse.json(
      { error: userFacingErrorMessage(error, "阅读状态更新失败") },
      { status: apiErrorStatus(error) }
    );
  }
}

export async function PATCH(request: NextRequest) {
  return updateUserState(request);
}

export async function POST(request: NextRequest) {
  return updateUserState(request);
}
