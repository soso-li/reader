"use client";

import { createOperationId } from "./event-user-state.ts";
import type { ObjectUserStateType } from "./object-user-state.ts";

export type ClientUserStatePatch = {
  object_type: ObjectUserStateType;
  object_id: number;
  operation_id?: string;
  read_status?: string;
  read_later?: boolean;
  starred?: boolean;
};

export type ObjectUserStateMutation = ClientUserStatePatch & {
  operation_id: string;
};

export function createObjectUserStateMutation(
  patch: ClientUserStatePatch,
  operationId = createOperationId()
): ObjectUserStateMutation {
  const fields = [patch.read_status, patch.read_later, patch.starred].filter(
    (value) => value !== undefined
  );
  if (fields.length !== 1) {
    throw new Error("每次对象状态操作必须只提交一个状态值");
  }
  return { ...patch, operation_id: operationId };
}

export async function sendClientUserState(patch: ClientUserStatePatch, options: { beacon?: boolean } = {}) {
  const mutation = createObjectUserStateMutation(
    patch,
    patch.operation_id ?? createOperationId()
  );
  const body = JSON.stringify(mutation);
  if (options.beacon !== false && navigator.sendBeacon?.("/actions/client-user-state", new Blob([body], { type: "application/json" }))) {
    return;
  }

  let response: Response;
  try {
    response = await patchMutation(body);
  } catch {
    response = await patchMutation(body);
  }
  if (response.status >= 500) response = await patchMutation(body);
  if (!response.ok) throw new Error("阅读状态更新失败");
}

function patchMutation(body: string) {
  return fetch("/actions/client-user-state", {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body,
    keepalive: true
  });
}
