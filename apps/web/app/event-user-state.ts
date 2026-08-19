"use client";

export type EventSavedStateAction = "starred_set" | "read_later_set";
export type EventReadStatus = "unread" | "summary_seen" | "original_opened";
export type EventUserStateAction = EventSavedStateAction | "read_status_set";

type EventUserStateMutationBase = {
  event_uid: string;
  observed_revision_uid: string;
  operation_id: string;
};

export type EventUserStateMutation = EventUserStateMutationBase &
  (
    | {
        action: EventSavedStateAction;
        value: boolean;
      }
    | {
        action: "read_status_set";
        value: EventReadStatus;
        source_id?: number;
        evidence_version_uid?: string;
      }
  );

type EventUserStateMutationResultBase = {
  event_uid: string;
  observed_revision_uid: string;
  operation_id: string;
  read_later: boolean;
  starred: boolean;
  updated_at: string;
};

export type EventUserStateMutationResult = EventUserStateMutationResultBase &
  (
    | {
        action: EventSavedStateAction;
        value: boolean;
      }
    | {
        action: "read_status_set";
        value: EventReadStatus;
        source_id?: number;
        evidence_version_uid?: string;
        read_status: string;
        seen_revision_uid: string | null;
        current_revision_differs_from_seen: boolean;
        has_material_update: boolean;
        material_update_revision_uid: string | null;
      }
  );

export type ConfirmedEventStatePatch =
  | { starred: boolean }
  | { read_later: boolean }
  | {
      read_status: string;
      seen_revision_uid: string | null;
      current_revision_differs_from_seen: boolean;
      has_material_update: boolean;
      material_update_revision_uid: string | null;
    };

type RenderedEventIdentity = {
  event_uid: string | null;
  current_revision_uid: string | null;
};

export type EventReadTarget = {
  event_uid: string | null;
  observed_revision_uid: string | null;
  evidence?: {
    source_id: number;
    evidence_version_uid: string;
  };
};

type OperationIdCrypto = {
  randomUUID?: () => string;
  getRandomValues: (array: Uint8Array) => Uint8Array;
};

export function createEventUserStateMutation(
  identity: RenderedEventIdentity,
  action: EventSavedStateAction,
  value: boolean,
  operationId = createOperationId()
): EventUserStateMutation {
  const base = eventMutationIdentity(identity, operationId);
  return {
    ...base,
    action,
    value
  };
}

export function createEventReadStateMutation(
  target: EventReadTarget,
  value: EventReadStatus,
  operationId = createOperationId()
): EventUserStateMutation {
  const base = eventMutationIdentity(
    {
      event_uid: target.event_uid,
      current_revision_uid: target.observed_revision_uid
    },
    operationId
  );
  if (value === "original_opened") {
    if (!target.evidence) {
      throw new Error("打开原文时缺少精确来源证据，请刷新后重试");
    }
    return {
      ...base,
      action: "read_status_set",
      value,
      source_id: target.evidence.source_id,
      evidence_version_uid: target.evidence.evidence_version_uid
    };
  }
  if (target.evidence !== undefined) {
    throw new Error("只有打开原文可以提交来源");
  }
  return { ...base, action: "read_status_set", value };
}

function eventMutationIdentity(
  identity: RenderedEventIdentity,
  operationId: string
): EventUserStateMutationBase {
  if (!identity.event_uid || !identity.current_revision_uid) {
    throw new Error("事件身份或实际渲染版本缺失，请刷新后重试");
  }
  return {
    event_uid: identity.event_uid,
    observed_revision_uid: identity.current_revision_uid,
    operation_id: operationId
  };
}

export function createOperationId(
  cryptoSource: OperationIdCrypto = globalThis.crypto
): string {
  if (typeof cryptoSource.randomUUID === "function") {
    return cryptoSource.randomUUID();
  }
  const bytes = cryptoSource.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0"));
  return [
    hex.slice(0, 4).join(""),
    hex.slice(4, 6).join(""),
    hex.slice(6, 8).join(""),
    hex.slice(8, 10).join(""),
    hex.slice(10, 16).join("")
  ].join("-");
}

export function confirmedEventStatePatch(
  result: EventUserStateMutationResult
): ConfirmedEventStatePatch {
  if (result.action === "starred_set") return { starred: result.starred };
  if (result.action === "read_later_set") {
    return { read_later: result.read_later };
  }
  if (!("read_status" in result)) {
    throw new Error("事件状态回应缺少阅读结果");
  }
  return {
    read_status: result.read_status,
    seen_revision_uid: result.seen_revision_uid,
    current_revision_differs_from_seen: result.current_revision_differs_from_seen,
    has_material_update: result.has_material_update,
    material_update_revision_uid: result.material_update_revision_uid
  };
}

export async function sendEventUserStateMutation(
  mutation: EventUserStateMutation,
  options: { beacon?: boolean } = {}
): Promise<EventUserStateMutationResult | undefined> {
  const body = JSON.stringify(mutation);
  if (
    options.beacon !== false &&
    navigator.sendBeacon?.(
      "/actions/event-user-state",
      new Blob([body], { type: "application/json" })
    )
  ) {
    return undefined;
  }

  let response: Response;
  try {
    response = await postMutation(body);
  } catch {
    response = await postMutation(body);
  }
  if (response.status >= 500) response = await postMutation(body);
  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as
      | { error?: unknown; detail?: unknown }
      | null;
    const detail = payload?.detail ?? payload?.error;
    throw new Error(
      typeof detail === "string" ? detail : `状态更新失败（${response.status}）`
    );
  }
  return (await response.json()) as EventUserStateMutationResult;
}

function postMutation(body: string) {
  return fetch("/actions/event-user-state", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
    keepalive: true
  });
}
