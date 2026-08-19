import type { EventReadStatus, EventReadTarget } from "./event-user-state.ts";

export type SummarySeenTrigger =
  | "detail_dwell"
  | "scroll_past"
  | "selection_leave";

export type OriginalOpenedTrigger =
  | "title"
  | "source"
  | "shortcut";

export type DetailPresentationTrigger =
  | "automatic"
  | "direct_url"
  | "user_selection"
  | "history_navigation";

type SummarySeenContext = {
  readStatus: string;
  skip: boolean;
  userPresented: boolean;
  evidencePresented?: boolean;
  currentRevisionDiffersFromSeen?: boolean;
};

export type EventReadErrorSurface = "list" | "detail";

export type EventReadOperation = {
  clusterId: number;
  operationId: string;
  requestedStatus: EventReadStatus;
  target: EventReadTarget;
  surface: EventReadErrorSurface;
};

export type EventReadError = EventReadOperation & {
  message: string;
};

type VisibilityAwareElement = Pick<HTMLElement, "getClientRects" | "isConnected"> & {
  checkVisibility?: (options?: {
    checkOpacity?: boolean;
    checkVisibilityCSS?: boolean;
  }) => boolean;
};

export function summarySeenIntent(
  _trigger: SummarySeenTrigger,
  context: SummarySeenContext
): EventReadStatus | null {
  if (
    context.skip ||
    !context.userPresented ||
    context.evidencePresented === false ||
    !summarySeenEligible(
      context.readStatus,
      context.currentRevisionDiffersFromSeen ?? false
    )
  ) {
    return null;
  }
  return "summary_seen";
}

export function detailEvidencePresented(
  sourceMode: boolean,
  sourceText: string,
  synthesisBlockCount: number
): boolean {
  return sourceMode ? sourceText.trim().length > 0 : synthesisBlockCount > 0;
}

export function summarySeenEligible(
  readStatus: string,
  currentRevisionDiffersFromSeen: boolean
): boolean {
  return readStatus === "unread" || currentRevisionDiffersFromSeen;
}

export function detailSummarySeenAllowed(
  trigger: DetailPresentationTrigger,
  clusterId: number | null
): boolean {
  return clusterId !== null && trigger !== "automatic";
}

export function explicitReadStatusPresentationAllowed(
  current: boolean,
  requestedStatus: EventReadStatus
): boolean {
  return requestedStatus === "unread" ? false : current;
}

export function recordEventReadFailure(
  errors: readonly EventReadError[],
  failed: EventReadOperation,
  latest: EventReadOperation | undefined,
  message: string = failed.requestedStatus === "original_opened"
    ? "原文已打开，但看过记录保存失败，请重试"
    : "阅读状态保存失败，请重试"
): EventReadError[] {
  if (latest && latest.operationId !== failed.operationId) {
    if (failed.requestedStatus !== "original_opened") return [...errors];
    if (
      latest.requestedStatus === "original_opened" &&
      sameEvidence(latest.target, failed.target)
    ) {
      return [...errors];
    }
  }
  return [
    ...errors.filter(
      (error) =>
        error.operationId !== failed.operationId &&
        !sameUnresolvedReadTarget(error, failed)
    ),
    { ...failed, message }
  ];
}

function sameUnresolvedReadTarget(
  error: EventReadError,
  failed: EventReadOperation
): boolean {
  if (error.clusterId !== failed.clusterId) return false;
  if (failed.requestedStatus !== "original_opened") {
    return error.requestedStatus !== "original_opened";
  }
  return (
    error.requestedStatus === "original_opened" &&
    sameEvidence(error.target, failed.target)
  );
}

export function clearEventReadErrorsAfterSuccess(
  errors: readonly EventReadError[],
  succeeded: EventReadOperation
): EventReadError[] {
  return errors.filter((error) => {
    if (error.clusterId !== succeeded.clusterId) return true;
    if (error.requestedStatus !== "original_opened") return false;
    return !(
      succeeded.requestedStatus === "original_opened" &&
      sameEvidence(error.target, succeeded.target)
    );
  });
}

function sameEvidence(left: EventReadTarget, right: EventReadTarget): boolean {
  return (
    left.observed_revision_uid === right.observed_revision_uid &&
    left.evidence?.source_id === right.evidence?.source_id &&
    left.evidence?.evidence_version_uid === right.evidence?.evidence_version_uid
  );
}

export function visibleEventReadErrors(
  errors: readonly EventReadError[],
  surface: EventReadErrorSurface,
  selectedClusterId: number | null
): EventReadError[] {
  const visible = errors.filter(
    (error) =>
      error.surface === surface &&
      (surface === "list" || error.clusterId === selectedClusterId)
  );
  return visible.filter(
    (error, index) =>
      visible.findIndex((candidate) => candidate.message === error.message) === index
  );
}

export function isInteractionSurfacePresented(
  element: VisibilityAwareElement | null,
  pageVisibilityState: DocumentVisibilityState =
    typeof document === "undefined" ? "visible" : document.visibilityState
): boolean {
  if (pageVisibilityState !== "visible") return false;
  if (!element?.isConnected) return false;
  if (
    typeof element.checkVisibility === "function" &&
    !element.checkVisibility({
      checkOpacity: true,
      checkVisibilityCSS: true
    })
  ) {
    return false;
  }
  return element.getClientRects().length > 0;
}

export function originalOpenedIntent(
  _trigger: OriginalOpenedTrigger,
  sourceId: number
): { value: "original_opened"; sourceId: number } {
  if (!Number.isInteger(sourceId) || sourceId < 1) {
    throw new Error("打开原文时缺少来源，请刷新后重试");
  }
  return { value: "original_opened", sourceId };
}

export function readStatusToggleIntent(readStatus: string): EventReadStatus {
  return readStatus === "summary_seen" || readStatus === "original_opened"
    ? "unread"
    : "summary_seen";
}
