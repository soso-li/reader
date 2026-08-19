"use client";

import { useEffect, useRef } from "react";

import { sendClientUserState } from "./client-user-state";
import type { ObjectUserStateType } from "./object-user-state";
import {
  shouldStartVisibleDwellAttempt,
  startVisibleDwell
} from "./visible-dwell";

export default function SummarySeenMarker({
  id,
  objectType = "report",
  attemptKey,
  onSeen,
  persist = true,
  readStatus,
  eligible,
  skip,
  deferMs = 0
}: {
  id: number;
  objectType?: ObjectUserStateType;
  attemptKey?: string;
  onSeen?: () => void;
  persist?: boolean;
  readStatus: string;
  eligible?: boolean;
  skip?: boolean;
  deferMs?: number;
}) {
  const onSeenRef = useRef(onSeen);
  const attemptedKeyRef = useRef<string | null>(null);

  useEffect(() => {
    onSeenRef.current = onSeen;
  }, [onSeen]);

  useEffect(() => {
    const markerEligible = eligible ?? readStatus === "unread";
    if (
      skip ||
      (attemptKey === undefined
        ? !markerEligible
        : !shouldStartVisibleDwellAttempt({
            eligible: markerEligible,
            attemptKey,
            attemptedKey: attemptedKeyRef.current
          }))
    ) {
      return;
    }
    const markSeen = () => {
      if (attemptKey !== undefined) attemptedKeyRef.current = attemptKey;
      onSeenRef.current?.();
      if (persist) {
        void sendClientUserState({ object_type: objectType, object_id: id, read_status: "summary_seen" }).catch(() => undefined);
      }
    };
    return startVisibleDwell({
      deferMs,
      isVisible: () => document.visibilityState === "visible",
      markSeen,
      addVisibilityListener: (listener) =>
        document.addEventListener("visibilitychange", listener),
      removeVisibilityListener: (listener) =>
        document.removeEventListener("visibilitychange", listener),
      setTimer: (callback, delayMs) => window.setTimeout(callback, delayMs),
      clearTimer: (timerId) => window.clearTimeout(timerId)
    });
  }, [attemptKey, deferMs, eligible, id, objectType, persist, readStatus, skip]);

  return null;
}
