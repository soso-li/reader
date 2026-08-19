"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { RefObject } from "react";

export type PullRefreshStatus = "idle" | "refreshing" | "success" | "error";

const PULL_THRESHOLD = 72;
const PULL_MAX = 96;
const REFRESH_JOB_KEY = "reader:pull-refresh-job";
const REFRESH_JOB_EVENT = "reader:pull-refresh-job";
const POLL_INTERVAL_MS = 2_000;
const SUCCESS_DELAY_MS = 400;
const ERROR_DELAY_MS = 3_000;

type PullRefreshOptions = {
  detailOpen: boolean;
  enabled: boolean;
};

type FetchJobResponse = {
  job_id?: string | null;
};

type FetchJobStatusResponse = {
  status?: "running" | "complete" | "failed";
};

export function pullRefreshEnabled(
  filter: string,
  query: string,
  filteredOnly = false
) {
  const normalizedFilter = filter === "all" ? "" : filter;
  return (
    !filteredOnly &&
    !query.trim() &&
    (normalizedFilter === "" || normalizedFilter === "unread")
  );
}

export default function usePullRefresh(
  ref: RefObject<HTMLElement | null>,
  { detailOpen, enabled }: PullRefreshOptions
) {
  const [distance, setDistance] = useState(0);
  const [status, setStatus] = useState<PullRefreshStatus>("idle");
  const [jobId, setJobId] = useState("");
  const startX = useRef<number | null>(null);
  const startY = useRef<number | null>(null);
  const distanceRef = useRef(0);
  const statusRef = useRef<PullRefreshStatus>("idle");
  const detailOpenRef = useRef(detailOpen);
  const errorTimer = useRef<number | null>(null);
  const reloadTimer = useRef<number | null>(null);

  statusRef.current = status;
  detailOpenRef.current = detailOpen;

  const fail = useCallback(() => {
    window.sessionStorage.removeItem(REFRESH_JOB_KEY);
    setJobId("");
    statusRef.current = "error";
    setStatus("error");
    if (errorTimer.current !== null) window.clearTimeout(errorTimer.current);
    errorTimer.current = window.setTimeout(() => {
      statusRef.current = "idle";
      setStatus("idle");
      errorTimer.current = null;
    }, ERROR_DELAY_MS);
  }, []);

  useEffect(() => {
    const resume = (nextJobId: string) => {
      if (!nextJobId) return;
      statusRef.current = "refreshing";
      setStatus("refreshing");
      setJobId(nextJobId);
    };
    const storedJobId = window.sessionStorage.getItem(REFRESH_JOB_KEY) ?? "";
    resume(storedJobId);
    const onRefreshJob = (event: Event) => {
      resume((event as CustomEvent<string>).detail ?? "");
    };
    window.addEventListener(REFRESH_JOB_EVENT, onRefreshJob);
    return () => window.removeEventListener(REFRESH_JOB_EVENT, onRefreshJob);
  }, []);

  useEffect(() => {
    if (!jobId || status !== "refreshing") return;
    let cancelled = false;
    let timer: number | null = null;

    const poll = async () => {
      try {
        const response = await fetch(
          `/api/jobs/fetch/${encodeURIComponent(jobId)}`,
          { cache: "no-store" }
        );
        if (!response.ok) throw new Error("refresh status unavailable");
        const payload = (await response.json()) as FetchJobStatusResponse;
        if (cancelled) return;
        if (payload.status === "complete") {
          statusRef.current = "success";
          setStatus("success");
          return;
        }
        if (payload.status === "failed") {
          fail();
          return;
        }
        if (payload.status !== "running") throw new Error("invalid refresh status");
      } catch {
        // Keep following the server-owned job; a temporary status outage is not a failed refresh.
      }
      if (!cancelled) timer = window.setTimeout(poll, POLL_INTERVAL_MS);
    };

    void poll();
    return () => {
      cancelled = true;
      if (timer !== null) window.clearTimeout(timer);
    };
  }, [fail, jobId, status]);

  useEffect(() => {
    const root = ref.current;
    if (!root || status !== "success") return;

    const reloadWhenSafe = () => {
      if (
        detailOpenRef.current ||
        root.scrollTop > 0 ||
        reloadTimer.current !== null
      ) {
        return;
      }
      reloadTimer.current = window.setTimeout(() => {
        reloadTimer.current = null;
        if (detailOpenRef.current || root.scrollTop > 0) return;
        window.sessionStorage.removeItem(REFRESH_JOB_KEY);
        window.location.reload();
      }, SUCCESS_DELAY_MS);
    };

    reloadWhenSafe();
    root.addEventListener("scroll", reloadWhenSafe, { passive: true });
    return () => {
      root.removeEventListener("scroll", reloadWhenSafe);
      if (reloadTimer.current !== null) {
        window.clearTimeout(reloadTimer.current);
        reloadTimer.current = null;
      }
    };
  }, [detailOpen, ref, status]);

  useEffect(() => {
    const root = ref.current;
    if (!root) return;
    let disposed = false;

    const resetPull = () => {
      startX.current = null;
      startY.current = null;
      distanceRef.current = 0;
      setDistance(0);
    };

    const startRefresh = async () => {
      statusRef.current = "refreshing";
      setStatus("refreshing");
      try {
        const response = await fetch("/api/jobs/fetch", {
          method: "POST",
          headers: { Accept: "application/json" }
        });
        if (!response.ok) throw new Error("refresh request failed");
        const payload = (await response.json()) as FetchJobResponse;
        const nextJobId = payload.job_id?.trim() ?? "";
        if (!nextJobId) throw new Error("refresh job missing");
        window.sessionStorage.setItem(REFRESH_JOB_KEY, nextJobId);
        window.dispatchEvent(
          new window.CustomEvent<string>(REFRESH_JOB_EVENT, {
            detail: nextJobId
          })
        );
        if (!disposed) setJobId(nextJobId);
      } catch {
        if (!disposed) fail();
      }
    };

    const touchStart = (event: TouchEvent) => {
      if (
        !enabled ||
        statusRef.current !== "idle" ||
        root.scrollTop > 0 ||
        event.touches.length !== 1
      ) {
        return;
      }
      startX.current = event.touches[0]?.clientX ?? 0;
      startY.current = event.touches[0]?.clientY ?? null;
    };

    const touchMove = (event: TouchEvent) => {
      if (startY.current === null || startX.current === null || root.scrollTop > 0) {
        return;
      }
      const touch = event.touches[0];
      if (!touch) return;
      const deltaY = touch.clientY - startY.current;
      const deltaX = Math.abs(touch.clientX - startX.current);
      if (deltaY <= 0 || deltaX >= deltaY) {
        resetPull();
        return;
      }
      if (deltaY > 12) event.preventDefault();
      distanceRef.current = Math.min(deltaY, PULL_MAX);
      setDistance(distanceRef.current);
    };

    const touchEnd = () => {
      const shouldRefresh = distanceRef.current >= PULL_THRESHOLD;
      resetPull();
      if (!shouldRefresh || !enabled || statusRef.current !== "idle") return;
      void startRefresh();
    };

    const touchCancel = () => resetPull();

    root.addEventListener("touchstart", touchStart, { passive: true });
    root.addEventListener("touchmove", touchMove, { passive: false });
    root.addEventListener("touchend", touchEnd);
    root.addEventListener("touchcancel", touchCancel);
    return () => {
      disposed = true;
      root.removeEventListener("touchstart", touchStart);
      root.removeEventListener("touchmove", touchMove);
      root.removeEventListener("touchend", touchEnd);
      root.removeEventListener("touchcancel", touchCancel);
    };
  }, [enabled, fail, ref]);

  useEffect(
    () => () => {
      if (errorTimer.current !== null) window.clearTimeout(errorTimer.current);
      if (reloadTimer.current !== null) window.clearTimeout(reloadTimer.current);
    },
    []
  );

  return {
    distance,
    ready: distance >= PULL_THRESHOLD,
    status
  };
}
