"use client";

import type { EventUserStateMutationResult } from "./event-user-state";

export const SCROLL_MARK_FLUSH_IDLE_MS = 1500;
export const SCROLL_MARK_FLUSH_MAX_PENDING = 10;
// 与 schemas.EVENT_READ_BATCH_LIMIT、/actions/event-user-state-batch 路由保持同步。
export const SCROLL_MARK_BATCH_LIMIT = 50;

export type EventReadBatchMark = {
  event_uid: string;
  observed_revision_uid: string;
  operation_id: string;
};

export type EventReadBatchItem = {
  operation_id: string;
  result?: EventUserStateMutationResult;
  error?: string;
};

type PendingMark = {
  mark: EventReadBatchMark;
  resolve: (result: EventUserStateMutationResult) => void;
  reject: (error: Error) => void;
};

export type ScrollMarkCollector = {
  enqueue(mark: EventReadBatchMark): Promise<EventUserStateMutationResult>;
  flushNow(): Promise<void>;
  flushOnPagehide(): void;
};

export function pagehideFlushAbort(): DOMException {
  return new DOMException("页面卸载，滚动标记改由后台通道提交", "AbortError");
}

export async function sendEventReadBatch(
  marks: EventReadBatchMark[]
): Promise<EventReadBatchItem[]> {
  const body = JSON.stringify({ marks });
  let response: Response;
  try {
    response = await postBatch(body);
  } catch {
    response = await postBatch(body);
  }
  if (response.status >= 500) response = await postBatch(body);
  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as
      | { error?: unknown; detail?: unknown }
      | null;
    const detail = payload?.detail ?? payload?.error;
    throw new Error(
      typeof detail === "string" ? detail : `状态更新失败（${response.status}）`
    );
  }
  const payload = (await response.json()) as {
    results?: EventReadBatchItem[];
  };
  if (!Array.isArray(payload.results)) throw new Error("批量状态回应缺少结果");
  return payload.results;
}

function postBatch(body: string) {
  return fetch("/actions/event-user-state-batch", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
    keepalive: true
  });
}

export function createScrollMarkCollector(
  options: {
    send?: typeof sendEventReadBatch;
    idleMs?: number;
    maxPending?: number;
    batchLimit?: number;
    setTimeoutFn?: (callback: () => void, delayMs: number) => number;
    clearTimeoutFn?: (timer: number) => void;
  } = {}
): ScrollMarkCollector {
  const send = options.send ?? sendEventReadBatch;
  const idleMs = options.idleMs ?? SCROLL_MARK_FLUSH_IDLE_MS;
  const maxPending = options.maxPending ?? SCROLL_MARK_FLUSH_MAX_PENDING;
  const batchLimit = options.batchLimit ?? SCROLL_MARK_BATCH_LIMIT;
  const setTimer =
    options.setTimeoutFn ??
    ((callback: () => void, delayMs: number) =>
      window.setTimeout(callback, delayMs));
  const clearTimer =
    options.clearTimeoutFn ?? ((timer: number) => window.clearTimeout(timer));
  let pending: PendingMark[] = [];
  let inFlight: PendingMark[] = [];
  let timer: number | null = null;

  function clearIdleTimer() {
    if (timer === null) return;
    clearTimer(timer);
    timer = null;
  }

  async function flush(drainLateArrivals = false): Promise<void> {
    clearIdleTimer();
    // 只结算进入 flush 时已到期的标记；在途请求期间新到的标记保留自己的
    // 空闲计时器或满额触发，避免持续滚动时退化成每个 RTT 一条小批量。
    // 护栏：满额触发在 pending 达到 maxPending 时同步进入 flush，因此入口
    // 快照不会超过一个分片（maxPending ≤ batchLimit 必须保持）；若调参打破
    // 该不变式，首片在途期间 due 余量将躲过 flushOnPagehide 的兜底快照。
    let due = pending.splice(0, pending.length);
    while (due.length) {
      const chunk = due.splice(0, batchLimit);
      inFlight = [...inFlight, ...chunk];
      try {
        const results = await send(chunk.map((entry) => entry.mark));
        const byOperation = new Map(
          results.map((item) => [item.operation_id, item])
        );
        for (const entry of chunk) {
          const item = byOperation.get(entry.mark.operation_id);
          if (item?.result) entry.resolve(item.result);
          else entry.reject(new Error(item?.error || "批量状态更新失败"));
        }
      } catch (error) {
        const failure =
          error instanceof Error ? error : new Error("批量状态更新失败");
        for (const entry of chunk) entry.reject(failure);
      } finally {
        inFlight = inFlight.filter((entry) => !chunk.includes(entry));
      }
      if (drainLateArrivals && !due.length && pending.length) {
        clearIdleTimer();
        due = pending.splice(0, pending.length);
      }
    }
  }

  return {
    enqueue(mark) {
      return new Promise<EventUserStateMutationResult>((resolve, reject) => {
        pending.push({ mark, resolve, reject });
        if (pending.length >= maxPending) {
          void flush();
          return;
        }
        clearIdleTimer();
        timer = setTimer(() => {
          timer = null;
          void flush();
        }, idleMs);
      });
    },
    flushNow() {
      return flush(true);
    },
    flushOnPagehide() {
      const entries = [...inFlight, ...pending];
      if (!entries.length) return;
      pending = [];
      clearIdleTimer();
      for (let index = 0; index < entries.length; index += batchLimit) {
        const chunk = entries.slice(index, index + batchLimit);
        navigator.sendBeacon?.(
          "/actions/event-user-state-batch",
          new Blob(
            [JSON.stringify({ marks: chunk.map((entry) => entry.mark) })],
            { type: "application/json" }
          )
        );
      }
      // 后台通道拿不到逐目标确认；以 AbortError 静默回退，服务端结果由重放保证。
      const abort = pagehideFlushAbort();
      for (const entry of entries) entry.reject(abort);
    }
  };
}
