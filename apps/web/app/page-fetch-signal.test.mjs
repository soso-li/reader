import assert from "node:assert/strict";
import test from "node:test";

import { PAGE_FETCH_TIMEOUT_MS, withPageTimeout } from "./page-fetch-signal.ts";

async function abortReason(signal) {
  // AbortSignal.timeout 的内部定时器是 unref 的，需要一个 ref 定时器保持事件循环存活。
  const keepAlive = setTimeout(() => {}, 2000);
  try {
    return await new Promise((resolve) => {
      if (signal.aborted) {
        resolve(signal.reason);
        return;
      }
      signal.addEventListener("abort", () => resolve(signal.reason), { once: true });
    });
  } finally {
    clearTimeout(keepAlive);
  }
}

test("page fetch timeout has a browser-scale default", () => {
  assert.equal(PAGE_FETCH_TIMEOUT_MS, 15000);
});

test("a hung request aborts with TimeoutError instead of AbortError", async () => {
  const controller = new AbortController();
  const signal = withPageTimeout(controller.signal, 5);
  const reason = await abortReason(signal);
  assert.equal(reason.name, "TimeoutError");
});

test("a manual abort still surfaces AbortError so it stays silent", async () => {
  const controller = new AbortController();
  const signal = withPageTimeout(controller.signal, 60000);
  controller.abort(new DOMException("stop", "AbortError"));
  const reason = await abortReason(signal);
  assert.equal(reason.name, "AbortError");
});
