import assert from "node:assert/strict";
import test from "node:test";

import {
  createScrollMarkCollector,
  sendEventReadBatch,
  SCROLL_MARK_BATCH_LIMIT,
  SCROLL_MARK_FLUSH_IDLE_MS,
  SCROLL_MARK_FLUSH_MAX_PENDING
} from "./event-read-batch.ts";

function mark(id) {
  return {
    event_uid: `event-${id}`,
    observed_revision_uid: `revision-${id}`,
    operation_id: `operation-${id}`
  };
}

function readResult(id) {
  return {
    operation_id: `operation-${id}`,
    event_uid: `event-${id}`,
    observed_revision_uid: `revision-${id}`,
    action: "read_status_set",
    value: "summary_seen",
    starred: false,
    updated_at: "2026-08-29T00:00:00Z",
    read_status: "summary_seen",
    seen_revision_uid: `revision-${id}`,
    current_revision_differs_from_seen: false,
    has_material_update: false,
    material_update_revision_uid: null
  };
}

function manualTimer() {
  const timers = new Map();
  let nextId = 1;
  return {
    setTimeoutFn: (callback, delayMs) => {
      const id = nextId++;
      timers.set(id, { callback, delayMs });
      return id;
    },
    clearTimeoutFn: (id) => timers.delete(id),
    fire() {
      const pending = [...timers.entries()];
      timers.clear();
      for (const [, timer] of pending) timer.callback();
    },
    get size() {
      return timers.size;
    },
    delays() {
      return [...timers.values()].map((timer) => timer.delayMs);
    }
  };
}

test("collector defaults match the shared batching contract", () => {
  assert.equal(SCROLL_MARK_FLUSH_IDLE_MS, 1500);
  assert.equal(SCROLL_MARK_FLUSH_MAX_PENDING, 10);
  assert.equal(SCROLL_MARK_BATCH_LIMIT, 50);
});

test("marks flush together after the idle timer and resolve per operation", async () => {
  const calls = [];
  const timer = manualTimer();
  const collector = createScrollMarkCollector({
    send: async (marks) => {
      calls.push(marks);
      return marks.map((entry) => ({
        operation_id: entry.operation_id,
        result: readResult(entry.operation_id.replace("operation-", ""))
      }));
    },
    ...timer
  });

  const first = collector.enqueue(mark(1));
  const second = collector.enqueue(mark(2));
  assert.equal(calls.length, 0);
  assert.deepEqual(timer.delays(), [SCROLL_MARK_FLUSH_IDLE_MS]);

  timer.fire();
  const results = await Promise.all([first, second]);
  assert.equal(calls.length, 1);
  assert.deepEqual(
    calls[0].map((entry) => entry.operation_id),
    ["operation-1", "operation-2"]
  );
  assert.equal(results[0].operation_id, "operation-1");
  assert.equal(results[1].operation_id, "operation-2");
});

test("reaching max pending flushes immediately without waiting for the timer", async () => {
  const calls = [];
  const timer = manualTimer();
  const collector = createScrollMarkCollector({
    maxPending: 2,
    send: async (marks) => {
      calls.push(marks);
      return marks.map((entry) => ({
        operation_id: entry.operation_id,
        result: readResult(entry.operation_id.replace("operation-", ""))
      }));
    },
    ...timer
  });

  const first = collector.enqueue(mark(1));
  const second = collector.enqueue(mark(2));
  await Promise.all([first, second]);
  assert.equal(calls.length, 1);
  assert.equal(timer.size, 0, "immediate flush clears the idle timer");
});

test("oversized batches split into chunks and marks never repeat across requests", async () => {
  const calls = [];
  const timer = manualTimer();
  const collector = createScrollMarkCollector({
    batchLimit: 2,
    maxPending: 5,
    send: async (marks) => {
      calls.push(marks.map((entry) => entry.operation_id));
      return marks.map((entry) => ({
        operation_id: entry.operation_id,
        result: readResult(entry.operation_id.replace("operation-", ""))
      }));
    },
    ...timer
  });

  const settled = Promise.all(
    [1, 2, 3, 4, 5].map((id) => collector.enqueue(mark(id)))
  );
  await settled;
  assert.deepEqual(calls, [
    ["operation-1", "operation-2"],
    ["operation-3", "operation-4"],
    ["operation-5"]
  ]);
  const seen = calls.flat();
  assert.equal(new Set(seen).size, seen.length);
});

test("marks arriving during an in-flight batch wait for their own cadence trigger", async () => {
  const calls = [];
  const resolvers = [];
  const timer = manualTimer();
  const collector = createScrollMarkCollector({
    send: (marks) =>
      new Promise((resolve) => {
        calls.push(marks.map((entry) => entry.operation_id));
        resolvers.push(() =>
          resolve(
            marks.map((entry) => ({
              operation_id: entry.operation_id,
              result: readResult(entry.operation_id.replace("operation-", ""))
            }))
          )
        );
      }),
    ...timer
  });

  const capped = Array.from({ length: 10 }, (_, index) =>
    collector.enqueue(mark(index + 1))
  );
  assert.equal(calls.length, 1, "满 10 条立即成批上送");

  const late = [11, 12, 13].map((id) => collector.enqueue(mark(id)));
  resolvers.shift()();
  await Promise.all(capped);
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(
    calls.length,
    1,
    "在途批次结束后，不足 10 条的后续标记不得立即上送"
  );
  assert.equal(timer.size, 1, "后续标记保留自己的空闲计时器");

  timer.fire();
  await new Promise((resolve) => setImmediate(resolve));
  resolvers.shift()();
  await Promise.all(late);
  assert.deepEqual(calls[1], ["operation-11", "operation-12", "operation-13"]);
  assert.equal(calls.length, 2);
});

test("flushNow drains marks that arrive while the drain is in flight", async () => {
  const calls = [];
  const resolvers = [];
  const timer = manualTimer();
  const collector = createScrollMarkCollector({
    send: (marks) =>
      new Promise((resolve) => {
        calls.push(marks.map((entry) => entry.operation_id));
        resolvers.push(() =>
          resolve(
            marks.map((entry) => ({
              operation_id: entry.operation_id,
              result: readResult(entry.operation_id.replace("operation-", ""))
            }))
          )
        );
      }),
    ...timer
  });

  const first = collector.enqueue(mark(1));
  const drained = collector.flushNow();
  const second = collector.enqueue(mark(2));
  resolvers.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  resolvers.shift()();
  await drained;
  await Promise.all([first, second]);
  assert.deepEqual(calls, [["operation-1"], ["operation-2"]]);
});

test("a per-target error rejects only its own promise", async () => {
  const timer = manualTimer();
  const collector = createScrollMarkCollector({
    maxPending: 2,
    send: async (marks) => [
      { operation_id: marks[0].operation_id, result: readResult("1") },
      { operation_id: marks[1].operation_id, error: "Event 已被后继事件取代，请刷新" }
    ],
    ...timer
  });

  const first = collector.enqueue(mark(1));
  const second = collector.enqueue(mark(2));
  assert.equal((await first).operation_id, "operation-1");
  await assert.rejects(second, /Event 已被后继事件取代/);
});

test("a transport failure rejects the whole in-flight chunk", async () => {
  const timer = manualTimer();
  const collector = createScrollMarkCollector({
    maxPending: 2,
    send: async () => {
      throw new Error("状态更新失败（502）");
    },
    ...timer
  });

  const first = collector.enqueue(mark(1));
  const second = collector.enqueue(mark(2));
  await assert.rejects(first, /状态更新失败/);
  await assert.rejects(second, /状态更新失败/);
});

test("pagehide flush beacons pending marks and rejects with silent AbortError", async () => {
  const beacons = [];
  const previousNavigator = globalThis.navigator;
  Object.defineProperty(globalThis, "navigator", {
    configurable: true,
    value: {
      sendBeacon: (url, blob) => {
        beacons.push({ url, blob });
        return true;
      }
    }
  });
  const previousBlob = globalThis.Blob;
  if (typeof globalThis.Blob !== "function") {
    globalThis.Blob = class {
      constructor(parts) {
        this.parts = parts;
      }
    };
  }
  try {
    const timer = manualTimer();
    const collector = createScrollMarkCollector({
      send: async () => {
        throw new Error("不应触发网络发送");
      },
      ...timer
    });
    const pending = collector.enqueue(mark(1));
    collector.flushOnPagehide();
    assert.equal(beacons.length, 1);
    assert.equal(beacons[0].url, "/actions/event-user-state-batch");
    await assert.rejects(pending, (error) => error.name === "AbortError");
    assert.equal(timer.size, 0);
  } finally {
    if (previousNavigator === undefined) delete globalThis.navigator;
    else
      Object.defineProperty(globalThis, "navigator", {
        configurable: true,
        value: previousNavigator
      });
    globalThis.Blob = previousBlob;
  }
});

test("sendEventReadBatch retries the identical body on network error and 5xx", async () => {
  const bodies = [];
  const previousFetch = globalThis.fetch;
  let attempt = 0;
  globalThis.fetch = async (url, init) => {
    bodies.push(init.body);
    attempt += 1;
    if (attempt === 1) throw new TypeError("network down");
    if (attempt === 2) return new Response("{}", { status: 502 });
    return new Response(
      JSON.stringify({
        results: [{ operation_id: "operation-1", result: readResult("1") }]
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  };
  try {
    const results = await sendEventReadBatch([mark(1)]);
    assert.equal(results[0].operation_id, "operation-1");
    assert.equal(bodies.length, 3);
    assert.equal(new Set(bodies).size, 1, "每次重试的请求体逐字节一致");
  } finally {
    globalThis.fetch = previousFetch;
  }
});

test("sendEventReadBatch surfaces the server detail for business errors", async () => {
  const previousFetch = globalThis.fetch;
  globalThis.fetch = async () =>
    new Response(JSON.stringify({ error: "Event 状态操作无效" }), {
      status: 400,
      headers: { "Content-Type": "application/json" }
    });
  try {
    await assert.rejects(sendEventReadBatch([mark(1)]), /Event 状态操作无效/);
  } finally {
    globalThis.fetch = previousFetch;
  }
});
