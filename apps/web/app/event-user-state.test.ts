import assert from "node:assert/strict";
import test from "node:test";

import {
  createOperationId,
  confirmedEventStatePatch,
  createEventReadStateMutation,
  createEventUserStateMutation,
  sendEventUserStateMutation,
  type EventUserStateMutation
} from "./event-user-state.ts";
import {
  clearEventReadErrorsAfterSuccess,
  detailEvidencePresented,
  detailSummarySeenAllowed,
  explicitReadStatusPresentationAllowed,
  isInteractionSurfacePresented,
  originalOpenedIntent,
  recordEventReadFailure,
  readStatusToggleIntent,
  summarySeenEligible,
  summarySeenIntent,
  visibleEventReadErrors,
  type OriginalOpenedTrigger,
  type SummarySeenTrigger
} from "./event-read-boundary.ts";
import {
  shouldStartVisibleDwellAttempt,
  startVisibleDwell
} from "./visible-dwell.ts";
import {
  scrollPastReadStatusTransition,
  scrollPastRowTransition
} from "./scroll-past-seen.ts";
import {
  createObjectUserStateMutation,
  sendClientUserState
} from "./client-user-state.ts";
import { isObjectUserStateType } from "./object-user-state.ts";
import { apiErrorStatus, apiFetch } from "./lib/api.ts";
import {
  bulkReadConfirmationFromPrepared,
  bulkReadPreparationFailure,
  confirmBulkReadWithRetry,
  isBulkReadConfirmationReady,
  serializeBulkReadBatch
} from "./bulk-read.ts";
import { applyAllUnreadCountDelta, effectiveUnreadCountDelta } from "./live-unread-count.ts";

const identity = {
  event_uid: "11111111-1111-4111-8111-111111111111",
  current_revision_uid: "22222222-2222-4222-8222-222222222222"
};

test("bulk read keeps only the opaque server batch before confirmation", () => {
  const prepared = {
    batch_id: "10101010-1010-4010-8010-101010101010",
    target_count: 42
  };

  const confirmation = bulkReadConfirmationFromPrepared(prepared);
  assert.ok(confirmation.batch);
  prepared.batch_id = "99999999-9999-4999-8999-999999999999";
  assert.equal(
    serializeBulkReadBatch(confirmation.batch),
    JSON.stringify({ batch_id: "10101010-1010-4010-8010-101010101010" })
  );
});

test("bulk read enters confirmation only for a non-empty server batch", () => {
  const empty = bulkReadConfirmationFromPrepared({ target_count: 0 });
  assert.equal(empty.batch, null);
  assert.equal(empty.hint, "当前范围没有未读内容");
  assert.equal(isBulkReadConfirmationReady(empty.batch), false);

  const prepared = {
    batch_id: "20202020-2020-4020-8020-202020202020",
    target_count: 42
  };
  const confirmation = bulkReadConfirmationFromPrepared(prepared);
  assert.equal(isBulkReadConfirmationReady(confirmation.batch), true);
  assert.equal(confirmation.hint, "将标记 42 条，再点确认");

  prepared.batch_id = "30303030-3030-4030-8030-303030303030";
  assert.equal(
    confirmation.batch?.batch_id,
    "20202020-2020-4020-8020-202020202020"
  );
});

test("bulk read preparation failure never enters confirmation", () => {
  const failed = bulkReadPreparationFailure(new Error("准备失败"));
  const networkFailure = bulkReadPreparationFailure(
    new TypeError("Failed to fetch")
  );
  const safariNetworkFailure = bulkReadPreparationFailure(
    new TypeError("Load failed")
  );
  const parseFailure = bulkReadPreparationFailure(
    new SyntaxError("Unexpected token '<'")
  );

  assert.equal(failed.batch, null);
  assert.equal(failed.hint, "准备失败");
  assert.equal(isBulkReadConfirmationReady(failed.batch), false);
  assert.equal(networkFailure.hint, "准备批量已读失败");
  assert.equal(safariNetworkFailure.hint, "准备批量已读失败");
  assert.equal(parseFailure.hint, "准备批量已读失败");
});

test("bulk read confirmation retry reuses the exact server batch id", async () => {
  const confirmation = bulkReadConfirmationFromPrepared({
    batch_id: "40404040-4040-4040-8040-404040404040",
    target_count: 1
  });
  assert.ok(confirmation.batch);
  const bodies: string[] = [];
  let attempts = 0;

  const result = await confirmBulkReadWithRetry(
    confirmation.batch,
    async (body) => {
      bodies.push(body);
      attempts += 1;
      if (attempts === 1) throw new TypeError("response lost");
      return "confirmed";
    },
    () => true
  );

  assert.equal(result, "confirmed");
  assert.equal(bodies.length, 2);
  assert.equal(new Set(bodies).size, 1);
  assert.equal(
    JSON.parse(bodies[0]).batch_id,
    "40404040-4040-4040-8040-404040404040"
  );
});

test("creates one explicit object set mutation with a caller-owned operation", () => {
  assert.deepEqual(
    createObjectUserStateMutation(
      {
        object_type: "item",
        object_id: 42,
        starred: true
      },
      "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
    ),
    {
      object_type: "item",
      object_id: 42,
      operation_id: "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa",
      starred: true
    }
  );
  assert.throws(
    () =>
      createObjectUserStateMutation(
        {
          object_type: "report",
          object_id: 20260714,
          starred: true,
          read_later: true
        },
        "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
      ),
    /一个状态值/
  );
});

test("object state targets exclude Cluster projections", () => {
  assert.equal(isObjectUserStateType("item"), true);
  assert.equal(isObjectUserStateType("report"), true);
  assert.equal(isObjectUserStateType("topic"), true);
  assert.equal(isObjectUserStateType("cluster"), false);
});

test("object beacon fallback reuses the exact operation body", async () => {
  const originalNavigator = globalThis.navigator;
  const originalFetch = globalThis.fetch;
  const bodies: string[] = [];
  Object.defineProperty(globalThis, "navigator", {
    configurable: true,
    value: { sendBeacon: () => false }
  });
  globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
    bodies.push(String(init?.body));
    return new Response(JSON.stringify({ ok: true }), {
      status: 200,
      headers: { "Content-Type": "application/json" }
    });
  }) as typeof fetch;

  try {
    await sendClientUserState({
      object_type: "topic",
      object_id: 7,
      operation_id: "cccccccc-3333-4333-8333-cccccccccccc",
      read_status: "summary_seen"
    });
    assert.equal(bodies.length, 1);
    assert.deepEqual(JSON.parse(bodies[0]), {
      object_type: "topic",
      object_id: 7,
      operation_id: "cccccccc-3333-4333-8333-cccccccccccc",
      read_status: "summary_seen"
    });
  } finally {
    Object.defineProperty(globalThis, "navigator", {
      configurable: true,
      value: originalNavigator
    });
    globalThis.fetch = originalFetch;
  }
});

test("object fetch retry reuses the exact operation body after an uncertain result", async () => {
  const originalNavigator = globalThis.navigator;
  const originalFetch = globalThis.fetch;
  const bodies: string[] = [];
  let attempt = 0;
  Object.defineProperty(globalThis, "navigator", {
    configurable: true,
    value: { sendBeacon: () => false }
  });
  globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
    bodies.push(String(init?.body));
    attempt += 1;
    if (attempt === 1) throw new TypeError("response lost");
    return new Response(
      JSON.stringify(attempt === 2 ? { error: "temporary" } : { ok: true }),
      {
        status: attempt === 2 ? 503 : 200,
        headers: { "Content-Type": "application/json" }
      }
    );
  }) as typeof fetch;

  try {
    await sendClientUserState(
      {
        object_type: "item",
        object_id: 42,
        operation_id: "dddddddd-4444-4444-8444-dddddddddddd",
        starred: true
      },
      { beacon: false }
    );
    assert.equal(bodies.length, 3);
    assert.equal(new Set(bodies).size, 1);
    assert.equal(
      JSON.parse(bodies[0]).operation_id,
      "dddddddd-4444-4444-8444-dddddddddddd"
    );
  } finally {
    Object.defineProperty(globalThis, "navigator", {
      configurable: true,
      value: originalNavigator
    });
    globalThis.fetch = originalFetch;
  }
});

test("object fetch does not retry a business error", async () => {
  const originalNavigator = globalThis.navigator;
  const originalFetch = globalThis.fetch;
  let fetchCalls = 0;
  Object.defineProperty(globalThis, "navigator", {
    configurable: true,
    value: { sendBeacon: () => false }
  });
  globalThis.fetch = (async () => {
    fetchCalls += 1;
    return new Response(JSON.stringify({ error: "operation_id 已用于另一项操作" }), {
      status: 409,
      headers: { "Content-Type": "application/json" }
    });
  }) as typeof fetch;

  try {
    await assert.rejects(
      sendClientUserState(
        {
          object_type: "topic",
          object_id: 7,
          operation_id: "eeeeeeee-5555-4555-8555-eeeeeeeeeeee",
          starred: true
        },
        { beacon: false }
      ),
      /阅读状态更新失败/
    );
    assert.equal(fetchCalls, 1);
  } finally {
    Object.defineProperty(globalThis, "navigator", {
      configurable: true,
      value: originalNavigator
    });
    globalThis.fetch = originalFetch;
  }
});

test("API client preserves an upstream business error status", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async () =>
    new Response(JSON.stringify({ detail: "operation_id 已用于另一项操作" }), {
      status: 409,
      headers: { "Content-Type": "application/json" }
    })) as typeof fetch;

  try {
    await assert.rejects(
      apiFetch("/user-state/topic/7", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          operation_id: "ffffffff-6666-4666-8666-ffffffffffff",
          starred: true
        })
      }),
      (error: unknown) => {
        assert.equal((error as { status?: number }).status, 409);
        assert.equal(apiErrorStatus(error), 409);
        return true;
      }
    );
    assert.equal(apiErrorStatus(new TypeError("fetch failed")), 502);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("object sendBeacon accepts the caller operation without a fetch", async () => {
  const originalNavigator = globalThis.navigator;
  const originalFetch = globalThis.fetch;
  const beaconBodies: Blob[] = [];
  let fetchCalls = 0;
  Object.defineProperty(globalThis, "navigator", {
    configurable: true,
    value: {
      sendBeacon: (_url: string, body?: BodyInit | null) => {
        assert.ok(body instanceof Blob);
        beaconBodies.push(body);
        return true;
      }
    }
  });
  globalThis.fetch = (async () => {
    fetchCalls += 1;
    return new Response(null, { status: 204 });
  }) as typeof fetch;

  try {
    await sendClientUserState({
      object_type: "report",
      object_id: 20260714,
      operation_id: "abababab-7777-4777-8777-abababababab",
      read_status: "summary_seen"
    });
    assert.equal(fetchCalls, 0);
    assert.equal(beaconBodies.length, 1);
    assert.equal(
      JSON.parse(await beaconBodies[0].text()).operation_id,
      "abababab-7777-4777-8777-abababababab"
    );
  } finally {
    Object.defineProperty(globalThis, "navigator", {
      configurable: true,
      value: originalNavigator
    });
    globalThis.fetch = originalFetch;
  }
});

test("creates an explicit set mutation from the rendered Event revision", () => {
  const mutation = createEventUserStateMutation(
    identity,
    "starred_set",
    false,
    "33333333-3333-4333-8333-333333333333"
  );

  assert.deepEqual(mutation, {
    event_uid: identity.event_uid,
    observed_revision_uid: identity.current_revision_uid,
    operation_id: "33333333-3333-4333-8333-333333333333",
    action: "starred_set",
    value: false
  });
});

test("creates a UUID operation ID when randomUUID is unavailable on HTTP", () => {
  const operationId = createOperationId({
    getRandomValues: (array) => {
      const bytes = array as Uint8Array;
      for (let index = 0; index < bytes.length; index += 1) bytes[index] = index;
      return array;
    }
  });

  assert.equal(operationId, "00010203-0405-4607-8809-0a0b0c0d0e0f");
});

test("creates revision-bound read mutations and requires the opened source", () => {
  const readTarget = {
    event_uid: identity.event_uid,
    observed_revision_uid: identity.current_revision_uid
  };
  assert.deepEqual(
    createEventReadStateMutation(
      readTarget,
      "summary_seen",
      "77777777-7777-4777-8777-777777777777"
    ),
    {
      event_uid: identity.event_uid,
      observed_revision_uid: identity.current_revision_uid,
      operation_id: "77777777-7777-4777-8777-777777777777",
      action: "read_status_set",
      value: "summary_seen"
    }
  );
  assert.deepEqual(
    createEventReadStateMutation(
      {
        ...readTarget,
        evidence: {
          source_id: 42,
          evidence_version_uid: "88888888-8888-4888-8888-888888888889"
        }
      },
      "original_opened",
      "88888888-8888-4888-8888-888888888888"
    ),
    {
      event_uid: identity.event_uid,
      observed_revision_uid: identity.current_revision_uid,
      operation_id: "88888888-8888-4888-8888-888888888888",
      action: "read_status_set",
      value: "original_opened",
      source_id: 42,
      evidence_version_uid: "88888888-8888-4888-8888-888888888889"
    }
  );
  assert.throws(
    () =>
      createEventReadStateMutation(
        readTarget,
        "original_opened",
        "99999999-9999-4999-8999-999999999999"
      ),
    /来源/
  );
});

test("does not create summary-seen intent for automatic or hidden detail", () => {
  assert.equal(
    summarySeenIntent("detail_dwell", {
      readStatus: "unread",
      skip: false,
      userPresented: false
    }),
    null
  );
  assert.equal(
    summarySeenIntent("selection_leave", {
      readStatus: "unread",
      skip: false,
      userPresented: false
    }),
    null
  );
  assert.equal(
    summarySeenIntent("detail_dwell", {
      readStatus: "unread",
      skip: false,
      userPresented: true,
      evidencePresented: false
    }),
    null
  );
  assert.equal(
    isInteractionSurfacePresented({
      isConnected: true,
      checkVisibility: () => false,
      getClientRects: () => ({ length: 1 }) as DOMRectList
    }),
    false
  );
  assert.equal(
    isInteractionSurfacePresented({
      isConnected: true,
      checkVisibility: () => true,
      getClientRects: () => ({ length: 0 }) as DOMRectList
    }),
    false
  );
  assert.equal(
    isInteractionSurfacePresented({
      isConnected: true,
      checkVisibility: () => true,
      getClientRects: () => ({ length: 1 }) as DOMRectList
    }),
    true
  );
  assert.equal(
    isInteractionSurfacePresented(
      {
        isConnected: true,
        checkVisibility: () => true,
        getClientRects: () => ({ length: 1 }) as DOMRectList
      },
      "hidden"
    ),
    false
  );
});

test("requires rendered evidence before detail dwell can record summary seen", () => {
  assert.equal(detailEvidencePresented(true, "", 0), false);
  assert.equal(detailEvidencePresented(true, " source body ", 0), true);
  assert.equal(detailEvidencePresented(false, "source body", 0), false);
  assert.equal(detailEvidencePresented(false, "", 1), true);
});

test("history navigation restores an explicit detail reading boundary", () => {
  assert.equal(detailSummarySeenAllowed("automatic", 42), false);
  assert.equal(detailSummarySeenAllowed("direct_url", 42), true);
  assert.equal(detailSummarySeenAllowed("user_selection", 42), true);
  assert.equal(detailSummarySeenAllowed("history_navigation", 42), true);
  assert.equal(detailSummarySeenAllowed("history_navigation", null), false);
});

test("explicit unread suppresses dwell until a new detail presentation", () => {
  const selected = detailSummarySeenAllowed("user_selection", 42);
  assert.equal(selected, true);
  assert.equal(
    explicitReadStatusPresentationAllowed(selected, "unread"),
    false
  );
  assert.equal(detailSummarySeenAllowed("user_selection", 42), true);
});

test("hidden detail starts a fresh dwell when the page becomes visible", () => {
  let visible = true;
  let listener: (() => void) | undefined;
  let nextTimerId = 0;
  const timers = new Map<number, () => void>();
  let seenCount = 0;

  const cleanup = startVisibleDwell({
    deferMs: 1800,
    isVisible: () => visible,
    markSeen: () => {
      seenCount += 1;
    },
    addVisibilityListener: (callback) => {
      listener = callback;
    },
    removeVisibilityListener: (callback) => {
      if (listener === callback) listener = undefined;
    },
    setTimer: (callback) => {
      nextTimerId += 1;
      timers.set(nextTimerId, callback);
      return nextTimerId;
    },
    clearTimer: (timerId) => {
      timers.delete(timerId as number);
    }
  });

  assert.equal(timers.size, 1);
  visible = false;
  listener?.();
  assert.equal(timers.size, 0);
  visible = true;
  listener?.();
  assert.equal(timers.size, 1);
  const timer = [...timers.values()][0];
  timer();
  assert.equal(seenCount, 1);
  cleanup();
  assert.equal(listener, undefined);
});

test("a failed dwell is not automatically retried until a new presentation", () => {
  const attemptKey = "cluster-42:revision-a:presentation-1";
  assert.equal(
    shouldStartVisibleDwellAttempt({
      eligible: true,
      attemptKey,
      attemptedKey: null
    }),
    true
  );
  assert.equal(
    shouldStartVisibleDwellAttempt({
      eligible: true,
      attemptKey,
      attemptedKey: attemptKey
    }),
    false
  );
  assert.equal(
    shouldStartVisibleDwellAttempt({
      eligible: true,
      attemptKey: "cluster-42:revision-a:presentation-2",
      attemptedKey: attemptKey
    }),
    true
  );
});

test("only the latest read operation may present a failure", () => {
  const sourceA = {
    clusterId: 42,
    operationId: "source-a",
    requestedStatus: "original_opened" as const,
    target: {
      event_uid: identity.event_uid,
      observed_revision_uid: identity.current_revision_uid,
      evidence: { source_id: 101, evidence_version_uid: "evidence-a" }
    },
    surface: "detail" as const
  };
  const sourceB = {
    ...sourceA,
    operationId: "source-b",
    target: {
      ...sourceA.target,
      evidence: { source_id: 202, evidence_version_uid: "evidence-b" }
    }
  };

  const failedA = recordEventReadFailure([], sourceA, sourceB);
  assert.deepEqual(failedA, [
    {
      ...sourceA,
      message: "原文已打开，但看过记录保存失败，请重试"
    }
  ]);
  assert.deepEqual(clearEventReadErrorsAfterSuccess(failedA, sourceB), failedA);
  assert.deepEqual(clearEventReadErrorsAfterSuccess(failedA, sourceA), []);

  const sourceARetry = { ...sourceA, operationId: "source-a-retry" };
  assert.deepEqual(
    recordEventReadFailure(failedA, sourceARetry, sourceARetry),
    [
      {
        ...sourceARetry,
        message: "原文已打开，但看过记录保存失败，请重试"
      }
    ]
  );
});

test("read failures remain scoped to their cluster and evidence", () => {
  const originalFailure = {
    clusterId: 42,
    operationId: "source-a",
    requestedStatus: "original_opened" as const,
    target: {
      event_uid: identity.event_uid,
      observed_revision_uid: identity.current_revision_uid,
      evidence: { source_id: 101, evidence_version_uid: "evidence-a" }
    },
    surface: "detail" as const
  };
  const stateFailure = {
    clusterId: 42,
    operationId: "list-seen",
    requestedStatus: "summary_seen" as const,
    target: {
      event_uid: identity.event_uid,
      observed_revision_uid: identity.current_revision_uid
    },
    surface: "list" as const
  };
  const errors = recordEventReadFailure(
    recordEventReadFailure([], originalFailure, originalFailure),
    stateFailure,
    stateFailure
  );

  assert.deepEqual(
    visibleEventReadErrors(errors, "detail", 42).map((error) => error.operationId),
    ["source-a"]
  );
  assert.deepEqual(visibleEventReadErrors(errors, "detail", 84), []);
  assert.deepEqual(
    clearEventReadErrorsAfterSuccess(errors, {
      clusterId: 84,
      operationId: "other-cluster",
      requestedStatus: "summary_seen",
      target: {
        event_uid: identity.event_uid,
        observed_revision_uid: identity.current_revision_uid
      },
      surface: "detail"
    }),
    errors
  );

  const clearedStateFailure = clearEventReadErrorsAfterSuccess(errors, {
    clusterId: 42,
    operationId: "detail-seen",
    requestedStatus: "summary_seen",
    target: {
      event_uid: identity.event_uid,
      observed_revision_uid: identity.current_revision_uid
    },
    surface: "detail"
  });
  assert.deepEqual(
    clearedStateFailure.map((error) => error.operationId),
    ["source-a"]
  );
  assert.deepEqual(visibleEventReadErrors(clearedStateFailure, "list", 42), []);

  const repeatedListFailure = {
    ...stateFailure,
    clusterId: 84,
    operationId: "another-list-seen",
    message: "事件信息不完整，请刷新后重试"
  };
  const firstListFailure = {
    ...stateFailure,
    message: "事件信息不完整，请刷新后重试"
  };
  assert.equal(
    visibleEventReadErrors([firstListFailure, repeatedListFailure], "list", null).length,
    1
  );
});

test("records only physically presented dwell, scroll-past, and selection-leave boundaries", () => {
  const triggers: SummarySeenTrigger[] = [
    "detail_dwell",
    "scroll_past",
    "selection_leave"
  ];
  for (const trigger of triggers) {
    assert.equal(
      summarySeenIntent(trigger, {
        readStatus: "unread",
        skip: false,
        userPresented: true
      }),
      "summary_seen"
    );
    assert.equal(
      summarySeenIntent(trigger, {
        readStatus: "summary_seen",
        skip: false,
        userPresented: true
      }),
      null
    );
  }
  assert.equal(
    summarySeenIntent("scroll_past", {
      readStatus: "unread",
      skip: true,
      userPresented: true
    }),
    null
  );
});

test("advances seen evidence when the user dwells on a newer current revision", () => {
  assert.equal(summarySeenEligible("summary_seen", true), true);
  assert.equal(summarySeenEligible("summary_seen", false), false);
  for (const readStatus of ["summary_seen", "original_opened"]) {
    assert.equal(
      summarySeenIntent("detail_dwell", {
        readStatus,
        skip: false,
        userPresented: true,
        currentRevisionDiffersFromSeen: true
      }),
      "summary_seen"
    );
  }
  assert.equal(
    summarySeenIntent("detail_dwell", {
      readStatus: "summary_seen",
      skip: false,
      userPresented: true,
      currentRevisionDiffersFromSeen: false
    }),
    null
  );
  assert.equal(
    summarySeenIntent("detail_dwell", {
      readStatus: "summary_seen",
      skip: false,
      userPresented: false,
      currentRevisionDiffersFromSeen: true
    }),
    null
  );
});

test("scroll-past requires a physically visible row before it can be marked", () => {
  const root = { top: 100, bottom: 500 };

  assert.deepEqual(
    scrollPastRowTransition({
      row: { top: -20, bottom: 80 },
      root,
      surfacePresented: true,
      wasPresented: false
    }),
    { presented: false, markSeen: false }
  );
  assert.deepEqual(
    scrollPastRowTransition({
      row: { top: 120, bottom: 180 },
      root,
      surfacePresented: false,
      wasPresented: false
    }),
    { presented: false, markSeen: false }
  );
  assert.deepEqual(
    scrollPastRowTransition({
      row: { top: 120, bottom: 180 },
      root,
      surfacePresented: true,
      wasPresented: false
    }),
    { presented: true, markSeen: false }
  );
  assert.deepEqual(
    scrollPastRowTransition({
      row: { top: -20, bottom: 80 },
      root,
      surfacePresented: true,
      wasPresented: true
    }),
    { presented: true, markSeen: true }
  );
});

test("explicit unread resets scroll-past tracking without duplicating an in-flight mark", () => {
  assert.deepEqual(
    scrollPastReadStatusTransition({
      previousReadStatus: "unread",
      nextReadStatus: "unread",
      marked: true,
      presented: true
    }),
    { marked: true, presented: true }
  );
  assert.deepEqual(
    scrollPastReadStatusTransition({
      previousReadStatus: "summary_seen",
      nextReadStatus: "unread",
      marked: true,
      presented: true
    }),
    { marked: false, presented: false }
  );
});

test("a newer current revision resets scroll-past tracking even when status is already seen", () => {
  assert.deepEqual(
    scrollPastReadStatusTransition({
      previousReadStatus: "summary_seen",
      nextReadStatus: "summary_seen",
      previousRevisionUid: "revision-a",
      nextRevisionUid: "revision-b",
      marked: true,
      presented: true
    }),
    { marked: false, presented: false }
  );
});

test("title, source, and O shortcut preserve the exact opened source", () => {
  const triggers: OriginalOpenedTrigger[] = [
    "title",
    "source",
    "shortcut"
  ];
  for (const trigger of triggers) {
    assert.deepEqual(originalOpenedIntent(trigger, 42), {
      value: "original_opened",
      sourceId: 42
    });
  }
  assert.throws(() => originalOpenedIntent("source", 0), /来源/);
});

test("read mutations freeze the rendered revision and exact opened evidence", () => {
  const renderedRevisionUid = "33333333-3333-4333-8333-333333333333";
  const evidenceVersionUid = "44444444-4444-4444-8444-444444444444";
  assert.deepEqual(
    createEventReadStateMutation(
      {
        event_uid: identity.event_uid,
        observed_revision_uid: renderedRevisionUid,
        evidence: {
          source_id: 42,
          evidence_version_uid: evidenceVersionUid
        }
      },
      "original_opened",
      "55555555-5555-4555-8555-555555555555"
    ),
    {
      event_uid: identity.event_uid,
      observed_revision_uid: renderedRevisionUid,
      operation_id: "55555555-5555-4555-8555-555555555555",
      action: "read_status_set",
      value: "original_opened",
      source_id: 42,
      evidence_version_uid: evidenceVersionUid
    }
  );
});

test("explicit read toggle uses set intents without erasing seen history locally", () => {
  assert.equal(readStatusToggleIntent("unread"), "summary_seen");
  assert.equal(readStatusToggleIntent("summary_seen"), "unread");
  assert.equal(readStatusToggleIntent("original_opened"), "unread");
});

test("unread counts follow confirmed effective Event state", () => {
  assert.equal(
    effectiveUnreadCountDelta(
      { read_status: "unread", has_material_update: false },
      { read_status: "summary_seen", has_material_update: false }
    ),
    -1
  );
  assert.equal(
    effectiveUnreadCountDelta(
      { read_status: "summary_seen", has_material_update: false },
      { read_status: "unread", has_material_update: false }
    ),
    1
  );
  assert.equal(
    effectiveUnreadCountDelta(
      { read_status: "summary_seen", has_material_update: true },
      { read_status: "original_opened", has_material_update: false }
    ),
    -1
  );
  assert.equal(
    effectiveUnreadCountDelta(
      { read_status: "summary_seen", has_material_update: true },
      { read_status: "summary_seen", has_material_update: true }
    ),
    0
  );
  assert.deepEqual(
    applyAllUnreadCountDelta(
      [{ id: 1, unread_count: 4, all_unread_count: 9 }],
      -1
    ),
    [{ id: 1, unread_count: 4, all_unread_count: 8 }]
  );
});

test("confirms only the field owned by each operation result", () => {
  const baseResult = {
    event_uid: identity.event_uid,
    observed_revision_uid: identity.current_revision_uid,
    operation_id: "66666666-6666-4666-8666-666666666666",
    value: true,
    read_status: "unread",
    read_later: false,
    starred: true,
    seen_revision_uid: null,
    current_revision_differs_from_seen: true,
    has_material_update: false,
    material_update_revision_uid: null,
    updated_at: "2026-07-14T05:00:00Z"
  };

  assert.deepEqual(
    confirmedEventStatePatch({ ...baseResult, action: "starred_set" }),
    { starred: true }
  );
  assert.deepEqual(
    confirmedEventStatePatch({
      ...baseResult,
      action: "read_later_set",
      read_later: true
    }),
    { read_later: true }
  );
  assert.deepEqual(
    confirmedEventStatePatch({
      ...baseResult,
      action: "read_status_set",
      value: "summary_seen",
      read_status: "summary_seen",
      seen_revision_uid: identity.current_revision_uid,
      current_revision_differs_from_seen: false,
      has_material_update: false,
      material_update_revision_uid: null
    }),
    {
      read_status: "summary_seen",
      seen_revision_uid: identity.current_revision_uid,
      current_revision_differs_from_seen: false,
      has_material_update: false,
      material_update_revision_uid: null
    }
  );
});

test("fetch retry reuses the exact operation body", async () => {
  const bodies: string[] = [];
  const originalFetch = globalThis.fetch;
  let attempt = 0;
  globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
    bodies.push(String(init?.body));
    attempt += 1;
    return new Response(
      JSON.stringify(attempt === 1 ? { error: "temporary" } : { ok: true }),
      {
        status: attempt === 1 ? 503 : 200,
        headers: { "Content-Type": "application/json" }
      }
    );
  }) as typeof fetch;

  try {
    const mutation: EventUserStateMutation = {
      event_uid: identity.event_uid,
      observed_revision_uid: identity.current_revision_uid,
      operation_id: "44444444-4444-4444-8444-444444444444",
      action: "read_later_set",
      value: true
    };
    await sendEventUserStateMutation(mutation, { beacon: false });
    assert.equal(bodies.length, 2);
    assert.equal(bodies[0], bodies[1]);
    assert.equal(JSON.parse(bodies[0]).operation_id, mutation.operation_id);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("sendBeacon sends the caller-created operation without making another one", async () => {
  const originalNavigator = globalThis.navigator;
  const originalFetch = globalThis.fetch;
  const beaconBodies: Blob[] = [];
  let fetchCalls = 0;
  Object.defineProperty(globalThis, "navigator", {
    configurable: true,
    value: {
      sendBeacon: (_url: string, body?: BodyInit | null) => {
        assert.ok(body instanceof Blob);
        beaconBodies.push(body);
        return true;
      }
    }
  });
  globalThis.fetch = (async () => {
    fetchCalls += 1;
    return new Response(null, { status: 204 });
  }) as typeof fetch;

  try {
    const mutation = createEventUserStateMutation(
      identity,
      "starred_set",
      true,
      "55555555-5555-4555-8555-555555555555"
    );
    await sendEventUserStateMutation(mutation);
    assert.equal(fetchCalls, 0);
    assert.equal(beaconBodies.length, 1);
    assert.equal(JSON.parse(await beaconBodies[0].text()).operation_id, mutation.operation_id);
  } finally {
    Object.defineProperty(globalThis, "navigator", {
      configurable: true,
      value: originalNavigator
    });
    globalThis.fetch = originalFetch;
  }
});
