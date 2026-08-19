import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  citationTarget,
  originalOpenedSelectionForView,
  renderedEventReadTarget,
  synthesisRequestAvailable,
  synthesisStatusLabel,
  synthesisViewAvailable
} from "./event-synthesis.ts";
import { createEventReadStateMutation } from "./event-user-state.ts";

const eventUid = "11111111-1111-4111-8111-111111111111";

test("freezes structured targets for stale synthesis, current sources, and a new synthesis", () => {
  const stale = {
    source_view_revision_uid: "33333333-3333-4333-8333-333333333333",
    current: {
      target_revision_uid: "22222222-2222-4222-8222-222222222222"
    }
  };
  assert.deepEqual(
    renderedEventReadTarget(eventUid, stale, "synthesis", null),
    {
      event_uid: eventUid,
      observed_revision_uid: stale.current.target_revision_uid
    }
  );
  assert.deepEqual(renderedEventReadTarget(eventUid, stale, "source", null), {
    event_uid: eventUid,
    observed_revision_uid: stale.source_view_revision_uid
  });
  const current = {
    ...stale,
    current: { target_revision_uid: stale.source_view_revision_uid }
  };
  assert.deepEqual(
    renderedEventReadTarget(eventUid, current, "synthesis", null),
    {
      event_uid: eventUid,
      observed_revision_uid: current.source_view_revision_uid
    }
  );
});

test("original-opened distinguishes current sources from stale synthesis evidence", () => {
  const synthesis = {
    source_view_revision_uid: "33333333-3333-4333-8333-333333333333",
    current: {
      target_revision_uid: "22222222-2222-4222-8222-222222222222",
      blocks: [
        {
          citations: [
            {
              evidence_version_uid: "44444444-4444-4444-8444-444444444444",
              legacy_content_item_id_snapshot: 99,
              source: { source_id: 7 },
              url: "https://example.com/frozen"
            },
            {
              evidence_version_uid: "55555555-5555-4555-8555-555555555555",
              legacy_content_item_id_snapshot: 100,
              source: { source_id: 8 },
              url: "https://example.com/historical"
            }
          ]
        }
      ]
    }
  };
  const evidence = [
    {
      evidence_version_uid: "66666666-6666-4666-8666-666666666666",
      source_id: 7,
      legacy_content_item_id_snapshot: 99
    }
  ];
  const currentSource = originalOpenedSelectionForView({
    event_uid: eventUid,
    synthesis,
    current_revision_uid: synthesis.source_view_revision_uid,
    mode: "source",
    source_view_evidence: evidence,
    source: { source_id: 7, item_id: 99, url: "https://example.com/current" }
  });
  assert.deepEqual(
    currentSource,
    {
      item_id: 99,
      url: "https://example.com/current",
      target: {
        event_uid: eventUid,
        observed_revision_uid: synthesis.source_view_revision_uid,
        evidence: {
          source_id: 7,
          evidence_version_uid: evidence[0].evidence_version_uid
        }
      }
    }
  );
  const staleSource = originalOpenedSelectionForView({
    event_uid: eventUid,
    synthesis,
    current_revision_uid: synthesis.source_view_revision_uid,
    mode: "synthesis",
    source_view_evidence: evidence,
    source: { source_id: 7, item_id: 99, url: "https://example.com/current" }
  });
  assert.deepEqual(
    staleSource,
    {
      item_id: 99,
      url: "https://example.com/frozen",
      target: {
        event_uid: eventUid,
        observed_revision_uid: synthesis.current.target_revision_uid,
        evidence: {
          source_id: 7,
          evidence_version_uid:
            synthesis.current.blocks[0].citations[0].evidence_version_uid
        }
      }
    }
  );
  const historicalSource = originalOpenedSelectionForView({
    event_uid: eventUid,
    synthesis,
    current_revision_uid: synthesis.source_view_revision_uid,
    mode: "synthesis",
    source_view_evidence: evidence,
    source: {
      source_id: 8,
      evidence_version_uid: "55555555-5555-4555-8555-555555555555"
    }
  });
  assert.deepEqual(
    historicalSource,
    {
      item_id: 100,
      url: "https://example.com/historical",
      target: {
        event_uid: eventUid,
        observed_revision_uid: synthesis.current.target_revision_uid,
        evidence: {
          source_id: 8,
          evidence_version_uid: "55555555-5555-4555-8555-555555555555"
        }
      }
    }
  );
  const staleFallback = originalOpenedSelectionForView({
    event_uid: eventUid,
    synthesis,
    current_revision_uid: synthesis.source_view_revision_uid,
    mode: "synthesis",
    source_view_evidence: evidence,
    source: {
      source_id: 9,
      item_id: 101,
      url: "https://example.com/new-current-source"
    },
    fallback_to_first_synthesis_evidence: true
  });
  assert.deepEqual(staleFallback, staleSource);
  assert.deepEqual(
    createEventReadStateMutation(
      currentSource.target,
      "original_opened",
      "current-source-operation"
    ),
    {
      event_uid: eventUid,
      observed_revision_uid: synthesis.source_view_revision_uid,
      operation_id: "current-source-operation",
      action: "read_status_set",
      value: "original_opened",
      source_id: 7,
      evidence_version_uid: evidence[0].evidence_version_uid
    }
  );
  assert.deepEqual(
    createEventReadStateMutation(
      staleFallback.target,
      "original_opened",
      "stale-synthesis-operation"
    ),
    {
      event_uid: eventUid,
      observed_revision_uid: synthesis.current.target_revision_uid,
      operation_id: "stale-synthesis-operation",
      action: "read_status_set",
      value: "original_opened",
      source_id: 7,
      evidence_version_uid:
        synthesis.current.blocks[0].citations[0].evidence_version_uid
    }
  );
  assert.deepEqual(
    createEventReadStateMutation(
      historicalSource.target,
      "original_opened",
      "historical-source-operation"
    ),
    {
      event_uid: eventUid,
      observed_revision_uid: synthesis.current.target_revision_uid,
      operation_id: "historical-source-operation",
      action: "read_status_set",
      value: "original_opened",
      source_id: 8,
      evidence_version_uid: "55555555-5555-4555-8555-555555555555"
    }
  );
  assert.equal(
    originalOpenedSelectionForView({
      event_uid: eventUid,
      synthesis,
      current_revision_uid: synthesis.source_view_revision_uid,
      mode: "synthesis",
      source_view_evidence: evidence,
      source: {
        source_id: 7,
        item_id: 99,
        evidence_version_uid: "77777777-7777-4777-8777-777777777777",
        url: "https://example.com/current"
      }
    }),
    null
  );
  assert.equal(
    originalOpenedSelectionForView({
      event_uid: eventUid,
      synthesis,
      current_revision_uid: synthesis.source_view_revision_uid,
      mode: "source",
      source_view_evidence: undefined,
      source: { source_id: 7, item_id: 99, url: "https://example.com/current" }
    }),
    null
  );
});

const citation = {
  evidence_version_uid: "11111111-1111-4111-8111-111111111111",
  evidence_type: "article",
  role: "material",
  side: "support",
  source: {
    source_id: 7,
    name: "Frozen source",
    feed_url: "https://example.com/feed",
    site_url: "https://example.com",
    media_type: "article"
  },
  legacy_content_item_id_snapshot: 99,
  title: "Frozen title",
  url: "https://example.com/frozen-article",
  published_at: null
};

test("citation targets the current source item when it is still present", () => {
  assert.deepEqual(
    citationTarget(citation, [
      { id: 99, source_id: 7, url: "https://example.com/current-article" }
    ], [
      {
        evidence_version_uid: citation.evidence_version_uid,
        source_id: 7,
        legacy_content_item_id_snapshot: 99
      }
    ]),
    { kind: "source", itemId: 99 }
  );
});

test("citation stays frozen when the current item has a different evidence version", () => {
  assert.deepEqual(
    citationTarget(
      citation,
      [{ id: 99, source_id: 7, url: citation.url }],
      [
        {
          evidence_version_uid: "22222222-2222-4222-8222-222222222222",
          source_id: 7,
          legacy_content_item_id_snapshot: 99
        }
      ]
    ),
    {
      kind: "external",
      url: citation.url,
      sourceId: 7
    }
  );
});

test("citation falls back to its frozen URL when the source item is absent", () => {
  assert.deepEqual(citationTarget(citation, [], []), {
    kind: "external",
    url: "https://example.com/frozen-article",
    sourceId: 7
  });
});

test("synthesis copy does not expose internal provenance identifiers", async () => {
  const source = await readFile(new URL("./cluster-view.tsx", import.meta.url), "utf8");

  assert.doesNotMatch(source, /snapshot_uid/);
  assert.match(source, /renderedEventReadTarget/);
  assert.doesNotMatch(source, /citation\.side/);
  assert.match(source, /title={`查看来源：\${citation\.source\.name}`}/);
});

test("browser seam uses the revision-safe reading HTML and keeps original navigation independent", async () => {
  const source = await readFile(new URL("./cluster-view.tsx", import.meta.url), "utf8");

  assert.match(source, /data-event-read-mode=/);
  assert.match(source, /data-observed-revision-uid=/);
  assert.match(source, /data-source-view-revision-uid=/);
  assert.match(source, /originalOpenedSelectionFor\(item, undefined, true\)/);
  assert.match(source, /window\.open\(selection\?\.url \?\? item\.url/);
  assert.match(source, /html={selectedSourceItem\?\.reading_html}/);
  assert.doesNotMatch(source, new RegExp(["read", "ability"].join(""), "i"));
  assert.match(source, /原文已打开，但看过记录暂未保存/);
});

test("mobile source controls keep 44px touch targets", async () => {
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");

  assert.match(
    css,
    /@media \(max-width: 700px\)[\s\S]*?\.event-source-title-button\s*\{[^}]*min-height: 44px;/
  );
  assert.match(
    css,
    /@media \(max-width: 700px\)[\s\S]*?\.event-source-open-original\s*\{[^}]*min-width: 44px;[^}]*min-height: 44px;/
  );
  assert.match(
    css,
    /html\.reader-force-mobile[\s\S]*?button\.icon[\s\S]*?min-width: 44px;[\s\S]*?min-height: 44px;/
  );
  assert.match(
    css,
    /html\.reader-force-mobile[\s\S]*?\.folder-summary[\s\S]*?min-height: 44px;/
  );
  assert.match(
    css,
    /html\.reader-force-mobile \.event-source-title-button\s*\{[^}]*min-height: 44px;/
  );
  assert.match(
    css,
    /html\.reader-force-mobile \.event-source-open-original\s*\{[^}]*min-width: 44px;[^}]*min-height: 44px;/
  );
});

test("an old synthesis stays reachable after current evidence drops to one source", async () => {
  assert.equal(
    synthesisViewAvailable({ source_count: 1, current: { version_uid: "old" } }),
    true
  );
  assert.equal(synthesisViewAvailable({ source_count: 1, current: null }), false);

  const source = await readFile(new URL("./cluster-view.tsx", import.meta.url), "utf8");
  assert.match(source, /当前来源已不再等同于这份合成稿的证据范围/);
});

test("ordinary notice renders the cumulative sources added since the synthesis", async () => {
  assert.equal(synthesisStatusLabel("unreviewed"), "有未审新证据");
  assert.equal(synthesisStatusLabel("stale"), "有新证据尚未纳入");
  assert.equal(synthesisStatusLabel("current", 2), "新增 2 个来源");
  assert.equal(synthesisStatusLabel("current", 0), "");
  assert.equal(synthesisStatusLabel("missing"), "");

  const source = await readFile(new URL("./cluster-view.tsx", import.meta.url), "utf8");
  assert.match(source, /新增 \{synthesis\.new_source_count\} 个来源/);
});

test("material updates have a separate compact label", () => {
  assert.equal(synthesisStatusLabel("stale", 0, true), "看过后有更新");
  assert.equal(synthesisStatusLabel("unreviewed", 0, false), "有未审新证据");
  assert.equal(synthesisStatusLabel("current", 2, false), "新增 2 个来源");
});

test("manual synthesis is available for missing, unreviewed, or stale evidence without an active task", () => {
  const state = {
    status: "unreviewed",
    can_generate: true,
    task_status: "complete"
  };

  assert.equal(synthesisRequestAvailable(state), true);
  assert.equal(synthesisRequestAvailable({ ...state, task_status: "failed" }), true);
  assert.equal(synthesisRequestAvailable({ ...state, task_status: "blocked" }), true);
  assert.equal(synthesisRequestAvailable({ ...state, task_status: "apply_pending" }), false);
  assert.equal(synthesisRequestAvailable({ ...state, task_status: "apply_failed" }), false);
  assert.equal(synthesisRequestAvailable({ ...state, task_status: "stale_result" }), true);
  assert.equal(synthesisRequestAvailable({ ...state, status: "missing" }), true);
  assert.equal(synthesisRequestAvailable({ ...state, status: "stale" }), true);
  assert.equal(synthesisRequestAvailable({ ...state, status: "current" }), false);
  assert.equal(synthesisRequestAvailable({ ...state, task_status: "pending" }), false);
  assert.equal(synthesisRequestAvailable({ ...state, task_status: "running" }), false);
  assert.equal(synthesisRequestAvailable({ ...state, task_status: "canceled" }), false);
  assert.equal(synthesisRequestAvailable({ ...state, can_generate: false }), false);
});

test("stale synthesis copy distinguishes progress, failure, retry, and current sources", async () => {
  const source = await readFile(new URL("./cluster-view.tsx", import.meta.url), "utf8");

  assert.match(source, /有新证据尚未纳入/);
  assert.match(source, /synthesisTaskMessage\(synthesis\.task_status, "stale", blockedMessage\)/);
  assert.match(source, /请在设置 → 订阅管理调整后重试/);
  assert.match(source, /请在设置 → 任务分配调整后重试/);
  assert.match(source, /重试更新/);
});

test("unreviewed synthesis copy distinguishes review progress, failure, and retry", async () => {
  const source = await readFile(new URL("./cluster-view.tsx", import.meta.url), "utf8");
  const unreviewed = source.slice(
    source.indexOf('{synthesis.status === "unreviewed"'),
    source.indexOf('{synthesis.status === "stale"')
  );

  assert.match(unreviewed, /synthesisTaskMessage\(synthesis\.task_status, "unreviewed", blockedMessage\)/);
  assert.match(unreviewed, /synthesis\.task_status === "failed" \? "重试更新" : "更新合成稿"/);
});
