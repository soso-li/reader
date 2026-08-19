import assert from "node:assert/strict";
import test from "node:test";

import { JSDOM } from "jsdom";
import React, { act } from "react";
import { createRoot } from "react-dom/client";

import ClusterView from "./cluster-view.tsx";

const R1 = "11111111-1111-4111-8111-111111111111";
const R2 = "22222222-2222-4222-8222-222222222222";

test("multi-source events honor the source-first default while keeping synthesis available", async () => {
  const cluster = clusterFixture({ id: 11, defaultView: "source" });
  const mounted = await mountReader([cluster], cluster);

  try {
    const detail = mounted.container.querySelector(".detail-body");
    assert.equal(detail?.getAttribute("data-event-read-mode"), "source");
    assert.equal(buttonWithText(mounted.container, "来源").getAttribute("aria-selected"), "true");

    await click(buttonWithText(mounted.container, "合成稿"));
    assert.equal(detail?.getAttribute("data-event-read-mode"), "synthesis");
  } finally {
    await mounted.unmount();
  }
});

test("list previews never render as detail while stale full-body requests are cancelled", async () => {
  const first = clusterFixture({ id: 11, defaultView: "source" });
  const second = clusterFixture({ id: 22, title: "第二条事件", defaultView: "source" });
  const preview = (cluster) => ({
    ...cluster,
    generated_summary: `列表摘要不是正文 ${cluster.id}`,
    items: cluster.items.map((item) => ({
      ...item,
      summary: `列表摘要不是正文 ${cluster.id}`,
      content_text: "",
      reading_html: null
    })),
    synthesis: null,
    source_view_evidence: undefined
  });
  const responses = new Map([[11, deferred()], [22, deferred()]]);
  const aborted = [];
  const mounted = await mountReader([preview(first), preview(second)], null, {
    fetchImpl: async (input, init) => {
      const id = Number(String(input).match(/clusters\/(\d+)/)?.[1]);
      const response = responses.get(id);
      init.signal.addEventListener("abort", () => {
        aborted.push(id);
        response.reject(new DOMException("cancelled", "AbortError"));
      }, { once: true });
      return response.promise;
    }
  });

  try {
    const links = mounted.container.querySelectorAll(".stretched-row-link");
    await click(links[0]);
    let loading = mounted.container.querySelector(".detail-loading");
    assert.ok(loading);
    assert.match(loading.textContent ?? "", /正在加载全文/);
    assert.doesNotMatch(loading.textContent ?? "", /列表摘要不是正文 11/);
    assert.equal(mounted.container.querySelector(".event-detail-tabs"), null);
    assert.equal(mounted.container.querySelector("[data-event-read-mode]"), null);

    await click(links[1]);
    await flushReact();
    loading = mounted.container.querySelector(".detail-loading");
    assert.deepEqual(aborted, [11]);
    assert.match(loading?.textContent ?? "", /第二条事件/);
    assert.doesNotMatch(loading?.textContent ?? "", /列表摘要不是正文 22/);

    responses.get(22).resolve(jsonResponse(second));
    await flushReact();

    assert.equal(mounted.container.querySelector(".detail-loading"), null);
    assert.match(mounted.container.querySelector(".detail-body")?.textContent ?? "", /第二条事件.*来源正文 1/s);
    assert.ok(mounted.container.querySelector(".event-detail-tabs"));
  } finally {
    await mounted.unmount();
  }
});

test("cluster scope changes start list and count requests together", async () => {
  const cluster = clusterFixture({ id: 11 });
  const list = deferred();
  const count = deferred();
  const calls = [];
  const mounted = await mountReader([cluster], null, {
    skipSeen: false,
    fetchImpl: async (input) => {
      const path = new URL(String(input)).pathname;
      calls.push(path);
      if (path === "/api/clusters") return list.promise;
      if (path === "/api/clusters/count") return count.promise;
      throw new Error(`unexpected fetch: ${path}`);
    }
  });

  try {
    await click(mounted.container.querySelector("a.filter-all"));
    assert.deepEqual(calls, ["/api/clusters", "/api/clusters/count"]);

    list.resolve(jsonResponse([cluster]));
    count.resolve(jsonResponse({ count: 1 }));
    await flushReact();
  } finally {
    await mounted.unmount();
  }
});

test("rapid mobile filter cycling cancels stale loads and commits only the latest view", async () => {
  const cluster = clusterFixture({ id: 11 });
  const lists = [];
  const listSignals = [];
  const mounted = await mountReader([cluster], null, {
    skipSeen: false,
    fetchImpl: async (input, init) => {
      const path = new URL(String(input)).pathname;
      if (path === "/api/clusters") {
        const list = deferred();
        lists.push(list);
        listSignals.push(init.signal);
        return list.promise;
      }
      if (path === "/api/clusters/count") return jsonResponse({ count: 1 });
      throw new Error(`unexpected fetch: ${path}`);
    }
  });

  try {
    mounted.container.className = "app-shell mobile-list";
    await click(mounted.container.querySelector("a.filter-all"));
    await click(mounted.container.querySelector("a.filter-starred"));
    assert.equal(listSignals[0].aborted, true);

    await click(mounted.container.querySelector("a.filter-sources"));
    assert.equal(listSignals[1].aborted, true);
    assert.match(window.location.search, /pane=sources/);

    await click(mounted.container.querySelector("a.filter-unread"));
    lists[2].resolve(jsonResponse([cluster]));
    await flushReact();
    lists[0].resolve(jsonResponse([]));
    lists[1].resolve(jsonResponse([]));
    await flushReact();

    assert.equal(listSignals[2].aborted, false);
    assert.match(window.location.search, /filter=unread/);
    assert.match(window.location.search, /pane=list/);
    assert.match(mounted.container.querySelector("a.filter-unread").className, /active/);
    assert.match(mounted.container.className, /mobile-list/);
  } finally {
    await mounted.unmount();
  }
});

test("history navigation can restore a source pane while changing filters", async () => {
  const cluster = clusterFixture({ id: 11 });
  const lists = [];
  const mounted = await mountReader([cluster], null, {
    skipSeen: false,
    fetchImpl: async (input) => {
      const path = new URL(String(input)).pathname;
      if (path === "/api/clusters") {
        const list = deferred();
        lists.push(list);
        return list.promise;
      }
      if (path === "/api/clusters/count") return jsonResponse({ count: 1 });
      throw new Error(`unexpected fetch: ${path}`);
    }
  });

  try {
    mounted.container.className = "app-shell mobile-list";
    await click(mounted.container.querySelector("a.filter-all"));
    lists[0].resolve(jsonResponse([cluster]));
    await flushReact();

    await act(async () => {
      window.history.pushState({}, "", "/?view=clusters&filter=unread&pane=sources");
      window.dispatchEvent(new window.PopStateEvent("popstate"));
      await Promise.resolve();
    });
    lists[1].resolve(jsonResponse([cluster]));
    await flushReact();

    assert.doesNotMatch(mounted.container.className, /mobile-(?:list|detail)/);
    assert.match(mounted.container.querySelector("a.filter-sources").className, /active/);
  } finally {
    await mounted.unmount();
  }
});

test("cluster detail exposes read later to pointer users", async () => {
  const cluster = clusterFixture({ id: 11 });
  const requests = [];
  const mounted = await mountReader([cluster], cluster, {
    fetchImpl: async (_url, init) => {
      const request = JSON.parse(init.body);
      requests.push(request);
      return jsonResponse({
        ...request,
        action: "read_later_set",
        read_later: true,
        starred: false,
        updated_at: "2026-07-16T03:00:00Z"
      });
    }
  });

  try {
    const button = mounted.container.querySelector("button[aria-label='稍后阅读']");
    assert.ok(button);
    assert.match(button.closest(".toolbar-more-row")?.textContent ?? "", /稍后读/);
    await click(button);
    assert.equal(requests.length, 1);
    assert.equal(requests[0].action, "read_later_set");
    assert.equal(requests[0].value, true);
  } finally {
    await mounted.unmount();
  }
});

test("material r2 returns a seen r1 event to the unread list without changing its order", async () => {
  const first = clusterFixture({
    id: 11,
    hasMaterialUpdate: true,
    materialUpdateRevisionUid: R2,
    synthesisTargetRevisionUid: R1,
    synthesisStatus: "stale"
  });
  const second = clusterFixture({ id: 22, title: "第二条事件" });
  const staleSynthesisReads = [];
  const mounted = await mountReader([first, second], first, {
    skipSeen: false,
    visibleSurfaces: true,
    immediateDwell: true,
    fetchImpl: async (_url, init) => {
      const request = JSON.parse(init.body);
      staleSynthesisReads.push(request);
      return jsonResponse(readResult(request, "summary_seen", false, null));
    }
  });

  try {
    await flushReact();
    const entries = [...mounted.container.querySelectorAll(".cluster-list-entry")];
    assert.deepEqual(
      entries.map((entry) => entry.getAttribute("data-scroll-seen-id")),
      ["11", "22"]
    );
    assert.match(entries[0].textContent ?? "", /看过后有更新/);
    assert.equal(
      entries[0].getAttribute("data-material-update-revision-uid"),
      R2
    );
    assert.equal(
      mounted.container.querySelector(".detail-body")?.getAttribute(
        "data-observed-revision-uid"
      ),
      R1,
      "viewing the stale synthesis must stay bound to r1"
    );
    assert.equal(
      staleSynthesisReads.length,
      0,
      "viewing stale r1 again must not acknowledge material r2"
    );
    assert.equal(
      mounted.container.querySelector(".detail-body [role='status']")?.textContent?.trim(),
      "看过后有更新"
    );
    assert.ok(
      mounted.container.querySelector("button[aria-label='标记未读']"),
      "effective unread must not overwrite the persisted seen state"
    );
  } finally {
    await mounted.unmount();
  }
});

test("unread cluster pagination continues after the last loaded event", async () => {
  const requests = [];
  const nextRows = [
    clusterFixture({ id: 10, title: "第三条事件" }),
    clusterFixture({ id: 9, title: "第四条事件" })
  ];
  const mounted = await mountReader(
    [clusterFixture({ id: 22 }), clusterFixture({ id: 11 })],
    null,
    {
      intersectImmediately: true,
      pageSize: 2,
      fetchImpl: async (input) => {
        const url = new URL(String(input));
        requests.push(url);
        return jsonResponse(requests.length === 1 ? nextRows : []);
      }
    }
  );

  try {
    await flushReact();
    await flushReact();

    assert.equal(requests[0].searchParams.get("cursor_id"), "11");
    assert.equal(requests[0].searchParams.has("offset"), false);
    assert.deepEqual(
      [...mounted.container.querySelectorAll(".cluster-list-entry")].map(
        (entry) => entry.getAttribute("data-scroll-seen-id")
      ),
      ["22", "11", "10", "9"]
    );
  } finally {
    await mounted.unmount();
  }
});

test("current Source r2 clears optimistically, rolls back on failure, and stays clear after confirmation", async () => {
  const cluster = clusterFixture({
    id: 11,
    hasMaterialUpdate: true,
    materialUpdateRevisionUid: R2,
    synthesisTargetRevisionUid: R1,
    synthesisStatus: "stale"
  });
  const responses = [deferred(), deferred()];
  const requests = [];
  const mounted = await mountReader([cluster], cluster, {
    skipSeen: true,
    fetchImpl: async (url, init) => {
      assert.equal(url, "/actions/event-user-state");
      requests.push(JSON.parse(init.body));
      return responses[requests.length - 1].promise;
    }
  });

  try {
    assert.equal(mounted.container.querySelector(".list-status strong")?.textContent, "1");
    await click(buttonWithText(mounted.container, "来源"));
    const detail = mounted.container.querySelector(".detail-body");
    assert.equal(detail?.getAttribute("data-event-read-mode"), "source");
    assert.equal(detail?.getAttribute("data-observed-revision-uid"), R2);
    assert.equal(detail?.getAttribute("data-has-material-update"), "true");

    const currentSource = mounted.container.querySelector(".detail-title-link");
    assert.ok(currentSource);
    currentSource.addEventListener("click", (event) => event.preventDefault());
    await click(currentSource);
    assert.equal(requests.length, 1);
    assert.equal(requests[0].action, "read_status_set");
    assert.equal(requests[0].value, "original_opened");
    assert.equal(requests[0].observed_revision_uid, R2);
    assert.equal(detail?.getAttribute("data-has-material-update"), "false");
    assert.equal(
      mounted.container
        .querySelector(".cluster-list-entry")
        ?.getAttribute("data-has-material-update"),
      "false"
    );
    assert.equal(
      mounted.container.querySelector(".material-update-notice"),
      null,
      "the optimistic patch may clear only for the exact material target"
    );
    assert.equal(mounted.container.querySelector(".list-status strong")?.textContent, "1");

    responses[0].resolve(jsonResponse({ detail: "save failed" }, 409));
    await flushReact();
    assert.equal(mounted.container.querySelector(".list-status strong")?.textContent, "1");
    assert.equal(detail?.getAttribute("data-has-material-update"), "true");
    assert.equal(
      mounted.container
        .querySelector(".cluster-list-entry")
        ?.getAttribute("data-has-material-update"),
      "true"
    );
    assert.equal(
      mounted.container.querySelector(".material-update-notice")?.textContent?.trim(),
      "看过后有更新"
    );
    assert.match(
      mounted.container.querySelector("[role='alert']")?.textContent ?? "",
      /保存失败/
    );

    const retrySource = mounted.container.querySelector(".detail-title-link");
    assert.ok(retrySource);
    retrySource.addEventListener("click", (event) => event.preventDefault());
    await click(retrySource);
    assert.equal(requests.length, 2);
    responses[1].resolve(
      jsonResponse(readResult(requests[1], "original_opened", false, null))
    );
    await flushReact();
    assert.equal(mounted.container.querySelector(".list-status strong")?.textContent, "0");
    assert.equal(detail?.getAttribute("data-has-material-update"), "false");
    assert.equal(
      mounted.container
        .querySelector(".cluster-list-entry")
        ?.getAttribute("data-has-material-update"),
      "false"
    );
    assert.equal(mounted.container.querySelector(".material-update-notice"), null);
    assert.equal(mounted.container.querySelector("[role='alert']"), null);
    assert.ok(mounted.container.querySelector("button[aria-label='标记未读']"));
  } finally {
    await mounted.unmount();
  }
});

test("reading a new synthesis covering r2 clears the material update", async () => {
  const cluster = clusterFixture({
    id: 11,
    hasMaterialUpdate: true,
    materialUpdateRevisionUid: R2,
    synthesisTargetRevisionUid: R2,
    synthesisStatus: "current"
  });
  const requests = [];
  const mounted = await mountReader([cluster], cluster, {
    skipSeen: false,
    visibleSurfaces: true,
    immediateDwell: true,
    fetchImpl: async (url, init) => {
      assert.equal(url, "/actions/event-user-state");
      const request = JSON.parse(init.body);
      requests.push(request);
      return jsonResponse(readResult(request, "summary_seen", false, null));
    }
  });

  try {
    await flushReact();
    assert.equal(requests.length, 1);
    assert.equal(requests[0].action, "read_status_set");
    assert.equal(requests[0].value, "summary_seen");
    assert.equal(requests[0].observed_revision_uid, R2);
    const detail = mounted.container.querySelector(".detail-body");
    assert.equal(detail?.getAttribute("data-event-read-mode"), "synthesis");
    assert.equal(detail?.getAttribute("data-observed-revision-uid"), R2);
    assert.equal(detail?.getAttribute("data-has-material-update"), "false");
    assert.equal(
      mounted.container
        .querySelector(".cluster-list-entry")
        ?.getAttribute("data-has-material-update"),
      "false"
    );
    assert.equal(mounted.container.querySelector(".material-update-notice"), null);
  } finally {
    await mounted.unmount();
  }
});

test("ordinary added sources keep the seen state and never borrow the material notice", async () => {
  const ordinary = clusterFixture({
    id: 11,
    hasMaterialUpdate: false,
    synthesisTargetRevisionUid: R1,
    synthesisStatus: "current",
    newSourceCount: 2
  });
  const mounted = await mountReader([ordinary], ordinary, { skipSeen: true });

  try {
    const listEntry = mounted.container.querySelector(".cluster-list-entry");
    assert.match(listEntry?.textContent ?? "", /新增 2 个来源/);
    assert.doesNotMatch(listEntry?.textContent ?? "", /看过后有更新/);
    assert.equal(
      mounted.container.querySelector(".detail-body")?.getAttribute(
        "data-has-material-update"
      ),
      "false"
    );
    assert.equal(mounted.container.querySelector(".material-update-notice"), null);
    assert.ok(mounted.container.querySelector("button[aria-label='标记未读']"));
  } finally {
    await mounted.unmount();
  }
});

test("source title keeps native navigation when event tracking identity is missing", async () => {
  const cluster = {
    ...clusterFixture({ id: 11 }),
    event_uid: null,
    current_revision_uid: null,
    source_view_evidence: []
  };
  const mounted = await mountReader([cluster], cluster, { skipSeen: true });

  try {
    await click(buttonWithText(mounted.container, "来源"));
    const title = mounted.container.querySelector(".detail-title-link");
    assert.ok(title);
    let preventedByComponent = null;
    window.addEventListener(
      "click",
      (event) => {
        preventedByComponent = event.defaultPrevented;
        event.preventDefault();
      },
      { once: true }
    );

    await click(title);

    assert.equal(preventedByComponent, false);
    assert.match(
      mounted.container.querySelector("[role='alert']")?.textContent ?? "",
      /原文已打开，但看过记录暂未保存/
    );
  } finally {
    await mounted.unmount();
  }
});

function clusterFixture({
  id,
  title = "第一条事件",
  defaultView = "synthesis",
  hasMaterialUpdate = false,
  materialUpdateRevisionUid = null,
  synthesisTargetRevisionUid = R1,
  synthesisStatus = "current",
  newSourceCount = 0
}) {
  const eventUid = `${String(id).padStart(8, "0")}-aaaa-4aaa-8aaa-aaaaaaaaaaaa`;
  const items = [1, 2].map((position) => ({
    id: id * 10 + position,
    source_id: id * 10 + position,
    source_name: `来源 ${position}`,
    source_site_url: `https://source${position}.example`,
    title: `${title} 来源 ${position}`,
    title_translation: "",
    summary: "",
    summary_translation: "",
    content_text: `这是 ${title} 的来源正文 ${position}。`,
    content_translation: "",
    image_url: "",
    url: `https://source${position}.example/${id}`,
    published_at: `2026-07-16T0${position}:00:00Z`,
    read_status: "unread",
    read_later: false,
    starred: false
  }));
  const citation = {
    evidence_version_uid: `${String(id).padStart(8, "0")}-bbbb-4bbb-8bbb-bbbbbbbbbbbb`,
    evidence_type: "article",
    role: "material",
    side: "support",
    source: {
      source_id: items[0].source_id,
      name: items[0].source_name,
      feed_url: "https://source1.example/feed",
      site_url: items[0].source_site_url,
      media_type: "article"
    },
    legacy_content_item_id_snapshot: items[0].id,
    title: items[0].title,
    url: items[0].url,
    published_at: items[0].published_at
  };
  const current = {
    version_uid: `${String(id).padStart(8, "0")}-cccc-4ccc-8ccc-cccccccccccc`,
    snapshot_uid: `${String(id).padStart(8, "0")}-dddd-4ddd-8ddd-dddddddddddd`,
    target_revision_uid: synthesisTargetRevisionUid,
    source_count: 2,
    provider: "legacy",
    model: "test",
    prompt_version: "test-v1",
    schema_version: "test-v1",
    generation_fingerprint: "a".repeat(64),
    snapshot_created_at: "2026-07-16T02:00:00Z",
    created_at: "2026-07-16T02:00:00Z",
    blocks: [
      {
        block_uid: `${String(id).padStart(8, "0")}-eeee-4eee-8eee-eeeeeeeeeeee`,
        position: 1,
        kind: "summary",
        body: `${title} 合成稿正文。`,
        attribution: "",
        citations: [citation]
      }
    ]
  };
  return {
    id,
    event_uid: eventUid,
    current_revision_uid: R2,
    seen_revision_uid: R1,
    current_revision_differs_from_seen: true,
    has_material_update: hasMaterialUpdate,
    material_update_revision_uid: materialUpdateRevisionUid,
    title,
    generated_title: title,
    generated_title_translation: "",
    generated_summary: `${title} 摘要`,
    generated_content: "",
    citations: "",
    first_seen_at: "2026-07-16T01:00:00Z",
    last_seen_at: "2026-07-16T02:00:00Z",
    item_count: items.length,
    read_status: "summary_seen",
    read_later: false,
    starred: false,
    items,
    source_view_evidence: items.map((item, index) => ({
      evidence_version_uid:
        index === 0
          ? citation.evidence_version_uid
          : `${String(id).padStart(8, "0")}-ffff-4fff-8fff-ffffffffffff`,
      source_id: item.source_id,
      legacy_content_item_id_snapshot: item.id
    })),
    synthesis_freshness: {
      status: synthesisStatus,
      current_revision_uid: R2,
      covered_revision_uid: synthesisTargetRevisionUid,
      reviewed_revision_uid: synthesisStatus === "stale" ? R2 : synthesisTargetRevisionUid,
      new_source_count: newSourceCount,
      unreviewed_evidence_count: 0,
      unreviewed_source_count: 0
    },
    synthesis: {
      status: synthesisStatus,
      current_revision_uid: R2,
      covered_revision_uid: synthesisTargetRevisionUid,
      reviewed_revision_uid: synthesisStatus === "stale" ? R2 : synthesisTargetRevisionUid,
      new_source_count: newSourceCount,
      unreviewed_evidence_count: 0,
      unreviewed_source_count: 0,
      target_revision_uid: R2,
      source_view_revision_uid: R2,
      source_count: items.length,
      can_generate: true,
      default_view: defaultView,
      task_status: "idle",
      current
    }
  };
}

async function mountReader(
  rows,
  detail,
  {
    fetchImpl,
    skipSeen = true,
    visibleSurfaces = false,
    immediateDwell = false,
    intersectImmediately = false,
    pageSize = 50
  } = {}
) {
  const dom = installDom({
    fetchImpl,
    visibleSurfaces,
    immediateDwell,
    intersectImmediately
  });
  const root = createRoot(dom.container);
  await act(async () => {
    root.render(
      React.createElement(ClusterView, {
        apiUrl: "http://reader.test/api",
        currentFilter: "unread",
        currentFilterCount: rows.length,
        initialDetail: detail,
        initialPageCount: rows.length,
        initialSelectedClusterId: detail?.id ?? null,
        offset: 0,
        pageSize,
        query: "",
        rows,
        scope: { filter: "unread" },
        skipSeen,
        listBackHref: "/"
      })
    );
  });
  return {
    container: dom.container,
    async unmount() {
      await act(async () => root.unmount());
      dom.restore();
    }
  };
}

function installDom({
  fetchImpl,
  visibleSurfaces,
  immediateDwell,
  intersectImmediately
}) {
  const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", {
    url: "http://reader.test/?filter=unread&cluster_id=11"
  });
  const previous = new Map();
  const setGlobal = (name, value) => {
    previous.set(name, Object.getOwnPropertyDescriptor(globalThis, name));
    Object.defineProperty(globalThis, name, {
      configurable: true,
      writable: true,
      value
    });
  };
  setGlobal("window", dom.window);
  setGlobal("document", dom.window.document);
  setGlobal("navigator", dom.window.navigator);
  setGlobal("localStorage", dom.window.localStorage);
  setGlobal("HTMLElement", dom.window.HTMLElement);
  setGlobal("Element", dom.window.Element);
  setGlobal("Node", dom.window.Node);
  setGlobal("Event", dom.window.Event);
  setGlobal("MouseEvent", dom.window.MouseEvent);
  setGlobal("KeyboardEvent", dom.window.KeyboardEvent);
  setGlobal("DOMException", dom.window.DOMException);
  setGlobal("Blob", dom.window.Blob);
  setGlobal("getComputedStyle", dom.window.getComputedStyle.bind(dom.window));
  setGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  setGlobal(
    "fetch",
    fetchImpl ??
      (async () => {
        throw new Error("unexpected fetch");
      })
  );
  setGlobal(
    "IntersectionObserver",
    class {
      constructor(callback) {
        this.callback = callback;
      }
      observe(target) {
        if (intersectImmediately) {
          this.callback([{ isIntersecting: true, target }]);
        }
      }
      disconnect() {}
    }
  );
  const nativeSetTimeout = dom.window.setTimeout.bind(dom.window);
  if (immediateDwell) {
    dom.window.setTimeout = (callback, delay, ...args) =>
      nativeSetTimeout(callback, delay === 1800 ? 0 : delay, ...args);
  }
  dom.window.requestAnimationFrame = (callback) => nativeSetTimeout(callback, 0);
  dom.window.cancelAnimationFrame = (timer) => dom.window.clearTimeout(timer);
  dom.window.HTMLElement.prototype.scrollTo = () => {};
  dom.window.HTMLElement.prototype.scrollIntoView = () => {};
  if (visibleSurfaces) {
    dom.window.HTMLElement.prototype.getClientRects = () => [
      { bottom: 100, height: 100, left: 0, right: 100, top: 0, width: 100 }
    ];
  }
  Object.defineProperty(dom.window.document, "visibilityState", {
    configurable: true,
    value: "visible"
  });

  return {
    container: dom.window.document.getElementById("root"),
    restore() {
      for (const [name, descriptor] of previous) {
        if (descriptor) Object.defineProperty(globalThis, name, descriptor);
        else delete globalThis[name];
      }
      dom.window.close();
    }
  };
}

function buttonWithText(container, text) {
  const button = [...container.querySelectorAll("button")].find(
    (candidate) => candidate.textContent?.trim() === text
  );
  assert.ok(button, `missing button: ${text}`);
  return button;
}

async function click(element) {
  await act(async () => {
    element.dispatchEvent(
      new window.MouseEvent("click", { bubbles: true, cancelable: true })
    );
    await Promise.resolve();
  });
}

async function flushReact() {
  await act(async () => {
    await Promise.resolve();
    await new Promise((resolve) => setTimeout(resolve, 0));
    await Promise.resolve();
  });
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((complete, fail) => {
    resolve = complete;
    reject = fail;
  });
  return { promise, reject, resolve };
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}

function readResult(request, readStatus, hasMaterialUpdate, materialRevisionUid) {
  return {
    operation_id: request.operation_id,
    event_uid: request.event_uid,
    observed_revision_uid: request.observed_revision_uid,
    action: "read_status_set",
    value: readStatus,
    source_id: request.source_id,
    evidence_version_uid: request.evidence_version_uid,
    read_status: readStatus,
    seen_revision_uid: request.observed_revision_uid,
    current_revision_differs_from_seen: request.observed_revision_uid !== R2,
    has_material_update: hasMaterialUpdate,
    material_update_revision_uid: materialRevisionUid,
    read_later: false,
    starred: false,
    updated_at: "2026-07-16T03:00:00Z"
  };
}
