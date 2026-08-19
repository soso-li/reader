import { JSDOM } from "jsdom";
import React, { act, Profiler } from "react";
import { createRoot } from "react-dom/client";

import BrowseView from "./browse-view.tsx";
import ClusterView from "./cluster-view.tsx";

const ROW_COUNTS = [50, 200];
const SAMPLES = 7;
const WARMUPS = 4;

console.log("surface\trows\tscenario\trender_ms_median\trender_ms_p95\twall_ms_median\tcommits_median");
await benchmarkSurface("cluster", 200, clusterFixture, ClusterView, clusterProps, false);
await benchmarkSurface("browse", 200, browseFixture, BrowseView, browseProps, false);
for (const rowCount of ROW_COUNTS) {
  await benchmarkSurface("cluster", rowCount, clusterFixture, ClusterView, clusterProps);
  await benchmarkSurface("browse", rowCount, browseFixture, BrowseView, browseProps);
}

async function benchmarkSurface(name, rowCount, fixture, Component, propsFor, report = true) {
  const rows = Array.from({ length: rowCount }, (_, index) => fixture(index + 1, false));
  const fullRows = new Map(rows.map((row) => [row.id, fixture(row.id, true)]));
  const props = propsFor(rows);
  const dom = installDom(async (input) => {
    const id = Number(String(input).match(/\/(\d+)$/)?.[1]);
    const row = fullRows.get(id);
    if (!row) throw new Error(`unexpected fetch: ${input}`);
    return Response.json(row);
  });
  const commits = [];
  const onRender = (_id, _phase, actualDuration) => commits.push(actualDuration);
  const root = createRoot(dom.container);
  const element = () => React.createElement(
    Profiler,
    { id: name, onRender },
    React.createElement(Component, props)
  );

  try {
    await act(async () => root.render(element()));
    await flushReact();

    for (let index = 0; index < WARMUPS; index += 1) {
      await act(async () => {
        const id = index % 2 ? 1 : 2;
        dom.container.querySelector(`[data-scroll-seen-id="${id}"] .stretched-row-link`)?.click();
        await flushPromises();
      });
    }
    const selectionSamples = [];
    for (let index = 0; index < SAMPLES; index += 1) {
      commits.length = 0;
      const id = index % 2 ? 1 : 2;
      const startedAt = performance.now();
      await act(async () => {
        dom.container.querySelector(`[data-scroll-seen-id="${id}"] .stretched-row-link`)?.click();
        await flushPromises();
      });
      selectionSamples.push(sample(commits, performance.now() - startedAt));
    }

    const detail = dom.container.querySelector(".pane.detail");
    Object.defineProperties(detail, {
      clientHeight: { configurable: true, value: 600 },
      scrollHeight: { configurable: true, value: 2400 }
    });
    const scrollSamples = [];
    for (let index = 0; index < SAMPLES; index += 1) {
      commits.length = 0;
      detail.scrollTop = 100 + index * 100;
      const startedAt = performance.now();
      await act(async () => detail.dispatchEvent(new window.Event("scroll", { bubbles: true })));
      scrollSamples.push(sample(commits, performance.now() - startedAt));
    }

    for (let index = 0; index < WARMUPS; index += 1) {
      await act(async () => root.render(element()));
    }
    const unchangedSamples = [];
    for (let index = 0; index < SAMPLES; index += 1) {
      commits.length = 0;
      const startedAt = performance.now();
      await act(async () => root.render(element()));
      unchangedSamples.push(sample(commits, performance.now() - startedAt));
    }

    if (report) {
      printResult(name, rowCount, "selection", selectionSamples);
      printResult(name, rowCount, "detail-scroll", scrollSamples);
      printResult(name, rowCount, "unchanged-props", unchangedSamples);
    }
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
}

function clusterProps(rows) {
  return {
    apiUrl: "/api",
    currentFilter: "",
    currentFilterCount: rows.length,
    detailError: "",
    initialDetail: null,
    initialPageCount: rows.length,
    initialSelectedClusterId: null,
    listBackHref: "/",
    listError: "",
    offset: 0,
    pageSize: rows.length,
    query: "",
    rows,
    scope: { filter: "all", view: "clusters" },
    skipSeen: false
  };
}

function browseProps(items) {
  return {
    apiUrl: "/api",
    currentFilter: "",
    detailError: "",
    filteredOnly: false,
    initialPageCount: items.length,
    initialSelectedItemId: null,
    items,
    listBackHref: "/",
    listError: "",
    media: "podcast",
    offset: 0,
    pageSize: items.length,
    query: "",
    scope: { filter: "all", media: "podcast", view: "browse" },
    selectedItem: null,
    thumbnailMode: "auto"
  };
}

function clusterFixture(id, full) {
  const item = browseFixture(id, full);
  return {
    id,
    event_uid: `${String(id).padStart(8, "0")}-aaaa-4aaa-8aaa-aaaaaaaaaaaa`,
    current_revision_uid: `${String(id).padStart(8, "0")}-bbbb-4bbb-8bbb-bbbbbbbbbbbb`,
    seen_revision_uid: null,
    current_revision_differs_from_seen: false,
    has_material_update: false,
    material_update_revision_uid: null,
    title: item.title,
    generated_title: "",
    generated_title_translation: "",
    generated_summary: item.summary,
    generated_content: "",
    citations: "",
    first_seen_at: item.published_at,
    last_seen_at: item.published_at,
    item_count: 1,
    read_status: "unread",
    read_later: false,
    starred: false,
    items: [item],
    synthesis: null,
    synthesis_freshness: null
  };
}

function browseFixture(id, full) {
  return {
    id,
    source_id: id,
    source_name: `来源 ${id}`,
    source_site_url: `https://source-${id}.example`,
    title: `条目 ${id}`,
    title_translation: `Item ${id}`,
    summary: `摘要 ${id}`,
    summary_translation: "",
    image_url: "",
    media_url: "",
    media_kind: "",
    media_duration: 0,
    content_text: full ? `正文 ${id}` : "",
    content_translation: full ? `Content ${id}` : "",
    url: `https://source-${id}.example/${id}`,
    published_at: "2026-07-26T00:00:00Z",
    read_status: "unread",
    read_later: false,
    starred: false,
    filtered: false,
    filter_rules: []
  };
}

function sample(commits, wallMs) {
  return {
    commits: commits.length,
    renderMs: commits.reduce((total, duration) => total + duration, 0),
    wallMs
  };
}

function printResult(surface, rows, scenario, samples) {
  const renderTimes = samples.map((entry) => entry.renderMs).sort((a, b) => a - b);
  const wallTimes = samples.map((entry) => entry.wallMs).sort((a, b) => a - b);
  const commitCounts = samples.map((entry) => entry.commits).sort((a, b) => a - b);
  console.log([
    surface,
    rows,
    scenario,
    percentile(renderTimes, 0.5).toFixed(2),
    percentile(renderTimes, 0.95).toFixed(2),
    percentile(wallTimes, 0.5).toFixed(2),
    percentile(commitCounts, 0.5)
  ].join("\t"));
}

function percentile(values, ratio) {
  return values[Math.min(values.length - 1, Math.ceil(values.length * ratio) - 1)];
}

async function flushReact() {
  await act(async () => flushPromises());
}

async function flushPromises() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

function installDom(fetchImpl) {
  const dom = new JSDOM("<!doctype html><html><body><div class='app-shell'><div id='root'></div></div></body></html>", {
    url: "http://reader.test/"
  });
  const previous = new Map();
  const setGlobal = (name, value) => {
    previous.set(name, Object.getOwnPropertyDescriptor(globalThis, name));
    Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
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
  setGlobal("fetch", fetchImpl);
  setGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  setGlobal("IntersectionObserver", class {
    observe() {}
    disconnect() {}
  });
  const nativeSetTimeout = dom.window.setTimeout.bind(dom.window);
  dom.window.setTimeout = (callback, delay, ...args) =>
    nativeSetTimeout(callback, delay === 120 ? 0 : delay, ...args);
  dom.window.requestAnimationFrame = (callback) => nativeSetTimeout(callback, 0);
  dom.window.cancelAnimationFrame = (timer) => dom.window.clearTimeout(timer);
  dom.window.HTMLElement.prototype.scrollTo = () => {};
  dom.window.HTMLElement.prototype.scrollIntoView = () => {};
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
