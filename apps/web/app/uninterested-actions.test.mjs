import assert from "node:assert/strict";
import test from "node:test";

import { JSDOM } from "jsdom";
import React, { act } from "react";
import { createRoot } from "react-dom/client";

import { UninterestedList } from "./uninterested-actions.tsx";

test("restoring a target immediately decrements the visible count", async () => {
  const calls = [];
  const dom = installDom(async (_input, init) => {
    calls.push(JSON.parse(String(init?.body)));
    return Response.json({
      target_kind: "item",
      event_uid: null,
      observed_revision_uid: null,
      cluster_id: null,
      item_id: 42,
      affected_item_ids: [42],
      uninterested: false,
      reason: null,
      note: null,
      marked_at: null
    });
  });
  const root = createRoot(dom.container);
  try {
    await act(async () => {
      root.render(React.createElement(UninterestedList, {
        initialCount: 1,
        initialTargets: [{
          target_kind: "item",
          event_uid: null,
          current_revision_uid: null,
          cluster_id: null,
          item_id: 42,
          item_ids: [42],
          title: "测试条目",
          summary: "测试摘要",
          source_ids: [1],
          source_names: ["测试来源"],
          media_type: "article",
          item_count: 1,
          reason: "promotion",
          note: null,
          marked_at: "2026-07-27T11:00:00Z"
        }]
      }));
    });
    assert.match(dom.container.textContent, /共 1 项/);

    const restore = buttonWithText(dom.container, "恢复");
    await act(async () => restore.click());

    assert.equal(calls.length, 1);
    assert.equal(calls[0].value, false);
    assert.match(dom.container.textContent, /共 0 项/);
    assert.doesNotMatch(dom.container.textContent, /测试条目/);
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

function buttonWithText(container, label) {
  const button = Array.from(container.querySelectorAll("button")).find(
    (candidate) => candidate.textContent.trim() === label
  );
  assert.ok(button, `missing button: ${label}`);
  return button;
}

function installDom(fetchImpl) {
  const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", {
    url: "http://reader.test/uninterested"
  });
  const previous = new Map();
  const setGlobal = (name, value) => {
    previous.set(name, Object.getOwnPropertyDescriptor(globalThis, name));
    Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
  };
  setGlobal("window", dom.window);
  setGlobal("document", dom.window.document);
  setGlobal("navigator", dom.window.navigator);
  setGlobal("HTMLElement", dom.window.HTMLElement);
  setGlobal("Element", dom.window.Element);
  setGlobal("Node", dom.window.Node);
  setGlobal("Event", dom.window.Event);
  setGlobal("MouseEvent", dom.window.MouseEvent);
  setGlobal("fetch", fetchImpl);
  setGlobal("IS_REACT_ACT_ENVIRONMENT", true);
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
