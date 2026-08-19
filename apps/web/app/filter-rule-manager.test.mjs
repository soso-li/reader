import assert from "node:assert/strict";
import test from "node:test";

import { JSDOM } from "jsdom";
import React, { act } from "react";
import { createRoot } from "react-dom/client";

import FilterRuleManager from "./filter-rule-manager.tsx";

test("filter rule editor requires a current preview before updating", async () => {
  const calls = [];
  const mounted = await mountManager(async (_input, init) => {
    const payload = JSON.parse(String(init?.body));
    calls.push(payload);
    if (payload.action === "preview") {
      return Response.json({
        count: 1,
        items: [{ id: 8, source_name: "示例源", title: "Sponsored post", summary: "推广内容", content_text: "", published_at: null }]
      });
    }
    return Response.json({
      id: 5,
      source_id: null,
      source_name: "",
      match_type: "literal",
      pattern: "sponsored",
      enabled: true,
      match_count: 1,
      created_at: "2026-07-20T08:00:00Z",
      updated_at: "2026-07-20T08:00:00Z"
    });
  });
  try {
    await act(async () => buttonWithText(mounted.container, "编辑").click());

    const apply = buttonWithText(mounted.container, "确认更新");
    assert.equal(apply.disabled, true);
    await act(async () => buttonWithText(mounted.container, "预览匹配").click());
    assert.equal(calls[0].action, "preview");
    assert.match(mounted.container.textContent, /匹配 1 条/);
    assert.equal(apply.disabled, false);

    await act(async () => apply.click());
    assert.equal(calls[1].action, "update");
    assert.match(mounted.container.textContent, /当前匹配 1 条/);
  } finally {
    await mounted.unmount();
  }
});

test("filter rule deletion uses an in-page confirmation", async () => {
  const calls = [];
  const mounted = await mountManager(async (_input, init) => {
    calls.push(JSON.parse(String(init?.body)));
    return Response.json({ ok: true });
  });
  try {
    await act(async () => buttonWithText(mounted.container, "删除").click());
    let confirmation = mounted.container.querySelector("dialog.action-dialog[open]");
    assert.ok(confirmation);
    assert.match(confirmation.textContent, /永久删除过滤规则“sponsored”/);
    assert.deepEqual(calls, []);

    await act(async () => buttonWithText(confirmation, "取消").click());
    assert.match(mounted.container.textContent, /sponsored/);

    await act(async () => buttonWithText(mounted.container, "删除").click());
    confirmation = mounted.container.querySelector("dialog.action-dialog[open]");
    await act(async () => buttonWithText(confirmation, "永久删除").click());
    assert.deepEqual(calls, [{ action: "delete", id: 5 }]);
    assert.doesNotMatch(mounted.container.querySelector(".filter-rule-list").textContent, /sponsored/);
  } finally {
    await mounted.unmount();
  }
});

function buttonWithText(container, label) {
  const button = Array.from(container.querySelectorAll("button")).find(
    (candidate) => candidate.textContent.trim() === label
  );
  assert.ok(button, `missing button: ${label}`);
  return button;
}

async function mountManager(fetchImpl) {
  const dom = installDom(fetchImpl);
  const root = createRoot(dom.container);
  await act(async () => {
    root.render(React.createElement(FilterRuleManager, {
      initialRules: [{
        id: 5,
        source_id: null,
        source_name: "",
        match_type: "literal",
        pattern: "sponsored",
        enabled: true,
        match_count: 1,
        created_at: "2026-07-20T08:00:00Z",
        updated_at: "2026-07-20T08:00:00Z"
      }],
      sources: [{ id: 1, name: "示例源" }]
    }));
  });
  return {
    container: dom.container,
    async unmount() {
      await act(async () => root.unmount());
      dom.restore();
    }
  };
}

function installDom(fetchImpl) {
  const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", { url: "http://reader.test/?view=settings&settings_section=filters" });
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
