import assert from "node:assert/strict";
import test from "node:test";

import { JSDOM } from "jsdom";
import React, { act } from "react";
import { createRoot } from "react-dom/client";

import CustomToolbar from "./custom-toolbar.tsx";
import { ReduceSimilarButton } from "./uninterested-actions.tsx";

test("toolbar customization uses a native modal dialog and closes on cancel", async () => {
  const dom = installDom();
  const root = createRoot(dom.container);
  let showModalCalls = 0;
  dom.window.HTMLDialogElement.prototype.showModal = function showModal() {
    showModalCalls += 1;
    this.setAttribute("open", "");
    this.querySelector("button")?.focus();
  };
  dom.window.HTMLDialogElement.prototype.close = function close() {
    this.removeAttribute("open");
    this.dispatchEvent(new dom.window.Event("close"));
  };

  try {
    await act(async () => {
      root.render(
        React.createElement(CustomToolbar, {
          actions: [
            { id: "read-toggle", label: "已读", node: React.createElement("button", null, "已读") },
            { id: "copy", label: "复制", node: React.createElement("button", null, "复制") }
          ],
          storageKey: "toolbar-test"
        })
      );
    });

    const customizeButton = buttonWithText(dom.container, "自定义工具栏");
    customizeButton.focus();
    await act(async () => {
      customizeButton.click();
    });

    const dialog = dom.container.querySelector("dialog[aria-label='自定义工具栏']");
    assert.equal(showModalCalls, 1);
    assert.equal(dialog.open, true);
    assert.equal(dialog.contains(document.activeElement), true);

    const scheduledFocus = [];
    dom.window.setTimeout = (callback) => {
      scheduledFocus.push(callback);
      return scheduledFocus.length;
    };
    await act(async () => {
      dialog.dispatchEvent(new dom.window.Event("cancel", { bubbles: false, cancelable: true }));
    });
    assert.equal(scheduledFocus.length, 1);
    scheduledFocus[0]();
    assert.equal(dialog.open, false);
    assert.equal(document.activeElement, dom.container.querySelector("summary[aria-label='更多工具']"));
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

test("reduce-similar uses the same icon treatment as other toolbar actions", async () => {
  const dom = installDom();
  const root = createRoot(dom.container);
  try {
    await act(async () => {
      root.render(
        React.createElement(CustomToolbar, {
          actions: [{
            id: "uninterested",
            label: "减少此类",
            node: React.createElement(ReduceSimilarButton, {
              compact: true,
              target: {
                target_type: "event",
                event_uid: "evt-1",
                observed_revision_uid: "rev-1"
              }
            })
          }],
          storageKey: "toolbar-uninterested-test"
        })
      );
    });

    const row = dom.container.querySelector(".toolbar-more-row");
    assert.ok(row);
    assert.equal(row.textContent.trim(), "减少此类");
    assert.ok(row.querySelector("button.icon[aria-label='减少此类']"));
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

test("reduce-similar can use a close icon without changing its accessible label", async () => {
  const dom = installDom();
  const root = createRoot(dom.container);
  try {
    await act(async () => {
      root.render(
        React.createElement(ReduceSimilarButton, {
          compact: true,
          dismissIcon: true,
          target: { target_type: "article", item_id: 42 }
        })
      );
    });

    const button = dom.container.querySelector("button.icon[aria-label='减少此类']");
    assert.ok(button);
    assert.ok(button.querySelector("svg.lucide-x"));
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

function buttonWithText(container, text) {
  const button = Array.from(container.querySelectorAll("button")).find(
    (candidate) => candidate.textContent.trim() === text
  );
  assert.ok(button, `missing button: ${text}`);
  return button;
}

function installDom() {
  const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", {
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
  setGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  return {
    container: dom.window.document.getElementById("root"),
    window: dom.window,
    restore() {
      for (const [name, descriptor] of previous) {
        if (descriptor) Object.defineProperty(globalThis, name, descriptor);
        else delete globalThis[name];
      }
      dom.window.close();
    }
  };
}
