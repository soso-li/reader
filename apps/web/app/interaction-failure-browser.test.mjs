import assert from "node:assert/strict";
import test from "node:test";

import { JSDOM } from "jsdom";
import React, { act } from "react";
import { createRoot } from "react-dom/client";

import FetchForm from "./fetch-form.tsx";
import SynthesisSettingsControl from "./synthesis-settings-control.tsx";
import TopicDeleteForm from "./topic-delete-form.tsx";
import TranslationSettingsControl from "./translation-settings-control.tsx";

test("topic deletion requires an in-page confirmation", async () => {
  const dom = installDom();
  const root = createRoot(dom.container);
  let submissions = 0;
  try {
    await act(async () => root.render(React.createElement(TopicDeleteForm, { topicId: 7, topicName: "测试议题" })));
    const form = dom.container.querySelector('form[action="/actions/topic"]');
    form.addEventListener("submit", (event) => {
      submissions += 1;
      event.preventDefault();
    });

    await act(async () => buttonWithText(dom.container, "删除议题组").click());
    let dialog = dom.container.querySelector("dialog[open]");
    assert.ok(dialog);
    assert.match(dialog.textContent, /永久删除议题组“测试议题”/);

    await act(async () => buttonWithText(dialog, "取消").click());
    assert.equal(submissions, 0);

    await act(async () => buttonWithText(dom.container, "删除议题组").click());
    dialog = dom.container.querySelector("dialog[open]");
    await act(async () => buttonWithText(dialog, "永久删除").click());
    assert.equal(submissions, 1);
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

test("cloud key deletion requires an in-page confirmation", async () => {
  const dom = installDom();
  const root = createRoot(dom.container);
  const originalFetch = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (_url, init) => {
    const payload = JSON.parse(init.body);
    requests.push(payload);
    if (payload.clear_translation_api_key) {
      return Response.json({
        ...translationSettings,
        translation_provider: "local",
        translation_api_key_configured: false
      });
    }
    return Response.json({
      ...synthesisSettings,
      synthesis_remote_api_key_configured: false
    });
  };

  try {
    await act(async () => root.render(React.createElement(React.Fragment, null,
      React.createElement(TranslationSettingsControl, { apiUrl: "/api", settings: translationSettings }),
      React.createElement(SynthesisSettingsControl, { apiUrl: "/api", settings: synthesisSettings })
    )));

    await act(async () => buttonWithText(dom.container, "切回本地并清除云端密钥").click());
    let dialog = dom.container.querySelector("dialog.action-dialog[open]");
    assert.match(dialog?.textContent ?? "", /永久清除.*无法恢复/);
    await act(async () => buttonWithText(dialog, "取消").click());
    assert.equal(requests.length, 0);

    await act(async () => buttonWithText(dom.container, "切回本地并清除云端密钥").click());
    dialog = dom.container.querySelector("dialog.action-dialog[open]");
    await act(async () => {
      buttonWithText(dialog, "清除密钥").click();
      await Promise.resolve();
    });
    assert.deepEqual(requests[0], {
      translation_provider: "local",
      translation_base_url: translationSettings.translation_local_base_url,
      translation_model: translationSettings.translation_local_model,
      clear_translation_api_key: true
    });

    await act(async () => buttonWithText(dom.container, "清除云端合成密钥").click());
    dialog = dom.container.querySelector("dialog.action-dialog[open]");
    assert.match(dialog?.textContent ?? "", /永久清除.*无法恢复/);
    await act(async () => buttonWithText(dialog, "取消").click());
    assert.equal(requests.length, 1);

    await act(async () => buttonWithText(dom.container, "清除云端合成密钥").click());
    dialog = dom.container.querySelector("dialog.action-dialog[open]");
    await act(async () => {
      buttonWithText(dialog, "清除密钥").click();
      await Promise.resolve();
    });
    assert.deepEqual(requests[1], { clear_synthesis_remote_api_key: true });
  } finally {
    globalThis.fetch = originalFetch;
    await act(async () => root.unmount());
    dom.restore();
  }
});

const translationSettings = {
  translation_provider: "openai_compatible",
  translation_base_url: "https://translation.example/v1",
  translation_local_base_url: "http://local.test",
  translation_local_model: "local-model",
  translation_cloud_base_url: "https://translation.example/v1",
  translation_cloud_model: "cloud-model",
  translation_api_key_configured: true,
  translation_endpoint: "https://translation.example/v1/chat/completions",
  translation_model: "cloud-model"
};

const synthesisSettings = {
  synthesis_provider: "openai_compatible",
  synthesis_remote_base_url: "https://synthesis.example/v1",
  synthesis_remote_model: "synthesis-model",
  synthesis_remote_api_key_configured: true
};

function buttonWithText(container, text) {
  const button = Array.from(container.querySelectorAll("button")).find((candidate) => candidate.textContent.trim() === text);
  assert.ok(button, `missing button: ${text}`);
  return button;
}

test("RSS refresh failures remain visible beside the trigger", async () => {
  const dom = installDom();
  const root = createRoot(dom.container);
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    throw new TypeError("Failed to fetch");
  };
  try {
    await act(async () => root.render(React.createElement(FetchForm, { compact: true })));
    await act(async () => dom.container.querySelector("form").dispatchEvent(new dom.window.SubmitEvent("submit", { bubbles: true, cancelable: true })));

    assert.equal(dom.container.querySelector("[role=alert]")?.textContent, "刷新失败，请检查网络连接后重试");
    assert.equal(dom.container.querySelector("button")?.disabled, false);
  } finally {
    globalThis.fetch = originalFetch;
    await act(async () => root.unmount());
    dom.restore();
  }
});

function installDom() {
  const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", { url: "http://reader.test/" });
  const previous = new Map();
  const setGlobal = (name, value) => {
    previous.set(name, Object.getOwnPropertyDescriptor(globalThis, name));
    Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
  };
  for (const [name, value] of [
    ["window", dom.window],
    ["document", dom.window.document],
    ["navigator", dom.window.navigator],
    ["HTMLElement", dom.window.HTMLElement],
    ["Element", dom.window.Element],
    ["Node", dom.window.Node],
    ["Event", dom.window.Event],
    ["SubmitEvent", dom.window.SubmitEvent],
    ["IS_REACT_ACT_ENVIRONMENT", true]
  ]) setGlobal(name, value);
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
