import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import test from "node:test";

import { JSDOM } from "jsdom";
import React, { act, useState } from "react";
import { createRoot } from "react-dom/client";

import { useActionDialog } from "./action-dialog.tsx";

test("shared action dialog confirms, prompts, cancels, and restores focus in-page", async () => {
  const dom = installDom();
  const root = createRoot(dom.container);
  try {
    await act(async () => root.render(React.createElement(Harness)));
    const confirmTrigger = buttonWithText(dom.container, "打开确认");
    confirmTrigger.focus();
    await act(async () => confirmTrigger.click());
    await tick();

    let dialog = dom.container.querySelector("dialog[open]");
    assert.ok(dialog);
    assert.equal(dialog.getAttribute("aria-modal"), "true");
    assert.equal(dialog.querySelector("form").noValidate, true);
    assert.match(dialog.textContent, /确认删除测试数据/);
    assert.equal(document.activeElement, buttonWithText(dialog, "取消"));

    await act(async () => buttonWithText(dialog, "取消").click());
    await tick();
    assert.equal(dom.container.querySelector("output").textContent, "confirm:false");
    assert.equal(document.activeElement, confirmTrigger);

    const promptTrigger = buttonWithText(dom.container, "打开输入");
    promptTrigger.focus();
    await act(async () => promptTrigger.click());
    await tick();
    dialog = dom.container.querySelector("dialog[open]");
    const input = dialog.querySelector('input[name="action_dialog_value"]');
    assert.equal(document.activeElement, input);
    await act(async () => setInputValue(input, "新名称"));
    await act(async () => buttonWithText(dialog, "保存").click());
    await tick();
    assert.equal(dom.container.querySelector("output").textContent, "prompt:新名称");
    assert.equal(document.activeElement, promptTrigger);

    await act(async () => confirmTrigger.click());
    await tick();
    dialog = dom.container.querySelector("dialog[open]");
    await act(async () => dialog.dispatchEvent(new window.Event("cancel", { cancelable: true })));
    await tick();
    assert.equal(dom.container.querySelector("output").textContent, "confirm:false");
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

test("web client code contains no browser-native confirm, prompt, or alert calls", async () => {
  const files = (await readdir(new URL(".", import.meta.url), { recursive: true })).filter((name) => name.endsWith(".tsx"));
  const offenders = [];
  for (const file of files) {
    const source = await readFile(new URL(file, import.meta.url), "utf8");
    if (/\bwindow\.(?:confirm|prompt|alert)\s*\(/.test(source)) offenders.push(file);
  }
  assert.deepEqual(offenders, []);
});

test("constrained forms keep validation inside Reader", async () => {
  const files = (await readdir(new URL(".", import.meta.url), { recursive: true })).filter((name) => name.endsWith(".tsx"));
  const offenders = [];
  for (const file of files) {
    const source = await readFile(new URL(file, import.meta.url), "utf8");
    for (const [form] of source.matchAll(/<form\b[^>]*>[\s\S]*?<\/form>/g)) {
      if (/(?:\brequired\b|type="(?:url|number)")/.test(form) && !/\bnoValidate\b/.test(form)) offenders.push(file);
    }
  }
  assert.deepEqual(offenders, []);
});

function Harness() {
  const actionDialog = useActionDialog();
  const [result, setResult] = useState("");
  return React.createElement(
    React.Fragment,
    null,
    React.createElement("button", {
      onClick: () => void actionDialog.confirm({ title: "删除", message: "确认删除测试数据？", confirmLabel: "永久删除", danger: true }).then((value) => setResult(`confirm:${value}`))
    }, "打开确认"),
    React.createElement("button", {
      onClick: () => void actionDialog.prompt({ title: "重命名", message: "输入新名称", inputLabel: "名称", defaultValue: "旧名称", confirmLabel: "保存" }).then((value) => setResult(`prompt:${value}`))
    }, "打开输入"),
    actionDialog.dialog,
    React.createElement("output", null, result)
  );
}

function buttonWithText(container, text) {
  const button = Array.from(container.querySelectorAll("button")).find((candidate) => candidate.textContent.trim() === text);
  assert.ok(button, `missing button: ${text}`);
  return button;
}

function setInputValue(input, value) {
  const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value").set;
  input.focus();
  setter.call(input, value);
  input.dispatchEvent(new window.Event("input", { bubbles: true }));
  const propertyChange = new window.Event("propertychange", { bubbles: true });
  Object.defineProperty(propertyChange, "propertyName", { value: "value" });
  input.dispatchEvent(propertyChange);
}

function tick() {
  return act(async () => new Promise((resolve) => window.setTimeout(resolve, 0)));
}

function installDom() {
  const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", { url: "http://reader.test/" });
  dom.window.HTMLDialogElement.prototype.showModal = function showModal() { this.setAttribute("open", ""); };
  dom.window.HTMLDialogElement.prototype.close = function close() { this.removeAttribute("open"); };
  dom.window.HTMLElement.prototype.attachEvent = function attachEvent(name, listener) { this.addEventListener(name.slice(2), listener); };
  dom.window.HTMLElement.prototype.detachEvent = function detachEvent(name, listener) { this.removeEventListener(name.slice(2), listener); };
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
    ["MouseEvent", dom.window.MouseEvent],
    ["IS_REACT_ACT_ENVIRONMENT", true]
  ]) setGlobal(name, value);
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
