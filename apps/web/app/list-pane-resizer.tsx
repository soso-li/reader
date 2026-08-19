"use client";

import { type KeyboardEvent, type PointerEvent, useEffect, useState } from "react";

type Mode = "context" | "list";

const CONFIG = {
  context: {
    bodyClass: "reader-context-resizing",
    className: "context-panel-resizer",
    cssVar: "--reader-context-width",
    label: "调整文件夹栏宽度",
    storageKey: "reader:context-panel-width:paper-v1",
    targetSelector: ".context-panel"
  },
  list: {
    bodyClass: "reader-list-resizing",
    className: "list-pane-resizer",
    cssVar: "--reader-list-width",
    label: "调整条目栏宽度",
    storageKey: "reader:list-pane-width:paper-v1",
    targetSelector: ".list-pane"
  }
} as const;

export default function ListPaneResizer({ mode = "list" }: { mode?: Mode }) {
  const config = CONFIG[mode];
  const [ariaValue, setAriaValue] = useState({ now: 50, text: "" });
  useEffect(() => {
    const savedWidth = readSavedWidth(config.storageKey);
    setAriaValue(paneAriaValue(mode, applyPaneWidth(mode, savedWidth || defaultPaneWidth(mode))));

    function handleResize() {
      setAriaValue(paneAriaValue(mode, applyPaneWidth(mode, readSavedWidth(config.storageKey) || defaultPaneWidth(mode))));
    }

    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, [config.storageKey, config.targetSelector, mode]);

  function handlePointerDown(event: PointerEvent<HTMLDivElement>) {
    if (event.button !== 0) return;
    event.preventDefault();
    const startX = event.clientX;
    const startWidth = currentPaneWidth(config.targetSelector);
    const controller = new AbortController();
    document.body.classList.add(config.bodyClass);

    function finishDrag() {
      setAriaValue(paneAriaValue(mode, persistPaneWidth(mode, currentPaneWidth(config.targetSelector))));
      document.body.classList.remove(config.bodyClass);
      controller.abort();
    }

    document.addEventListener(
      "pointermove",
      (moveEvent) => {
        setAriaValue(paneAriaValue(mode, applyPaneWidth(mode, startWidth + moveEvent.clientX - startX)));
      },
      { signal: controller.signal }
    );
    document.addEventListener("pointerup", finishDrag, { once: true, signal: controller.signal });
    document.addEventListener("pointercancel", finishDrag, { once: true, signal: controller.signal });
  }

  function handleKeyDown(event: KeyboardEvent<HTMLDivElement>) {
    const width = currentPaneWidth(config.targetSelector);
    if (event.key === "ArrowLeft") {
      event.preventDefault();
      setAriaValue(paneAriaValue(mode, persistPaneWidth(mode, width - 24)));
    } else if (event.key === "ArrowRight") {
      event.preventDefault();
      setAriaValue(paneAriaValue(mode, persistPaneWidth(mode, width + 24)));
    } else if (event.key === "Home") {
      event.preventDefault();
      setAriaValue(paneAriaValue(mode, persistPaneWidth(mode, minPaneWidth(mode))));
    } else if (event.key === "End") {
      event.preventDefault();
      setAriaValue(paneAriaValue(mode, persistPaneWidth(mode, maxPaneWidth(mode))));
    }
  }

  return <div className={config.className} role="separator" aria-label={config.label} aria-orientation="vertical" aria-valuemin={0} aria-valuemax={100} aria-valuenow={ariaValue.now} aria-valuetext={ariaValue.text} tabIndex={0} onKeyDown={handleKeyDown} onPointerDown={handlePointerDown} />;
}

function currentPaneWidth(selector: string) {
  return document.querySelector<HTMLElement>(selector)?.getBoundingClientRect().width ?? window.innerWidth * 0.34;
}

function applyPaneWidth(mode: Mode, width: number) {
  const next = clampPaneWidth(mode, width);
  const shell = document.querySelector<HTMLElement>(".app-shell");
  if (shell) shell.style.setProperty(CONFIG[mode].cssVar, `${next}px`);
  return next;
}

function persistPaneWidth(mode: Mode, width: number) {
  const next = clampPaneWidth(mode, width);
  applyPaneWidth(mode, next);
  try {
    window.localStorage.setItem(CONFIG[mode].storageKey, String(next));
  } catch {
    // localStorage can be unavailable in private contexts; the drag still works for this session.
  }
  return next;
}

function paneAriaValue(mode: Mode, width: number) {
  const min = minPaneWidth(mode);
  const max = maxPaneWidth(mode);
  const now = max === min ? 0 : Math.round(((width - min) / (max - min)) * 100);
  return { now, text: `${width} 像素` };
}

function readSavedWidth(storageKey: string) {
  try {
    const value = Number(window.localStorage.getItem(storageKey));
    return Number.isFinite(value) && value > 0 ? value : 0;
  } catch {
    return 0;
  }
}

function clampPaneWidth(mode: Mode, width: number) {
  return Math.round(Math.min(Math.max(width, minPaneWidth(mode)), maxPaneWidth(mode)));
}

function minPaneWidth(mode: Mode) {
  if (mode === "list") return 320;
  return contextBaseWidth() * 0.875;
}

function maxPaneWidth(mode: Mode) {
  if (mode === "list") return Math.max(minPaneWidth(mode), Math.min(520, window.innerWidth * 0.4));
  return contextBaseWidth() * 1.5;
}

function defaultPaneWidth(mode: Mode) {
  return mode === "list" ? 400 : contextBaseWidth();
}

function contextBaseWidth() {
  const shell = document.querySelector<HTMLElement>(".app-shell");
  const value = shell ? Number.parseFloat(getComputedStyle(shell).getPropertyValue("--reader-context-default-width")) : 0;
  return value || 240;
}
