"use client";

import { Columns2, RotateCcw } from "lucide-react";
import type { ReactNode } from "react";
import { useEffect, useState } from "react";

type LayoutMode = "auto" | "compact";

const COOKIE_NAME = "reader-force-mode";
const OPTIONS: { mode: LayoutMode; label: string; icon: ReactNode }[] = [
  { mode: "auto", label: "自动", icon: <RotateCcw size={15} /> },
  { mode: "compact", label: "紧凑/单栏版式", icon: <Columns2 size={15} /> }
];

export default function LayoutModeControl() {
  const [mode, setMode] = useState<LayoutMode>("auto");

  useEffect(() => {
    const html = document.documentElement;
    const saved = validMode(window.localStorage.getItem(COOKIE_NAME));
    const initial = saved ?? validMode(html.dataset.layoutMode) ?? "auto";
    applyMode(initial);
    setMode(initial);
  }, []);

  function choose(nextMode: LayoutMode) {
    applyMode(nextMode);
    setMode(nextMode);
  }

  return (
    <div className="layout-mode-control" role="group" aria-label="版式">
      {OPTIONS.map((option) => (
        <button key={option.mode} type="button" className={mode === option.mode ? "active" : ""} onClick={() => choose(option.mode)}>
          {option.icon}
          {option.label}
        </button>
      ))}
    </div>
  );
}

function validMode(value: string | undefined | null): LayoutMode | null {
  if (value === "mobile") return "compact";
  return value === "auto" || value === "compact" ? value : null;
}

function applyMode(mode: LayoutMode) {
  const html = document.documentElement;
  html.dataset.layoutMode = mode;
  html.classList.toggle("reader-force-mobile", mode === "compact");
  html.classList.remove("reader-force-desktop");
  if (mode === "auto") {
    window.localStorage.removeItem(COOKIE_NAME);
    document.cookie = `${COOKIE_NAME}=; path=/; max-age=0; samesite=lax`;
    return;
  }
  window.localStorage.setItem(COOKIE_NAME, mode);
  document.cookie = `${COOKIE_NAME}=${mode}; path=/; max-age=${60 * 60 * 24 * 365}; samesite=lax`;
}
