"use client";

import { type UIEvent, useRef, useState } from "react";
import { ArrowUp } from "lucide-react";

export function useDetailScroll() {
  const detailPaneRef = useRef<HTMLElement | null>(null);
  const [detailScrollProgress, setDetailScrollProgress] = useState(0);
  const [showDetailTopButton, setShowDetailTopButton] = useState(false);

  function updateDetailScroll(pane: HTMLElement) {
    const max = pane.scrollHeight - pane.clientHeight;
    setDetailScrollProgress(max > 0 ? Math.min(1, Math.max(0, pane.scrollTop / max)) : 0);
    setShowDetailTopButton(pane.scrollTop > pane.clientHeight);
  }

  function onDetailScroll(event: UIEvent<HTMLElement>) {
    updateDetailScroll(event.currentTarget);
  }

  function resetDetailScroll() {
    detailPaneRef.current?.scrollTo({ top: 0 });
    setDetailScrollProgress(0);
    setShowDetailTopButton(false);
  }

  function scrollDetailToTop() {
    detailPaneRef.current?.scrollTo({ top: 0, behavior: "smooth" });
  }

  return { detailPaneRef, detailScrollProgress, onDetailScroll, resetDetailScroll, scrollDetailToTop, showDetailTopButton };
}

export function DetailScrollProgress({ progress }: { progress: number }) {
  return (
    <div className="detail-progress" aria-hidden="true">
      <span style={{ transform: `scaleX(${progress})` }} />
    </div>
  );
}

export function DetailScrollTopButton({ onClick, visible }: { onClick: () => void; visible: boolean }) {
  return (
    <button className={`floating-top-button ${visible ? "visible" : ""}`} type="button" title="回到顶部" aria-label="回到顶部" aria-hidden={!visible} tabIndex={visible ? 0 : -1} onClick={onClick}>
      <ArrowUp size={20} />
    </button>
  );
}
