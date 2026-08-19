"use client";

import { useEffect, useRef } from "react";
import type { RefObject } from "react";

import {
  isInteractionSurfacePresented,
  summarySeenEligible
} from "./event-read-boundary";
import {
  scrollPastReadStatusTransition,
  scrollPastRowTransition
} from "./scroll-past-seen";

type SeenRow = {
  id: number;
  read_status: string;
  current_revision_uid?: string | null;
  current_revision_differs_from_seen?: boolean;
};

export default function useScrollPastSeen<T extends SeenRow>({
  onSeen,
  rootRef,
  rows,
  enabled = true,
  selector = "[data-scroll-seen-id]"
}: {
  onSeen: (row: T) => void;
  rootRef: RefObject<HTMLElement | null>;
  rows: T[];
  enabled?: boolean;
  selector?: string;
}) {
  const lastTop = useRef(0);
  const marked = useRef(new Set<number>());
  const presented = useRef(new Set<number>());
  const onSeenRef = useRef(onSeen);
  const rowsRef = useRef(rows);

  useEffect(() => {
    onSeenRef.current = onSeen;
  }, [onSeen]);

  useEffect(() => {
    const previousRows = new Map(rowsRef.current.map((row) => [row.id, row]));
    const nextIds = new Set(rows.map((row) => row.id));
    rows.forEach((row) => {
      const tracking = scrollPastReadStatusTransition({
        previousReadStatus: previousRows.get(row.id)?.read_status,
        nextReadStatus: row.read_status,
        previousRevisionUid:
          previousRows.get(row.id)?.current_revision_uid,
        nextRevisionUid: row.current_revision_uid,
        marked: marked.current.has(row.id),
        presented: presented.current.has(row.id)
      });
      if (!tracking.marked) marked.current.delete(row.id);
      if (!tracking.presented) presented.current.delete(row.id);
    });
    marked.current.forEach((id) => {
      if (!nextIds.has(id)) marked.current.delete(id);
    });
    presented.current.forEach((id) => {
      if (!nextIds.has(id)) presented.current.delete(id);
    });
    rowsRef.current = rows;
  }, [rows]);

  useEffect(() => {
    const currentRoot = rootRef.current;
    if (!currentRoot || !enabled) return;
    const scrollRoot: HTMLElement = currentRoot;
    let ticking = false;
    let initialFrame: number | null = null;
    lastTop.current = scrollRoot.scrollTop;

    function checkRows(markPassedRows = true) {
      ticking = false;
      const currentRows = new Map(rowsRef.current.map((row) => [row.id, row]));
      const rootBounds = scrollRoot.getBoundingClientRect();
      const rootPresented = isInteractionSurfacePresented(scrollRoot);
      scrollRoot.querySelectorAll<HTMLElement>(selector).forEach((element) => {
        const id = Number(element.dataset.scrollSeenId);
        const row = currentRows.get(id);
        if (
          !row ||
          !summarySeenEligible(
            row.read_status,
            row.current_revision_differs_from_seen ?? false
          ) ||
          marked.current.has(id)
        ) {
          return;
        }
        const transition = scrollPastRowTransition({
          row: element.getBoundingClientRect(),
          root: rootBounds,
          surfacePresented:
            rootPresented && isInteractionSurfacePresented(element),
          wasPresented: presented.current.has(id)
        });
        if (transition.presented) presented.current.add(id);
        if (!markPassedRows || !transition.markSeen) return;
        marked.current.add(id);
        onSeenRef.current(row);
      });
    }

    function onScroll() {
      const nextTop = scrollRoot.scrollTop;
      if (nextTop <= lastTop.current) {
        lastTop.current = nextTop;
        return;
      }
      lastTop.current = nextTop;
      if (ticking) return;
      ticking = true;
      window.requestAnimationFrame(() => checkRows());
    }

    scrollRoot.addEventListener("scroll", onScroll, { passive: true });
    initialFrame = window.requestAnimationFrame(() => checkRows(false));
    return () => {
      scrollRoot.removeEventListener("scroll", onScroll);
      if (initialFrame !== null) window.cancelAnimationFrame(initialFrame);
    };
  }, [enabled, rootRef, selector]);
}
