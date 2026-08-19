"use client";

import { useEffect } from "react";

const LAUNCH_PARAMS = new Set(["filter", "order", "pane", "view"]);

export function legacyInstalledLaunchTarget(href: string, standalone: boolean, navigationType?: string) {
  if (!standalone || navigationType !== "navigate") return null;

  const url = new URL(href);
  const view = url.searchParams.get("view");
  const pane = url.searchParams.get("pane");
  const order = url.searchParams.get("order");
  if (
    url.pathname !== "/"
    || (view !== null && view !== "clusters")
    || url.searchParams.get("filter") !== "all"
    || (pane !== null && pane !== "list")
    || (order !== null && order !== "asc" && order !== "desc")
    || [...url.searchParams.keys()].some((key) => !LAUNCH_PARAMS.has(key))
  ) return null;

  url.searchParams.set("filter", "unread");
  url.searchParams.set("pane", "list");
  return `${url.pathname}${url.search}`;
}

export default function InstalledReaderLaunch() {
  useEffect(() => {
    const standalone = window.matchMedia("(display-mode: standalone)").matches
      || Boolean((navigator as Navigator & { standalone?: boolean }).standalone);
    const navigationType = (performance.getEntriesByType("navigation")[0] as PerformanceNavigationTiming | undefined)?.type;
    const target = legacyInstalledLaunchTarget(window.location.href, standalone, navigationType);
    if (target) window.location.replace(target);
  }, []);

  return null;
}
