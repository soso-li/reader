export const READER_LIST_NAVIGATION_EVENT = "reader:list-navigation";
export const READER_LIST_NAVIGATION_COMMITTED_EVENT = "reader:list-navigation-committed";

export type ReaderListNavigation = {
  filter: string;
  folderId: number | null;
  href: string;
  query: string;
  sourceId: number | null;
  view: "clusters" | "browse";
};

export function parseReaderListNavigation(href: string, base = "http://reader.local/"): ReaderListNavigation | null {
  const url = new URL(href, base);
  const view = url.searchParams.get("view") ?? "clusters";
  if (view !== "clusters" && view !== "browse") return null;
  return {
    filter: url.searchParams.get("filter") ?? "",
    folderId: positiveId(url.searchParams.get("folder_id")),
    href: `${url.pathname}${url.search}`,
    query: url.searchParams.get("q") ?? "",
    sourceId: positiveId(url.searchParams.get("source_id")),
    view
  };
}

export function dispatchReaderListNavigation(href: string) {
  if (typeof window === "undefined") return false;
  const navigation = parseReaderListNavigation(href, window.location.href);
  if (!navigation) return false;
  window.dispatchEvent(new window.CustomEvent<ReaderListNavigation>(READER_LIST_NAVIGATION_EVENT, { detail: navigation }));
  return true;
}

export function dispatchReaderListNavigationCommitted(href: string) {
  if (typeof window === "undefined") return false;
  const navigation = parseReaderListNavigation(href, window.location.href);
  if (!navigation) return false;
  window.dispatchEvent(new window.CustomEvent<ReaderListNavigation>(READER_LIST_NAVIGATION_COMMITTED_EVENT, { detail: navigation }));
  return true;
}

export function scopeId(value: string | number | null | undefined) {
  return positiveId(value == null ? null : String(value));
}

function positiveId(value: string | null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}
