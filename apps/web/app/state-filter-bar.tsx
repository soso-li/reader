"use client";

import { MouseEvent, useEffect, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { Bell, FileText, FolderTree, Star } from "lucide-react";

import { setMobilePane } from "./mobile-pane";
import { queryString } from "./url-state";
import { dispatchReaderListNavigation, READER_LIST_NAVIGATION_COMMITTED_EVENT, type ReaderListNavigation } from "./reader-list-navigation";

type Scope = Record<string, string | number | null | undefined>;
export type FilterNavigation = { filter: string; href: string };

export default function StateFilterBar({ currentFilter, onNavigate, pending: externalPending = false, scope }: { currentFilter: string; onNavigate?: (navigation: FilterNavigation) => void; pending?: boolean; scope: Scope }) {
  const router = useRouter();
  const [pending, startTransition] = useTransition();
  const [committedScope, setCommittedScope] = useState<Scope | null>(null);
  const view = scope.view === "browse" ? "browse" : "clusters";
  const activeScope = committedScope?.view === view ? committedScope : scope;
  const activeFilter = String(activeScope.filter ?? currentFilter).replace(/^all$/, "");
  const filters = [
    { value: "starred", label: "收藏", icon: <Star size={18} /> },
    { value: "unread", label: "未读", icon: <Bell size={18} /> },
    { value: "sources", label: "文件夹视图", icon: <FolderTree size={18} /> },
    { value: "all", label: "全部", icon: <FileText size={18} /> }
  ];

  const busy = pending || externalPending;

  useEffect(() => {
    setCommittedScope(null);
    const syncCommittedNavigation = (event: Event) => {
      const navigation = (event as CustomEvent<ReaderListNavigation>).detail;
      if (!navigation || navigation.view !== view) return;
      const url = new URL(navigation.href, window.location.href);
      setCommittedScope({ ...Object.fromEntries(url.searchParams), view });
    };
    window.addEventListener(READER_LIST_NAVIGATION_COMMITTED_EVENT, syncCommittedNavigation);
    return () => window.removeEventListener(READER_LIST_NAVIGATION_COMMITTED_EVENT, syncCommittedNavigation);
  }, [view]);

  function navigate(event: MouseEvent<HTMLAnchorElement>, filter: string, href: string) {
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button > 0) return;
    event.preventDefault();
    if (filter === "sources") {
      window.history.pushState({}, "", href);
      setMobilePane("sources");
      dispatchReaderListNavigation(href);
    } else if (onNavigate) onNavigate({ filter, href });
    else if (dispatchReaderListNavigation(href)) return;
    else startTransition(() => router.push(href));
  }

  return (
    <nav className={`bottom-state-filters ${busy ? "pending" : ""}`} aria-busy={busy} aria-label="状态筛选">
      {filters.map((filter) => {
        const active = activeScope.pane === "sources" ? filter.value === "sources" : filter.value === "all" ? activeFilter === "" : activeFilter === filter.value;
        const href = `/?${queryString(filter.value === "sources" ? { ...activeScope, pane: "sources", offset: undefined, cluster_id: undefined, item_id: undefined } : { ...activeScope, pane: activeScope.pane === "sources" ? "list" : activeScope.pane, filter: filter.value, offset: undefined, cluster_id: undefined, item_id: undefined })}`;
        return (
          <a key={filter.value} className={`filter-${filter.value} ${active ? "active" : ""}`} href={href} title={filter.label} aria-label={filter.label} aria-current={active ? "page" : undefined} onClick={(event) => navigate(event, filter.value, href)}>
            {filter.icon}
            <span>{filter.label}</span>
          </a>
        );
      })}
    </nav>
  );
}
