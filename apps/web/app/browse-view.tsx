"use client";

import { memo, type MouseEvent, type ReactNode, type RefObject, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Columns2, ExternalLink, Grid2X2, MoreHorizontal } from "lucide-react";
import { useRouter } from "next/navigation";

import { BrowseImageCard, BrowseListRow, BrowseSocialCard, BrowseVideoCard, browseImageUrl, browseSourceIconUrl, browseSourceName, formatBrowseDuration, type BrowseCardItem } from "./browse-item-card";
import BulkReadForm from "./bulk-read-form";
import { sendClientUserState } from "./client-user-state";
import { DetailScrollProgress, DetailScrollTopButton, useDetailScroll } from "./detail-scroll";
import Favicon from "./favicon";
import ListPaneResizer from "./list-pane-resizer";
import { listFilterQuery, loadBrowseList, normalizeListFilter } from "./list-api";
import { setMobileDetail, setMobileList } from "./mobile-pane";
import PullRefresh from "./pull-refresh";
import SearchBox from "./search-box";
import StateFilterBar from "./state-filter-bar";
import { TimeText } from "./time-text";
import { StateButton } from "./toolbar-buttons";
import { rssImageSrc } from "./rss-image";
import { dispatchReaderListNavigationCommitted, READER_LIST_NAVIGATION_EVENT, scopeId, type ReaderListNavigation } from "./reader-list-navigation";
import { queryString } from "./url-state";
import TranslatedArticleContent, { TranslatedTitle } from "./translated-article-content";
import {
  ReduceSimilarButton,
  uninterestedReasonLabel,
  type UninterestedReason
} from "./uninterested-actions";
import usePullRefresh, { pullRefreshEnabled } from "./use-pull-refresh";
import useScrollPastSeen from "./use-scroll-past-seen";

type ThumbnailMode = "always" | "auto" | "never";
type Scope = Record<string, string | number | null | undefined>;
type ListRange = { folderId: number | null; sourceId: number | null };
type Item = BrowseCardItem & {
  source_id: number;
  content_translation: string;
  uninterested: boolean;
  uninterested_reason: string | null;
  uninterested_note: string | null;
  uninterested_at: string | null;
};
type ItemStatePatch = Partial<Pick<Item, "read_status" | "read_later" | "starred">>;
type BrowseListAction = "original" | "read" | "star";

export default function BrowseView({
  apiUrl,
  currentFilter,
  detailError,
  initialSelectedItemId,
  initialPageCount,
  items,
  listBackHref,
  listError,
  media,
  filteredOnly,
  offset,
  pageSize,
  query,
  scope,
  selectedItem,
  thumbnailMode,
  mobileNavigation
}: {
  apiUrl: string;
  currentFilter: string;
  detailError: string;
  initialSelectedItemId: number | null;
  initialPageCount: number;
  items: Item[];
  listBackHref: string;
  listError: string;
  media: string;
  filteredOnly: boolean;
  offset: number;
  pageSize: number;
  query: string;
  scope: Scope;
  selectedItem: Item | null;
  thumbnailMode: ThumbnailMode;
  mobileNavigation?: ReactNode;
}) {
  const router = useRouter();
  const [loadedItems, setLoadedItems] = useState(items);
  const [activeFilter, setActiveFilter] = useState(currentFilter);
  const [activeQuery, setActiveQuery] = useState(query);
  const [activeFolderId, setActiveFolderId] = useState(scopeId(scope.folder_id));
  const [activeSourceId, setActiveSourceId] = useState(scopeId(scope.source_id));
  const [activeDetail, setActiveDetail] = useState(selectedItem);
  const [listPending, setListPending] = useState(false);
  const [clientListError, setClientListError] = useState("");
  const [serverListError, setServerListError] = useState(listError);
  const [serverDetailError, setServerDetailError] = useState(detailError);
  const [clientDetailError, setClientDetailError] = useState("");
  const [listStateError, setListStateError] = useState("");
  const [detailStateError, setDetailStateError] = useState("");
  const [detailPending, setDetailPending] = useState(false);
  const [nextOffset, setNextOffset] = useState(offset + initialPageCount);
  const [hasMore, setHasMore] = useState(initialPageCount >= pageSize);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadError, setLoadError] = useState("");
  const [overrides, setOverrides] = useState<Record<number, ItemStatePatch>>({});
  const [pendingState, setPendingState] = useState("");
  const [imageLayout, setImageLayout] = useState<"masonry" | "grid">("masonry");
  const listPaneRef = useRef<HTMLElement | null>(null);
  const loadMoreRef = useRef<HTMLDivElement | null>(null);
  const loadingMoreRef = useRef(false);
  const listRequestId = useRef(0);
  const listAbortController = useRef<AbortController | null>(null);
  const pageRequestId = useRef(0);
  const pageAbortController = useRef<AbortController | null>(null);
  const uninterestedRemovedRef = useRef<Map<number, { item: Item; index: number }>>(new Map());
  const detailRequestId = useRef(0);
  const detailAbortController = useRef<AbortController | null>(null);
  const detailRequestedId = useRef<number | null>(null);
  const detailTargetId = useRef<number | null>(initialSelectedItemId ?? selectedItem?.id ?? null);
  const { detailPaneRef, detailScrollProgress, onDetailScroll, resetDetailScroll, scrollDetailToTop, showDetailTopButton } = useDetailScroll();
  const pullRefresh = usePullRefresh(listPaneRef, {
    detailOpen: activeDetail !== null,
    enabled: pullRefreshEnabled(activeFilter, activeQuery, filteredOnly)
  });
  const rows = useMemo(
    () => loadedItems.map((item) => overrides[item.id] ? { ...item, ...overrides[item.id] } : item),
    [loadedItems, overrides]
  );
  const eagerFaviconIds = useMemo(() => new Set(rows.slice(0, 12).map((item) => item.id)), [rows]);
  const isBrowseSurface = media === "social" || media === "image" || media === "video";
  const selected = activeDetail ? { ...activeDetail, ...overrides[activeDetail.id] } : null;
  const selectedId = selected?.id ?? null;
  const usesImageGrid = media === "image";
  const showStateFilters = !filteredOnly && (!isBrowseSurface || media === "image" || media === "video");
  const clientScope: Scope = useMemo(() => ({
    ...scope,
    filter: activeFilter || "all",
    folder_id: activeFolderId ?? undefined,
    q: activeQuery || undefined,
    source_id: activeSourceId ?? undefined,
    offset: undefined
  }), [activeFilter, activeFolderId, activeQuery, activeSourceId, scope]);
  const selectItemRef = useRef<(event: MouseEvent<HTMLAnchorElement>, item: Item, href: string) => void>(() => undefined);
  const listActionRef = useRef<(action: BrowseListAction, item: Item) => void>(() => undefined);
  const handleSelectItem = useCallback((event: MouseEvent<HTMLAnchorElement>, item: Item, href: string) => {
    selectItemRef.current(event, item, href);
  }, []);
  const handleListAction = useCallback((action: BrowseListAction, item: Item) => {
    listActionRef.current(action, item);
  }, []);
  const filteredListHref = `/?${queryString({
    view: "browse",
    media,
    filtered: "1",
    folder_id: activeFolderId ?? undefined,
    source_id: activeSourceId ?? undefined,
    q: activeQuery || undefined,
    pane: "list"
  })}`;
  const completeStreamHref = `/?${queryString({
    view: media === "article" ? "clusters" : "browse",
    media: media === "article" ? undefined : media,
    folder_id: activeFolderId ?? undefined,
    source_id: activeSourceId ?? undefined,
    q: activeQuery || undefined,
    filter: "all",
    pane: "list"
  })}`;
  const uninterestedListHref = `/uninterested?${queryString({
    source_id: activeSourceId ?? undefined,
    q: activeQuery || undefined
  })}`;

  function hideUninterestedItems(itemIds: number[]) {
    const ids = new Set(itemIds);
    setLoadedItems((current) => {
      current.forEach((item, index) => {
        if (ids.has(item.id) && !uninterestedRemovedRef.current.has(item.id)) {
          uninterestedRemovedRef.current.set(item.id, { item, index });
        }
      });
      return current.filter((item) => !ids.has(item.id));
    });
  }

  function restoreUninterestedItems(itemIds: number[]) {
    const removed = itemIds
      .map((id) => uninterestedRemovedRef.current.get(id))
      .filter((item): item is { item: Item; index: number } => Boolean(item))
      .sort((left, right) => left.index - right.index);
    setLoadedItems((current) => removed.reduce(
      (next, entry) => insertBrowseItem(next, entry.index, entry.item),
      current
    ));
    itemIds.forEach((id) => uninterestedRemovedRef.current.delete(id));
  }

  function cancelPendingListNavigation() {
    if (!listAbortController.current) return;
    listAbortController.current?.abort();
    listAbortController.current = null;
    listRequestId.current += 1;
    setListPending(false);
  }

  function loadList(
    filter: string,
    nextQuery: string,
    href: string,
    updateHistory = true,
    restoreItemId: number | null = null,
    nextRange: ListRange = { folderId: activeFolderId, sourceId: activeSourceId }
  ) {
    const targetPane = new URL(href, window.location.href).searchParams.get("pane");
    invalidatePagination();
    invalidateDetailRequest(restoreItemId);
    const normalizedFilter = filter === "all" ? "" : filter;
    const nextScope = {
      ...scope,
      filter: normalizedFilter || "all",
      folder_id: nextRange.folderId ?? undefined,
      q: nextQuery || undefined,
      source_id: nextRange.sourceId ?? undefined,
      offset: undefined,
      item_id: undefined
    };
    const controller = new AbortController();
    listAbortController.current?.abort();
    listAbortController.current = controller;
    const requestId = ++listRequestId.current;
    setListPending(true);
    setClientListError("");
    void loadBrowseList<Item>(apiUrl, nextScope, pageSize, controller.signal)
      .then((nextRows) => {
        if (requestId !== listRequestId.current) return;
        if (listAbortController.current === controller) listAbortController.current = null;
        setActiveFilter(normalizedFilter);
        setActiveQuery(nextQuery);
        setActiveFolderId(nextRange.folderId);
        setActiveSourceId(nextRange.sourceId);
        setLoadedItems(nextRows);
        setNextOffset(nextRows.length);
        setHasMore(nextRows.length >= pageSize);
        setLoadError("");
        setOverrides({});
        setServerListError("");
        setServerDetailError("");
        setClientDetailError("");
        setListStateError("");
        setDetailStateError("");
        const restoredItem = selectBrowseDetailAfterListLoad(nextRows, restoreItemId);
        setActiveDetail(restoredItem);
        setMobileDetail(Boolean(restoreItemId));
        setMobileList(!restoreItemId && targetPane !== "sources");
        if (restoreItemId && !restoredItem) loadDetail(restoreItemId);
        if (updateHistory) {
          window.history.pushState({}, "", href);
        }
        dispatchReaderListNavigationCommitted(href);
      })
      .catch((error: unknown) => {
        if (!(error instanceof DOMException && error.name === "AbortError") && requestId === listRequestId.current) {
          setClientListError("列表加载失败，请重试。");
        }
      })
      .finally(() => {
        if (requestId === listRequestId.current) {
          if (listAbortController.current === controller) listAbortController.current = null;
          setListPending(false);
        }
      });
  }

  function openItem(item: Item, href: string, updateHistory = true) {
    cancelPendingListNavigation();
    invalidateDetailRequest(item.id);
    setServerDetailError("");
    setClientDetailError("");
    setDetailStateError("");
    setActiveDetail(item);
    setDetailPending(!item.content_text.trim());
    setMobileDetail(true);
    setMobileList(false);
    if (updateHistory) window.history.pushState({}, "", href);
  }

  function loadDetail(itemId: number) {
    invalidateDetailRequest(itemId);
    detailRequestedId.current = itemId;
    const controller = new AbortController();
    detailAbortController.current?.abort();
    detailAbortController.current = controller;
    const requestId = ++detailRequestId.current;
    setDetailPending(true);
    setClientDetailError("");
    void fetch(`${apiUrl.replace(/\/$/, "")}/items/${itemId}`, { cache: "no-store", priority: "high", signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error("详情加载失败");
        return response.json();
      })
      .then((item: Item | null) => {
        if (item && requestId === detailRequestId.current && detailTargetId.current === item.id) {
          setActiveDetail(item);
          setServerDetailError("");
        }
        if (requestId === detailRequestId.current) setDetailPending(false);
      })
      .catch((error: unknown) => {
        if (!(error instanceof DOMException && error.name === "AbortError") && requestId === detailRequestId.current) {
          detailRequestedId.current = null;
          setClientDetailError("详情加载失败，请重试。");
          setDetailPending(false);
        }
      });
  }

  function invalidatePagination() {
    pageAbortController.current?.abort();
    pageRequestId.current += 1;
    loadingMoreRef.current = false;
    setLoadingMore(false);
  }

  function invalidateDetailRequest(nextId: number | null) {
    detailAbortController.current?.abort();
    detailRequestId.current += 1;
    detailRequestedId.current = null;
    detailTargetId.current = nextId;
    setDetailPending(false);
  }

  useEffect(() => {
    listAbortController.current?.abort();
    listRequestId.current += 1;
    invalidatePagination();
    invalidateDetailRequest(initialSelectedItemId ?? selectedItem?.id ?? null);
    setLoadedItems(items);
    uninterestedRemovedRef.current.clear();
    setActiveFilter(currentFilter);
    setActiveQuery(query);
    setActiveFolderId(scopeId(scope.folder_id));
    setActiveSourceId(scopeId(scope.source_id));
    setActiveDetail(selectedItem);
    setServerListError(listError);
    setServerDetailError(detailError);
    setClientListError("");
    setClientDetailError("");
    setListStateError("");
    setDetailStateError("");
    setNextOffset(offset + initialPageCount);
    setHasMore(initialPageCount >= pageSize);
    setLoadingMore(false);
    setLoadError("");
    loadingMoreRef.current = false;
  }, [currentFilter, detailError, initialSelectedItemId, items, initialPageCount, listError, offset, pageSize, query, scope.folder_id, scope.source_id, selectedItem]);

  useEffect(() => {
    if (!activeDetail || activeDetail.content_text.trim()) {
      setDetailPending(false);
      return;
    }
    if (detailRequestedId.current === activeDetail.id) return;
    loadDetail(activeDetail.id);
  }, [activeDetail, apiUrl]);

  useEffect(() => {
    const onPopState = () => {
      const params = new URLSearchParams(window.location.search);
      const nextFilter = normalizeListFilter(params.get("filter"));
      const nextQuery = params.get("q") ?? "";
      const nextRange = {
        folderId: scopeId(params.get("folder_id")),
        sourceId: scopeId(params.get("source_id"))
      };
      const itemId = Number(params.get("item_id"));
      const restoreItemId = Number.isFinite(itemId) && itemId > 0 ? itemId : null;
      if (
        nextFilter !== activeFilter ||
        nextQuery !== activeQuery ||
        nextRange.folderId !== activeFolderId ||
        nextRange.sourceId !== activeSourceId
      ) {
        loadList(
          nextFilter,
          nextQuery,
          `${window.location.pathname}${window.location.search}`,
          false,
          restoreItemId,
          nextRange
        );
        return;
      }
      cancelPendingListNavigation();
      const nextItem = restoreItemId
        ? rows.find((item) => item.id === restoreItemId) ?? null
        : null;
      invalidateDetailRequest(restoreItemId);
      setActiveDetail(nextItem);
      setMobileDetail(Boolean(restoreItemId));
      setMobileList(!restoreItemId && params.get("pane") === "list");
      if (restoreItemId && !nextItem) loadDetail(restoreItemId);
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, [activeFilter, activeFolderId, activeQuery, activeSourceId, rows]);

  useEffect(() => {
    const onListNavigation = (event: Event) => {
      const navigation = (event as CustomEvent<ReaderListNavigation>).detail;
      if (!navigation || navigation.view !== "browse") return;
      if (new URL(navigation.href, window.location.href).searchParams.get("pane") === "sources") {
        cancelPendingListNavigation();
        dispatchReaderListNavigationCommitted(navigation.href);
        return;
      }
      loadList(
        normalizeListFilter(navigation.filter),
        navigation.query,
        navigation.href,
        true,
        null,
        { folderId: navigation.folderId, sourceId: navigation.sourceId }
      );
    };
    window.addEventListener(READER_LIST_NAVIGATION_EVENT, onListNavigation);
    return () => window.removeEventListener(READER_LIST_NAVIGATION_EVENT, onListNavigation);
  }, [activeFilter, activeFolderId, activeQuery, activeSourceId]);

  useEffect(() => () => {
    listAbortController.current?.abort();
    pageAbortController.current?.abort();
    detailAbortController.current?.abort();
  }, []);

  useEffect(() => {
    if (!isBrowseSurface) return;
    const shell = document.querySelector(".app-shell");
    shell?.classList.toggle("browse-surface-mode", selectedId === null);
    shell?.classList.toggle("browse-media-detail-mode", selectedId !== null || detailPending);
  }, [detailPending, isBrowseSurface, selectedId]);

  useEffect(() => () => {
    const shell = document.querySelector(".app-shell");
    shell?.classList.remove("browse-surface-mode", "browse-media-detail-mode");
  }, []);

  useEffect(() => {
    resetDetailScroll();
  }, [selectedId]);

  const loadMore = useCallback(() => {
    if (loadingMoreRef.current || !hasMore) return;
    loadingMoreRef.current = true;
    setLoadingMore(true);
    setLoadError("");
    const controller = new AbortController();
    pageAbortController.current?.abort();
    pageAbortController.current = controller;
    const requestId = ++pageRequestId.current;
    fetch(`${apiUrl.replace(/\/$/, "")}/items?${queryString({ ...listFilterQuery(clientScope.filter), media_type: clientScope.media, folder_id: clientScope.folder_id, source_id: clientScope.source_id, q: clientScope.q, filtered_only: filteredOnly ? "true" : undefined, limit: pageSize, offset: nextOffset, include_content: false })}`, { cache: "no-store", signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error("更多条目加载失败");
        return response.json();
      })
      .then((nextRows: Item[]) => {
        if (requestId !== pageRequestId.current) return;
        setLoadedItems((current) => {
          const existing = new Set(current.map((item) => item.id));
          return [...current, ...nextRows.filter((item) => !existing.has(item.id))];
        });
        setNextOffset((current) => current + nextRows.length);
        setHasMore(nextRows.length >= pageSize);
      })
      .catch((error: unknown) => {
        if (!(error instanceof DOMException && error.name === "AbortError")) setLoadError("更多条目加载失败");
      })
      .finally(() => {
        if (requestId !== pageRequestId.current) return;
        loadingMoreRef.current = false;
        setLoadingMore(false);
      });
  }, [apiUrl, clientScope.filter, clientScope.folder_id, clientScope.media, clientScope.q, clientScope.source_id, filteredOnly, hasMore, nextOffset, pageSize]);

  useEffect(() => {
    const root = document.querySelector(".browse-list-pane");
    const target = loadMoreRef.current;
    if (!root || !target || !hasMore) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) loadMore();
      },
      { root, rootMargin: "500px 0px 500px 0px" }
    );
    observer.observe(target);
    return () => observer.disconnect();
  }, [hasMore, loadMore]);

  function selectItem(event: MouseEvent<HTMLAnchorElement>, item: Item, href: string) {
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button > 0) return;
    event.preventDefault();
    if (item.read_status === "unread") markSelectionSeen(item, "detail");
    openItem(item, href);
  }
  selectItemRef.current = selectItem;

  function markSelectionSeen(item: Item, surface: "list" | "detail" = "list") {
    const patch = { read_status: "summary_seen" };
    if (surface === "list") setListStateError("");
    else setDetailStateError("");
    setOverrides((current) => ({ ...current, [item.id]: { ...current[item.id], ...patch } }));
    void sendClientUserState({ object_type: "item", object_id: item.id, ...patch }, { beacon: false }).catch(() => {
      setOverrides((current) => ({ ...current, [item.id]: { ...current[item.id], read_status: item.read_status } }));
      if (surface === "list") setListStateError("阅读状态保存失败，请重试。");
      else setDetailStateError("阅读状态保存失败，请重试。");
    });
  }

  useScrollPastSeen({
    rootRef: listPaneRef,
    rows,
    onSeen: (item) => markSelectionSeen(item)
  });

  function updateItemState(item: Item, patch: ItemStatePatch, pendingKey: string, surface: "list" | "detail") {
    setPendingState(pendingKey);
    if (surface === "list") setListStateError("");
    else setDetailStateError("");
    setOverrides((current) => ({ ...current, [item.id]: { ...current[item.id], ...patch } }));
    void sendClientUserState({ object_type: "item", object_id: item.id, ...patch })
      .then(() => undefined)
      .catch(() => {
        setOverrides((current) => ({ ...current, [item.id]: { ...current[item.id], read_status: item.read_status, read_later: item.read_later, starred: item.starred } }));
        if (surface === "list") setListStateError("阅读状态保存失败，请重试。");
        else setDetailStateError("阅读状态保存失败，请重试。");
      })
      .finally(() => setPendingState(""));
  }

  function toggleRead(item: Item, surface: "list" | "detail") {
    updateItemState(item, { read_status: isSeenStatus(item.read_status) ? "unread" : "summary_seen" }, "read", surface);
  }

  function toggleReadLater(item: Item, surface: "list" | "detail") {
    updateItemState(item, { read_later: !item.read_later }, "read-later", surface);
  }

  function toggleStar(item: Item, surface: "list" | "detail") {
    updateItemState(item, { starred: !item.starred }, "star", surface);
  }

  function openOriginal(item: Item, surface: "list" | "detail") {
    updateItemState(item, { read_status: "original_opened" }, "original", surface);
  }

  function performListAction(action: BrowseListAction, item: Item) {
    if (action === "read") toggleRead(item, "list");
    else if (action === "star") toggleStar(item, "list");
    else openOriginal(item, "list");
  }
  listActionRef.current = performListAction;

  function showList(event: MouseEvent<HTMLAnchorElement>) {
    event.preventDefault();
    cancelPendingListNavigation();
    invalidateDetailRequest(null);
    setServerDetailError("");
    setClientDetailError("");
    setActiveDetail(null);
    setMobileDetail(false);
    setMobileList(true);
    window.history.pushState({}, "", `/?${queryString({ ...clientScope, pane: "list", item_id: undefined })}`);
  }

  function renderDetailToolbar(item: Item) {
    return (
      <div className="toolbar browse-toolbar">
        <a className="mobile-back mobile-toolbar-back" href={`/?${queryString({ ...clientScope, pane: "list", item_id: undefined })}`} aria-label="返回列表" onClick={showList}>
          ←
        </a>
        <StateButton active={isSeenStatus(item.read_status)} disabled={pendingState === "read"} label={isSeenStatus(item.read_status) ? "标记未读" : "标记看过"} onClick={() => toggleRead(item, "detail")} />
        <StateButton active={item.starred} disabled={pendingState === "star"} icon object={{ id: item.id, starred: item.starred }} onClick={() => toggleStar(item, "detail")} />
        <StateButton active={item.read_later} disabled={pendingState === "read-later"} label="稍后阅读" onClick={() => toggleReadLater(item, "detail")} />
        <ReduceSimilarButton
          compact
          key={`uninterested-${item.id}`}
          initialFeedback={item.uninterested ? {
            reason: item.uninterested_reason as UninterestedReason | null,
            note: item.uninterested_note
          } : undefined}
          target={{ target_type: "article", item_id: item.id }}
          onHidden={(result) => hideUninterestedItems(result.affected_item_ids)}
          onRestored={(result) => restoreUninterestedItems(result.affected_item_ids)}
        />
        {item.url ? (
          <a className="icon-link" href={item.url} target="_blank" rel="noreferrer" title="打开原文" aria-label="打开原文" onClick={() => openOriginal(item, "detail")} onAuxClick={(event) => event.button === 1 && openOriginal(item, "detail")}>
            <ExternalLink size={17} />
          </a>
        ) : null}
      </div>
    );
  }

  function renderDetailContent(item: Item) {
    if (media === "image") return renderImageDetail(item);
    if (media === "video") return renderVideoDetail(item);
    if (media === "podcast") return renderAudioDetail(item);
    return renderDefaultDetail(item);
  }

  function renderDetailArticle(item: Item) {
    return (
      <div>
        <TranslatedArticleContent
          apiUrl={apiUrl}
          sourceId={item.source_id}
          html={item.reading_html}
          text={browseBodyText(item)}
          initialTranslation={browseBodyTranslation(item)}
          translationNeeded={item.reading_translation_needed}
        />
      </div>
    );
  }

  function renderDefaultDetail(item: Item) {
    return (
      <>
        {renderDetailToolbar(item)}
        {item.filtered ? <div className="filtered-item-notice"><strong>已过滤</strong><span>{item.filter_rules.join("；")}</span></div> : null}
        {item.uninterested ? <div className="filtered-item-notice"><strong>不感兴趣</strong><span>{item.uninterested_reason ? uninterestedReasonLabel(item.uninterested_reason) : "未选择原因"}{item.uninterested_note ? ` · ${item.uninterested_note}` : ""}</span></div> : null}
        <h2><TranslatedTitle text={item.title || "无标题"} initialTranslation={item.title_translation} /></h2>
        <p className="item-meta item-meta-source">
          <Favicon eager url={browseSourceIconUrl(item)} label={browseSourceName(item)} />
          <span>{browseSourceName(item)}</span>
          <span>·</span>
          <TimeText value={item.published_at} />
        </p>
        {item.media_url && item.media_kind !== "image" ? (
          <div className="browse-media-box">
            <span>{mediaKindLabel(item.media_kind) || mediaLabel(media)}{item.media_duration ? ` · ${formatBrowseDuration(item.media_duration)}` : ""}</span>
            <a className="browse-media-link" href={item.media_url} target="_blank" rel="noreferrer" onClick={() => openOriginal(item, "detail")}>
              <ExternalLink size={16} />
              打开{mediaKindLabel(item.media_kind) || mediaLabel(media)}
            </a>
          </div>
        ) : null}
        {item.image_url ? (
          <a className="browse-image-large" href={item.image_url} target="_blank" rel="noreferrer" onClick={() => openOriginal(item, "detail")}>
            <img className="article-image browse-detail-image" src={rssImageSrc(item.image_url)} alt="" loading="lazy" decoding="async" />
          </a>
        ) : null}
        {renderDetailArticle(item)}
      </>
    );
  }

  function renderImageDetail(item: Item) {
    const imageUrl = browseImageUrl(item);
    return (
      <>
        {renderDetailToolbar(item)}
        {imageUrl ? (
          <a className="browse-image-single" href={imageUrl} target="_blank" rel="noreferrer" onClick={() => openOriginal(item, "detail")}>
            <img src={rssImageSrc(imageUrl)} alt="" loading="lazy" decoding="async" />
          </a>
        ) : (
          <div className="browse-media-empty">无图片</div>
        )}
        <div className="browse-media-caption">
          <h2><TranslatedTitle text={item.title || "无标题"} initialTranslation={item.title_translation} /></h2>
          <p className="item-meta item-meta-source">
            <Favicon eager url={browseSourceIconUrl(item)} label={browseSourceName(item)} />
            <span>{browseSourceName(item)}</span>
            <span>·</span>
            <TimeText value={item.published_at} />
          </p>
          {renderDetailArticle(item)}
        </div>
      </>
    );
  }

  function renderVideoDetail(item: Item) {
    const imageUrl = browseImageUrl(item);
    const videoUrl = directVideoUrl(item);
    const embedUrl = embeddedVideoUrl(item);
    return (
      <>
        {renderDetailToolbar(item)}
        <div className="browse-video-player">
          {videoUrl ? (
            <video controls poster={imageUrl ? rssImageSrc(imageUrl) : undefined} src={videoUrl} />
          ) : embedUrl ? (
            <iframe
              src={embedUrl}
              title={`播放 ${item.title || "视频"}`}
              allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
              allowFullScreen
              referrerPolicy="strict-origin-when-cross-origin"
            />
          ) : imageUrl ? (
            <img src={rssImageSrc(imageUrl)} alt="" loading="lazy" decoding="async" />
          ) : (
            <span>无缩略图</span>
          )}
          {item.media_duration ? <span className="browse-duration">{formatBrowseDuration(item.media_duration)}</span> : null}
        </div>
        <div className="browse-media-caption">
          <h2><TranslatedTitle text={item.title || "无标题"} initialTranslation={item.title_translation} /></h2>
          <p className="item-meta item-meta-source">
            <Favicon eager url={browseSourceIconUrl(item)} label={browseSourceName(item)} />
            <span>{browseSourceName(item)}</span>
            <span>·</span>
            <TimeText value={item.published_at} />
          </p>
          {(item.url || item.media_url) && !videoUrl ? (
            <div className="browse-media-box">
              <span>{mediaKindLabel(item.media_kind) || "视频"}{item.media_duration ? ` · ${formatBrowseDuration(item.media_duration)}` : ""}</span>
              <a className="browse-media-link" href={item.url || item.media_url} target="_blank" rel="noreferrer" onClick={() => openOriginal(item, "detail")}>
                <ExternalLink size={16} />
                在原站打开
              </a>
            </div>
          ) : null}
          {renderDetailArticle(item)}
        </div>
      </>
    );
  }

  function renderAudioDetail(item: Item) {
    const audioUrl = item.media_kind === "audio" ? item.media_url : "";
    return (
      <>
        {renderDetailToolbar(item)}
        <div className="browse-audio-player">
          {audioUrl ? <audio controls preload="metadata" src={audioUrl}>当前浏览器无法播放此音频。</audio> : <span>无可播放音频</span>}
        </div>
        <div className="browse-media-caption">
          <h2><TranslatedTitle text={item.title || "无标题"} initialTranslation={item.title_translation} /></h2>
          <p className="item-meta item-meta-source">
            <Favicon eager url={browseSourceIconUrl(item)} label={browseSourceName(item)} />
            <span>{browseSourceName(item)}</span>
            <span>·</span>
            <TimeText value={item.published_at} />
          </p>
          {item.media_url ? (
            <div className="browse-media-box">
              <span>音频{item.media_duration ? ` · ${formatBrowseDuration(item.media_duration)}` : ""}</span>
              <a className="browse-media-link" href={item.media_url} target="_blank" rel="noreferrer" onClick={() => openOriginal(item, "detail")}>
                <ExternalLink size={16} />
                打开音频
              </a>
            </div>
          ) : null}
          {renderDetailArticle(item)}
        </div>
      </>
    );
  }

  return (
    <>
      <section className={`pane list-pane browse-list-pane browse-media-${media}`} ref={listPaneRef}>
        {!isBrowseSurface ? <ListPaneResizer /> : null}
        <div className="pane-header">
          <div className="list-header">
            <a className="mobile-back mobile-list-back" href={listBackHref} aria-label="返回订阅">
              ←
            </a>
            <div className="list-status" aria-live="polite">
              <PullRefresh {...pullRefresh} />
              {pullRefresh.status === "error" ? null : (
                <>
                  <span className="stream-label">{filteredOnly ? "排查" : "完整流"}</span>
                  <span>{listPending ? "正在加载…" : filteredOnly ? "已过滤" : `${mediaLabel(media)} · ${filterLabel(activeFilter)}`}</span>
                  <strong>{rows.length}</strong>
                </>
              )}
            </div>
            <div className="list-tools">
              <a className="filtered-list-link" href={uninterestedListHref}>不感兴趣</a>
              <a className="filtered-list-link" href={filteredOnly ? completeStreamHref : filteredListHref}>{filteredOnly ? "返回完整流" : "已过滤"}</a>
              {usesImageGrid ? (
                <button
                  className="icon"
                  title={imageLayout === "masonry" ? "切换为网格" : "切换为瀑布流"}
                  aria-label={imageLayout === "masonry" ? "切换为网格" : "切换为瀑布流"}
                  type="button"
                  onClick={() => setImageLayout((current) => (current === "masonry" ? "grid" : "masonry"))}
                >
                  {imageLayout === "masonry" ? <Grid2X2 size={16} /> : <Columns2 size={16} />}
                </button>
              ) : null}
              {!filteredOnly ? <BulkReadForm objectType="item" scope={clientScope} /> : null}
            </div>
            <SearchBox
              onNavigate={({ href, query: nextQuery }) => loadList(activeFilter, nextQuery, href)}
              pending={listPending}
              placeholder={filteredOnly ? "搜索已过滤条目" : "搜索条目、正文、来源"}
              query={activeQuery}
              scope={clientScope}
            />
            {mobileNavigation}
          </div>
        </div>
        {serverListError || clientListError ? <p className="error-line">{clientListError || serverListError}</p> : null}
        {listStateError ? <p className="error-line" role="alert">{listStateError}</p> : null}
        <BrowseList
          activeQuery={activeQuery}
          eagerFaviconIds={eagerFaviconIds}
          hasMore={hasMore}
          imageLayout={imageLayout}
          loadError={loadError}
          loadMoreRef={loadMoreRef}
          loadingMore={loadingMore}
          media={media}
          onAction={handleListAction}
          onSelect={handleSelectItem}
          pendingState={pendingState}
          rows={rows}
          scope={clientScope}
          selectedId={selectedId}
          thumbnailMode={thumbnailMode}
        />
        {showStateFilters ? (
          <StateFilterBar
            currentFilter={activeFilter}
            onNavigate={({ filter, href }) => {
              if (filter === "sources") router.push(href);
              else loadList(filter, activeQuery, href);
            }}
            pending={listPending}
            scope={clientScope}
          />
        ) : null}
      </section>

      {!isBrowseSurface || selected || detailPending || serverDetailError || clientDetailError || detailStateError ? <article className="pane detail browse-detail-pane" ref={detailPaneRef} onScroll={onDetailScroll}>
        <DetailScrollProgress progress={detailScrollProgress} />
        {selected ? (
          <div className={`detail-body ${media === "image" ? "browse-media-detail browse-image-detail" : media === "video" ? "browse-media-detail browse-video-detail" : media === "podcast" ? "browse-media-detail browse-audio-detail" : ""}`}>
            <DetailScrollTopButton visible={showDetailTopButton} onClick={scrollDetailToTop} />
            {detailPending ? <p className="list-status" role="status">正在加载详情…</p> : null}
            {serverDetailError || clientDetailError ? (
              <p className="error-line" role="alert">
                {clientDetailError || serverDetailError}{" "}
                {detailTargetId.current ? <button type="button" onClick={() => loadDetail(detailTargetId.current!)}>重试</button> : null}
              </p>
            ) : null}
            {detailStateError ? <p className="error-line" role="alert">{detailStateError}</p> : null}
            {renderDetailContent(selected)}
          </div>
        ) : (
          <div className="placeholder">
            <a className="mobile-back mobile-toolbar-back" href={`/?${queryString({ ...clientScope, pane: "list", item_id: undefined })}`} aria-label="返回列表" onClick={showList}>
              ←
            </a>
            {detailPending ? "正在加载详情…" : clientDetailError || serverDetailError || detailStateError || "选择一个条目查看详情。"}
            {(clientDetailError || serverDetailError) && detailTargetId.current ? <button type="button" onClick={() => loadDetail(detailTargetId.current!)}>重试</button> : null}
          </div>
        )}
      </article> : null}
    </>
  );
}

const BrowseList = memo(function BrowseList({
  activeQuery,
  eagerFaviconIds,
  hasMore,
  imageLayout,
  loadError,
  loadMoreRef,
  loadingMore,
  media,
  onAction,
  onSelect,
  pendingState,
  rows,
  scope,
  selectedId,
  thumbnailMode
}: {
  activeQuery: string;
  eagerFaviconIds: Set<number>;
  hasMore: boolean;
  imageLayout: "masonry" | "grid";
  loadError: string;
  loadMoreRef: RefObject<HTMLDivElement | null>;
  loadingMore: boolean;
  media: string;
  onAction: (action: BrowseListAction, item: Item) => void;
  onSelect: (event: MouseEvent<HTMLAnchorElement>, item: Item, href: string) => void;
  pendingState: string;
  rows: Item[];
  scope: Scope;
  selectedId: number | null;
  thumbnailMode: ThumbnailMode;
}) {
  const usesImageGrid = media === "image";
  return (
    <div className={media === "social" ? "browse-social-feed" : media === "video" ? "browse-video-grid" : usesImageGrid ? `browse-image-grid layout-${imageLayout}` : "cluster-list"}>
      {rows.length ? rows.map((item) => (
        <BrowseListItem
          active={item.id === selectedId}
          eagerFavicon={eagerFaviconIds.has(item.id)}
          item={item}
          key={item.id}
          media={media}
          onAction={onAction}
          onSelect={onSelect}
          pendingState={pendingState}
          scope={scope}
          thumbnailMode={thumbnailMode}
        />
      )) : (
        <div className="placeholder">{activeQuery ? "当前搜索没有匹配条目。" : "当前范围没有条目。"}</div>
      )}
      <div className="list-footer" ref={loadMoreRef}>
        {loadingMore ? "正在加载..." : loadError || (!hasMore && rows.length ? "没有更多条目" : "")}
      </div>
    </div>
  );
});

const BrowseListItem = memo(function BrowseListItem({
  active,
  eagerFavicon,
  item,
  media,
  onAction,
  onSelect,
  pendingState,
  scope,
  thumbnailMode
}: {
  active: boolean;
  eagerFavicon: boolean;
  item: Item;
  media: string;
  onAction: (action: BrowseListAction, item: Item) => void;
  onSelect: (event: MouseEvent<HTMLAnchorElement>, item: Item, href: string) => void;
  pendingState: string;
  scope: Scope;
  thumbnailMode: ThumbnailMode;
}) {
  const href = `/?${queryString({ ...scope, item_id: item.id, pane: "detail" })}`;
  const onNavigate = (event: MouseEvent<HTMLAnchorElement>) => onSelect(event, item, href);
  if (media === "social") {
    return <BrowseSocialCard active={active} eagerFavicon={eagerFavicon} href={href} item={item} onNavigate={onNavigate} />;
  }
  if (media === "image") {
    return <BrowseImageCard active={active} actions={<BrowseCardActions item={item} onAction={onAction} pendingState={pendingState} />} eagerFavicon={eagerFavicon} href={href} item={item} onNavigate={onNavigate} />;
  }
  if (media === "video") {
    return <BrowseVideoCard active={active} actions={<BrowseCardActions item={item} onAction={onAction} pendingState={pendingState} />} eagerFavicon={eagerFavicon} href={href} item={item} onNavigate={onNavigate} />;
  }
  const imageUrl = browseImageUrl(item);
  return <BrowseListRow active={active} eagerFavicon={eagerFavicon} href={href} item={item} onNavigate={onNavigate} showThumbnail={thumbnailMode === "always" || (thumbnailMode === "auto" && Boolean(imageUrl))} />;
});

function BrowseCardActions({ item, onAction, pendingState }: {
  item: Item;
  onAction: (action: BrowseListAction, item: Item) => void;
  pendingState: string;
}) {
  return (
    <div className="browse-card-actions" aria-label="条目操作">
      <StateButton active={isSeenStatus(item.read_status)} disabled={pendingState === "read"} label={isSeenStatus(item.read_status) ? "标记未读" : "标记看过"} onClick={() => onAction("read", item)} />
      <StateButton active={item.starred} disabled={pendingState === "star"} icon object={{ id: item.id, starred: item.starred }} onClick={() => onAction("star", item)} />
      {item.url ? (
        <a className="icon-link" href={item.url} target="_blank" rel="noreferrer" title="打开原文" aria-label="打开原文" onClick={() => onAction("original", item)} onAuxClick={(event) => event.button === 1 && onAction("original", item)}>
          <MoreHorizontal size={17} />
        </a>
      ) : (
        <button className="icon" type="button" title="无原文" aria-label="无原文" disabled>
          <MoreHorizontal size={17} />
        </button>
      )}
    </div>
  );
}

function directVideoUrl(item: Item) {
  const url = item.media_kind === "video" ? item.media_url : "";
  return /\.(mp4|m4v|mov|webm|ogv|ogg)(?:$|[?#])/i.test(url) ? url : "";
}

export function embeddedVideoUrl(item: Pick<BrowseCardItem, "media_url" | "url">) {
  for (const raw of [item.url, item.media_url]) {
    if (!raw) continue;
    let url: URL;
    try {
      url = new URL(raw);
    } catch {
      continue;
    }
    if (!["http:", "https:"].includes(url.protocol)) continue;
    const host = url.hostname.toLowerCase();
    let videoId = "";
    if (host === "youtu.be") {
      videoId = url.pathname.split("/").filter(Boolean)[0] ?? "";
    } else if (["youtube.com", "www.youtube.com", "m.youtube.com"].includes(host)) {
      videoId = url.pathname === "/watch"
        ? url.searchParams.get("v") ?? ""
        : url.pathname.match(/^\/(?:embed|shorts|v)\/([A-Za-z0-9_-]+)/)?.[1] ?? "";
    }
    if (/^[A-Za-z0-9_-]{6,20}$/.test(videoId)) {
      return `https://www.youtube-nocookie.com/embed/${videoId}`;
    }
    if (["bilibili.com", "www.bilibili.com", "m.bilibili.com"].includes(host)) {
      const bvid = url.pathname.match(/^\/video\/(BV[A-Za-z0-9]{10})(?:\/|$)/)?.[1] ?? "";
      if (bvid) return `https://player.bilibili.com/player.html?bvid=${bvid}&autoplay=0`;
    }
  }
  return "";
}

function browseBodyText(item: Item) {
  if (item.content_text.trim()) return item.content_text;
  if (item.summary.trim()) return item.summary;
  return item.title || "无标题";
}

function browseBodyTranslation(item: Item) {
  if (item.content_text.trim()) return item.content_translation;
  if (item.summary.trim()) return item.summary_translation;
  return item.title_translation;
}

function mediaKindLabel(kind: string) {
  if (kind === "image") return "图片";
  if (kind === "video") return "视频";
  if (kind === "audio") return "音频";
  return "";
}

function isSeenStatus(status: string) {
  return status === "summary_seen" || status === "original_opened";
}

export function selectBrowseDetailAfterListLoad<T extends { id: number }>(rows: T[], restoreItemId: number | null): T | null {
  if (restoreItemId) return rows.find((item) => item.id === restoreItemId) ?? null;
  return null;
}

function filterLabel(filter: string) {
  if (filter === "starred") return "收藏";
  if (filter === "read_later") return "稍后";
  if (filter === "dismissed") return "忽略";
  if (filter === "") return "全部";
  return "未读";
}

function mediaLabel(media: string) {
  if (media === "article") return "文章";
  if (media === "social") return "社交";
  if (media === "notification") return "通知";
  if (media === "image") return "图片";
  if (media === "video") return "视频";
  if (media === "podcast") return "音频";
  return "浏览";
}

function insertBrowseItem(rows: Item[], index: number, item: Item) {
  if (rows.some((row) => row.id === item.id)) return rows;
  const position = index < 0 ? rows.length : Math.min(index, rows.length);
  return [...rows.slice(0, position), item, ...rows.slice(position)];
}
