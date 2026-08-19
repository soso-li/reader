"use client";

import { type MouseEvent, type ReactNode, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { MessageCircle, Sparkles } from "lucide-react";
import { useRouter } from "next/navigation";

import ArticleContent from "./article-content";
import BulkReadForm from "./bulk-read-form";
import { clusterItemsByTime } from "./cluster-items";
import type { ClusterEventIdentity } from "./cluster-event-identity";
import type {
  ClusterSynthesisFields,
  OriginalOpenedSelection,
  SourceViewEvidence,
  SynthesisBlock,
  SynthesisCitation
} from "./event-synthesis";
import {
  citationTarget,
  originalOpenedSelectionForView,
  renderedEventReadTarget,
  synthesisRequestAvailable,
  synthesisViewAvailable
} from "./event-synthesis";
import { synthesisTaskMessage } from "./generation-task-status";
import {
  confirmedEventStatePatch,
  createOperationId,
  createEventReadStateMutation,
  createEventUserStateMutation,
  sendEventUserStateMutation,
  type EventReadStatus,
  type EventReadTarget,
  type EventUserStateMutationResult
} from "./event-user-state";
import {
  clearEventReadErrorsAfterSuccess,
  detailEvidencePresented,
  detailSummarySeenAllowed,
  explicitReadStatusPresentationAllowed,
  isInteractionSurfacePresented,
  originalOpenedIntent,
  recordEventReadFailure,
  readStatusToggleIntent,
  summarySeenEligible,
  summarySeenIntent,
  visibleEventReadErrors,
  type EventReadError,
  type EventReadErrorSurface,
  type EventReadOperation,
  type OriginalOpenedTrigger,
  type SummarySeenTrigger
} from "./event-read-boundary";
import ClusterList from "./cluster-list";
import CustomToolbar from "./custom-toolbar";
import type { ToolbarAction } from "./custom-toolbar";
import { DetailScrollProgress, DetailScrollTopButton, useDetailScroll } from "./detail-scroll";
import Favicon from "./favicon";
import ListPaneResizer from "./list-pane-resizer";
import { loadClusterCount, loadClusterList, normalizeListFilter } from "./list-api";
import { dispatchReaderUnreadCountChanged, effectiveUnreadCountDelta } from "./live-unread-count";
import { dispatchReaderListNavigation, dispatchReaderListNavigationCommitted, READER_LIST_NAVIGATION_EVENT, scopeId, type ReaderListNavigation } from "./reader-list-navigation";
import { setMobileDetail, setMobileList } from "./mobile-pane";
import PullRefresh from "./pull-refresh";
import { rssImageSrc } from "./rss-image";
import SearchBox from "./search-box";
import { CopyLinkButton, CopyMarkdownButton, PrintButton, ShareButton } from "./share-actions";
import SpeechButton from "./speech-button";
import StateFilterBar from "./state-filter-bar";
import { displaySourceName } from "./source-name";
import SummarySeenMarker from "./report-seen-marker";
import { formatExactTime } from "./time-format";
import { TimeText } from "./time-text";
import { StateButton } from "./toolbar-buttons";
import TranslatedArticleContent, { TranslatedTitle } from "./translated-article-content";
import { ReduceSimilarButton, type UninterestedReason } from "./uninterested-actions";
import { queryString } from "./url-state";
import usePullRefresh, { pullRefreshEnabled } from "./use-pull-refresh";
import useScrollPastSeen from "./use-scroll-past-seen";

type Item = {
  id: number;
  source_id: number;
  source_name: string;
  source_site_url: string;
  title: string;
  title_translation: string;
  summary: string;
  summary_translation: string;
  content_text: string;
  content_translation: string;
  reading_html?: string | null;
  reading_translation_needed?: boolean;
  image_url: string;
  url: string;
  published_at: string | null;
  read_status: string;
  read_later: boolean;
  starred: boolean;
  filtered: boolean;
  filter_rules: string[];
};

type Cluster = ClusterEventIdentity & ClusterSynthesisFields & {
  id: number;
  title: string;
  generated_title: string;
  generated_title_translation: string;
  generated_summary: string;
  generated_content: string;
  citations: string;
  model_version?: string;
  prompt_version?: string;
  first_seen_at: string | null;
  last_seen_at: string | null;
  item_count: number;
  read_status: string;
  read_later: boolean;
  starred: boolean;
  items?: Item[];
  source_view_evidence?: SourceViewEvidence[];
};

type ClusterImage = { url: string; alt: string; sources: string[] };
type DetailMode = "synthesis" | "source";
type Scope = Record<string, string | number | null | undefined>;
type ListRange = { folderId: number | null; sourceId: number | null };
type EventStateField = "starred" | "read_later";
type ClusterStatePatch = Partial<
  Pick<
    Cluster,
    | "read_status"
    | "read_later"
    | "starred"
    | "seen_revision_uid"
    | "current_revision_differs_from_seen"
    | "has_material_update"
    | "material_update_revision_uid"
  >
>;
type ConfirmedReadState = Pick<
  Cluster,
  | "read_status"
  | "seen_revision_uid"
  | "current_revision_differs_from_seen"
  | "has_material_update"
  | "material_update_revision_uid"
>;
export default function ClusterView({
  apiUrl,
  currentFilter,
  currentFilterCount,
  initialDetail,
  initialPageCount,
  initialSelectedClusterId,
  offset,
  pageSize,
  query,
  rows,
  scope,
  skipSeen,
  listBackHref,
  mobileNavigation,
  reportReminder,
  listError,
  detailError
}: {
  apiUrl: string;
  currentFilter: string;
  currentFilterCount: number | null;
  initialDetail: Cluster | null;
  initialPageCount: number;
  initialSelectedClusterId: number | null;
  offset: number;
  pageSize: number;
  query: string;
  rows: Cluster[];
  scope: Scope;
  skipSeen: boolean;
  listBackHref: string;
  mobileNavigation?: ReactNode;
  reportReminder?: ReactNode;
  listError?: string;
  detailError?: string;
}) {
  const router = useRouter();
  const fallbackSelectedId = initialSelectedClusterId ?? initialDetail?.id ?? null;
  const [selectedId, setSelectedId] = useState<number | null>(fallbackSelectedId);
  const [detail, setDetail] = useState<Cluster | null>(initialDetail ?? null);
  const [listRows, setListRows] = useState(rows);
  const [listSeedRows, setListSeedRows] = useState(rows);
  const [activeFilter, setActiveFilter] = useState(currentFilter);
  const [activeFilterCount, setActiveFilterCount] = useState(currentFilterCount);
  const [activeQuery, setActiveQuery] = useState(query);
  const [activeFolderId, setActiveFolderId] = useState(scopeId(scope.folder_id));
  const [activeSourceId, setActiveSourceId] = useState(scopeId(scope.source_id));
  const [listPageCount, setListPageCount] = useState(initialPageCount);
  const [listOffset, setListOffset] = useState(offset);
  const [listPending, setListPending] = useState(false);
  const [clientListError, setClientListError] = useState("");
  const [serverListError, setServerListError] = useState(listError ?? "");
  const [serverDetailError, setServerDetailError] = useState(detailError ?? "");
  const [clientDetailError, setClientDetailError] = useState("");
  const [eventStateErrors, setEventStateErrors] = useState<Partial<Record<EventStateField, string>>>({});
  const [detailRetry, setDetailRetry] = useState(0);
  const listRequestId = useRef(0);
  const listAbortController = useRef<AbortController | null>(null);
  const unreadCountAbortController = useRef<AbortController | null>(null);
  const unreadCountRefreshTimer = useRef<number | null>(null);
  const currentClientScope = useRef<Scope>({});
  const eventPendingRef = useRef<Set<string>>(new Set());
  const eventReadQueueRef = useRef<Map<number, Promise<void>>>(new Map());
  const eventReadLatestRef = useRef<Map<number, EventReadOperation>>(new Map());
  const eventReadConfirmedRef = useRef<Map<number, ConfirmedReadState>>(new Map());
  const uninterestedRemovedRef = useRef<Map<number, {
    cluster: Cluster;
    listIndex: number;
    seedIndex: number;
  }>>(new Map());
  const [eventPendingKeys, setEventPendingKeys] = useState<ReadonlySet<string>>(
    () => new Set()
  );
  const [selectedSourceItemId, setSelectedSourceItemId] = useState<number | null>(null);
  const [detailMode, setDetailMode] = useState<DetailMode>(() =>
    synthesisDefaultView(initialDetail)
  );
  const [bionic, setBionic] = useState(false);
  const [interactionReady, setInteractionReady] = useState(false);
  const [eventReadErrors, setEventReadErrors] = useState<EventReadError[]>([]);
  const [allowSummarySeen, setAllowSummarySeen] = useState(
    detailSummarySeenAllowed(
      initialSelectedClusterId === null ? "automatic" : "direct_url",
      fallbackSelectedId
    )
  );
  const [detailPresentationEpoch, setDetailPresentationEpoch] = useState(0);
  const [rowOverrides, setRowOverrides] = useState<Record<number, ClusterStatePatch>>({});
  const detailRequestId = useRef(0);
  const listPaneRef = useRef<HTMLElement | null>(null);
  const { detailPaneRef, detailScrollProgress, onDetailScroll, resetDetailScroll, scrollDetailToTop, showDetailTopButton } = useDetailScroll();
  const pullRefresh = usePullRefresh(listPaneRef, {
    detailOpen: selectedId !== null,
    enabled: pullRefreshEnabled(activeFilter, activeQuery)
  });
  const selectedClusterBase = detail?.id === selectedId ? detail : listRows.find((cluster) => cluster.id === selectedId) ?? null;
  const selectedCluster = selectedClusterBase ? { ...selectedClusterBase, ...rowOverrides[selectedClusterBase.id] } : null;
  const detailLoadError = clientDetailError || serverDetailError;
  const scrollRows = useMemo(
    () => skipSeen ? [] : listRows.map((cluster) => rowOverrides[cluster.id] ? { ...cluster, ...rowOverrides[cluster.id] } : cluster),
    [listRows, rowOverrides, skipSeen]
  );
  const listEventErrors = visibleEventReadErrors(eventReadErrors, "list", selectedCluster?.id ?? null);
  const detailEventErrors = visibleEventReadErrors(eventReadErrors, "detail", selectedCluster?.id ?? null);
  const selectedIndex = selectedId === null ? -1 : listRows.findIndex((cluster) => cluster.id === selectedId);
  const nextCluster = selectedIndex >= 0 ? listRows[selectedIndex + 1] ?? null : null;
  const selectedDetailReady = detail?.id === selectedId;
  const sourceItems = clusterItemsByTime(selectedCluster?.items);
  const selectedSourceItem = selectedSourceItemFor(sourceItems, selectedSourceItemId);
  const sourceMode =
    detailMode === "source" ||
    (selectedCluster?.item_count === 1 &&
      !synthesisViewAvailable(selectedCluster.synthesis));
  const renderedTarget = renderedEventReadTarget(
    selectedCluster?.event_uid,
    selectedCluster?.synthesis,
    sourceMode ? "source" : "synthesis",
    selectedCluster?.current_revision_uid
  );
  const renderedRevisionUid = renderedTarget.observed_revision_uid;
  const selectedSourceEvidenceText = selectedSourceItem?.content_text || "";
  const selectedOriginalText = itemOriginalText(selectedSourceItem);
  const selectedOriginalTranslation = itemOriginalTranslation(selectedSourceItem);
  const synthesisText = selectedCluster?.synthesis?.current?.blocks
    .map((block) => block.body)
    .join("\n\n") ?? "";
  const hasPresentedEvidence = detailEvidencePresented(
    sourceMode,
    selectedSourceEvidenceText,
    selectedCluster?.synthesis?.current?.blocks.length ?? 0
  );
  const selectedSpeechText = sourceMode ? selectedOriginalText : synthesisText;
  const detailTitle = sourceMode
    ? selectedSourceItem?.title || (selectedCluster?.title ?? "")
    : selectedCluster?.title ?? "";
  const detailTitleTranslation = sourceMode
    ? selectedSourceItem?.title_translation || ""
    : "";
  const markdownSourceItem = selectedSourceItem ?? sourceItems[0] ?? null;
  const selectedSourceHref = selectedSourceItem
    ? `/?${queryString({ view: "clusters", source_id: selectedSourceItem.source_id, filter: "unread", pane: "list" })}`
    : "";
  const clientScope: Scope = useMemo(() => ({
    ...scope,
    filter: activeFilter || "all",
    folder_id: activeFolderId ?? undefined,
    q: activeQuery || undefined,
    source_id: activeSourceId ?? undefined,
    offset: listOffset || undefined
  }), [activeFilter, activeFolderId, activeQuery, activeSourceId, listOffset, scope]);
  const selectClusterRef = useRef<(event: MouseEvent<HTMLElement>, cluster: Cluster, href: string) => void>(() => undefined);
  const handleSelectCluster = useCallback((event: MouseEvent<HTMLElement>, cluster: Cluster, href: string) => {
    selectClusterRef.current(event, cluster, href);
  }, []);
  const filteredListHref = `/?${queryString({
    view: "browse",
    media: "article",
    filtered: "1",
    folder_id: activeFolderId ?? undefined,
    source_id: activeSourceId ?? undefined,
    q: activeQuery || undefined,
    pane: "list"
  })}`;
  const uninterestedListHref = `/uninterested?${queryString({
    source_id: activeSourceId ?? undefined,
    q: activeQuery || undefined
  })}`;

  function hideUninterestedCluster(cluster: Cluster) {
    if (!uninterestedRemovedRef.current.has(cluster.id)) {
      uninterestedRemovedRef.current.set(cluster.id, {
        cluster,
        listIndex: listRows.findIndex((item) => item.id === cluster.id),
        seedIndex: listSeedRows.findIndex((item) => item.id === cluster.id)
      });
    }
    setListRows((current) => current.filter((item) => item.id !== cluster.id));
    setListSeedRows((current) => current.filter((item) => item.id !== cluster.id));
    setActiveFilterCount((current) => current === null ? null : Math.max(0, current - 1));
  }

  function restoreUninterestedCluster(cluster: Cluster) {
    const removed = uninterestedRemovedRef.current.get(cluster.id);
    if (!removed) return;
    setListRows((current) => insertAt(current, removed.listIndex, removed.cluster));
    setListSeedRows((current) => insertAt(current, removed.seedIndex, removed.cluster));
    setActiveFilterCount((current) => current === null ? null : current + 1);
    uninterestedRemovedRef.current.delete(cluster.id);
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
    restoreClusterId: number | null = null,
    nextRange: ListRange = { folderId: activeFolderId, sourceId: activeSourceId }
  ) {
    const targetPane = new URL(href, window.location.href).searchParams.get("pane");
    const normalizedFilter = filter === "all" ? "" : filter;
    const nextScope = {
      ...scope,
      filter: normalizedFilter || "all",
      folder_id: nextRange.folderId ?? undefined,
      q: nextQuery || undefined,
      source_id: nextRange.sourceId ?? undefined,
      offset: undefined,
      cluster_id: undefined
    };
    const controller = new AbortController();
    listAbortController.current?.abort();
    listAbortController.current = controller;
    const requestId = ++listRequestId.current;
    setListPending(true);
    setClientListError("");
    const listRequest = loadClusterList<Cluster>(apiUrl, nextScope, pageSize, controller.signal);
    const countRequest = loadClusterCount(apiUrl, nextScope, controller.signal).catch(() => null);
    void listRequest
      .then((nextRows) => {
        if (requestId !== listRequestId.current) return;
        if (listAbortController.current === controller) listAbortController.current = null;
        setActiveFilter(normalizedFilter);
        setActiveFilterCount(null);
        setActiveQuery(nextQuery);
        setActiveFolderId(nextRange.folderId);
        setActiveSourceId(nextRange.sourceId);
        setListRows(nextRows);
        setListSeedRows(nextRows);
        setListPageCount(nextRows.length);
        setListOffset(0);
        setRowOverrides({});
        const restoredCluster = restoreClusterId
          ? nextRows.find((cluster) => cluster.id === restoreClusterId) ?? null
          : null;
        setSelectedId(restoreClusterId);
        setDetail(null);
        setDetailMode(synthesisDefaultView(restoredCluster));
        setSelectedSourceItemId(null);
        setDetailPresentationEpoch((current) => current + 1);
        setAllowSummarySeen(detailSummarySeenAllowed("history_navigation", restoreClusterId));
        setMobileDetail(Boolean(restoreClusterId));
        setMobileList(!restoreClusterId && targetPane !== "sources");
        setServerListError("");
        setServerDetailError("");
        setClientDetailError("");
        setEventStateErrors({});
        if (updateHistory) {
          window.history.pushState({}, "", href);
        }
        dispatchReaderListNavigationCommitted(href);
        void countRequest
          .then((count) => {
            if (count !== null && requestId === listRequestId.current) setActiveFilterCount(count);
          });
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

  useLayoutEffect(() => {
    currentClientScope.current = clientScope;
  }, [clientScope]);

  function switchDetailMode(mode: DetailMode) {
    if (mode === detailMode) return;
    setDetailMode(mode);
  }

  useEffect(() => {
    const nextSelectedId = initialSelectedClusterId ?? initialDetail?.id ?? null;
    setSelectedId(nextSelectedId);
    setDetail(initialDetail ?? null);
    setListRows(rows);
    setListSeedRows(rows);
    uninterestedRemovedRef.current.clear();
    setActiveFilter(currentFilter);
    setActiveFilterCount(currentFilterCount);
    setActiveQuery(query);
    setActiveFolderId(scopeId(scope.folder_id));
    setActiveSourceId(scopeId(scope.source_id));
    setListPageCount(initialPageCount);
    setListOffset(offset);
    setListPending(false);
    setClientListError("");
    setServerListError(listError ?? "");
    setServerDetailError(detailError ?? "");
    setClientDetailError("");
    setEventStateErrors({});
    setRowOverrides({});
    setAllowSummarySeen(
      detailSummarySeenAllowed(
        initialSelectedClusterId === null ? "automatic" : "direct_url",
        nextSelectedId
      )
    );
    setSelectedSourceItemId(null);
    setDetailMode(synthesisDefaultView(initialDetail));
  }, [currentFilter, currentFilterCount, detailError, initialDetail, initialPageCount, initialSelectedClusterId, listError, offset, query, rows, scope.folder_id, scope.source_id]);

  useEffect(() => {
    setSelectedSourceItemId(null);
    setDetailMode(synthesisDefaultView(selectedCluster));
  }, [selectedCluster?.id, selectedCluster?.synthesis?.default_view]);

  useEffect(() => {
    resetDetailScroll();
  }, [detailMode, selectedCluster?.id, selectedSourceItemId]);

  useEffect(() => {
    const timer = window.setTimeout(() => setInteractionReady(true), 120);
    return () => {
      window.clearTimeout(timer);
      listAbortController.current?.abort();
      unreadCountAbortController.current?.abort();
      if (unreadCountRefreshTimer.current !== null) {
        window.clearTimeout(unreadCountRefreshTimer.current);
      }
    };
  }, []);

  useEffect(() => {
    if (selectedId === null || selectedDetailReady) return;
    const controller = new AbortController();
    const requestId = ++detailRequestId.current;
    setClientDetailError("");
    fetch(`${apiUrl.replace(/\/$/, "")}/clusters/${selectedId}`, { cache: "no-store", priority: "high", signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error("详情加载失败");
        return response.json();
      })
      .then((data: Cluster | null) => {
        if (requestId !== detailRequestId.current) return;
        if (!data) throw new Error("详情加载失败");
        setDetail(data);
        setDetailMode(synthesisDefaultView(data));
        setServerDetailError("");
      })
      .catch((error: unknown) => {
        if (!(error instanceof DOMException && error.name === "AbortError")) {
          if (requestId === detailRequestId.current) {
            setClientDetailError("详情加载失败，请重试。");
          }
        }
      });
    return () => {
      controller.abort();
    };
  }, [apiUrl, detailRetry, selectedDetailReady, selectedId]);

  useEffect(() => {
    const onPopState = () => {
      const params = new URLSearchParams(window.location.search);
      const nextFilter = normalizeListFilter(params.get("filter"));
      const nextQuery = params.get("q") ?? "";
      const nextRange = {
        folderId: scopeId(params.get("folder_id")),
        sourceId: scopeId(params.get("source_id"))
      };
      const clusterId = Number(params.get("cluster_id"));
      const nextId = Number.isFinite(clusterId) && clusterId > 0 ? clusterId : null;
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
          nextId,
          nextRange
        );
        return;
      }
      cancelPendingListNavigation();
      setSelectedSourceItemId(null);
      setDetailPresentationEpoch((current) => current + 1);
      setSelectedId(nextId);
      setDetail((current) => (current?.id === nextId ? current : null));
      setAllowSummarySeen(
        detailSummarySeenAllowed("history_navigation", nextId)
      );
      setMobileDetail(Number.isFinite(clusterId) && clusterId > 0);
      setMobileList(!(Number.isFinite(clusterId) && clusterId > 0) && params.get("pane") === "list");
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, [activeFilter, activeFolderId, activeQuery, activeSourceId, listRows]);

  useEffect(() => {
    const onListNavigation = (event: Event) => {
      const navigation = (event as CustomEvent<ReaderListNavigation>).detail;
      if (!navigation || navigation.view !== "clusters") return;
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

  function selectCluster(event: MouseEvent<HTMLElement>, cluster: Cluster, href: string) {
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button > 0) return;
    event.preventDefault();
    openCluster(cluster, href);
  }
  selectClusterRef.current = selectCluster;

  function openCluster(cluster: Cluster, href: string) {
    cancelPendingListNavigation();
    const previous = selectedCluster;
    if (previous && previous.id !== cluster.id) {
      markSummarySeen(
        previous,
        "selection_leave",
        allowSummarySeen &&
          selectedDetailReady && isInteractionSurfacePresented(detailPaneRef.current),
        renderedTarget
      );
    }
    setAllowSummarySeen(
      detailSummarySeenAllowed("user_selection", cluster.id)
    );
    setDetailPresentationEpoch((current) => current + 1);
    setServerDetailError("");
    setClientDetailError("");
    setEventStateErrors({});
    setSelectedId(cluster.id);
    setDetail(null);
    setDetailMode(synthesisDefaultView(cluster));
    setSelectedSourceItemId(null);
    window.history.pushState({}, "", href);
    setMobileDetail(true);
  }

  function nextClusterHref(cluster: Cluster) {
    return `/?${queryString({ ...clientScope, cluster_id: cluster.id })}`;
  }

  function switchToNextCluster() {
    if (!nextCluster) return;
    openCluster(nextCluster, nextClusterHref(nextCluster));
  }

  function backToList(event: MouseEvent<HTMLAnchorElement>) {
    event.preventDefault();
    showList();
  }

  function showList() {
    cancelPendingListNavigation();
    window.history.pushState({}, "", `/?${queryString({ ...clientScope, pane: "list" })}`);
    setSelectedId(null);
    setDetail(null);
    setSelectedSourceItemId(null);
    setAllowSummarySeen(
      detailSummarySeenAllowed("history_navigation", null)
    );
    setMobileDetail(false);
    setMobileList(true);
  }

  function updateEventState(
    field: EventStateField,
    value: boolean,
    pendingKey: string
  ) {
    if (!selectedCluster) return;
    const cluster = selectedCluster;
    const operationKey = `${cluster.id}:${pendingKey}`;
    if (eventPendingRef.current.has(operationKey)) return;
    let mutation;
    try {
      mutation = createEventUserStateMutation(
        cluster,
        field === "starred" ? "starred_set" : "read_later_set",
        value
      );
    } catch {
      setEventStateErrors((current) => ({ ...current, [field]: "事件状态保存失败，请重试。" }));
      return;
    }
    const previousValue = cluster[field];
    const patch = { [field]: value } as Pick<Cluster, typeof field>;
    setEventStateErrors((current) => ({ ...current, [field]: undefined }));
    setEventPending(operationKey, true);
    setDetail((current) => (current?.id === cluster.id ? { ...current, ...patch } : current));
    setRowOverrides((current) => ({
      ...current,
      [cluster.id]: { ...current[cluster.id], ...patch }
    }));
    sendEventUserStateMutation(mutation, { beacon: false })
      .then((result) => {
        if (!result) return;
        const confirmed = confirmedEventStatePatch(result);
        setDetail((current) =>
          current?.id === cluster.id ? { ...current, ...confirmed } : current
        );
        setRowOverrides((current) => ({
          ...current,
          [cluster.id]: { ...current[cluster.id], ...confirmed }
        }));
        setEventStateErrors((current) => ({ ...current, [field]: undefined }));
      })
      .catch(() => {
        const reverted = { [field]: previousValue } as Pick<Cluster, typeof field>;
        setDetail((current) =>
          current?.id === cluster.id && current[field] === value
            ? { ...current, ...reverted }
            : current
        );
        setRowOverrides((current) => {
          if (current[cluster.id]?.[field] !== value) return current;
          return {
            ...current,
            [cluster.id]: { ...current[cluster.id], ...reverted }
          };
        });
        setEventStateErrors((current) => ({ ...current, [field]: "事件状态保存失败，请重试。" }));
      })
      .finally(() => setEventPending(operationKey, false));
  }

  function updateEventReadState(
    cluster: Cluster,
    value: EventReadStatus,
    target: EventReadTarget,
    errorSurface: EventReadErrorSurface = "detail"
  ) {
    const operationId = createOperationId();
    const operation: EventReadOperation = {
      clusterId: cluster.id,
      operationId,
      requestedStatus: value,
      target,
      surface: errorSurface
    };
    let mutation;
    try {
      mutation = createEventReadStateMutation(target, value, operationId);
    } catch {
      setEventReadErrors((current) =>
        recordEventReadFailure(
          current,
          operation,
          operation,
          "该条目缺少事件身份，阅读状态暂时无法保存。"
        )
      );
      return;
    }

    const clusterId = cluster.id;
    const listRequestAtStart = listRequestId.current;
    const unreadFilterAtStart = activeFilter === "unread";
    unreadCountAbortController.current?.abort();
    if (unreadCountRefreshTimer.current !== null) {
      window.clearTimeout(unreadCountRefreshTimer.current);
      unreadCountRefreshTimer.current = null;
    }
    const operationKey = `${clusterId}:read-status`;
    if (!eventReadConfirmedRef.current.has(clusterId)) {
      eventReadConfirmedRef.current.set(clusterId, readStateFrom(cluster));
    }
    eventReadLatestRef.current.set(clusterId, operation);
    setEventPending(operationKey, true);
    applyClusterPatch(
      clusterId,
      optimisticReadPatch(cluster, value, target.observed_revision_uid)
    );

    const previous = eventReadQueueRef.current.get(clusterId) ?? Promise.resolve();
    const task = previous
      .catch(() => undefined)
      .then(async () => {
        let result: EventUserStateMutationResult | undefined;
        try {
          result = await sendEventUserStateMutation(mutation, { beacon: false });
        } catch {
          const latest = eventReadLatestRef.current.get(clusterId);
          if (latest?.operationId === operation.operationId) {
            const confirmed = eventReadConfirmedRef.current.get(clusterId);
            if (confirmed) applyClusterPatch(clusterId, confirmed);
          }
          setEventReadErrors((current) =>
            recordEventReadFailure(current, operation, latest)
          );
          return;
        }
        if (!result || result.action !== "read_status_set") return;
        const confirmed = confirmedEventStatePatch(result) as ConfirmedReadState;
        const previousConfirmed = eventReadConfirmedRef.current.get(clusterId);
        eventReadConfirmedRef.current.set(clusterId, confirmed);
        if (previousConfirmed) {
          const delta = effectiveUnreadCountDelta(previousConfirmed, confirmed);
          if (delta !== 0) {
            if (unreadFilterAtStart && listRequestAtStart === listRequestId.current) {
              setActiveFilterCount((current) =>
                current === null ? null : Math.max(0, current + delta)
              );
            }
            dispatchReaderUnreadCountChanged(delta);
          }
        }
        setEventReadErrors((current) =>
          clearEventReadErrorsAfterSuccess(current, operation)
        );
        if (eventReadLatestRef.current.get(clusterId)?.operationId === operation.operationId) {
          applyClusterPatch(clusterId, confirmed);
        }
      })
      .finally(() => {
        if (eventReadQueueRef.current.get(clusterId) !== task) return;
        eventReadQueueRef.current.delete(clusterId);
        eventReadConfirmedRef.current.delete(clusterId);
        if (eventReadQueueRef.current.size === 0) scheduleUnreadCountRefresh();
        if (eventReadLatestRef.current.get(clusterId)?.operationId === operation.operationId) {
          eventReadLatestRef.current.delete(clusterId);
          setEventPending(operationKey, false);
        }
      });
    eventReadQueueRef.current.set(clusterId, task);
  }

  function scheduleUnreadCountRefresh() {
    if (unreadCountRefreshTimer.current !== null) {
      window.clearTimeout(unreadCountRefreshTimer.current);
    }
    unreadCountRefreshTimer.current = window.setTimeout(() => {
      unreadCountRefreshTimer.current = null;
      if (eventReadQueueRef.current.size > 0) return;
      const nextScope = currentClientScope.current;
      if (normalizeListFilter(String(nextScope.filter ?? "")) !== "unread") return;
      const requestId = listRequestId.current;
      const controller = new AbortController();
      unreadCountAbortController.current?.abort();
      unreadCountAbortController.current = controller;
      void loadClusterCount(apiUrl, nextScope, controller.signal)
        .then((count) => {
          if (requestId === listRequestId.current) setActiveFilterCount(count);
        })
        .catch(() => undefined)
        .finally(() => {
          if (unreadCountAbortController.current === controller) {
            unreadCountAbortController.current = null;
          }
        });
    }, 200);
  }

  function applyClusterPatch(clusterId: number, patch: ClusterStatePatch) {
    setDetail((current) =>
      current?.id === clusterId ? { ...current, ...patch } : current
    );
    setRowOverrides((current) => ({
      ...current,
      [clusterId]: { ...current[clusterId], ...patch }
    }));
  }

  function markSummarySeen(
    cluster: Cluster,
    trigger: SummarySeenTrigger,
    userPresented: boolean,
    target: EventReadTarget = {
      event_uid: cluster.event_uid,
      observed_revision_uid: cluster.current_revision_uid
    }
  ) {
    const intent = summarySeenIntent(trigger, {
      readStatus: cluster.read_status,
      skip: skipSeen,
      userPresented,
      evidencePresented: trigger === "scroll_past" || hasPresentedEvidence,
      currentRevisionDiffersFromSeen:
        cluster.current_revision_differs_from_seen &&
        target.observed_revision_uid !== cluster.seen_revision_uid
    });
    if (intent) {
      updateEventReadState(
        cluster,
        intent,
        target,
        trigger === "scroll_past" ? "list" : "detail"
      );
    }
  }

  function setEventPending(operationKey: string, pending: boolean) {
    const next = new Set(eventPendingRef.current);
    if (pending) next.add(operationKey);
    else next.delete(operationKey);
    eventPendingRef.current = next;
    setEventPendingKeys(next);
  }

  useScrollPastSeen({
    enabled: interactionReady,
    rootRef: listPaneRef,
    rows: scrollRows,
    onSeen: (cluster) => {
      markSummarySeen(
        cluster,
        "scroll_past",
        isInteractionSurfacePresented(listPaneRef.current)
      );
    }
  });

  function markOriginalOpened(
    item: Pick<Item, "source_id"> & Partial<Pick<Item, "id" | "url">>,
    trigger: OriginalOpenedTrigger,
    evidenceVersionUid?: string,
    fallbackToFirstSynthesisEvidence = false
  ): void {
    if (!selectedCluster) return;
    const selection = originalOpenedSelectionFor(
      item,
      evidenceVersionUid,
      fallbackToFirstSynthesisEvidence
    );
    if (!selection) {
      recordMissingOriginalEvidence(item, evidenceVersionUid);
      return;
    }
    markOriginalOpenedSelection(selection, trigger);
  }

  function originalOpenedSelectionFor(
    item: Pick<Item, "source_id"> & Partial<Pick<Item, "id" | "url">>,
    evidenceVersionUid?: string,
    fallbackToFirstSynthesisEvidence = false
  ): OriginalOpenedSelection | null {
    if (!selectedCluster) return null;
    return originalOpenedSelectionForView({
      event_uid: selectedCluster.event_uid,
      synthesis: selectedCluster.synthesis,
      current_revision_uid: selectedCluster.current_revision_uid,
      mode: sourceMode ? "source" : "synthesis",
      source_view_evidence: selectedCluster.source_view_evidence,
      source: {
        source_id: item.source_id,
        item_id: item.id,
        evidence_version_uid: evidenceVersionUid,
        url: item.url
      },
      fallback_to_first_synthesis_evidence: fallbackToFirstSynthesisEvidence
    });
  }

  function markOriginalOpenedSelection(
    selection: OriginalOpenedSelection,
    trigger: OriginalOpenedTrigger
  ): void {
    if (!selectedCluster || !selection.target.evidence) return;
    const intent = originalOpenedIntent(
      trigger,
      selection.target.evidence.source_id
    );
    updateEventReadState(selectedCluster, intent.value, selection.target);
  }

  function recordMissingOriginalEvidence(
    item: Pick<Item, "source_id">,
    evidenceVersionUid?: string
  ): void {
    if (!selectedCluster) return;
    const operation: EventReadOperation = {
      clusterId: selectedCluster.id,
      operationId: createOperationId(),
      requestedStatus: "original_opened",
      target: {
        event_uid: selectedCluster.event_uid,
        observed_revision_uid: renderedTarget.observed_revision_uid,
        evidence: evidenceVersionUid
          ? {
              source_id: item.source_id,
              evidence_version_uid: evidenceVersionUid
            }
          : undefined
      },
      surface: "detail"
    };
    setEventReadErrors((current) =>
      recordEventReadFailure(
        current,
        operation,
        operation,
        "原文已打开，但看过记录暂未保存。"
      )
    );
  }

  function toggleReadStatus() {
    if (!selectedCluster) return;
    const nextStatus = readStatusToggleIntent(selectedCluster.read_status);
    setAllowSummarySeen((current) =>
      explicitReadStatusPresentationAllowed(current, nextStatus)
    );
    updateEventReadState(
      selectedCluster,
      nextStatus,
      renderedTarget
    );
  }

  function toggleStar() {
    if (!selectedCluster) return;
    updateEventState("starred", !selectedCluster.starred, "star");
  }

  function toggleReadLater() {
    if (!selectedCluster) return;
    updateEventState("read_later", !selectedCluster.read_later, "read-later");
  }

  function openOriginal() {
    const item = selectedSourceItem ?? sourceItems[0];
    if (!item?.url) return;
    const selection = originalOpenedSelectionFor(item, undefined, true);
    if (!selection) {
      recordMissingOriginalEvidence(item);
    } else {
      markOriginalOpenedSelection(selection, "shortcut");
    }
    window.open(selection?.url ?? item.url, "_blank", "noopener,noreferrer");
  }

  function openCitation(citation: SynthesisCitation) {
    const target = citationTarget(
      citation,
      sourceItems,
      selectedCluster?.source_view_evidence ?? []
    );
    if (target.kind === "external") {
      const selection = originalOpenedSelectionFor(
        { source_id: target.sourceId },
        citation.evidence_version_uid
      );
      if (!selection) {
        recordMissingOriginalEvidence(
          { source_id: target.sourceId },
          citation.evidence_version_uid
        );
      } else {
        markOriginalOpenedSelection(selection, "source");
      }
      window.open(selection?.url ?? target.url, "_blank", "noopener,noreferrer");
      return;
    }
    const item = sourceItems.find((candidate) => candidate.id === target.itemId);
    if (!item) return;
    setDetailMode("source");
    setSelectedSourceItemId(item.id === sourceItems[0]?.id ? null : item.id);
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => {
        const row = document.getElementById(`event-source-${item.id}`);
        row?.scrollIntoView({ block: "center", behavior: "smooth" });
        row?.focus({ preventScroll: true });
      });
    });
  }

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if (event.metaKey || event.ctrlKey || event.altKey || isEditableTarget(event.target)) return;
      if (
        event.composedPath().some((target) => target instanceof HTMLDialogElement) ||
        document.querySelector('dialog[open], [role="dialog"][aria-modal="true"]')
      ) return;
      const key = event.key === "Escape" ? "escape" : event.key.toLowerCase();
      if (!["j", "k", "o", "m", "s", "l", "/", "escape"].includes(key)) return;
      event.preventDefault();
      if (key === "/") {
        window.dispatchEvent(new Event("reader:focus-search"));
        return;
      }
      if (key === "escape") {
        if (document.querySelector(".search-box.expanded")) {
          window.dispatchEvent(new Event("reader:collapse-search"));
          return;
        }
        const params = new URLSearchParams(window.location.search);
        if (params.has("assistant") || params.has("assistant_ask")) {
          params.delete("assistant");
          params.delete("assistant_ask");
          router.push(`/?${params.toString()}`);
          return;
        }
        if (document.querySelector(".app-shell.mobile-detail")) showList();
        return;
      }
      if (key === "j") {
        if (nextCluster) openCluster(nextCluster, nextClusterHref(nextCluster));
        return;
      }
      if (key === "k") {
        const previousCluster = selectedIndex > 0 ? listRows[selectedIndex - 1] : null;
        if (previousCluster) openCluster(previousCluster, nextClusterHref(previousCluster));
        return;
      }
      if (key === "o") openOriginal();
      if (key === "m") toggleReadStatus();
      if (key === "s") toggleStar();
      if (key === "l") toggleReadLater();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [listRows, nextCluster, router, selectedIndex, selectedCluster, selectedSourceItem, sourceItems]);

  const selectedIsSeen = selectedCluster ? isSeenStatus(selectedCluster.read_status) : false;
  const readToggleLabel = selectedIsSeen ? "标记未读" : "标记看过";
  const toolbarActions = selectedCluster
    ? ([
        { id: "read-toggle", label: readToggleLabel, node: <StateButton active={selectedIsSeen} disabled={eventPendingKeys.has(`${selectedCluster.id}:read-status`)} label={readToggleLabel} onClick={toggleReadStatus} /> },
        { id: "read-later", label: "稍后读", node: <StateButton active={selectedCluster.read_later} disabled={eventPendingKeys.has(`${selectedCluster.id}:read-later`)} label="稍后阅读" onClick={toggleReadLater} /> },
        { id: "star", label: "星标", node: <StateButton disabled={eventPendingKeys.has(`${selectedCluster.id}:star`)} icon object={selectedCluster} onClick={toggleStar} /> },
        selectedCluster.event_uid &&
        selectedCluster.synthesis &&
        synthesisRequestAvailable(selectedCluster.synthesis)
          ? {
              id: "summary",
              label: selectedCluster.synthesis.status === "missing" ? "AI 合成" : "更新合成稿",
              node: (
                <form action="/actions/synthesize-cluster" method="post">
                  <input type="hidden" name="event_uid" value={selectedCluster.event_uid} />
                  <input type="hidden" name="redirect" value={`/?${queryString({ ...clientScope, cluster_id: selectedCluster.id })}`} />
                  <button
                    className="icon"
                    type="submit"
                    title={selectedCluster.synthesis.status === "missing" ? "AI 合成" : "更新合成稿"}
                    aria-label={selectedCluster.synthesis.status === "missing" ? "AI 合成" : "更新合成稿"}
                  >
                    <Sparkles size={17} />
                  </button>
                </form>
              )
            }
          : null,
        selectedCluster.event_uid && selectedCluster.current_revision_uid
          ? {
              id: "uninterested",
              label: "减少此类",
              node: (
                <ReduceSimilarButton
                  compact
                  key={`uninterested-${selectedCluster.id}`}
                  initialFeedback={selectedCluster.uninterested ? {
                    reason: selectedCluster.uninterested_reason as UninterestedReason | null,
                    note: selectedCluster.uninterested_note
                  } : undefined}
                  target={{
                    target_type: "event",
                    event_uid: selectedCluster.event_uid,
                    observed_revision_uid: selectedCluster.current_revision_uid
                  }}
                  onHidden={() => hideUninterestedCluster(selectedCluster)}
                  onRestored={() => restoreUninterestedCluster(selectedCluster)}
                />
              )
            }
          : null,
        { id: "bionic", label: "Bionic", node: <StateButton active={bionic} label="Bionic" onClick={() => setBionic((current) => !current)} /> },
        { id: "speech", label: "朗读", node: <SpeechButton text={selectedSpeechText} /> },
        { id: "share", label: "分享", node: <ShareButton sourceUrl={selectedSourceItem?.url || sourceItems[0]?.url || ""} title={selectedSourceItem?.title_translation || selectedSourceItem?.title || clusterTitle(selectedCluster)} /> },
        { id: "copy-link", label: "复制链接", node: <CopyLinkButton sourceUrl={selectedSourceItem?.url || sourceItems[0]?.url || ""} /> },
        {
          id: "copy-markdown",
          label: "复制 Markdown 引用",
          node: (
            <CopyMarkdownButton
              publishedAt={markdownSourceItem?.published_at ?? selectedCluster.first_seen_at}
              sourceName={displaySourceName(markdownSourceItem?.source_name ?? "")}
              summary={selectedCluster.generated_summary || markdownSourceItem?.summary || markdownSourceItem?.content_text || ""}
              title={markdownSourceItem?.title || clusterTitle(selectedCluster)}
              url={markdownSourceItem?.url || ""}
            />
          )
        },
        { id: "print", label: "导出 PDF", node: <PrintButton /> }
      ].filter(Boolean) as ToolbarAction[])
    : [];

  return (
    <>
      <section className="pane list-pane" data-ready={interactionReady ? "1" : "0"} ref={listPaneRef}>
        <ListPaneResizer />
        <div className="pane-header">
          <div className="list-header">
            <a className="mobile-back mobile-list-back" href={listBackHref} aria-label="返回订阅">
              ←
            </a>
            <div className="list-status" aria-live="polite">
              <PullRefresh {...pullRefresh} />
              {pullRefresh.status === "error" ? null : (
                <>
                  <span className="stream-label">完整流</span>
                  <span>{listPending ? "正在加载…" : filterLabel(activeFilter)}</span>
                  <strong>{formatCount(activeFilterCount)}</strong>
                </>
              )}
            </div>
            <div className="list-tools">
              <a className="filtered-list-link" href={uninterestedListHref}>不感兴趣</a>
              <a className="filtered-list-link" href={filteredListHref}>已过滤</a>
              <BulkReadForm scope={clientScope} />
            </div>
            <SearchBox
              onNavigate={({ href, query: nextQuery }) => loadList(activeFilter, nextQuery, href)}
              pending={listPending}
              placeholder="搜索事件、正文、来源"
              query={activeQuery}
              scope={clientScope}
            />
            {mobileNavigation}
          </div>
        </div>
        {reportReminder}
        {serverListError || clientListError ? <p className="error-line">{clientListError || serverListError}</p> : null}
        {listEventErrors.map((error) => (
          <p className="error-line" role="alert" key={`${error.clusterId}:${error.operationId}`}>
            {error.message}
          </p>
        ))}
        <ClusterList
          apiUrl={apiUrl}
          initialPageCount={listPageCount}
          offset={listOffset}
          pageSize={pageSize}
          rowOverrides={rowOverrides}
          rows={listSeedRows}
          scope={clientScope}
          selectedId={selectedCluster?.id ?? selectedId}
          onSelect={handleSelectCluster}
          onRowsChange={setListRows}
        />
        <StateFilterBar
          currentFilter={activeFilter}
          onNavigate={({ filter, href }) => {
            if (filter === "sources") router.push(href);
            else loadList(filter, activeQuery, href);
          }}
          pending={listPending}
          scope={clientScope}
        />
      </section>

      <article className="pane detail" ref={detailPaneRef} onScroll={onDetailScroll}>
        <DetailScrollProgress progress={detailScrollProgress} />
        {selectedCluster && !selectedDetailReady ? (
          <div className="detail-body detail-loading" aria-busy={detailLoadError ? undefined : "true"}>
            <a className="mobile-back mobile-toolbar-back" href={`/?${queryString({ ...clientScope, pane: "list" })}`} aria-label="返回列表" onClick={backToList}>
              ←
            </a>
            <h2><TranslatedTitle text={clusterTitle(selectedCluster)} initialTranslation={clusterTitleTranslation(selectedCluster)} /></h2>
            {detailLoadError ? (
              <p className="error-line" role="alert">
                {detailLoadError}{" "}
                <button type="button" onClick={() => setDetailRetry((current) => current + 1)}>重试</button>
              </p>
            ) : (
              <>
                <p className="list-status" role="status">正在加载全文…</p>
                <div className="detail-loading-tabs" aria-hidden="true"><span /><span /></div>
                <div className="detail-loading-copy" aria-hidden="true"><span /><span /><span /><span /></div>
              </>
            )}
          </div>
        ) : selectedCluster ? (
          <div
            className="detail-body"
            data-event-read-mode={sourceMode ? "source" : "synthesis"}
            data-observed-revision-uid={renderedRevisionUid ?? undefined}
            data-source-view-revision-uid={
              selectedCluster.synthesis?.source_view_revision_uid ?? undefined
            }
            data-has-material-update={selectedCluster.has_material_update ? "true" : "false"}
            data-material-update-revision-uid={selectedCluster.material_update_revision_uid ?? undefined}
          >
            <SummarySeenMarker
              id={selectedCluster.id}
              attemptKey={[
                selectedCluster.id,
                renderedRevisionUid ?? "missing",
                detailPresentationEpoch
              ].join(":")}
              readStatus={selectedCluster.read_status}
              eligible={summarySeenEligible(
                selectedCluster.read_status,
                selectedCluster.current_revision_differs_from_seen &&
                  renderedRevisionUid !== selectedCluster.seen_revision_uid
              ) && hasPresentedEvidence}
              skip={skipSeen || !allowSummarySeen}
              deferMs={1800}
              persist={false}
              onSeen={() =>
                markSummarySeen(
                  selectedCluster,
                  "detail_dwell",
                  allowSummarySeen &&
                    isInteractionSurfacePresented(detailPaneRef.current),
                  renderedTarget
                )
              }
            />
            {serverDetailError || clientDetailError ? (
              <p className="error-line" role="alert">
                {clientDetailError || serverDetailError}{" "}
                {clientDetailError ? <button type="button" onClick={() => setDetailRetry((current) => current + 1)}>重试</button> : null}
              </p>
            ) : null}
            {detailEventErrors.map((error) => (
              <p className="error-line" role="alert" key={error.operationId}>
                {error.message}
              </p>
            ))}
            {Object.entries(eventStateErrors).map(([field, message]) => message ? (
              <p className="error-line" role="alert" key={field}>{message}</p>
            ) : null)}
            <CustomToolbar
              actions={toolbarActions}
              leading={
                <a className="mobile-back mobile-toolbar-back" href={`/?${queryString({ ...clientScope, pane: "list" })}`} aria-label="返回列表" onClick={backToList}>
                  ←
                </a>
              }
              storageKey="reader.clusterToolbar.20260705.4"
            />
            <a className="floating-ai-button" data-assistant-trigger="cluster" title="AI Assistant" aria-label="AI Assistant" href={`/?${queryString({ ...clientScope, cluster_id: selectedCluster.id, assistant: "cluster" })}`}>
              <MessageCircle size={20} />
            </a>
            <DetailScrollTopButton visible={showDetailTopButton} onClick={scrollDetailToTop} />
            <a className="mobile-float-back" href={`/?${queryString({ ...clientScope, pane: "list" })}`} aria-label="返回列表" onClick={backToList}>
              ←
            </a>
            {selectedCluster.has_material_update ? (
              <p className="material-update-notice" role="status">
                看过后有更新
              </p>
            ) : null}
            <h2>
              {sourceMode && selectedSourceItem?.url ? (
                <a className="detail-title-link" href={selectedSourceItem.url} target="_blank" rel="noreferrer" onClick={() => markOriginalOpened(selectedSourceItem, "title")} onAuxClick={(event) => { if (event.button === 1) markOriginalOpened(selectedSourceItem, "title"); }}>
                  <TranslatedTitle text={detailTitle} initialTranslation={detailTitleTranslation} />
                </a>
              ) : (
                <TranslatedTitle text={detailTitle} initialTranslation={detailTitleTranslation} />
              )}
            </h2>
            {sourceMode && selectedSourceItem ? (
              <p className="item-meta item-meta-source">
                <Favicon url={sourceIconUrl(selectedSourceItem)} label={displaySourceName(selectedSourceItem.source_name)} />
                <a
                  href={selectedSourceHref}
                  onClick={(event) => {
                    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button > 0) return;
                    if (dispatchReaderListNavigation(selectedSourceHref)) event.preventDefault();
                  }}
                >
                  {displaySourceName(selectedSourceItem.source_name)}
                </a>
                <span>·</span>
                <TimeText value={selectedSourceItem.published_at ?? selectedCluster.first_seen_at} />
                {selectedCluster.item_count > 1 ? (
                  <>
                    <span>·</span>
                    <span>{selectedCluster.item_count} 条来源</span>
                  </>
                ) : null}
              </p>
            ) : selectedCluster.synthesis?.current ? (
              <p className="item-meta">
                <span>{selectedCluster.synthesis.current.source_count} 个独立来源</span>
                <span>·</span>
                <span>截至 {formatExactTime(selectedCluster.synthesis.current.snapshot_created_at)}</span>
              </p>
            ) : (
              <p className="item-meta">
                <TimeText value={selectedCluster.first_seen_at} />
              </p>
            )}
            {!sourceMode && selectedCluster.items?.some((item) => item.filtered) ? (
              <p className="filtered-item-notice" role="status">
                <strong>证据已过滤</strong>
                <span>这份既有合成稿可能仍包含后来被过滤的来源；来源时间线保留用于排查，下一次重新合成会排除它们。</span>
              </p>
            ) : null}
            {synthesisViewAvailable(selectedCluster.synthesis) ? (
              <div className="event-detail-tabs" role="tablist" aria-label="事件阅读模式">
                <button
                  type="button"
                  role="tab"
                  aria-selected={detailMode === "synthesis"}
                  className={detailMode === "synthesis" ? "active" : ""}
                  onClick={() => switchDetailMode("synthesis")}
                >
                  合成稿
                </button>
                <button
                  type="button"
                  role="tab"
                  aria-selected={detailMode === "source"}
                  className={detailMode === "source" ? "active" : ""}
                  onClick={() => switchDetailMode("source")}
                >
                  来源
                </button>
              </div>
            ) : null}
            {sourceMode ? (
              <>
                <div className="cluster-sources">
                  <h3 className="ui-section-label">{selectedCluster.item_count > 1 ? (selectedSourceItemId ? "来源原文" : "最初原文") : "原文"}</h3>
                  <TranslatedArticleContent
                    apiUrl={apiUrl}
                    sourceId={selectedSourceItem?.source_id}
                    bionic={bionic}
                    html={selectedSourceItem?.reading_html}
                    text={selectedOriginalText}
                    initialTranslation={selectedOriginalTranslation}
                    translationNeeded={selectedSourceItem?.reading_translation_needed}
                  />
                </div>
                <div className="cluster-sources">
                  <h3 className="ui-section-label">来源时间线</h3>
                  <div className="event-source-list">
                    {sourceItems.map((item, index) => (
                      <div
                        id={`event-source-${item.id}`}
                        key={item.id}
                        tabIndex={-1}
                        className={`event-source-row ${selectedSourceItem?.id === item.id ? "active" : ""}`}
                      >
                        <span className={`event-source-rail source-tone-${index % 6}`} aria-hidden="true" />
                        <span className="event-source-index">{index + 1}</span>
                        <div className="event-source-main">
                          <div className="event-source-title">
                            <button
                              className="event-source-title-button"
                              type="button"
                              onClick={() => {
                                setSelectedSourceItemId(item.id === sourceItems[0]?.id ? null : item.id);
                              }}
                            >
                              <TranslatedTitle text={item.title} initialTranslation={item.title_translation} />
                            </button>
                            {item.filtered ? <span className="badge filtered-badge" title={item.filter_rules.join("；")}>已过滤</span> : null}
                            {item.url ? (
                              <a className="event-source-open-original" href={item.url} target="_blank" rel="noreferrer" onClick={() => markOriginalOpened(item, "source")} onAuxClick={(event) => { if (event.button === 1) markOriginalOpened(item, "source"); }}>
                                原文
                              </a>
                            ) : null}
                          </div>
                          <div className="item-meta">
                            <Favicon url={sourceIconUrl(item)} label={displaySourceName(item.source_name)} /> {displaySourceName(item.source_name)}
                          </div>
                        </div>
                        <div className="event-source-time">
                          <TimeText value={item.published_at} />
                        </div>
                        <div className="event-source-uninterested">
                          <ReduceSimilarButton
                            compact
                            dismissIcon
                            target={{ target_type: "article", item_id: item.id }}
                            onHidden={() => hideUninterestedCluster(selectedCluster)}
                            onRestored={() => restoreUninterestedCluster(selectedCluster)}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
                <ClusterImageList cluster={selectedCluster} currentText={selectedOriginalText} />
              </>
            ) : (
              <SynthesisPanel
                bionic={bionic}
                cluster={selectedCluster}
                onCitation={openCitation}
                redirect={`/?${queryString({ ...clientScope, cluster_id: selectedCluster.id })}`}
              />
            )}
            {nextCluster ? (
              <button className="next-cluster-pull" type="button" onClick={switchToNextCluster}>
                <span>下一条</span>
                <strong>{clusterTitle(nextCluster)}</strong>
              </button>
            ) : null}
          </div>
        ) : (
          <div className="placeholder">
            <a className="mobile-back mobile-toolbar-back" href={`/?${queryString({ ...clientScope, pane: "list" })}`} aria-label="返回列表" onClick={backToList}>
              ←
            </a>
            <span>{serverDetailError || clientDetailError || "选择一个事件聚类后在这里查看引用来源。"}</span>
            {selectedId && (serverDetailError || clientDetailError) ? (
              <button type="button" onClick={() => setDetailRetry((current) => current + 1)}>重试</button>
            ) : null}
          </div>
        )}
      </article>
    </>
  );
}

function filterLabel(filter: string) {
  if (filter === "starred") return "收藏";
  if (filter === "read_later") return "稍后读";
  if (filter === "unread") return "未读";
  if (filter === "dismissed") return "已忽略";
  return "全部";
}

function formatCount(value: number | null) {
  if (value === null) return "";
  if (value >= 10000) return `${(value / 10000).toFixed(1).replace(/\.0$/, "")}万`;
  return String(value);
}

function isSeenStatus(status: string) {
  return status === "summary_seen" || status === "original_opened";
}

function readStateFrom(cluster: Cluster): ConfirmedReadState {
  return {
    read_status: cluster.read_status,
    seen_revision_uid: cluster.seen_revision_uid,
    current_revision_differs_from_seen:
      cluster.current_revision_differs_from_seen,
    has_material_update: cluster.has_material_update,
    material_update_revision_uid: cluster.material_update_revision_uid
  };
}

function optimisticReadPatch(
  cluster: Cluster,
  value: EventReadStatus,
  observedRevisionUid: string | null
): ConfirmedReadState {
  if (value === "unread") {
    return { ...readStateFrom(cluster), read_status: "unread" };
  }
  const clearsMaterialUpdate =
    observedRevisionUid === cluster.current_revision_uid ||
    observedRevisionUid === cluster.material_update_revision_uid;
  return {
    read_status:
      value === "summary_seen" && cluster.read_status === "original_opened"
        ? "original_opened"
        : value,
    seen_revision_uid: observedRevisionUid,
    current_revision_differs_from_seen:
      observedRevisionUid !== cluster.current_revision_uid,
    has_material_update:
      clearsMaterialUpdate ? false : cluster.has_material_update,
    material_update_revision_uid: clearsMaterialUpdate
      ? null
      : cluster.material_update_revision_uid
  };
}

function isEditableTarget(target: EventTarget | null) {
  if (!(target instanceof HTMLElement)) return false;
  return Boolean(target.closest("input, textarea, select, [contenteditable='true']"));
}

function clusterTitle(cluster: Cluster) {
  if (cluster.item_count > 1 && (cluster.generated_title || "").trim()) return cluster.generated_title;
  return cluster.title;
}

function clusterTitleTranslation(cluster: Cluster) {
  if (cluster.item_count > 1 && (cluster.generated_title || "").trim()) return cluster.generated_title_translation || "";
  const item = cluster.items?.[0];
  return item?.title_translation || "";
}

function synthesisDefaultView(cluster: Cluster | null | undefined): DetailMode {
  return cluster?.synthesis?.default_view ?? "source";
}

function SynthesisPanel({
  bionic,
  cluster,
  onCitation,
  redirect
}: {
  bionic: boolean;
  cluster: Cluster;
  onCitation: (citation: SynthesisCitation) => void;
  redirect: string;
}) {
  const synthesis = cluster.synthesis;
  if (!synthesis) return null;
  const blockedMessage = synthesis.task?.admission_reason
    ? `${synthesis.task.admission_reason} 请在设置 → 任务分配调整后重试。`
    : `${synthesis.task?.privacy_reason || "当前来源不允许外部生成。"} 请在设置 → 订阅管理调整后重试。`;
  if (!synthesis.current) {
    const message = synthesisTaskMessage(synthesis.task_status, "missing", blockedMessage);
    return (
      <section className="synthesis-empty" aria-live="polite">
        <Sparkles size={20} />
        <div>
          <h3>可核验合成稿</h3>
          <p>{message}</p>
        </div>
        {synthesisRequestAvailable(synthesis) && cluster.event_uid ? (
          <form action="/actions/synthesize-cluster" method="post">
            <input type="hidden" name="event_uid" value={cluster.event_uid} />
            <input type="hidden" name="redirect" value={redirect} />
            <button type="submit">生成合成稿</button>
          </form>
        ) : null}
      </section>
    );
  }
  return (
    <section className="synthesis-document" aria-label="可核验合成稿">
      <div className="synthesis-provenance">
        <strong>合成依据</strong>
        <span>截至 {formatExactTime(synthesis.current.snapshot_created_at)}</span>
        <span>{synthesis.current.source_count} 个独立来源</span>
        {synthesis.status === "unreviewed" ? (
          <>
            <strong className="synthesis-unreviewed">有未审新证据</strong>
            <span>
              {synthesisTaskMessage(synthesis.task_status, "unreviewed", blockedMessage)}
            </span>
            {synthesisRequestAvailable(synthesis) && cluster.event_uid ? (
              <form action="/actions/synthesize-cluster" method="post">
                <input type="hidden" name="event_uid" value={cluster.event_uid} />
                <input type="hidden" name="redirect" value={redirect} />
                <button type="submit">
                  {synthesis.task_status === "failed" ? "重试更新" : "更新合成稿"}
                </button>
              </form>
            ) : null}
          </>
        ) : null}
        {synthesis.status === "stale" ? (
          <>
            <strong className="synthesis-unreviewed">有新证据尚未纳入</strong>
            <span>
              {synthesisTaskMessage(synthesis.task_status, "stale", blockedMessage)}
            </span>
            {synthesisRequestAvailable(synthesis) && cluster.event_uid ? (
              <form action="/actions/synthesize-cluster" method="post">
                <input type="hidden" name="event_uid" value={cluster.event_uid} />
                <input type="hidden" name="redirect" value={redirect} />
                <button type="submit">
                  {synthesis.task_status === "failed" ? "重试更新" : "更新合成稿"}
                </button>
              </form>
            ) : null}
          </>
        ) : null}
        {synthesis.status === "current" && synthesis.new_source_count > 0 ? (
          <>
            <strong className="synthesis-reviewed">
              新增 {synthesis.new_source_count} 个来源
            </strong>
            <span>新增证据已审定为普通佐证；当前合成稿和原引用保持不变，可切换到来源查看。</span>
          </>
        ) : null}
        {synthesis.status === "missing" ? (
          <span>当前来源已不再等同于这份合成稿的证据范围；请切换到来源查看。</span>
        ) : null}
      </div>
      {synthesis.current.blocks.map((block) => (
        <article className={`synthesis-block kind-${block.kind}`} key={block.block_uid}>
          <header>
            <span>{synthesisBlockLabel(block.kind)}</span>
            {block.attribution ? <strong>{block.attribution}</strong> : null}
          </header>
          <ArticleContent bionic={bionic} text={block.body} />
          <div className="synthesis-citations" aria-label="真实来源">
            {block.citations.map((citation, index) => (
              <button
                key={`${block.block_uid}:${citation.evidence_version_uid}`}
                type="button"
                onClick={() => onCitation(citation)}
                title={`查看来源：${citation.source.name}`}
              >
                <span>[{index + 1}]</span>
                <strong>{citation.source.name}</strong>
                <span>{citation.title || citation.url}</span>
                <TimeText value={citation.published_at} />
              </button>
            ))}
          </div>
        </article>
      ))}
    </section>
  );
}

function synthesisBlockLabel(kind: SynthesisBlock["kind"]) {
  if (kind === "summary") return "摘要";
  if (kind === "fact") return "事实";
  if (kind === "viewpoint") return "观点";
  if (kind === "disagreement") return "分歧";
  return "不确定性";
}

function selectedSourceItemFor(items: Item[], itemId: number | null) {
  if (!items.length) return null;
  return items.find((item) => item.id === itemId) ?? items[0];
}

function sourceIconUrl(item: Item) {
  return item.source_site_url || item.url;
}

function itemOriginalText(item: Item | null) {
  return item?.content_text || "正文暂不可用";
}

function itemOriginalTranslation(item: Item | null) {
  return item?.content_translation || "";
}

function ClusterImageList({ cluster, currentText }: { cluster: Cluster; currentText: string }) {
  const images = clusterImages(cluster, currentText);
  if (!images.length) return null;
  return (
    <div className="cluster-sources">
      <h3>来源图片</h3>
      <div className="cluster-image-grid">
        {images.map((image) => (
          <a key={canonicalImageKey(image.url)} className="cluster-image-card" href={image.url} target="_blank" rel="noreferrer">
            <img src={rssImageSrc(image.url)} alt={image.alt || "来源图片"} loading="lazy" decoding="async" />
            <span>{image.sources.join(" / ")}</span>
          </a>
        ))}
      </div>
    </div>
  );
}

function clusterImages(cluster: Cluster, currentText: string) {
  const images = new Map<string, ClusterImage>();
  const currentImageKeys = new Set(markdownImages(currentText).map((image) => canonicalImageKey(image.url)));
  for (const item of cluster.items ?? []) {
    const itemSeen = new Set<string>();
    for (const image of markdownImages(item.content_text || "")) {
      const key = canonicalImageKey(image.url);
      if (currentImageKeys.has(key)) continue;
      if (itemSeen.has(key)) continue;
      itemSeen.add(key);
      const sourceLabel = [displaySourceName(item.source_name), formatExactTime(item.published_at)].filter(Boolean).join(" · ");
      const existing = images.get(key);
      if (existing) {
        if (!existing.sources.includes(sourceLabel)) existing.sources.push(sourceLabel);
        continue;
      }
      images.set(key, { ...image, sources: [sourceLabel] });
    }
  }
  return [...images.values()];
}

function markdownImages(text: string) {
  const images: { url: string; alt: string }[] = [];
  const pattern = /!\[([^\]]*)]\(([^)\n]+)\)/g;
  for (const match of text.matchAll(pattern)) {
    const url = match[2].trim().replace(/^<(.+)>$/, "$1");
    if (!/^https?:\/\//.test(url)) continue;
    images.push({ alt: match[1].trim(), url });
  }
  return images;
}

function canonicalImageKey(raw: string) {
  try {
    const url = new URL(raw);
    const hostFromProxyPath = url.hostname.match(/^i\d\.wp\.com$/) ? url.pathname.split("/").filter(Boolean)[0] : "";
    const imageHost = hostFromProxyPath || url.hostname;
    const imagePath = hostFromProxyPath ? `/${url.pathname.split("/").filter(Boolean).slice(1).join("/")}` : url.pathname;
    const keepParams = new URLSearchParams(url.search);
    for (const key of ["fit", "h", "height", "quality", "resize", "ssl", "strip", "w", "width"]) {
      keepParams.delete(key);
    }
    const query = keepParams.toString();
    return `${url.protocol}//${imageHost}${imagePath}${query ? `?${query}` : ""}`;
  } catch {
    return raw.trim();
  }
}

function insertAt<T extends { id: number }>(rows: T[], index: number, row: T) {
  if (rows.some((item) => item.id === row.id)) return rows;
  const position = index < 0 ? rows.length : Math.min(index, rows.length);
  return [...rows.slice(0, position), row, ...rows.slice(position)];
}
