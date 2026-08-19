"use client";

import { memo, MouseEvent, useCallback, useEffect, useRef, useState } from "react";

import { clusterItemsByTime } from "./cluster-items";
import type { ClusterEventIdentity } from "./cluster-event-identity";
import {
  synthesisStatusLabel,
  type ClusterSynthesisFields
} from "./event-synthesis";
import ClusterRowLink from "./cluster-row-link";
import { listFilterQuery } from "./list-api";
import Favicon from "./favicon";
import { previewText } from "./text-preview";
import { displaySourceName } from "./source-name";
import { TimeText } from "./time-text";
import { queryString } from "./url-state";

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
  first_seen_at: string | null;
  last_seen_at: string | null;
  item_count: number;
  read_status: string;
  read_later: boolean;
  starred: boolean;
  items?: Item[];
};
type Scope = Record<string, string | number | null | undefined>;
type RowOverride = Partial<
  Pick<
    Cluster,
    | "read_status"
    | "read_later"
    | "starred"
    | "has_material_update"
    | "material_update_revision_uid"
  >
>;

function ClusterList({
  apiUrl,
  initialPageCount,
  offset,
  pageSize,
  rows: initialRows,
  rowOverrides = {},
  scope,
  selectedId,
  onSelect,
  onRowsChange
}: {
  apiUrl: string;
  initialPageCount: number;
  offset: number;
  pageSize: number;
  rows: Cluster[];
  rowOverrides?: Record<number, RowOverride>;
  scope: Scope;
  selectedId: number | null;
  onSelect?: (event: MouseEvent<HTMLElement>, cluster: Cluster, href: string) => void;
  onRowsChange?: (rows: Cluster[]) => void;
}) {
  const [rows, setRows] = useState(initialRows);
  const [nextCursorId, setNextCursorId] = useState(initialRows.at(-1)?.id ?? null);
  const [hasMore, setHasMore] = useState(initialPageCount >= pageSize);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadError, setLoadError] = useState("");
  const loadMoreRef = useRef<HTMLDivElement | null>(null);
  const loadingMoreRef = useRef(false);
  const rowsRef = useRef(initialRows);
  const pageRequestId = useRef(0);
  const pageAbortController = useRef<AbortController | null>(null);

  useEffect(() => {
    pageAbortController.current?.abort();
    pageRequestId.current += 1;
    rowsRef.current = initialRows;
    setRows(initialRows);
    setNextCursorId(initialRows.at(-1)?.id ?? null);
    setHasMore(initialPageCount >= pageSize);
    setLoadingMore(false);
    setLoadError("");
    loadingMoreRef.current = false;
  }, [initialRows, initialPageCount, offset, pageSize]);

  useEffect(() => () => pageAbortController.current?.abort(), []);

  const loadMore = useCallback(() => {
    if (loadingMoreRef.current || !hasMore) return;
    loadingMoreRef.current = true;
    setLoadingMore(true);
    setLoadError("");
    const controller = new AbortController();
    pageAbortController.current?.abort();
    pageAbortController.current = controller;
    const requestId = ++pageRequestId.current;
    fetch(`${apiUrl.replace(/\/$/, "")}/clusters?${queryString({ ...listFilterQuery(scope.filter), folder_id: scope.folder_id, source_id: scope.source_id, q: scope.q, limit: pageSize, cursor_id: nextCursorId, order: scope.order })}`, { cache: "no-store", signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error("更多聚类加载失败");
        return response.json();
      })
      .then((nextRows: Cluster[]) => {
        if (requestId !== pageRequestId.current) return;
        const existing = new Set(rowsRef.current.map((cluster) => cluster.id));
        const mergedRows = [
          ...rowsRef.current,
          ...nextRows.filter((cluster) => !existing.has(cluster.id))
        ];
        rowsRef.current = mergedRows;
        setRows(mergedRows);
        onRowsChange?.(mergedRows);
        setNextCursorId(nextRows.at(-1)?.id ?? null);
        setHasMore(nextRows.length >= pageSize);
      })
      .catch((error: unknown) => {
        if (!(error instanceof DOMException && error.name === "AbortError")) setLoadError("更多聚类加载失败");
      })
      .finally(() => {
        if (requestId !== pageRequestId.current) return;
        loadingMoreRef.current = false;
        setLoadingMore(false);
      });
  }, [apiUrl, hasMore, nextCursorId, onRowsChange, pageSize, scope.filter, scope.folder_id, scope.order, scope.q, scope.source_id]);

  useEffect(() => {
    const root = document.querySelector(".list-pane");
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

  if (!rows.length) return <div className="placeholder">暂无事件聚类。抓取 RSS 后会生成最小聚类结果。</div>;

  return (
    <>
      {rows.map((row, index) => (
        <ClusterListRow
          active={selectedId === row.id}
          apiUrl={apiUrl}
          eager={index < 12}
          key={row.id}
          onSelect={onSelect}
          override={rowOverrides[row.id]}
          row={row}
          scope={scope}
        />
      ))}
      <div className="list-footer" ref={loadMoreRef}>
        {loadingMore ? "正在加载..." : loadError || (!hasMore ? "没有更多聚类" : "")}
      </div>
    </>
  );
}

const ClusterListRow = memo(function ClusterListRow({
  active,
  apiUrl,
  eager,
  onSelect,
  override,
  row: baseRow,
  scope
}: {
  active: boolean;
  apiUrl: string;
  eager: boolean;
  onSelect?: (event: MouseEvent<HTMLElement>, cluster: Cluster, href: string) => void;
  override?: RowOverride;
  row: Cluster;
  scope: Scope;
}) {
  const row = override ? { ...baseRow, ...override } : baseRow;
  const orderedItems = clusterItemsByTime(row.items);
  const href = `/?${queryString({ ...scope, cluster_id: row.id })}`;
  return (
    <div
      className="cluster-list-entry"
      data-scroll-seen-id={row.id}
      data-has-material-update={row.has_material_update ? "true" : "false"}
      data-material-update-revision-uid={row.material_update_revision_uid ?? undefined}
    >
      <ClusterRowLink
        active={active}
        apiUrl={apiUrl}
        href={href}
        id={row.id}
        meta={clusterSourceMeta(row, orderedItems, eager)}
        onSelect={onSelect ? (event) => onSelect(event, row, href) : undefined}
        readLater={row.read_later}
        readStatus={row.read_status}
        starred={row.starred}
        sources={orderedItems}
        summary={clusterSummary(row, orderedItems)}
        thumbnailMode="auto"
        thumbnailUrl={thumbnailUrl(orderedItems)}
        title={clusterTitle(row, orderedItems)}
        titleTranslation={clusterTitleTranslation(row, orderedItems)}
      />
    </div>
  );
});

export default memo(ClusterList);

function thumbnailUrl(items: Item[]) {
  return items.find((item) => item.image_url)?.image_url || "";
}

function clusterTitle(cluster: Cluster, items = clusterItemsByTime(cluster.items)) {
  return items[0]?.title || cluster.title || cluster.generated_title;
}

function clusterTitleTranslation(cluster: Cluster, items = clusterItemsByTime(cluster.items)) {
  return items[0]?.title_translation || cluster.generated_title_translation || "";
}

function clusterSummary(cluster: Cluster, items = clusterItemsByTime(cluster.items)) {
  return clusterSourceSummary(cluster, items) || cluster.generated_summary;
}

function clusterSourceMeta(cluster: Cluster, items = clusterItemsByTime(cluster.items), eager = false) {
  const first = items[0];
  const last = items[items.length - 1];
  const firstTime = first?.published_at ?? cluster.first_seen_at;
  const lastTime = last?.published_at ?? cluster.last_seen_at;
  const firstSourceName = displaySourceName(first?.source_name ?? "");
  const statusLabel = synthesisStatusLabel(
    cluster.synthesis_freshness?.status,
    cluster.synthesis_freshness?.new_source_count,
    cluster.has_material_update
  );
  const status = statusLabel ? (
    <>
      <span
        className={
          cluster.synthesis_freshness?.status === "unreviewed" ||
          cluster.synthesis_freshness?.status === "stale"
            ? "synthesis-status-unreviewed"
            : "synthesis-status-reviewed"
        }
      >
        {statusLabel}
      </span>
      {" · "}
    </>
  ) : null;
  if (cluster.item_count <= 1) {
    return (
      <>
        {status}
        {first ? <Favicon eager={eager} url={sourceIconUrl(first)} label={firstSourceName} /> : null}
        {firstSourceName ? `${firstSourceName} · ` : ""}
        <TimeText value={firstTime} />
      </>
    );
  }
  return (
    <>
      {status}
      首发 {first ? <Favicon eager={eager} url={sourceIconUrl(first)} label={firstSourceName} /> : null}
      {firstSourceName ? `${firstSourceName} · ` : ""}
      <TimeText value={firstTime} />
      {lastTime && lastTime !== firstTime ? (
        <>
          {" · "}最新 <TimeText value={lastTime} />
        </>
      ) : null}
    </>
  );
}

function sourceIconUrl(item: Item) {
  return item.source_site_url || item.url;
}

function clusterSourceSummary(cluster: Cluster, items = clusterItemsByTime(cluster.items)) {
  const item = items[0];
  return previewText(item?.summary || item?.content_text || cluster.title);
}
