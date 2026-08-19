import { queryString } from "./url-state";

type ListScope = Record<string, string | number | null | undefined>;

export async function loadClusterList<T>(
  apiUrl: string,
  scope: ListScope,
  pageSize: number,
  signal?: AbortSignal
) {
  const base = apiUrl.replace(/\/$/, "");
  const filters = listFilterQuery(scope.filter);
  const shared = {
    ...filters,
    folder_id: scope.folder_id,
    source_id: scope.source_id,
    q: scope.q
  };
  const response = await fetch(`${base}/clusters?${queryString({ ...shared, limit: pageSize, offset: 0, order: scope.order })}`, {
    cache: "no-store",
    signal
  });
  if (!response.ok) throw new Error("列表加载失败");
  return response.json() as Promise<T[]>;
}

export async function loadClusterCount(
  apiUrl: string,
  scope: ListScope,
  signal?: AbortSignal
) {
  const response = await fetch(`${apiUrl.replace(/\/$/, "")}/clusters/count?${queryString({
    ...listFilterQuery(scope.filter),
    folder_id: scope.folder_id,
    source_id: scope.source_id,
    q: scope.q
  })}`, { cache: "no-store", signal });
  if (!response.ok) throw new Error("数量加载失败");
  const result = await response.json() as { count: number };
  return result.count;
}

export async function loadBrowseList<T>(
  apiUrl: string,
  scope: ListScope,
  pageSize: number,
  signal?: AbortSignal
) {
  const response = await fetch(`${apiUrl.replace(/\/$/, "")}/items?${queryString({
    ...listFilterQuery(scope.filter),
    media_type: scope.media,
    folder_id: scope.folder_id,
    source_id: scope.source_id,
    q: scope.q,
    filtered_only: scope.filtered === "1" ? "true" : undefined,
    limit: pageSize,
    offset: 0,
    include_content: false
  })}`, { cache: "no-store", signal });
  if (!response.ok) throw new Error("列表加载失败");
  return response.json() as Promise<T[]>;
}

export function listFilterQuery(filter: ListScope[string]) {
  const value = normalizeListFilter(typeof filter === "string" ? filter : "");
  return {
    read_status: value === "unread" || value === "dismissed" ? value : undefined,
    read_later: value === "read_later" ? "true" : undefined,
    starred: value === "starred" ? "true" : undefined
  };
}

export function normalizeListFilter(filter: string | null) {
  if (filter === "all") return "";
  if (filter && ["unread", "dismissed", "read_later", "starred"].includes(filter)) return filter;
  return "unread";
}
