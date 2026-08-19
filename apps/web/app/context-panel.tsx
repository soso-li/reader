"use client";

import { ChevronLeft, Star } from "lucide-react";
import Link from "next/link";
import { type MouseEvent, type ReactNode, useEffect, useRef, useState } from "react";

import ClusterRowLink from "./cluster-row-link";
import Favicon from "./favicon";
import { displaySourceName } from "./source-name";
import FetchForm from "./fetch-form";
import ListPaneResizer from "./list-pane-resizer";
import { applyAllUnreadCountDelta, READER_UNREAD_COUNT_CHANGED_EVENT } from "./live-unread-count";
import { friendlyFetchError } from "./source-detail-dialog";
import { TimeText } from "./time-text";
import { previewText } from "./text-preview";
import { dispatchReaderListNavigation, parseReaderListNavigation, READER_LIST_NAVIGATION_COMMITTED_EVENT, type ReaderListNavigation } from "./reader-list-navigation";
import { queryString } from "./url-state";
import type { SourceMediaType } from "./source-media";

const REPORT_PERIODS = [
  ["day", "日报"],
  ["week", "周报"],
  ["month", "月报"]
] as const;

type View = "clusters" | "browse" | "topics" | "reports" | "settings";
type SettingsSection = "subscriptions" | "filters" | "ai" | "local_models" | "tasks" | "general" | "import_export" | "about";
type Folder = { id: number; name: string; media_type: SourceMediaType };
type Source = {
  id: number;
  folder_id: number | null;
  name: string;
  url: string;
  site_url: string;
  enabled: boolean;
  unread_count: number;
  folder_unread_count: number;
  all_unread_count: number;
  starred_count: number;
  last_error: string;
};
type TopicGroup = {
  id: number;
  name: string;
  query: string;
  description: string;
  cluster_count: number;
  read_status: string;
  read_later: boolean;
  starred: boolean;
};
type ReadingNavigate = (event: MouseEvent<HTMLAnchorElement>, href: string) => void;

export default function ContextPanel({
  apiUrl,
  browseMedia,
  currentView,
  folders,
  sources,
  selectedFolder,
  selectedSource,
  topics,
  selectedTopic,
  currentReportPeriod,
  reportDate,
  reportGenerationUnavailableReason,
  currentSettingsStatus,
  currentSettingsSection,
  developerMode,
  filteredOnly,
  lastUpdatedAt,
  navError,
  mobileNavigation,
  mobileStateFilters
}: {
  apiUrl: string;
  browseMedia: string;
  currentView: View;
  folders: Folder[];
  sources: Source[];
  selectedFolder: number | null;
  selectedSource: number | null;
  topics: TopicGroup[];
  selectedTopic: TopicGroup | undefined;
  currentReportPeriod: string;
  reportDate: string;
  reportGenerationUnavailableReason: string;
  currentSettingsStatus: string;
  currentSettingsSection: SettingsSection;
  developerMode: boolean;
  filteredOnly: boolean;
  lastUpdatedAt: string | null;
  navError: string;
  mobileNavigation?: ReactNode;
  mobileStateFilters?: ReactNode;
}) {
  const [activeFolder, setActiveFolder] = useState(selectedFolder);
  const [activeSource, setActiveSource] = useState(selectedSource);
  const [liveSources, setLiveSources] = useState(sources);
  const contextPanelRef = useRef<HTMLDetailsElement>(null);
  const unreadCountRefreshTimer = useRef<number | null>(null);
  const unreadCountAbortController = useRef<AbortController | null>(null);

  useEffect(() => {
    setActiveFolder(selectedFolder);
    setActiveSource(selectedSource);
  }, [selectedFolder, selectedSource]);

  useEffect(() => setLiveSources(sources), [sources]);

  useEffect(() => {
    if (currentView === "settings") revealCurrentSettingsLink(contextPanelRef.current);
  }, [currentSettingsSection, currentView]);

  useEffect(() => {
    if (currentView !== "clusters") return;
    const updateUnreadCount = (event: Event) => {
      const delta = (event as CustomEvent<number>).detail;
      if (!Number.isFinite(delta) || delta === 0) return;
      unreadCountAbortController.current?.abort();
      setLiveSources((current) => applyAllUnreadCountDelta(current, delta));
      if (unreadCountRefreshTimer.current !== null) {
        window.clearTimeout(unreadCountRefreshTimer.current);
      }
      unreadCountRefreshTimer.current = window.setTimeout(() => {
        unreadCountRefreshTimer.current = null;
        const controller = new AbortController();
        unreadCountAbortController.current?.abort();
        unreadCountAbortController.current = controller;
        void fetch(`${apiUrl.replace(/\/$/, "")}/sources/navigation`, {
          cache: "no-store",
          signal: controller.signal
        })
          .then((response) => {
            if (!response.ok) throw new Error("导航数据加载失败");
            return response.json() as Promise<Source[]>;
          })
          .then((nextSources) => {
            const nextById = new Map(nextSources.map((source) => [source.id, source]));
            setLiveSources((current) =>
              current.map((source) => nextById.get(source.id) ?? source)
            );
          })
          .catch(() => undefined)
          .finally(() => {
            if (unreadCountAbortController.current === controller) {
              unreadCountAbortController.current = null;
            }
          });
      }, 200);
    };
    window.addEventListener(READER_UNREAD_COUNT_CHANGED_EVENT, updateUnreadCount);
    return () => {
      window.removeEventListener(READER_UNREAD_COUNT_CHANGED_EVENT, updateUnreadCount);
      unreadCountAbortController.current?.abort();
      if (unreadCountRefreshTimer.current !== null) {
        window.clearTimeout(unreadCountRefreshTimer.current);
      }
    };
  }, [apiUrl, currentView]);

  useEffect(() => {
    const syncCommittedNavigation = (event: Event) => {
      const navigation = (event as CustomEvent<ReaderListNavigation>).detail;
      if (!navigation || navigation.view !== currentView) return;
      setActiveFolder(navigation.folderId);
      setActiveSource(navigation.sourceId);
    };
    window.addEventListener(READER_LIST_NAVIGATION_COMMITTED_EVENT, syncCommittedNavigation);
    return () => window.removeEventListener(READER_LIST_NAVIGATION_COMMITTED_EVENT, syncCommittedNavigation);
  }, [currentView]);

  const navigateReading: ReadingNavigate = (event, href) => {
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button > 0) return;
    const navigation = parseReaderListNavigation(href, window.location.href);
    if (!navigation || navigation.view !== currentView) return;
    event.preventDefault();
    dispatchReaderListNavigation(href);
  };

  return (
    <details ref={contextPanelRef} className="pane context-panel" open>
      <summary className="context-toggle" title="折叠二级栏" aria-label="折叠二级栏">
        <ChevronLeft size={16} />
      </summary>
      <ListPaneResizer mode="context" />
      <div className="context-panel-inner">
        <div className="pane-header">
          <div className="brand">
            <div className="brand-title">
              <h1>Reader</h1>
              <span className="brand-subtitle">
                最近刷新 <TimeText value={lastUpdatedAt} />
              </span>
            </div>
            <FetchForm compact />
          </div>
        </div>
        {mobileNavigation}
        {currentView === "clusters" ? <ClusterContext folders={folders} sources={liveSources} selectedFolder={activeFolder} selectedSource={activeSource} navError={navError} onNavigate={navigateReading} /> : null}
        {currentView === "browse" ? <BrowseContext filteredOnly={filteredOnly} media={browseMedia} folders={folders} sources={sources} selectedFolder={activeFolder} selectedSource={activeSource} navError={navError} onNavigate={navigateReading} /> : null}
        {currentView === "topics" ? <TopicContext topics={topics} selectedTopic={selectedTopic} /> : null}
        {currentView === "reports" ? <ReportContext currentPeriod={currentReportPeriod} reportDate={reportDate} unavailableReason={reportGenerationUnavailableReason} /> : null}
        {currentView === "settings" ? <SettingsContext currentSection={currentSettingsSection} currentStatus={currentSettingsStatus} developerMode={developerMode} /> : null}
        {mobileStateFilters ? <div className="context-mobile-state-filters">{mobileStateFilters}</div> : null}
      </div>
    </details>
  );
}

export function revealCurrentSettingsLink(panel: ParentNode | null) {
  panel?.querySelector<HTMLElement>(".context-links [aria-current]")?.scrollIntoView({ block: "nearest" });
}

function ClusterContext({ folders, sources, selectedFolder, selectedSource, navError, onNavigate }: { folders: Folder[]; sources: Source[]; selectedFolder: number | null; selectedSource: number | null; navError: string; onNavigate: ReadingNavigate }) {
  const eagerSourceIds = new Set(sources.slice(0, 12).map((source) => source.id));
  return (
    <div className="section reader-source-section">
      <div className="reader-source-list">
        <Link prefetch={false} aria-current={selectedFolder === null && selectedSource === null ? "page" : undefined} className={`source-row ${selectedFolder === null && selectedSource === null ? "active" : ""}`} href={`/?${queryString({ view: "clusters", filter: "unread", pane: "list" })}`} onClick={(event) => onNavigate(event, `/?${queryString({ view: "clusters", filter: "unread", pane: "list" })}`)}>
          <span className="source-row-name">全部文章</span>
          <CountBadges unread={allUnreadCount(sources)} starred={starredTotal(sources)} />
        </Link>
        {folders.map((folder) => {
          const folderSources = sources.filter((source) => source.folder_id === folder.id);
          if (!folderSources.length) return null;
          return (
            <div key={folder.id} className="folder-group">
              <FolderHeader active={selectedFolder === folder.id && selectedSource === null} href={`/?${queryString({ view: "clusters", folder_id: folder.id, filter: "unread", pane: "list" })}`} label={folder.name} unread={folderUnreadCount(sources, folder.id)} starred={starredTotal(folderSources)} onNavigate={onNavigate} />
              <details className="folder-details" open={selectedFolder === folder.id || folderSources.some((source) => source.id === selectedSource)}>
                <summary className="folder-summary" title={`展开或收起${folder.name}`} aria-label={`展开或收起${folder.name}`} />
                {folderSources.map((source) => (
                  <SourceRow key={source.id} source={source} active={selectedSource === source.id} eager={eagerSourceIds.has(source.id)} onNavigate={onNavigate} />
                ))}
              </details>
            </div>
          );
        })}
        {sources.some((source) => source.folder_id === null) ? (
          <div className="folder-group">
            <div className="folder-title-row">
              <span className="folder-name-link">未分类</span>
              <span className="folder-title-rule" aria-hidden="true" />
              <CountBadges unread={folderUnreadCount(sources, null)} starred={starredTotal(sources.filter((source) => source.folder_id === null))} />
            </div>
            <details className="folder-details" open={sources.some((source) => source.folder_id === null && source.id === selectedSource)}>
              <summary className="folder-summary" title="展开或收起未分类" aria-label="展开或收起未分类" />
              {sources
                .filter((source) => source.folder_id === null)
                .map((source) => (
                  <SourceRow key={source.id} source={source} active={selectedSource === source.id} eager={eagerSourceIds.has(source.id)} onNavigate={onNavigate} />
                ))}
            </details>
          </div>
        ) : null}
        {navError ? <p className="error-line">{navError}</p> : null}
      </div>
    </div>
  );
}

function BrowseContext({ filteredOnly, folders, media, sources, selectedFolder, selectedSource, navError, onNavigate }: { filteredOnly: boolean; folders: Folder[]; media: string; sources: Source[]; selectedFolder: number | null; selectedSource: number | null; navError: string; onNavigate: ReadingNavigate }) {
  const eagerSourceIds = new Set(sources.slice(0, 12).map((source) => source.id));
  const scope = (values: Record<string, string | number | null | undefined>) => queryString({ view: "browse", media, filtered: filteredOnly ? "1" : undefined, ...values });
  return (
    <div className="section reader-source-section">
      <div className="reader-source-list">
        <Link prefetch={false} aria-current={selectedFolder === null && selectedSource === null ? "page" : undefined} className={`source-row ${selectedFolder === null && selectedSource === null ? "active" : ""}`} href={`/?${scope({ filter: filteredOnly ? undefined : "unread", pane: "list" })}`} onClick={(event) => onNavigate(event, `/?${scope({ filter: filteredOnly ? undefined : "unread", pane: "list" })}`)}>
          <span className="source-row-name">{filteredOnly ? "全部已过滤" : `全部${browseMediaLabel(media)}`}</span>
          {filteredOnly ? null : <CountBadges unread={allUnreadCount(sources)} starred={starredTotal(sources)} />}
        </Link>
        {folders.map((folder) => {
          const folderSources = sources.filter((source) => source.folder_id === folder.id);
          if (!folderSources.length) return null;
          return (
            <div key={folder.id} className="folder-group">
              <FolderHeader active={selectedFolder === folder.id && selectedSource === null} href={`/?${scope({ folder_id: folder.id, filter: filteredOnly ? undefined : "unread", pane: "list" })}`} label={folder.name} unread={filteredOnly ? 0 : folderUnreadCount(sources, folder.id)} starred={filteredOnly ? 0 : starredTotal(folderSources)} onNavigate={onNavigate} />
              <details className="folder-details" open={selectedFolder === folder.id || folderSources.some((source) => source.id === selectedSource)}>
                <summary className="folder-summary" title={`展开或收起${folder.name}`} aria-label={`展开或收起${folder.name}`} />
                {folderSources.map((source) => (
                  <BrowseSourceRow key={source.id} filteredOnly={filteredOnly} media={media} source={source} active={selectedSource === source.id} eager={eagerSourceIds.has(source.id)} onNavigate={onNavigate} />
                ))}
              </details>
            </div>
          );
        })}
        {sources.some((source) => source.folder_id === null) ? (
          <div className="folder-group">
            <div className="folder-title-row">
              <span className="folder-name-link">未分类</span>
              <span className="folder-title-rule" aria-hidden="true" />
              {filteredOnly ? null : <CountBadges unread={folderUnreadCount(sources, null)} starred={starredTotal(sources.filter((source) => source.folder_id === null))} />}
            </div>
            <details className="folder-details" open={sources.some((source) => source.folder_id === null && source.id === selectedSource)}>
              <summary className="folder-summary" title="展开或收起未分类" aria-label="展开或收起未分类" />
              {sources
                .filter((source) => source.folder_id === null)
                .map((source) => (
                  <BrowseSourceRow key={source.id} filteredOnly={filteredOnly} media={media} source={source} active={selectedSource === source.id} eager={eagerSourceIds.has(source.id)} onNavigate={onNavigate} />
                ))}
            </details>
          </div>
        ) : null}
        {!sources.length ? <div className="placeholder">当前类型还没有正式订阅源。</div> : null}
        {navError ? <p className="error-line">{navError}</p> : null}
      </div>
    </div>
  );
}

function FolderHeader({ active, href, label, starred, unread, onNavigate }: { active: boolean; href: string; label: string; starred: number; unread: number; onNavigate: ReadingNavigate }) {
  return (
    <div className="folder-title-row">
      <Link prefetch={false} aria-current={active ? "page" : undefined} className={`folder-name-link ${active ? "active" : ""}`} href={href} onClick={(event) => onNavigate(event, href)}>
        <span className="source-row-name">{label}</span>
      </Link>
      <span className="folder-title-rule" aria-hidden="true" />
      <CountBadges unread={unread} starred={starred} />
    </div>
  );
}

function SourceRow({ source, active, eager, onNavigate }: { source: Source; active: boolean; eager: boolean; onNavigate: ReadingNavigate }) {
  const href = `/?${queryString({ view: "clusters", folder_id: source.folder_id, source_id: source.id, filter: "unread", pane: "list" })}`;
  return (
    <Link prefetch={false} aria-current={active ? "page" : undefined} className={`source-row folder-source-row ${active ? "active" : ""}`} href={href} onClick={(event) => onNavigate(event, href)}>
      <UnreadDot show={source.unread_count > 0} />
      <Favicon eager={eager} url={source.site_url || source.url} label={displaySourceName(source.name)} />
      <span className="source-row-name">{displaySourceName(source.name)}</span>
      <SourceBadges source={source} />
    </Link>
  );
}

function BrowseSourceRow({ source, active, eager, filteredOnly, media, onNavigate }: { source: Source; active: boolean; eager: boolean; filteredOnly: boolean; media: string; onNavigate: ReadingNavigate }) {
  const href = `/?${queryString({ view: "browse", media, filtered: filteredOnly ? "1" : undefined, folder_id: source.folder_id, source_id: source.id, filter: filteredOnly ? undefined : "unread", pane: "list" })}`;
  return (
    <Link prefetch={false} aria-current={active ? "page" : undefined} className={`source-row folder-source-row ${active ? "active" : ""}`} href={href} onClick={(event) => onNavigate(event, href)}>
      <UnreadDot show={!filteredOnly && source.unread_count > 0} />
      <Favicon eager={eager} url={source.site_url || source.url} label={displaySourceName(source.name)} />
      <span className="source-row-name">{displaySourceName(source.name)}</span>
      <SourceBadges source={source} showCounts={!filteredOnly} />
    </Link>
  );
}

function TopicContext({ topics, selectedTopic }: { topics: TopicGroup[]; selectedTopic: TopicGroup | undefined }) {
  return (
    <div className="section">
      <details className="topic-create">
        <summary>新增议题组</summary>
        <form className="form-stack" action="/actions/topic" method="post" noValidate>
          <input aria-label="主题名称" name="name" placeholder="主题名称，例如 OpenAI" required />
          <input aria-label="关键词" name="query" placeholder="关键词，例如 OpenAI" required />
          <input aria-label="议题说明" name="description" placeholder="说明，可选" />
          <button type="submit">保存议题组</button>
        </form>
      </details>
      {topics.length ? (
        topics.map((topic) => (
          <ClusterRowLink
            key={topic.id}
            active={selectedTopic?.id === topic.id}
            href={`/?${queryString({ view: "topics", topic_id: topic.id })}`}
            id={topic.id}
            meta={`${topic.query} · ${topic.cluster_count} 个事件聚类`}
            objectType="topic"
            readLater={topic.read_later}
            readStatus={topic.read_status}
            starred={topic.starred}
            summary={topic.description || `追踪包含 ${topic.query} 的长期议题。`}
            title={topic.name}
          />
        ))
      ) : (
        <div className="placeholder">还没有议题组。添加一个关键词后会按时间串联相关事件聚类。</div>
      )}
    </div>
  );
}

function ReportContext({ currentPeriod, reportDate, unavailableReason }: { currentPeriod: string; reportDate: string; unavailableReason: string }) {
  const dateValue = reportDate || todayInputDate();
  const periodLabel = REPORT_PERIODS.find(([value]) => value === currentPeriod)?.[1] ?? "报告";
  return (
    <div className="section">
      <div className="state-filters">
        {REPORT_PERIODS.map(([value, label]) => (
          <Link key={value} aria-current={currentPeriod === value ? "page" : undefined} className={currentPeriod === value ? "active" : ""} href={`/?${queryString({ view: "reports", period: value, date: dateValue })}`}>
            {label}
          </Link>
        ))}
      </div>
      <form className="toolbar report-controls" action="/" method="get">
        <input type="hidden" name="view" value="reports" />
        <input type="hidden" name="period" value={currentPeriod} />
        <input type="date" name="date" defaultValue={dateValue} aria-label="报告日期" />
        <button type="submit">查看</button>
        <button type="submit" formAction="/actions/report" formMethod="post" disabled={Boolean(unavailableReason)} title={unavailableReason || undefined}>
          生成{periodLabel}
        </button>
      </form>
      {unavailableReason ? <p className="source-meta" role="status">暂不可生成：{unavailableReason}</p> : null}
    </div>
  );
}

function SettingsContext({ currentSection, currentStatus, developerMode }: { currentSection: SettingsSection; currentStatus: string; developerMode: boolean }) {
  const links: [SettingsSection, string][] = [
    ["subscriptions", "订阅源"],
    ["filters", "内容过滤"],
    ["ai", "AI 设置"],
    ["local_models", "模型与翻译"],
    ["tasks", "任务分配"],
    ["general", "通用"],
    ["import_export", "导入导出"],
    ["about", "关于"]
  ];
  return (
    <div className="section context-links">
      {links.map(([section, label]) => (
        <Link key={section} aria-current={currentSection === section ? "page" : undefined} className={`source-row ${currentSection === section ? "active" : ""}`} href={`/?${queryString({ view: "settings", settings_section: section, settings_status: currentStatus || undefined, dev: developerMode ? "1" : undefined })}`}>
          {label}
        </Link>
      ))}
      <Link className="source-row" href={`/?${queryString({ view: "settings", settings_section: currentSection, settings_status: currentStatus || undefined, dev: developerMode ? undefined : "1" })}`}>
        {developerMode ? "退出开发者模式" : "开发者模式"}
      </Link>
    </div>
  );
}

function UnreadDot({ show }: { show: boolean }) {
  return show ? <span className="unread-dot" aria-hidden="true" /> : null;
}

function CountBadges({ starred, unread }: { starred: number; unread: number }) {
  return unread > 0 || starred > 0 ? (
    <span className="source-badges">
      <CountBadge count={unread} label="未读" />
      <CountBadge count={starred} label="收藏" starred />
    </span>
  ) : null;
}

function SourceBadges({ source, showCounts = true }: { source: Source; showCounts?: boolean }) {
  if (source.last_error) {
    const error = friendlyFetchError(source.last_error);
    return (
      <span className="source-badges">
        <span className="source-error-badge" title={error} aria-label={`源抓取错误：${error}`}>
          !
        </span>
      </span>
    );
  }
  if (!source.enabled) {
    return (
      <span className="source-badges">
        <span className="source-count" title="只暂停后续抓取">暂停抓取</span>
        {showCounts ? <CountBadge count={source.unread_count} label="未读" /> : null}
        {showCounts ? <CountBadge count={source.starred_count} label="收藏" starred /> : null}
      </span>
    );
  }
  return showCounts ? <CountBadges unread={source.unread_count} starred={source.starred_count} /> : null;
}

function CountBadge({ count, label, starred = false }: { count: number; label: string; starred?: boolean }) {
  if (count <= 0) return null;
  return (
    <span className={starred ? "source-count starred" : "source-count"} title={label} aria-label={`${label} ${count}`}>
      {starred ? <Star size={11} fill="currentColor" /> : null}
      {String(count)}
    </span>
  );
}

function allUnreadCount(sources: Source[]) {
  return sources[0]?.all_unread_count ?? sources.reduce((total, source) => total + source.unread_count, 0);
}

function folderUnreadCount(sources: Source[], folderId: number | null) {
  return sources.find((source) => source.folder_id === folderId)?.folder_unread_count ?? sources.filter((source) => source.folder_id === folderId).reduce((total, source) => total + source.unread_count, 0);
}

function starredTotal(sources: Source[]) {
  return sources.reduce((total, source) => total + source.starred_count, 0);
}

function browseMediaLabel(media: string) {
  if (media === "article") return "文章";
  if (media === "social") return "社交";
  if (media === "notification") return "通知";
  if (media === "image") return "图片";
  if (media === "video") return "视频";
  if (media === "podcast") return "音频";
  return "媒体";
}

function todayInputDate() {
  const date = new Date();
  const parts = new Intl.DateTimeFormat("zh-CN", { day: "2-digit", month: "2-digit", timeZone: "Asia/Shanghai", year: "numeric" }).formatToParts(date);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return values.year && values.month && values.day ? `${values.year}-${values.month}-${values.day}` : "";
}
