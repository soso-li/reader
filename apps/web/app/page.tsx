import { randomUUID } from "node:crypto";

import { Bell, Download, FileText, Film, ImageIcon, Mic, Newspaper, Settings, Star, Tags, Upload, Users } from "lucide-react";
import { cookies } from "next/headers";
import Link from "next/link";
import { redirect } from "next/navigation";

import ArticleContent from "./article-content";
import AssistantChatWindow, { type AssistantResult } from "./assistant-chat-window";
import BrowseView from "./browse-view";
import type { ClusterEventIdentity } from "./cluster-event-identity";
import type { ClusterSynthesisFields } from "./event-synthesis";
import ClusterView from "./cluster-view";
import ContextPanel from "./context-panel";
import { apiFetch, userFacingErrorMessage } from "./lib/api";
import NavRail from "./nav-rail";
import PipelineOverviewPanel, { type PipelineOverview } from "./pipeline-overview-panel";
import ReportReminder from "./report-reminder";
import { isReportReminderDismissed, REPORT_REMINDER_DISMISSED_COOKIE } from "./report-reminder-cookie";
import ReadingPreferencesControl from "./reading-preferences-control";
import { readingPreferencesFromCookies } from "./reading-preferences";
import { stateActionIcon } from "./state-action-icon";
import StateFilterBar from "./state-filter-bar";
import SubscriptionManager from "./subscription-manager";
import type { SourceMediaType } from "./source-media";
import SynthesisSettingsControl from "./synthesis-settings-control";
import ThemeControl from "./theme-control";
import TopicDeleteForm from "./topic-delete-form";
import TranslationSettingsControl from "./translation-settings-control";
import { previewText } from "./text-preview";
import { formatExactTime } from "./time-format";
import { TimeText } from "./time-text";
import FetchForm from "./fetch-form";
import FilterRuleManager from "./filter-rule-manager";
import LayoutModeControl from "./layout-mode-control";
import GenerationControlPanel, { type GenerationControl, type GenerationRetention, type GenerationTask } from "./generation-control-panel";
import { queryString } from "./url-state";

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8007";
const REPORT_PERIODS = [
  ["day", "日报"],
  ["week", "周报"],
  ["month", "月报"]
] as const;
const REPORT_DATE_FORMATTER = new Intl.DateTimeFormat("zh-CN", { day: "2-digit", month: "2-digit", timeZone: "Asia/Shanghai", year: "numeric" });
const CLUSTER_PAGE_SIZE = 50;
const BROWSE_PAGE_SIZE = 80;
const BROWSE_MEDIA_TYPES = ["social", "image", "video", "podcast", "notification"] as const;
const SOURCE_STATUSES = [
  ["active", "正式"],
  ["trial", "考察"]
] as const;
const SOURCE_STATUS_FILTERS = [["", "全部"], ...SOURCE_STATUSES] as const;
const SETTINGS_SECTIONS = ["subscriptions", "filters", "ai", "local_models", "tasks", "general", "import_export", "about"] as const;
type SearchParams = Record<string, string | string[] | undefined>;
type Folder = { id: number; name: string; media_type: SourceMediaType };
type Source = {
  id: number;
  folder_id: number | null;
  name: string;
  url: string;
  site_url: string;
  status: string;
  media_type: SourceMediaType;
  enabled: boolean;
  fetch_full_content: boolean;
  article_selector: string | null;
  remove_selector: string | null;
  privacy_class: "unclassified" | "public" | "private";
  external_generation_allowed: boolean;
  unread_count: number;
  folder_unread_count: number;
  all_unread_count: number;
  feed_trust_score: number;
  fetched_count: number;
  read_count: number;
  opened_count: number;
  starred_count: number;
  read_later_count: number;
  cluster_count: number;
  duplicate_count: number;
  recent_entry_count_30d: number;
  last_error: string;
  last_fetched_at: string | null;
  status_changed_at: string | null;
};
type PipelineStatus = { completed_at: string | null };
type AboutInfo = {
  version: string;
  commit: string;
  build_time: string;
  deploy_url: string;
  docs: Array<{ label: string; href: string }>;
  health: Record<string, { label: string; ok: boolean; detail: string }>;
  article_image_cache?: { used_bytes: number; max_bytes: number };
};
type AISettings = {
  task_provider: string;
  synthesis_provider: string;
  base_url: string;
  translation_provider: string;
  translation_base_url: string;
  translation_local_base_url: string;
  translation_local_model: string;
  translation_cloud_base_url: string;
  translation_cloud_model: string;
  translation_api_key_configured: boolean;
  embedding_base_url: string;
  endpoint: string;
  translation_endpoint: string;
  embedding_endpoint: string;
  llm_model: string;
  translation_model: string;
  embedding_model: string;
  timeout_seconds: number;
  synthesis_remote_base_url: string;
  synthesis_remote_model: string;
  synthesis_remote_api_key_configured: boolean;
};
type Item = {
  id: number;
  source_id: number;
  source_name: string;
  source_site_url: string;
  title: string;
  title_translation: string;
  summary: string;
  summary_translation: string;
  image_url: string;
  media_url: string;
  media_kind: string;
  media_duration: number;
  content_text: string;
  content_translation: string;
  reading_html?: string | null;
  reading_translation_needed?: boolean;
  url: string;
  published_at: string | null;
  read_status: string;
  read_later: boolean;
  starred: boolean;
  filtered: boolean;
  filter_rules: string[];
  uninterested: boolean;
  uninterested_reason: string | null;
  uninterested_note: string | null;
  uninterested_at: string | null;
};
type FilterRule = {
  id: number;
  source_id: number | null;
  source_name: string;
  match_type: "literal" | "regex";
  pattern: string;
  enabled: boolean;
  match_count: number;
  created_at: string;
  updated_at: string;
};
type FilteredItemsPage = { count: number; items: Item[] };
type BrowseSourceSummary = {
  source_id: number;
  folder_id: number | null;
  name: string;
  media_type: SourceMediaType;
  unread_count: number;
  total_count: number;
};
type BrowseMediaSummary = {
  media_type: SourceMediaType;
  unread_count: number;
  total_count: number;
  sources: BrowseSourceSummary[];
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
};
type ReportSource = { source_name: string; title: string; url: string; published_at: string | null };
type ReportCitation = { citation_no?: number | null; cluster_id: number; event_uid: string | null; event_revision_uid: string | null; title: string; first_seen_at: string | null; last_seen_at: string | null; sources: ReportSource[] };
type ReportSnapshot = {
  period: string;
  title: string;
  status: string;
  body: string;
  items: number[];
  citations: ReportCitation[];
  start?: string;
  end?: string;
  updated_at?: string | null;
  model_version?: string;
  prompt_version?: string;
  object_id: number;
  read_status: string;
  read_later: boolean;
  starred: boolean;
};
type TopicGroup = {
  id: number;
  name: string;
  query: string;
  description: string;
  cluster_count: number;
  last_seen_at: string | null;
  read_status: string;
  read_later: boolean;
  starred: boolean;
  clusters?: Cluster[];
};
type BrowseMedia = (typeof BROWSE_MEDIA_TYPES)[number] | "article";
type View = "clusters" | "browse" | "topics" | "reports" | "settings";
type SettingsSection = (typeof SETTINGS_SECTIONS)[number];

export default async function Page({ searchParams }: { searchParams: Promise<SearchParams> }) {
  const params = await searchParams;
  const cookieStore = await cookies();
  const readingPreferences = readingPreferencesFromCookies((name) => cookieStore.get(name)?.value);
  const requestedView = one(params.view);
  if (requestedView && !["clusters", "browse", "topics", "reports", "settings"].includes(requestedView)) {
    redirect(
      `/?${queryString({
        view: "clusters",
        folder_id: positiveIntegerParam(params.folder_id),
        source_id: positiveIntegerParam(params.source_id),
        q: one(params.q),
        filter: one(params.filter) ?? "unread",
        pane: one(params.pane),
        offset: nonNegativeIntegerParam(params.offset) || undefined
      })}`
    );
  }
  const view = requestedView as View | undefined;
  const currentView: View = view && ["clusters", "browse", "topics", "reports", "settings"].includes(view) ? view : "clusters";
  const currentBrowseMedia = browseMediaParam(one(params.media));
  const filteredOnly = currentView === "browse" && one(params.filtered) === "1";
  const selectedFolder = positiveIntegerParam(params.folder_id);
  const selectedSource = positiveIntegerParam(params.source_id);
  const selectedClusterId = positiveIntegerParam(params.cluster_id);
  const selectedItemId = positiveIntegerParam(params.item_id);
  const selectedTopicId = positiveIntegerParam(params.topic_id);
  const query = one(params.q) ?? "";
  const assistantTarget = one(params.assistant);
  const assistantAsk = one(params.assistant_ask) ?? "";
  const skipSeen = one(params.skip_seen) === "1";
  const filter = one(params.filter);
  const currentFilter = filter === "all" ? "" : filter && ["unread", "dismissed", "read_later", "starred"].includes(filter) ? filter : "unread";
  const currentFilterParam = currentFilter || "all";
  const settingsStatus = one(params.settings_status) ?? "";
  const currentSettingsStatus = SOURCE_STATUS_FILTERS.some(([value]) => value === settingsStatus) ? settingsStatus : "";
  const requestedSettingsSection = one(params.settings_section);
  if (currentView === "settings" && requestedSettingsSection && !SETTINGS_SECTIONS.includes(requestedSettingsSection as SettingsSection)) {
    redirect(
      `/?${queryString({
        view: "settings",
        settings_section: "subscriptions",
        settings_status: currentSettingsStatus || undefined,
        dev: one(params.dev) === "1" ? "1" : undefined
      })}`
    );
  }
  const currentSettingsSection: SettingsSection = SETTINGS_SECTIONS.includes(requestedSettingsSection as SettingsSection) ? (requestedSettingsSection as SettingsSection) : "subscriptions";
  const createFilterFromSample =
    currentView === "settings"
    && currentSettingsSection === "filters"
    && one(params.new_filter) === "1";
  const initialFilterSourceIds = positiveIntegerListParam(params.filter_source_ids);
  const actionResultParam = one(params.action_result);
  const actionResult = currentView === "settings" && (actionResultParam === "ok" || actionResultParam === "error") ? actionResultParam : "";
  const actionMessage = currentView === "settings" ? (one(params.action_message) ?? "") : "";
  const developerMode = one(params.dev) === "1";
  const requestedMobilePane = one(params.pane);
  const mobilePane = requestedMobilePane === "sources" ? "sources" : requestedMobilePane === "list" ? "list" : requestedMobilePane === "detail" ? "detail" : "";
  const timelineOffset = nonNegativeIntegerParam(params.offset) ?? 0;
  const period = one(params.period) ?? "day";
  const currentReportPeriod = ["day", "week", "month"].includes(period) ? period : "day";
  const requestedReportDate = one(params.date);
  const reportDate = validDateParam(requestedReportDate) ?? "";
  if (currentView === "reports" && ((one(params.period) && period !== currentReportPeriod) || (requestedReportDate && !reportDate))) {
    redirect(`/?${queryString({ view: "reports", period: currentReportPeriod, date: reportDate || undefined })}`);
  }
  const previousReportDate = previousDayInputDate();
  const reportError = currentView === "reports" ? queryError(one(params.report_error), "报告生成失败") : "";
  const actionError = queryError(one(params.action_error), "操作失败");
  const readingView = currentView === "clusters" || currentView === "browse";
  const requestedBrowseMedia = one(params.media);
  const invalidBrowseMedia = currentView === "browse" && Boolean(requestedBrowseMedia) && browseMediaParam(requestedBrowseMedia) !== requestedBrowseMedia;
  const invalidReadingFilter = readingView && Boolean(filter) && filter !== "all" && !["unread", "dismissed", "read_later", "starred"].includes(filter ?? "");
  const invalidReadingPane = readingView && Boolean(requestedMobilePane) && !["sources", "list", "detail"].includes(requestedMobilePane ?? "");
  const invalidReadingNumber = readingView && (
    [params.folder_id, params.source_id, currentView === "clusters" ? params.cluster_id : params.item_id]
      .some((value) => Boolean(one(value)) && positiveIntegerParam(value) === null)
    || (Boolean(one(params.offset)) && nonNegativeIntegerParam(params.offset) === null)
  );
  if (currentView === "topics" && Boolean(one(params.topic_id)) && selectedTopicId === null) {
    redirect("/?view=topics");
  }
  if (invalidBrowseMedia || invalidReadingFilter || invalidReadingPane || invalidReadingNumber) {
    redirect(
      `/?${queryString({
        view: currentView,
        media: currentView === "browse" ? currentBrowseMedia : undefined,
        filtered: filteredOnly ? "1" : undefined,
        folder_id: selectedFolder,
        source_id: selectedSource,
        cluster_id: currentView === "clusters" ? selectedClusterId : undefined,
        item_id: currentView === "browse" ? selectedItemId : undefined,
        q: query,
        filter: currentFilterParam,
        pane: mobilePane || undefined,
        offset: timelineOffset || undefined,
        assistant: currentView === "clusters" ? assistantTarget : undefined,
        assistant_ask: currentView === "clusters" ? assistantAsk : undefined,
        skip_seen: skipSeen ? "1" : undefined
      })}`
    );
  }
  if (currentView === "browse" && currentBrowseMedia === "article" && !filteredOnly && selectedItemId === null) {
    redirect(
      `/?${queryString({
        view: "clusters",
        folder_id: selectedFolder,
        source_id: selectedSource,
        q: query,
        filter: currentFilterParam,
        pane: mobilePane === "sources" ? "sources" : "list",
        skip_seen: skipSeen ? "1" : undefined
      })}`
    );
  }
  const subscriptionsSettings = currentView === "settings" && currentSettingsSection === "subscriptions";
  const needsFolders = readingView || subscriptionsSettings;
  const needsSources = readingView || subscriptionsSettings || (currentView === "settings" && currentSettingsSection === "filters");
  const needsAISettings = currentView === "reports" || (currentView === "settings" && ["ai", "local_models", "tasks", "general"].includes(currentSettingsSection));
  const needsTaskSettings = currentView === "settings" && currentSettingsSection === "tasks";
  const needsGenerationControl = currentView === "reports" || needsTaskSettings;

  const [
    foldersResult,
    sourcesResult,
    browseSummaryResult,
    pipelineStatusResult,
    clustersResult,
    clusterCountResult,
    browseItemsResult,
    topicsResult,
    reportsResult,
    previousReportResult,
    aiSettingsResult,
    pipelineOverviewResult,
    generationControlResult,
    generationTasksResult,
    generationRetentionResult,
    aboutResult,
    filterRulesResult,
    filteredItemsResult
  ] = await Promise.all([
    needsFolders ? safeApi<Folder[]>("/folders", [], "导航数据加载失败") : Promise.resolve({ data: [], error: "" }),
    needsSources ? safeApi<Source[]>(subscriptionsSettings ? "/sources" : "/sources/navigation", [], "导航数据加载失败") : Promise.resolve({ data: [], error: "" }),
    safeApi<BrowseMediaSummary[]>("/browse/summary", [], "浏览统计加载失败"),
    safeApi<PipelineStatus>("/pipeline/status", { completed_at: null }, "刷新状态加载失败"),
    currentView === "clusters" ? safeApi<Cluster[]>(`/clusters?${queryString({ ...clusterFilterQuery(currentFilter), folder_id: selectedFolder, source_id: selectedSource, q: query, limit: CLUSTER_PAGE_SIZE, offset: timelineOffset || undefined, order: readingPreferences.clusterOrder })}`, [], "聚类列表加载失败") : Promise.resolve({ data: [], error: "" }),
    currentView === "clusters" ? safeApi<{ count: number }>(`/clusters/count?${queryString({ ...clusterFilterQuery(currentFilter), folder_id: selectedFolder, source_id: selectedSource, q: query })}`, { count: 0 }, "聚类数量加载失败") : Promise.resolve({ data: { count: 0 }, error: "" }),
    currentView === "browse" && !filteredOnly ? safeApi<Item[]>(`/items?${queryString({ ...clusterFilterQuery(currentFilter), media_type: currentBrowseMedia, folder_id: selectedFolder, source_id: selectedSource, q: query, limit: BROWSE_PAGE_SIZE, offset: timelineOffset || undefined, include_content: false })}`, [], "浏览列表加载失败") : Promise.resolve({ data: [], error: "" }),
    currentView === "topics" ? safeApi<TopicGroup[]>("/topics", [], "议题列表加载失败") : Promise.resolve({ data: [], error: "" }),
    currentView === "reports" ? safeApi<ReportSnapshot | null>(`/reports?${queryString({ period: currentReportPeriod, date: reportDate })}`, null, "报告加载失败") : Promise.resolve({ data: null, error: "" }),
    currentView === "clusters" ? safeApi<ReportSnapshot | null>(`/reports?${queryString({ period: "day", date: previousReportDate })}`, null, "前日报告加载失败") : Promise.resolve({ data: null, error: "" }),
    needsAISettings ? safeApi<AISettings | null>("/ai/settings", null, "AI 设置加载失败") : Promise.resolve({ data: null, error: "" }),
    needsTaskSettings ? safeApi<PipelineOverview | null>("/pipeline/overview", null, "后台任务状态加载失败") : Promise.resolve({ data: null, error: "" }),
    needsGenerationControl ? safeApi<GenerationControl | null>("/generation/control", null, "生成控制状态加载失败") : Promise.resolve({ data: null, error: "" }),
    needsTaskSettings ? safeApi<GenerationTask[]>("/generation/tasks?limit=100", [], "生成请求加载失败") : Promise.resolve({ data: [], error: "" }),
    needsTaskSettings ? safeApi<GenerationRetention | null>("/generation/retention", null, "保留清理状态加载失败") : Promise.resolve({ data: null, error: "" }),
    currentView === "settings" && currentSettingsSection === "about" ? safeApi<AboutInfo | null>("/about", null, "关于信息加载失败") : Promise.resolve({ data: null, error: "" }),
    currentView === "settings" && currentSettingsSection === "filters" ? safeApi<FilterRule[]>("/filter-rules", [], "过滤规则加载失败") : Promise.resolve({ data: [], error: "" }),
    filteredOnly
      ? safeApi<FilteredItemsPage>(`/filtered-items?${queryString({ media_type: currentView === "browse" ? currentBrowseMedia : "article", folder_id: selectedFolder, source_id: selectedSource, q: query, limit: filteredOnly ? BROWSE_PAGE_SIZE : 1, offset: filteredOnly ? timelineOffset || undefined : undefined })}`, { count: 0, items: [] }, "已过滤列表加载失败")
      : Promise.resolve({ data: { count: 0, items: [] }, error: "" })
  ]);
  const folders = foldersResult.data;
  const sources = sourcesResult.data;
  const browseSummary = browseSummaryResult.data;
  const browseSummaryByMedia = new Map(browseSummary.map((row) => [row.media_type, row]));
  const readingSources = sources.filter((source) => source.status === "active");
  const visibleReadingSources = readingSources.filter((source) => source.media_type === "article");
  const visibleReadingFolders = folders.filter((folder) => visibleReadingSources.some((source) => source.folder_id === folder.id));
  const clusterUnreadCount = browseSummaryByMedia.get("article")?.unread_count ?? visibleReadingSources[0]?.all_unread_count ?? visibleReadingSources.reduce((total, source) => total + source.unread_count, 0);
  const browseSources = browseSourcesFor(readingSources, browseSummaryByMedia, currentBrowseMedia);
  const browseFolders = folders.filter((folder) => browseSources.some((source) => source.folder_id === folder.id));
  const browseRailItems = BROWSE_MEDIA_TYPES.map((media) => {
    const summary = browseSummaryByMedia.get(media);
    return {
      media,
      href: `/?${queryString({ view: "browse", media, filter: "unread", pane: "list" })}`,
      enabled: Boolean(summary?.sources.length),
      unread_count: summary?.unread_count ?? 0
    };
  });
  const lastFetchedAt = latestFetchedAt(readingSources);
  const metaError = foldersResult.error || sourcesResult.error || browseSummaryResult.error;
  const pipelineCompletedAt = pipelineStatusResult.data.completed_at || lastFetchedAt;
  const clusters = clustersResult.data;
  const browseItems = filteredOnly ? filteredItemsResult.data.items : browseItemsResult.data;
  const topics = topicsResult.data;
  const report = reportsResult.data;
  const aiSettings = aiSettingsResult.data;
  const pipelineOverview = pipelineOverviewResult.data;
  const generationControl = generationControlResult.data;
  const generationTasks = generationTasksResult.data;
  const generationRetention = generationRetentionResult.data;
  const reportGenerationUnavailable = currentView === "reports"
    ? reportGenerationUnavailableReason(aiSettings, generationControl)
    : "";
  const aboutInfo = aboutResult.data;
  const filterRules = filterRulesResult.data;
  const clusterScope = { view: "clusters", folder_id: selectedFolder, source_id: selectedSource, q: query, filter: currentFilterParam, pane: mobilePane === "list" ? "list" : undefined, offset: timelineOffset || undefined, order: readingPreferences.clusterOrder };
  const browseScope = { view: "browse", media: currentBrowseMedia, filtered: filteredOnly ? "1" : undefined, folder_id: selectedFolder, source_id: selectedSource, q: query, filter: currentFilterParam, pane: mobilePane === "list" ? "list" : undefined, offset: timelineOffset || undefined };
  const listedSelectedCluster = clusters.find((cluster) => cluster.id === selectedClusterId);
  const selectedClusterResult =
    currentView === "clusters" && selectedClusterId !== null && !listedSelectedCluster ? await safeApi<Cluster | null>(`/clusters/${selectedClusterId}`, null, "聚类详情加载失败") : { data: null, error: "" };
  const focusedCluster = listedSelectedCluster ?? selectedClusterResult.data;
  const clusterRows = focusedCluster && !clusters.some((cluster) => cluster.id === focusedCluster.id) ? [focusedCluster, ...clusters] : clusters;
  const selectedCluster = focusedCluster ?? undefined;
  const listedSelectedItem = browseItems.find((item) => item.id === selectedItemId);
  const browseSurfaceMode = currentView === "browse" && isBrowseSurfaceMedia(currentBrowseMedia) && selectedItemId === null;
  const selectedBrowseItemResult =
    currentView === "browse" && selectedItemId !== null ? await safeApi<Item | null>(`/items/${selectedItemId}`, null, "浏览详情加载失败") : { data: null, error: "" };
  const selectedBrowseItem = selectedBrowseItemResult.data ?? (selectedItemId !== null ? listedSelectedItem ?? null : null);
  const selectedBrowseSource = selectedBrowseItem ? sources.find((source) => source.id === selectedBrowseItem.source_id) : undefined;
  const selectedBrowseMedia = selectedBrowseSource?.media_type ?? "";
  if (
    currentView === "browse"
    && selectedItemId !== null
    && selectedBrowseMedia !== currentBrowseMedia
    && (selectedBrowseMedia === "article" || BROWSE_MEDIA_TYPES.includes(selectedBrowseMedia as (typeof BROWSE_MEDIA_TYPES)[number]))
  ) {
    redirect(
      `/?${queryString({
        view: "browse",
        media: selectedBrowseMedia,
        filtered: filteredOnly ? "1" : undefined,
        folder_id: selectedFolder,
        source_id: selectedSource,
        q: query,
        item_id: selectedItemId,
        filter: currentFilterParam,
        pane: mobilePane || undefined,
        offset: timelineOffset || undefined
      })}`
    );
  }
  const browseMediaDetailMode = currentView === "browse" && (currentBrowseMedia === "image" || currentBrowseMedia === "video") && selectedItemId !== null && selectedBrowseItem !== null;
  const listedSelectedTopic = topics.find((topic) => topic.id === selectedTopicId);
  const selectedTopicResult =
    currentView === "topics" && selectedTopicId !== null && !listedSelectedTopic ? await safeApi<TopicGroup | null>(`/topics/${selectedTopicId}`, null, "议题详情加载失败") : { data: null, error: "" };
  const focusedTopic = listedSelectedTopic ?? selectedTopicResult.data;
  const topicRows = focusedTopic && !topics.some((topic) => topic.id === focusedTopic.id) ? [focusedTopic, ...topics] : topics;
  const selectedTopic = focusedTopic ?? (selectedTopicId === null ? topicRows[0] : undefined);
  const clusterDetailResult =
    currentView === "clusters" && selectedCluster ? await safeApi<Cluster | null>(`/clusters/${selectedCluster.id}`, null, "聚类详情加载失败") : { data: null, error: "" };
  const clusterDetail = clusterDetailResult.data;
  const topicDetailResult = currentView === "topics" && selectedTopic ? await safeApi<TopicGroup | null>(`/topics/${selectedTopic.id}`, null, "议题详情加载失败") : { data: null, error: "" };
  const topicDetail = topicDetailResult.data;
  const assistantContext =
    assistantTarget === "cluster" && currentView === "clusters" && clusterDetail
        ? {
            id: clusterDetail.id,
            title: clusterTitle(clusterDetail),
            closeHref: `/?${queryString({ ...clusterScope, cluster_id: clusterDetail.id })}`,
            formParams: { ...clusterScope, cluster_id: clusterDetail.id, assistant: "cluster" }
          }
        : null;
  const assistantChatResult =
    assistantContext && assistantAsk
      ? await safeApi<AssistantResult | null>(
          `/assistant?${queryString({
            q: assistantAsk,
            cluster_id: assistantContext.id
          })}`,
          null,
          "本地模型服务未连接，请检查 LM Studio"
        )
      : { data: null, error: "" };
  const navError = metaError || pipelineStatusResult.error;
  const clusterListError = currentView === "clusters" ? clustersResult.error : "";
  const clusterDetailError = currentView === "clusters" ? actionError || selectedClusterResult.error || clusterDetailResult.error : "";
  const browseListError = currentView === "browse" ? actionError || (filteredOnly ? filteredItemsResult.error : browseItemsResult.error) : "";
  const browseDetailError = currentView === "browse" ? selectedBrowseItemResult.error : "";
  const topicListError = currentView === "topics" ? actionError || topicsResult.error : "";
  const topicDetailError = currentView === "topics" ? selectedTopicResult.error || topicDetailResult.error : "";
  const reportLoadError = currentView === "reports" ? actionError || reportsResult.error : "";
  const settingsError = currentView === "settings" ? actionError || metaError || aiSettingsResult.error : "";
  const hasFocusedDetail = (currentView === "clusters" && selectedClusterId !== null) || (currentView === "browse" && selectedItemId !== null);
  const isReadingView = currentView === "clusters" || currentView === "browse";
  const isContentMode = !isReadingView;
  const hasMobileListScope = isReadingView && !hasFocusedDetail && mobilePane !== "sources";
  const sourceBackHref = `/?${queryString({ view: "clusters", pane: "sources" })}`;
  const browseBackHref = `/?${queryString({ view: "browse", media: currentBrowseMedia, filtered: filteredOnly ? "1" : undefined, pane: "sources" })}`;
  const navLinks = {
    clusters: `/?${queryString({ view: "clusters", filter: "unread", pane: "list" })}`,
    topics: "/?view=topics",
    reports: "/?view=reports&period=day",
    settings: "/?view=settings&settings_section=subscriptions"
  };
  const mobileNavigation = <MobileEntryMenu browseItems={browseRailItems} clusterUnreadCount={clusterUnreadCount} currentMedia={currentBrowseMedia} currentView={currentView} links={navLinks} />;
  const mobileStateFilters = isReadingView && !filteredOnly ? <StateFilterBar currentFilter={currentFilter} scope={{ ...(currentView === "browse" ? browseScope : clusterScope), pane: "sources" }} /> : null;

  return (
    <main id="reader-main" tabIndex={-1} className={`app-shell media-all ${isContentMode ? "content-mode" : ""} ${currentView === "settings" ? "settings-mode" : ""} ${browseSurfaceMode ? "browse-surface-mode" : ""} ${browseMediaDetailMode ? "browse-media-detail-mode" : ""} ${isReadingView ? "mobile-reading-shell" : ""} ${hasMobileListScope ? "mobile-list" : ""} ${hasFocusedDetail ? "mobile-detail" : ""}`}>
      <NavRail browseItems={browseRailItems} clusterUnreadCount={clusterUnreadCount} currentMedia={currentBrowseMedia} currentView={currentView} links={navLinks} />
      <ContextPanel
        apiUrl={API_URL}
        browseMedia={currentBrowseMedia}
        currentView={currentView}
        currentReportPeriod={currentReportPeriod}
        currentSettingsSection={currentSettingsSection}
        currentSettingsStatus={currentSettingsStatus}
        developerMode={developerMode}
        filteredOnly={filteredOnly}
        folders={currentView === "browse" ? browseFolders : visibleReadingFolders}
        lastUpdatedAt={pipelineCompletedAt}
        navError={currentView !== "settings" ? navError : ""}
        reportDate={reportDate}
        reportGenerationUnavailableReason={reportGenerationUnavailable}
        selectedFolder={selectedFolder}
        selectedSource={selectedSource}
        selectedTopic={selectedTopic}
        sources={currentView === "browse" ? browseSources : visibleReadingSources}
        topics={topicRows}
        mobileNavigation={mobileNavigation}
        mobileStateFilters={mobileStateFilters}
      />

      {currentView === "clusters" ? (
        <ClusterView
          apiUrl={API_URL}
          currentFilter={currentFilter}
          currentFilterCount={clusterCountResult.error ? null : clusterCountResult.data.count}
          initialDetail={clusterDetail}
          initialPageCount={clusters.length}
          initialSelectedClusterId={selectedClusterId}
          offset={timelineOffset}
          pageSize={CLUSTER_PAGE_SIZE}
          query={query}
          rows={clusterRows}
          scope={clusterScope}
          skipSeen={skipSeen}
          listBackHref={sourceBackHref}
          mobileNavigation={mobileNavigation}
          reportReminder={<ReportReminder key={previousReportDate} date={previousReportDate} error={previousReportResult.error} initialDismissed={isReportReminderDismissed(cookieStore.get(REPORT_REMINDER_DISMISSED_COOKIE)?.value, previousReportDate) || cookieStore.get(`reader_report_reminder_${previousReportDate}`)?.value === "1"} report={previousReportResult.data} />}
          listError={clusterListError}
          detailError={clusterDetailError}
        />
      ) : currentView === "browse" ? (
        <BrowseView
          apiUrl={API_URL}
          currentFilter={currentFilter}
          detailError={browseDetailError}
          initialSelectedItemId={selectedItemId}
          initialPageCount={browseItems.length}
          items={browseItems}
          listBackHref={browseBackHref}
          listError={browseListError}
          media={currentBrowseMedia}
          filteredOnly={filteredOnly}
          offset={timelineOffset}
          pageSize={BROWSE_PAGE_SIZE}
          query={query}
          scope={browseScope}
          selectedItem={selectedBrowseItem}
          thumbnailMode={readingPreferences.listThumbnails}
          mobileNavigation={mobileNavigation}
        />
      ) : (
        <section className="pane list-pane content-pane">
          <div className="pane-header">
            <h2 className="pane-title">{viewTitle(currentView)}</h2>
          </div>
          {currentView === "topics" ? (
            topicDetail ? (
              <div className="detail-body">
            {topicListError || topicDetailError ? <p className="error-line" role="alert">{topicListError || topicDetailError}</p> : null}
            <div className="toolbar">
              <StateForm objectType="topic" object={topicDetail} readStatus={isSeenStatus(topicDetail.read_status) ? "unread" : "summary_seen"} label={isSeenStatus(topicDetail.read_status) ? "标记未读" : "标记看过"} />
              {topicDetail.read_status !== "dismissed" ? <StateForm objectType="topic" object={topicDetail} readStatus="dismissed" label="忽略" /> : null}
              <StateForm objectType="topic" object={topicDetail} readLater={!topicDetail.read_later} label="稍后阅读" />
              <StateForm objectType="topic" object={topicDetail} starred={!topicDetail.starred} icon />
            </div>
            <p className="section-title">Story Line</p>
            <h2>{topicDetail.name}</h2>
            <p className="item-meta">
              关键词：{topicDetail.query} · {topicDetail.cluster_count} 个事件聚类
            </p>
            <div className="cluster-sources">
              <h3>议题摘要</h3>
              <ArticleContent text={topicSummary(topicDetail)} />
            </div>
            <div className="cluster-sources">
              <h3>最新进展</h3>
              <ArticleContent text={topicLatestProgress(topicDetail)} />
            </div>
            <form className="form-stack" action="/actions/topic" method="post" noValidate>
              <input type="hidden" name="topic_id" value={topicDetail.id} />
              <input aria-label="主题名称" name="name" defaultValue={topicDetail.name} placeholder="主题名称" required />
              <input aria-label="关键词" name="query" defaultValue={topicDetail.query} placeholder="关键词" required />
              <input aria-label="议题说明" name="description" defaultValue={topicDetail.description} placeholder="说明，可选" />
              <button type="submit">保存议题组</button>
            </form>
            <TopicDeleteForm topicId={topicDetail.id} topicName={topicDetail.name} />
            {topicDetail.description ? <ArticleContent text={topicDetail.description} /> : null}
            <div className="cluster-sources">
              <h3>Story Line（按时间）</h3>
              {topicDetail.clusters?.length ? (
                topicDetail.clusters.map((cluster) => (
                  <div key={cluster.id} className="cluster-source">
                    <div className="item-title"><a className="stretched-row-link" href={`/?${queryString({ view: "clusters", cluster_id: cluster.id })}`}>{clusterTitle(cluster)}</a></div>
                    <div className="item-meta">
                      <TimeText value={cluster.first_seen_at} /> · {cluster.item_count} 条来源
                    </div>
                    <div className="item-summary">{previewText(clusterSummary(cluster))}</div>
                  </div>
                ))
              ) : (
                <div className="placeholder">暂无匹配的事件聚类。</div>
              )}
            </div>
          </div>
            ) : (
              <div className="placeholder">{topicListError || topicDetailError ? <p className="error-line">{topicListError || topicDetailError}</p> : detailPlaceholder(currentView)}</div>
            )
          ) : null}
          {currentView === "reports" ? <ReportView report={report} currentPeriod={currentReportPeriod} reportDate={reportDate} reportError={reportError || reportLoadError} /> : null}
          {currentView === "settings" ? <SettingsView folders={folders} sources={sources} selectedFolder={selectedFolder} error={settingsError || filterRulesResult.error} aiSettings={aiSettings} pipelineOverview={pipelineOverview} pipelineOverviewError={pipelineOverviewResult.error} generationControl={generationControl} generationTasks={generationTasks} generationRetention={generationRetention} generationError={generationControlResult.error || generationTasksResult.error || generationRetentionResult.error} aboutInfo={aboutInfo} aboutError={aboutResult.error} filterRules={filterRules} currentStatus={currentSettingsStatus} currentSection={currentSettingsSection} actionResult={actionResult} actionMessage={actionMessage} developerMode={developerMode} createFilterFromSample={createFilterFromSample} initialFilterSourceIds={initialFilterSourceIds} /> : null}
        </section>
      )}
      {assistantContext ? <AssistantChatWindow context={assistantContext} error={assistantChatResult.error} question={assistantAsk} result={assistantChatResult.data} /> : null}
    </main>
  );
}

function MobileEntryMenu({ browseItems, clusterUnreadCount, currentMedia, currentView, links }: { browseItems: Array<{ media: BrowseMedia; href: string; enabled: boolean; unread_count: number }>; clusterUnreadCount: number; currentMedia: BrowseMedia; currentView: View; links: Record<"clusters" | "topics" | "reports" | "settings", string> }) {
  const items = [
    { id: "clusters", label: "聚类", href: links.clusters, active: currentView === "clusters", enabled: true, count: clusterUnreadCount, icon: <Newspaper size={20} /> },
    { id: "topics", label: "议题", href: links.topics, active: currentView === "topics", enabled: true, count: 0, icon: <Tags size={20} /> },
    { id: "reports", label: "日报", href: links.reports, active: currentView === "reports", enabled: true, count: 0, icon: <FileText size={20} /> },
    ...browseItems.map((item) => ({
      id: item.media,
      label: mobileMediaLabel(item.media),
      href: item.href,
      active: currentView === "browse" && currentMedia === item.media,
      enabled: item.enabled,
      count: item.unread_count,
      icon: mobileMediaIcon(item.media)
    })),
    { id: "settings", label: "设置", href: links.settings, active: currentView === "settings", enabled: true, count: 0, icon: <Settings size={20} /> }
  ];

  return (
    <nav className="mobile-entry-menu" aria-label="移动内容入口">
      {items.map((item) =>
        item.enabled ? (
          <Link key={item.id} className={`mobile-entry-button ${item.active ? "active" : ""}`} href={item.href} aria-current={item.active ? "page" : undefined}>
            {item.icon}
            <span>{item.label}</span>
            {item.count > 0 ? <small>{item.count > 99 ? "99+" : item.count}</small> : null}
          </Link>
        ) : (
          <span key={item.id} className="mobile-entry-button disabled" title={`${item.label}（无订阅源）`} aria-label={`${item.label}（无订阅源）`}>
            {item.icon}
            <span>{item.label}</span>
          </span>
        )
      )}
    </nav>
  );
}

function mobileMediaLabel(media: BrowseMedia) {
  if (media === "social") return "社交";
  if (media === "image") return "图片";
  if (media === "video") return "视频";
  if (media === "podcast") return "播客";
  return "通知";
}

function mobileMediaIcon(media: BrowseMedia) {
  if (media === "social") return <Users size={20} />;
  if (media === "image") return <ImageIcon size={20} />;
  if (media === "video") return <Film size={20} />;
  if (media === "podcast") return <Mic size={20} />;
  return <Bell size={20} />;
}

function SettingsView({
  folders,
  sources,
  selectedFolder,
  error,
  aiSettings,
  pipelineOverview,
  pipelineOverviewError,
  generationControl,
  generationTasks,
  generationRetention,
  generationError,
  aboutInfo,
  aboutError,
  filterRules,
  currentStatus,
  currentSection,
  actionResult,
  actionMessage,
  developerMode,
  createFilterFromSample,
  initialFilterSourceIds
}: {
  folders: Folder[];
  sources: Source[];
  selectedFolder: number | null;
  error: string;
  aiSettings: AISettings | null;
  pipelineOverview: PipelineOverview | null;
  pipelineOverviewError: string;
  generationControl: GenerationControl | null;
  generationTasks: GenerationTask[];
  generationRetention: GenerationRetention | null;
  generationError: string;
  aboutInfo: AboutInfo | null;
  aboutError: string;
  filterRules: FilterRule[];
  currentStatus: string;
  currentSection: SettingsSection;
  actionResult: string;
  actionMessage: string;
  developerMode: boolean;
  createFilterFromSample: boolean;
  initialFilterSourceIds: number[];
}) {
  const title = settingsSectionTitle(currentSection);
  const description = settingsSectionDescription(currentSection);
  return (
    <div className="settings-page">
      <div className="settings-titlebar">
        <div>
          <h2>{title}</h2>
          <p>{description}</p>
        </div>
      </div>
      {error ? <p className="error-line" role="alert">{error}</p> : null}
      {actionMessage ? <p className={actionResult === "error" ? "error-line" : "status-line success-line"} role={actionResult === "error" ? "alert" : "status"}>{actionMessage}</p> : null}

      {currentSection === "subscriptions" ? (
        <SubscriptionManager folders={folders} sources={sources} themeControl={<ThemeControl />} />
      ) : null}

      {currentSection === "filters" ? <FilterRuleManager autoCreate={createFilterFromSample} initialRules={filterRules} initialSourceIds={initialFilterSourceIds} sources={sources} /> : null}

      {currentSection === "ai" ? (
        <section id="settings-ai" className="settings-block">
          <h3>AI 设置</h3>
          <form className="form-stack" action="/actions/ai-settings" method="post">
            <label>
              LLM 地址
              <input name="base_url" defaultValue={aiSettings?.base_url ?? ""} placeholder="http://127.0.0.1:1234" />
            </label>
            <label>
              LLM 模型
              <input name="llm_model" defaultValue={aiSettings?.llm_model ?? ""} placeholder="qwen/qwen3.5-9b" />
            </label>
            <div className="source-meta">LLM Endpoint: {aiSettings?.endpoint ?? "未配置"}</div>
            <button type="submit">保存 AI 设置</button>
          </form>
          {aiSettings ? <SynthesisSettingsControl apiUrl={API_URL} settings={aiSettings} /> : <p className="error-line">合成稿设置加载失败</p>}
          <form className="settings-action-row" action="/actions/ai-test" method="post">
            <input type="hidden" name="model_type" value="llm" />
            <button type="submit">测试 LLM</button>
          </form>
        </section>
      ) : null}

      {currentSection === "local_models" ? (
        <section id="settings-local-models" className="settings-block">
          <h3>模型与翻译</h3>
          {aiSettings ? <TranslationSettingsControl apiUrl={API_URL} settings={aiSettings} /> : <p className="error-line">翻译设置加载失败</p>}
          <form className="form-stack" action="/actions/ai-settings" method="post">
            <label>
              Embedding 地址
              <input name="embedding_base_url" defaultValue={aiSettings?.embedding_base_url ?? ""} placeholder="http://127.0.0.1:1234" />
            </label>
            <label>
              Embedding 模型
              <input name="embedding_model" defaultValue={aiSettings?.embedding_model ?? ""} placeholder="text-embedding-qwen3-embedding-4b" />
            </label>
            <div className="source-meta">Embedding Endpoint: {aiSettings?.embedding_endpoint ?? "未配置"}</div>
            <button type="submit">保存 Embedding 设置</button>
          </form>
          <div className="settings-action-row">
            <form action="/actions/ai-test" method="post">
              <input type="hidden" name="model_type" value="translation" />
              <button type="submit">测试翻译</button>
            </form>
            <form action="/actions/ai-test" method="post">
              <input type="hidden" name="model_type" value="embedding" />
              <button type="submit">测试 Embedding</button>
            </form>
          </div>
        </section>
      ) : null}

      {currentSection === "tasks" ? (
        <>
          <GenerationControlPanel apiUrl={API_URL} initialControl={generationControl} initialTasks={generationTasks} initialRetention={generationRetention} initialError={generationError} />
          <PipelineOverviewPanel apiUrl={API_URL} initial={pipelineOverview} initialError={pipelineOverviewError} />
          <section id="settings-tasks" className="settings-block">
            <h3>任务分配</h3>
            <form className="form-stack" action="/actions/ai-settings" method="post">
              <label>
                生成任务
                <select name="task_provider" defaultValue={aiSettings?.task_provider ?? "local"}>
                  <option value="local">本地模型</option>
                  <option value="openai_compatible">远端兼容接口</option>
                </select>
              </label>
              <button type="submit">保存任务分配</button>
            </form>
          </section>
        </>
      ) : null}

      {currentSection === "general" ? (
        <section id="settings-general" className="settings-block">
          <h3>通用</h3>
          <div className="settings-subsection">
            <h4>主题</h4>
            <ThemeControl />
          </div>
          <div className="settings-subsection">
            <h4>版式</h4>
            <LayoutModeControl />
          </div>
          <div className="settings-subsection">
            <h4>阅读偏好</h4>
            <ReadingPreferencesControl />
          </div>
          <form className="form-stack" action="/actions/ai-settings" method="post">
            <label>
              超时秒数
              <input name="timeout_seconds" defaultValue={aiSettings?.timeout_seconds ?? 240} inputMode="decimal" />
            </label>
            <button type="submit">保存通用设置</button>
          </form>
        </section>
      ) : null}

      {currentSection === "import_export" ? (
        <section id="settings-import-export" className="settings-block">
          <h3>导入导出</h3>
          <form className="form-stack" action="/actions/import-opml" method="post" encType="multipart/form-data">
            <label>
              OPML 文件
              <input name="file" type="file" accept=".opml,.xml,text/xml" />
            </label>
            <button type="submit">
              <Upload size={15} /> 导入 OPML
            </button>
          </form>
          <a className="action-link" href={`${API_URL}/exports/opml`}>
            <Download size={15} /> 导出 OPML
          </a>
        </section>
      ) : null}

      {currentSection === "about" ? <AboutPanel about={aboutInfo} error={aboutError} /> : null}

      {developerMode ? (
        <section id="settings-dev" className="settings-block">
          <h3>开发者模式</h3>
          <FetchForm />
          <form action="/actions/embeddings" method="post">
            <button type="submit">生成 Embedding / 重聚类</button>
          </form>
          <a className="action-link" href={`/?${queryString({ view: "settings", settings_section: currentSection, settings_status: currentStatus || undefined })}`}>
            退出开发者模式
          </a>
        </section>
      ) : null}
    </div>
  );
}

function AboutPanel({ about, error }: { about: AboutInfo | null; error: string }) {
  const healthItems = about
    ? [
        ["db", about.health.db],
        ["redis", about.health.redis],
        ["llm", about.health.llm],
        ["embedding", about.health.embedding]
      ].filter((item): item is [string, { label: string; ok: boolean; detail: string }] => Boolean(item[1]))
    : [];
  return (
    <section id="settings-about" className="settings-block">
      <h3>关于</h3>
      {error ? <p className="error-line">{error}</p> : null}
      {about ? (
        <>
          <dl className="about-meta-grid">
            <div>
              <dt>版本</dt>
              <dd>{about.version}{about.commit ? ` · ${about.commit}` : ""}</dd>
            </div>
            <div>
              <dt>构建时间</dt>
              <dd>{formatAboutDate(about.build_time)}</dd>
            </div>
            <div>
              <dt>部署地址</dt>
              <dd>{about.deploy_url || "—"}</dd>
            </div>
            <div>
              <dt>正文图片缓存</dt>
              <dd>
                {about.article_image_cache
                  ? `${formatBytes(about.article_image_cache.used_bytes)} / ${formatBytes(about.article_image_cache.max_bytes)}`
                  : "—"}
              </dd>
            </div>
          </dl>
          <div className="about-health-grid">
            {healthItems.map(([key, item]) => (
              <div key={key} className="about-health-row">
                <span className={`pipeline-status-dot ${item.ok ? "ok" : "bad"}`} aria-hidden="true" />
                <span>{item.label}</span>
                <strong>{item.ok ? "正常" : "异常"}</strong>
                <small title={item.detail}>{item.detail || "—"}</small>
              </div>
            ))}
          </div>
          <div className="about-doc-links">
            {about.docs.map((doc) => (
              <a key={doc.href} href={doc.href}>
                {doc.label}
              </a>
            ))}
          </div>
        </>
      ) : (
        <p className="source-meta">关于信息不可用。</p>
      )}
    </section>
  );
}

function formatAboutDate(value: string) {
  if (!value || value === "development") return value || "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("zh-CN", { day: "2-digit", hour: "2-digit", minute: "2-digit", month: "2-digit", year: "numeric" });
}

function formatBytes(value: number) {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  const unit = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${(value / 1024 ** unit).toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
}

function settingsSectionTitle(section: SettingsSection) {
  if (section === "filters") return "内容过滤";
  if (section === "ai") return "AI 设置";
  if (section === "local_models") return "模型与翻译";
  if (section === "tasks") return "任务分配";
  if (section === "general") return "通用";
  if (section === "import_export") return "导入导出";
  if (section === "about") return "关于";
  return "订阅管理";
}

function settingsSectionDescription(section: SettingsSection) {
  if (section === "filters") return "用关键词或正则表达式过滤不想进入自动阅读流的条目。";
  if (section === "ai") return "配置生成摘要和报告使用的 LLM。";
  if (section === "local_models") return "选择本地或云端翻译，并配置本地 Embedding。";
  if (section === "tasks") return "选择生成任务使用本地模型还是远端兼容接口。";
  if (section === "general") return "配置主题、版式和模型请求超时。";
  if (section === "import_export") return "导入或导出 OPML 订阅列表。";
  if (section === "about") return "查看版本、部署地址、健康点和项目文档链接。";
  return "按内容类型管理 RSS 源。暂停只停止后续抓取，已有条目、聚类和合成保持不变；删除即永久删除。";
}

function ReportView({ report, currentPeriod, reportDate, reportError }: { report: ReportSnapshot | null; currentPeriod: string; reportDate: string; reportError: string }) {
  const dateValue = reportDate || dateInputValue(report?.start) || todayInputDate();
  const reportTitle = reportDisplayTitle(report, currentPeriod, dateValue);
  return (
    <div className="report-page">
      {reportError ? <p className="error-line">{reportError}</p> : null}
      {report?.status === "ready" ? (
        <div className="detail-body">
          <div className="toolbar">
            <StateForm objectType="report" object={{ id: report.object_id, starred: report.starred }} readStatus={isSeenStatus(report.read_status) ? "unread" : "summary_seen"} label={isSeenStatus(report.read_status) ? "标记未读" : "标记看过"} />
            {report.read_status !== "dismissed" ? <StateForm objectType="report" object={{ id: report.object_id, starred: report.starred }} readStatus="dismissed" label="忽略" /> : null}
            <StateForm objectType="report" object={{ id: report.object_id, starred: report.starred }} readLater={!report.read_later} label="稍后阅读" />
            <StateForm objectType="report" object={{ id: report.object_id, starred: report.starred }} starred={!report.starred} icon />
          </div>
          <h2>{reportTitle}</h2>
          {report.start && report.end ? (
            <p className="item-meta">
              范围：{reportRangeLabel(report.start, report.end)}
            </p>
          ) : null}
          {report.updated_at ? (
            <p className="item-meta">
              生成时间：<time dateTime={report.updated_at}>{formatExactTime(report.updated_at)}</time>
            </p>
          ) : null}
          <ArticleContent text={report.body} />
          <div className="cluster-sources">
            <h3>引用事件聚类</h3>
            {report.citations.map((citation, index) => (
              <div key={citation.cluster_id} className="cluster-source">
                <div className="item-title"><a className="stretched-row-link" href={citation.event_uid && citation.event_revision_uid ? `/events/${encodeURIComponent(citation.event_uid)}/revisions/${encodeURIComponent(citation.event_revision_uid)}?${queryString({ period: currentPeriod, date: dateValue })}` : `/?${queryString({ view: "clusters", cluster_id: citation.cluster_id })}`}>[{citation.citation_no ?? index + 1}] {citation.title}</a></div>
                <div className="item-meta">
                  <TimeText value={citation.first_seen_at} />
                </div>
                {citation.sources?.length ? (
                  <div className="item-summary">
                    {citation.sources.map((source) => `${source.source_name}: ${source.title}`).join(" · ")}
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        </div>
      ) : (
        <div className="placeholder">
          {reportTitle}还没有生成。
          {report?.start && report.end ? (
            <div className="item-meta">
              范围：{reportRangeLabel(report.start, report.end)}
            </div>
          ) : null}
        </div>
      )}
    </div>
  );
}

function StateForm({
  object,
  objectType = "item",
  readStatus,
  readLater,
  starred,
  label,
  icon
}: {
  object: { id: number; starred: boolean };
  objectType?: "item" | "report" | "topic";
  readStatus?: string;
  readLater?: boolean;
  starred?: boolean;
  label?: string;
  icon?: boolean;
}) {
  const title = icon ? "星标" : label || "操作";
  const active = label === "标记未读" || readLater === false || (icon && object.starred);
  return (
    <form action="/actions/user-state" method="post">
      <input type="hidden" name="object_type" value={objectType} />
      <input type="hidden" name="object_id" value={object.id} />
      <input type="hidden" name="operation_id" value={randomUUID()} />
      {readStatus ? <input type="hidden" name="read_status" value={readStatus} /> : null}
      {readLater !== undefined ? <input type="hidden" name="read_later" value={String(readLater)} /> : null}
      {starred !== undefined ? <input type="hidden" name="starred" value={String(starred)} /> : null}
      <button className={`icon ${active ? "active" : ""}`} title={title} aria-label={title}>
        {icon ? <Star size={17} fill={object.starred ? "currentColor" : "none"} /> : stateActionIcon(label, active)}
      </button>
    </form>
  );
}

async function safeApi<T>(path: string, fallback: T, errorFallback = "数据加载失败"): Promise<{ data: T; error: string }> {
  try {
    return { data: await apiFetch<T>(path), error: "" };
  } catch (err) {
    return { data: fallback, error: userFacingErrorMessage(err, errorFallback) };
  }
}

function queryError(value: string | undefined, fallback: string) {
  return value ? userFacingErrorMessage(value, fallback) : "";
}

function one(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value;
}

function positiveIntegerParam(value: string | string[] | undefined) {
  const raw = one(value);
  if (!raw || !/^[1-9]\d*$/.test(raw)) return null;
  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function positiveIntegerListParam(value: string | string[] | undefined) {
  return [...new Set(
    (one(value) ?? "")
      .split(",")
      .map((item) => Number(item))
      .filter((item) => Number.isSafeInteger(item) && item > 0)
  )];
}

function nonNegativeIntegerParam(value: string | string[] | undefined) {
  const raw = one(value);
  if (!raw || !/^(0|[1-9]\d*)$/.test(raw)) return null;
  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function validDateParam(value: string | string[] | undefined) {
  const raw = one(value);
  if (!raw || !/^\d{4}-\d{2}-\d{2}$/.test(raw)) return null;
  const [year, month, day] = raw.split("-").map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day ? raw : null;
}

function browseMediaParam(value: string | undefined): BrowseMedia {
  return value === "article" || BROWSE_MEDIA_TYPES.includes(value as (typeof BROWSE_MEDIA_TYPES)[number]) ? (value as BrowseMedia) : "social";
}

function reportGenerationUnavailableReason(aiSettings: AISettings | null, control: GenerationControl | null) {
  if (!aiSettings || !control) return "生成状态不可用";
  if (!["local", "openai_compatible"].includes(aiSettings.synthesis_provider)) return "报告生成服务未配置";
  const reasons: string[] = [];
  if (control.global_pause) reasons.push("生成任务已全局暂停");
  if (control.daily_budget_tokens === null) reasons.push("每日 Token 预算尚未配置");
  else if (control.remaining_tokens === 0) reasons.push("今日 Token 预算已用完");
  return reasons.join("；");
}

function isBrowseSurfaceMedia(media: string) {
  return media === "social" || media === "image" || media === "video";
}

function reportDisplayTitle(report: ReportSnapshot | null, period: string, dateValue: string) {
  const genericTitle = REPORT_PERIODS.find(([value]) => value === period)?.[1] ?? "报告";
  if (report?.title && report.title !== genericTitle) return report.title;
  if (report?.start && report.end && period !== "day") return `${reportRangeLabel(report.start, report.end)} ${genericTitle}`;
  return `${dateValue} ${genericTitle}`;
}

function reportRangeLabel(start: string, end: string) {
  const startDate = dateInputValue(start);
  const endTime = new Date(end).getTime();
  const endDate = Number.isFinite(endTime) ? dateInputValue(new Date(endTime - 1)) : "";
  if (!startDate || !endDate) return "未知时间";
  if (startDate === endDate) return startDate;
  const sameYear = startDate.slice(0, 4) === endDate.slice(0, 4);
  return sameYear ? `${startDate.slice(5)} - ${endDate.slice(5)}` : `${startDate} - ${endDate}`;
}

function todayInputDate() {
  return dateInputValue(new Date()) || "";
}

function previousDayInputDate() {
  return dateInputValue(new Date(Date.now() - 24 * 60 * 60 * 1000)) || todayInputDate();
}

function dateInputValue(value: string | Date | null | undefined) {
  const parts = localDateParts(value);
  return parts ? `${parts.year}-${parts.month}-${parts.day}` : "";
}

function localDateParts(value: string | Date | null | undefined) {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(date.getTime())) return null;
  const parts = Object.fromEntries(REPORT_DATE_FORMATTER.formatToParts(date).map((part) => [part.type, part.value]));
  return parts.year && parts.month && parts.day ? { day: parts.day, month: parts.month, year: parts.year } : null;
}

function formatDate(value: string | null) {
  if (!value) return "未知时间";
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  }).format(new Date(value));
}

function browseSourcesFor(sources: Source[], summaryByMedia: Map<string, BrowseMediaSummary>, media: BrowseMedia) {
  const mediaSummary = summaryByMedia.get(media);
  const countsBySource = new Map((mediaSummary?.sources ?? []).map((source) => [source.source_id, source]));
  const matched = sources.filter((source) => source.media_type === media);
  const folderUnreadCounts = new Map<number, number>();
  for (const source of matched) {
    const key = source.folder_id ?? -1;
    folderUnreadCounts.set(key, (folderUnreadCounts.get(key) ?? 0) + (countsBySource.get(source.id)?.unread_count ?? 0));
  }
  return matched.map((source) => ({
    ...source,
    unread_count: countsBySource.get(source.id)?.unread_count ?? 0,
    folder_unread_count: folderUnreadCounts.get(source.folder_id ?? -1) ?? 0,
    all_unread_count: mediaSummary?.unread_count ?? 0
  }));
}

function latestFetchedAt(sources: Source[]) {
  return sources.reduce<string | null>((latest, source) => {
    if (!source.last_fetched_at) return latest;
    if (!latest) return source.last_fetched_at;
    return new Date(source.last_fetched_at).getTime() > new Date(latest).getTime() ? source.last_fetched_at : latest;
  }, null);
}

function isSeenStatus(status: string) {
  return status === "summary_seen" || status === "original_opened";
}

function clusterFilterQuery(filter: string) {
  return {
    read_status: filter === "unread" || filter === "dismissed" ? filter : undefined,
    read_later: filter === "read_later" ? "true" : undefined,
    starred: filter === "starred" ? "true" : undefined
  };
}

function clusterTitle(cluster: Cluster) {
  if (cluster.item_count > 1 && (cluster.generated_title || "").trim()) return cluster.generated_title;
  return cluster.title;
}

function clusterSummary(cluster: Cluster) {
  return cluster.generated_summary || clusterSourceSummary(cluster);
}

function clusterSourceSummary(cluster: Cluster) {
  const item = cluster.items?.[0];
  return previewText(item?.summary || item?.content_text || cluster.title);
}

function topicSummary(topic: TopicGroup) {
  const clusters = topic.clusters ?? [];
  if (!clusters.length) return `围绕「${topic.query}」的 Story Line 还没有匹配到事件。`;
  const first = clusters[0];
  const latest = latestTopicCluster(topic);
  const range = latest && first.first_seen_at !== latest.first_seen_at ? `${formatDate(first.first_seen_at)} 至 ${formatDate(latest.first_seen_at)}` : formatDate(first.first_seen_at);
  return `围绕「${topic.query}」已串联 ${clusters.length} 个事件，时间范围：${range}。当前阶段按关键词匹配 Event Cluster，后续再接入自动议题追踪。`;
}

function topicLatestProgress(topic: TopicGroup) {
  const latest = latestTopicCluster(topic);
  if (!latest) return "暂无最新进展。";
  return `${formatDate(latest.first_seen_at)}：${clusterTitle(latest)}\n\n${previewText(clusterSummary(latest))}`;
}

function latestTopicCluster(topic: TopicGroup) {
  const clusters = topic.clusters ?? [];
  return clusters.reduce<Cluster | null>((latest, cluster) => {
    if (!latest) return cluster;
    const latestTime = latest.first_seen_at ? new Date(latest.first_seen_at).getTime() : 0;
    const clusterTime = cluster.first_seen_at ? new Date(cluster.first_seen_at).getTime() : 0;
    return clusterTime >= latestTime ? cluster : latest;
  }, null);
}

function viewTitle(view: View) {
  if (view === "clusters") return "事件聚类";
  if (view === "topics") return "议题组";
  if (view === "reports") return "报告";
  if (view === "settings") return "设置";
  return "事件聚类";
}

function detailPlaceholder(view: View) {
  if (view === "settings") return "订阅管理已移到设置，中间栏完成添加 RSS、OPML 导入导出和刷新。";
  if (view === "clusters") return "选择一个事件聚类后在这里查看引用来源。";
  if (view === "topics") return "选择一个议题组后在这里查看按时间排列的故事线。";
  if (view === "reports") return "报告生成后会在这里展示引用的事件聚类和原文来源。";
  return "选择一个事件聚类后在这里阅读。";
}
