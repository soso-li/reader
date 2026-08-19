"use client";

import { AlertTriangle, FolderInput, Lock, Plus, Search, Upload, X } from "lucide-react";
import { type FormEvent, type KeyboardEvent, type MouseEvent, type ReactNode, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";

import { useActionDialog, useNativeModal } from "./action-dialog";
import Favicon from "./favicon";
import { BrowseImageCard, BrowseListRow, BrowseSocialCard, BrowseVideoCard, type BrowseCardItem } from "./browse-item-card";
import { isLegacySourceStatus, isSourcePaused } from "./source-lifecycle";
import { SOURCE_MEDIA_OPTIONS, type SourceMediaType } from "./source-media";
import SourceDetailDialog, { type SourceDetailFolder, type SourceDetailSource } from "./source-detail-dialog";
import { dialogControls, errorMessage, recentEntryLabel } from "./subscription-ui";

const STATUS_OPTIONS = [["active", "正式"], ["trial", "考察"]] as const;
const SOURCE_PRIVACY_OPTIONS = [["unclassified", "未分类"], ["public", "公开"], ["private", "私密"]] as const;
const TRIAL_ATTENTION_MS = 30 * 24 * 60 * 60 * 1000;

type SourcePrivacyClass = (typeof SOURCE_PRIVACY_OPTIONS)[number][0];
type Folder = SourceDetailFolder;
type Source = SourceDetailSource & {
  status_changed_at: string | null;
};
type BulkSet = {
  folder_id?: number | null;
  media_type?: SourceMediaType;
  status?: string;
  enabled?: boolean;
  privacy_class?: SourcePrivacyClass;
  external_generation_allowed?: boolean;
};
type Feedback = { kind: "error" | "success"; message: string } | null;
type AddContext = { folderId: number | null; mediaType: SourceMediaType; locked: boolean };
type FeedDiscovery = {
  candidates: Array<{ title: string; url: string }>;
  entries: Array<{
    title: string;
    summary: string;
    image_url: string;
    media_url: string;
    media_kind: string;
    media_duration: number;
    url: string;
    published_at: string | null;
  }>;
  site_url: string;
  title: string;
};

export function needsAttention(source: Pick<Source, "enabled" | "last_error" | "status" | "status_changed_at">, now = Date.now()) {
  if (source.last_error || isSourcePaused(source)) return true;
  if (source.status !== "trial" || !source.status_changed_at) return false;
  const changedAt = Date.parse(source.status_changed_at);
  return Number.isFinite(changedAt) && now - changedAt >= TRIAL_ATTENTION_MS;
}

export function sourceTypeCounts(sources: Source[]) {
  return Object.fromEntries(SOURCE_MEDIA_OPTIONS.map(({ value }) => [value, sources.filter((source) => source.media_type === value).length]));
}

export function isDesktopDragEnabled(width: number) {
  return width >= 1100;
}

export default function SubscriptionManager({
  folders,
  sources,
  themeControl
}: {
  folders: Folder[];
  sources: Source[];
  themeControl?: ReactNode;
}) {
  const router = useRouter();
  const [currentType, setCurrentType] = useState<SourceMediaType>("article");
  const [query, setQuery] = useState("");
  const [attentionOnly, setAttentionOnly] = useState(false);
  const [selectedIds, setSelectedIds] = useState<number[]>([]);
  const [selectedSourceId, setSelectedSourceId] = useState<number | null>(null);
  const [moveFolderId, setMoveFolderId] = useState("");
  const [bulkStatus, setBulkStatus] = useState("active");
  const [bulkPrivacy, setBulkPrivacy] = useState<SourcePrivacyClass>("unclassified");
  const [feedback, setFeedback] = useState<Feedback>(null);
  const [busy, setBusy] = useState(false);
  const [draggedSourceId, setDraggedSourceId] = useState<number | null>(null);
  const [desktopDrag, setDesktopDrag] = useState(false);
  const [dialogDirty, setDialogDirty] = useState(false);
  const [addContext, setAddContext] = useState<AddContext | null>(null);
  const [addUrl, setAddUrl] = useState("");
  const [discovery, setDiscovery] = useState<FeedDiscovery | null>(null);
  const [addType, setAddType] = useState<SourceMediaType>("article");
  const [addFolderId, setAddFolderId] = useState<number | null>(null);
  const [previewType, setPreviewType] = useState<SourceMediaType>("article");
  const [addFeedback, setAddFeedback] = useState<Feedback>(null);
  const [addCreated, setAddCreated] = useState(false);
  const actionDialog = useActionDialog();
  const mutationInFlightRef = useRef(false);
  const sourceButtonRefs = useRef(new Map<number, HTMLButtonElement>());
  const addTriggerRef = useRef<HTMLElement | null>(null);
  const topAddTriggerRef = useRef<HTMLButtonElement | null>(null);
  const addDialogRef = useRef<HTMLDivElement | null>(null);
  const addUrlRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    const update = () => setDesktopDrag(isDesktopDragEnabled(window.innerWidth));
    update();
    window.addEventListener("resize", update);
    return () => window.removeEventListener("resize", update);
  }, []);

  useEffect(() => {
    if (selectedSourceId !== null && !sources.some((source) => source.id === selectedSourceId)) setSelectedSourceId(null);
    setSelectedIds((ids) => ids.filter((id) => sources.some((source) => source.id === id)));
  }, [selectedSourceId, sources]);

  useEffect(() => {
    if (addContext) addUrlRef.current?.focus();
  }, [addContext]);

  const counts = useMemo(() => sourceTypeCounts(sources), [sources]);
  const typeSources = useMemo(() => sources.filter((source) => source.media_type === currentType), [currentType, sources]);
  const attentionCount = useMemo(() => typeSources.filter((source) => needsAttention(source)).length, [typeSources]);
  const visibleSources = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return typeSources.filter((source) => {
      if (attentionOnly && !needsAttention(source)) return false;
      return !needle || [source.name, source.url, source.site_url, domainLabel(source.url)].join(" ").toLowerCase().includes(needle);
    });
  }, [attentionOnly, query, typeSources]);
  const lanes = useMemo(() => {
    const real = folders.filter((folder) => folder.media_type === currentType).map((folder) => ({ folder, id: String(folder.id), name: folder.name, sources: visibleSources.filter((source) => source.folder_id === folder.id) }));
    return [...real, { folder: null, id: "uncategorized", name: "未分类", sources: visibleSources.filter((source) => source.folder_id === null) }];
  }, [currentType, folders, visibleSources]);
  const selectedSet = new Set(selectedIds);
  const selectedSources = sources.filter((source) => selectedSet.has(source.id));
  const selectedSource = sources.find((source) => source.id === selectedSourceId);
  const moveFolders = folders.filter((folder) => folder.media_type === currentType);
  const canBulkAllowExternal = selectedSources.length > 0 && selectedSources.every((source) => source.privacy_class === "public");

  async function confirmDialogDiscard() {
    return !dialogDirty || actionDialog.confirm({
      title: "放弃更改",
      message: "放弃未保存的更改？",
      confirmLabel: "放弃更改",
      cancelLabel: "继续编辑",
      danger: true
    });
  }

  function openAdd(context: AddContext, trigger: HTMLElement) {
    if (busy || selectedSourceId !== null) return;
    addTriggerRef.current = trigger;
    setAddContext(context);
    setAddUrl("");
    setDiscovery(null);
    setAddType(context.mediaType);
    setAddFolderId(context.folderId);
    setPreviewType(context.mediaType);
    setAddFeedback(null);
    setAddCreated(false);
  }

  function closeAdd() {
    if (busy) return;
    setAddContext(null);
    setAddFeedback(null);
    window.setTimeout(() => addTriggerRef.current?.focus(), 0);
  }

  function changeAddType(nextType: SourceMediaType) {
    if (busy || addContext?.locked) return;
    setAddType(nextType);
    setAddFolderId(null);
    setPreviewType(nextType);
    setAddFeedback(null);
  }

  async function discoverFeed(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const url = addUrl.trim();
    if (busy || !url) return;
    if (!isHttpUrl(url)) {
      setAddFeedback({ kind: "error", message: "请输入以 http:// 或 https:// 开头的有效 Feed 地址" });
      return;
    }
    setAddFeedback(null);
    setDiscovery(null);
    setAddCreated(false);
    try {
      const result = await runMutation(() => requestJson<FeedDiscovery>("/api/sources/discover", { method: "POST", body: JSON.stringify({ url }) }));
      const candidate = result.candidates[0];
      if (!candidate) throw new Error("未发现可添加的 Feed");
      setDiscovery(result);
      setAddUrl(candidate.url);
    } catch (error) {
      setAddFeedback({ kind: "error", message: errorMessage(error) });
    }
  }

  function validateOpml(event: FormEvent<HTMLFormElement>) {
    const file = event.currentTarget.elements.namedItem("file");
    if (file instanceof HTMLInputElement && file.files?.length) return;
    event.preventDefault();
    setAddFeedback({ kind: "error", message: "请选择一个 OPML 文件" });
  }

  async function createSource() {
    if (busy || !discovery || addCreated) return;
    const candidate = discovery.candidates[0];
    if (!candidate) return;
    setAddFeedback(null);
    try {
      await runMutation(() => requestJson("/api/sources", {
        method: "POST",
        body: JSON.stringify({
          name: discovery.title || candidate.title || sourceNameFallback(candidate.url),
          url: candidate.url,
          folder_id: addFolderId,
          media_type: addType,
          status: "trial"
        })
      }));
      setAddCreated(true);
      setAddFeedback({ kind: "success", message: "已添加订阅源" });
      router.refresh();
    } catch (error) {
      setAddFeedback({ kind: "error", message: errorMessage(error) });
    }
  }

  function handleAddKeyDown(event: KeyboardEvent<HTMLDialogElement>) {
    if (event.key === "Escape") {
      event.preventDefault();
      closeAdd();
      return;
    }
    if (event.key !== "Tab") return;
    const controls = dialogControls(addDialogRef.current);
    if (!controls.length) return;
    const first = controls[0];
    const last = controls[controls.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }

  function handleAddOverlayMouseDown(event: MouseEvent<HTMLDialogElement>) {
    if (event.target === event.currentTarget) closeAdd();
  }

  async function switchType(mediaType: SourceMediaType) {
    if (mediaType === currentType || !(await confirmDialogDiscard())) return;
    if (selectedSourceId !== null) {
      setDialogDirty(false);
      setSelectedSourceId(null);
    }
    setCurrentType(mediaType);
    setSelectedIds([]);
    setAttentionOnly(false);
    setFeedback(null);
    setMoveFolderId("");
  }

  async function runMutation<T>(task: () => Promise<T>) {
    if (mutationInFlightRef.current) throw new Error("操作进行中");
    mutationInFlightRef.current = true;
    setBusy(true);
    try {
      return await task();
    } finally {
      mutationInFlightRef.current = false;
      setBusy(false);
    }
  }

  async function mutate<T>(task: () => Promise<T>, successMessage: string) {
    if (mutationInFlightRef.current) return null;
    setFeedback(null);
    try {
      const result = await runMutation(task);
      setFeedback({ kind: "success", message: successMessage });
      router.refresh();
      return result;
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
      return null;
    }
  }

  function requestForDialog<T>(path: string, init: RequestInit): Promise<T> {
    return runMutation(() => requestJson<T>(path, init));
  }

  function selectLane(ids: number[]) {
    setSelectedIds((current) => Array.from(new Set([...current, ...ids])));
    setFeedback(null);
  }

  async function toggleCard(sourceId: number, trigger: HTMLButtonElement) {
    if (busy) return;
    if (!selectedIds.length) {
      if (selectedSourceId !== null && selectedSourceId !== sourceId && !(await confirmDialogDiscard())) return;
      sourceButtonRefs.current.set(sourceId, trigger);
      setSelectedSourceId(sourceId);
      setFeedback(null);
      return;
    }
    setSelectedIds((ids) => ids.includes(sourceId) ? ids.filter((id) => id !== sourceId) : [...ids, sourceId]);
  }

  function updateSource(sourceId: number, changes: BulkSet, successMessage: string) {
    if (busy) return;
    return mutate(async () => {
      await requestJson(`/api/sources/${sourceId}`, { method: "PATCH", body: JSON.stringify(changes) });
    }, successMessage);
  }

  function applyBulk(changes: BulkSet, successMessage: string) {
    if (busy || !selectedIds.length) return;
    void mutate(async () => {
      await requestJson("/api/sources/bulk", { method: "POST", body: JSON.stringify({ ids: selectedIds, set: changes }) });
      setSelectedIds([]);
    }, successMessage);
  }

  async function createFolder() {
    if (busy) return;
    const name = await actionDialog.prompt({
      title: `新建${mediaLabel(currentType)}文件夹`,
      message: "输入新文件夹名称。",
      inputLabel: "文件夹名称",
      confirmLabel: "新建"
    });
    if (!name?.trim()) return;
    void mutate(async () => {
      await requestJson("/api/folders", { method: "POST", body: JSON.stringify({ name: name.trim(), media_type: currentType }) });
    }, "已新建文件夹");
  }

  async function renameFolder(folder: Folder) {
    if (busy) return;
    const name = await actionDialog.prompt({
      title: "重命名文件夹",
      message: `为“${folder.name}”输入新名称。`,
      inputLabel: "文件夹名称",
      defaultValue: folder.name,
      confirmLabel: "保存"
    });
    if (!name?.trim() || name.trim() === folder.name) return;
    void mutate(async () => {
      await requestJson(`/api/folders/${folder.id}`, { method: "PATCH", body: JSON.stringify({ name: name.trim() }) });
    }, "已重命名文件夹");
  }

  async function deleteFolder(folder: Folder) {
    if (busy) return;
    const folderSources = sources.filter((source) => source.folder_id === folder.id);
    if (!(await actionDialog.confirm({
      title: "删除文件夹",
      message: folderSources.length ? `将 ${folderSources.length} 个来源移至未分类后删除“${folder.name}”？` : `删除空文件夹“${folder.name}”？`,
      confirmLabel: "删除文件夹",
      danger: true
    }))) return;
    void mutate(async () => {
      if (folderSources.length) await requestJson("/api/sources/bulk", { method: "POST", body: JSON.stringify({ ids: folderSources.map((source) => source.id), set: { folder_id: null } }) });
      await requestJson(`/api/folders/${folder.id}`, { method: "DELETE" });
    }, folderSources.length ? "来源已移至未分类，文件夹已删除" : "已删除文件夹");
  }

  function restoreSelected() {
    if (busy || !selectedIds.length) return;
    const legacyIds = selectedSources.filter(isLegacySourceStatus).map((source) => source.id);
    const otherIds = selectedSources.filter((source) => !isLegacySourceStatus(source)).map((source) => source.id);
    void mutate(async () => {
      if (otherIds.length) await requestJson("/api/sources/bulk", { method: "POST", body: JSON.stringify({ ids: otherIds, set: { enabled: true } }) });
      if (legacyIds.length) await requestJson("/api/sources/bulk", { method: "POST", body: JSON.stringify({ ids: legacyIds, set: { enabled: true, status: "active" } }) });
      setSelectedIds([]);
    }, "已恢复抓取");
  }

  function dropIntoFolder(folderId: number | null) {
    if (busy) return;
    const source = sources.find((item) => item.id === draggedSourceId);
    setDraggedSourceId(null);
    if (!source || source.folder_id === folderId) return;
    void updateSource(source.id, { folder_id: folderId }, "已移动来源");
  }

  function dropIntoType(mediaType: SourceMediaType) {
    if (busy) return;
    const source = sources.find((item) => item.id === draggedSourceId);
    setDraggedSourceId(null);
    if (!source || source.media_type === mediaType) return;
    void updateSource(source.id, { media_type: mediaType, folder_id: null }, `已改为${mediaLabel(mediaType)}并移至未分类`);
  }

  return (
    <section id="settings-subscriptions" className="settings-block subscription-manager subscription-lanes">
      <div className="subscription-manager-toolbar">
        <label className="subscription-search">
          <Search size={16} />
          <input aria-label="搜索订阅源" name="source_search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder={`搜索${mediaLabel(currentType)}来源`} />
        </label>
        <button aria-pressed={attentionOnly} className={attentionOnly ? "active" : ""} type="button" onClick={() => { setAttentionOnly((value) => !value); setFeedback(null); }}>
          只看需处理 ({attentionCount})
        </button>
        <button ref={topAddTriggerRef} className="subscription-add-source" disabled={busy || selectedSourceId !== null} type="button" onClick={(event) => openAdd({ folderId: null, mediaType: currentType, locked: false }, event.currentTarget)}><Plus size={16} /> 添加订阅源</button>
        {themeControl ? <div className="subscription-theme-control">{themeControl}</div> : null}
      </div>

      <div className="subscription-type-tabs" aria-label="来源类型">
        {SOURCE_MEDIA_OPTIONS.map(({ value: mediaType, label }) => {
          const hasError = sources.some((source) => source.media_type === mediaType && Boolean(source.last_error));
          return (
            <button
              key={mediaType}
              aria-pressed={currentType === mediaType}
              className={currentType === mediaType ? "active" : ""}
              disabled={busy}
              type="button"
              onClick={() => void switchType(mediaType)}
              onDragOver={desktopDrag && !busy ? (event) => event.preventDefault() : undefined}
              onDrop={desktopDrag && !busy ? () => dropIntoType(mediaType) : undefined}
            >
              {label}<span>{counts[mediaType] ?? 0}</span>{hasError ? <AlertTriangle aria-label={`${label}有抓取错误`} size={13} /> : null}
            </button>
          );
        })}
      </div>

      {feedback ? <p className={feedback.kind === "error" ? "error-line" : "status-line success-line"} role={feedback.kind === "error" ? "alert" : "status"}>{feedback.message}</p> : null}

      <div className="subscription-lane-board" aria-label={`${mediaLabel(currentType)}文件夹泳道`}>
        {lanes.map((lane) => (
          <section
            key={lane.id}
            className="subscription-lane"
            onDragOver={desktopDrag && !busy ? (event) => event.preventDefault() : undefined}
            onDrop={desktopDrag && !busy ? () => dropIntoFolder(lane.folder?.id ?? null) : undefined}
          >
            <header className="subscription-lane-header">
              <div><strong>{lane.name}</strong><span>{lane.sources.length} 个来源</span></div>
              <details>
                <summary aria-label={`${lane.name}操作`}>•••</summary>
                <div className="subscription-lane-menu">
                  <button disabled={busy} type="button" onClick={() => selectLane(lane.sources.map((source) => source.id))}>全选本列</button>
                  {lane.folder ? <button disabled={busy} type="button" onClick={() => void renameFolder(lane.folder!)}>重命名</button> : null}
                  {lane.folder ? <button disabled={busy} type="button" onClick={() => void deleteFolder(lane.folder!)}>删除文件夹</button> : null}
                </div>
              </details>
            </header>
            <div className="subscription-lane-cards">
              {lane.sources.map((source) => (
                <article key={source.id} className={`subscription-lane-card ${cardStateClass(source)} ${selectedSet.has(source.id) ? "selected" : ""}`} draggable={desktopDrag && !busy} onDragStart={desktopDrag && !busy ? () => setDraggedSourceId(source.id) : undefined}>
                  <button ref={(element) => { if (element) sourceButtonRefs.current.set(source.id, element); else sourceButtonRefs.current.delete(source.id); }} aria-pressed={selectedIds.length ? selectedSet.has(source.id) : undefined} className="subscription-card-main" data-source-card-id={source.id} disabled={busy} type="button" onClick={(event) => void toggleCard(source.id, event.currentTarget)}>
                    <span className="subscription-card-title"><Favicon url={source.site_url || source.url} label={source.name} /> <strong>{source.name}</strong></span>
                    <span className="subscription-card-meta">{domainLabel(source.url)} · {recentEntryLabel(source.recent_entry_count_30d)}</span>
                    <CardIndicators source={source} />
                  </button>
                  <button
                    aria-checked={!isSourcePaused(source)}
                    aria-label={`${source.name}${isSourcePaused(source) ? "恢复抓取" : "暂停抓取"}`}
                    className="subscription-card-switch"
                    disabled={busy}
                    role="switch"
                    type="button"
                    onClick={() => void updateSource(source.id, isSourcePaused(source) ? (isLegacySourceStatus(source) ? { status: "active", enabled: true } : { enabled: true }) : { enabled: false }, isSourcePaused(source) ? "已恢复抓取" : "已暂停抓取")}
                  >
                    <span aria-hidden="true" />
                  </button>
                </article>
              ))}
              {!lane.sources.length ? <p className="subscription-lane-empty">没有匹配的来源</p> : null}
            </div>
            <button className="subscription-lane-add" disabled={busy || selectedSourceId !== null} type="button" onClick={(event) => openAdd({ folderId: lane.folder?.id ?? null, mediaType: currentType, locked: true }, event.currentTarget)}><Plus size={15} /> 添加订阅源</button>
          </section>
        ))}
        <button className="subscription-new-lane" disabled={busy} type="button" onClick={() => void createFolder()}><Plus size={16} /> 新建文件夹</button>
      </div>

      {selectedIds.length ? (
        <div className="subscription-bulk-bar">
          <strong>已选 {selectedIds.length}</strong>
          <label><FolderInput size={14} /><select aria-label="移动到文件夹" value={moveFolderId} onChange={(event) => setMoveFolderId(event.target.value)}><option value="">移动到未分类</option>{moveFolders.map((folder) => <option key={folder.id} value={folder.id}>{folder.name}</option>)}</select></label>
          <button disabled={busy} type="button" onClick={() => applyBulk({ folder_id: moveFolderId ? Number(moveFolderId) : null }, "已移动来源")}>移动</button>
          <button disabled={busy} type="button" onClick={() => applyBulk({ enabled: false }, "已暂停抓取")}>暂停抓取</button>
          <button disabled={busy} type="button" onClick={restoreSelected}>恢复抓取</button>
          <details>
            <summary>更多操作</summary>
            <div className="subscription-bulk-more">
              <label>状态<select aria-label="批量状态" value={bulkStatus} onChange={(event) => setBulkStatus(event.target.value)}>{STATUS_OPTIONS.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
              <button disabled={busy} type="button" onClick={() => applyBulk({ status: bulkStatus }, "已更新状态")}>改状态</button>
              <label>隐私<select aria-label="批量来源隐私分类" value={bulkPrivacy} onChange={(event) => setBulkPrivacy(event.target.value as SourcePrivacyClass)}>{SOURCE_PRIVACY_OPTIONS.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
              <button disabled={busy} type="button" onClick={() => applyBulk({ privacy_class: bulkPrivacy }, "已更新隐私")}>改隐私</button>
              <button disabled={busy || !canBulkAllowExternal} type="button" onClick={() => applyBulk({ external_generation_allowed: true }, "已允许外发")}>允许外发</button>
              <button disabled={busy} type="button" onClick={() => applyBulk({ external_generation_allowed: false }, "已禁止外发")}>禁止外发</button>
            </div>
          </details>
          <button disabled={busy} type="button" onClick={() => setSelectedIds([])}>取消</button>
        </div>
      ) : null}

      {selectedSource ? <SourceDetailDialog key={selectedSource.id} busy={busy} folders={folders} onClose={(sourceId) => { setDialogDirty(false); setSelectedSourceId(null); window.setTimeout(() => (sourceButtonRefs.current.get(sourceId) ?? document.querySelector<HTMLButtonElement>(`[data-source-card-id="${sourceId}"]`))?.focus(), 0); }} onDeleted={() => { setDialogDirty(false); setSelectedSourceId(null); setFeedback({ kind: "success", message: "已永久删除订阅源" }); router.refresh(); window.setTimeout(() => topAddTriggerRef.current?.focus(), 0); }} onDirtyChange={setDialogDirty} onRefresh={() => router.refresh()} onSaved={(updated) => setCurrentType(updated.media_type)} request={requestForDialog} source={selectedSource} /> : null}
      {addContext ? <SourceAddDialog addCreated={addCreated} addFolderId={addFolderId} addType={addType} busy={busy} context={addContext} discovery={discovery} folders={folders} onChangeFolder={setAddFolderId} onChangeType={changeAddType} onClose={closeAdd} onCreate={() => void createSource()} onDiscover={(event) => void discoverFeed(event)} onInputChange={(value) => { setAddUrl(value); setDiscovery(null); setAddCreated(false); setAddFeedback(null); }} onKeyDown={handleAddKeyDown} onOpmlSubmit={validateOpml} onOverlayMouseDown={handleAddOverlayMouseDown} previewType={previewType} setPreviewType={setPreviewType} feedback={addFeedback} dialogRef={addDialogRef} url={addUrl} urlRef={addUrlRef} /> : null}
      {actionDialog.dialog}
    </section>
  );
}

function SourceAddDialog({
  addCreated,
  addFolderId,
  addType,
  busy,
  context,
  dialogRef,
  discovery,
  feedback,
  folders,
  onChangeFolder,
  onChangeType,
  onClose,
  onCreate,
  onDiscover,
  onInputChange,
  onKeyDown,
  onOpmlSubmit,
  onOverlayMouseDown,
  previewType,
  setPreviewType,
  url,
  urlRef
}: {
  addCreated: boolean;
  addFolderId: number | null;
  addType: SourceMediaType;
  busy: boolean;
  context: AddContext;
  dialogRef: { current: HTMLDivElement | null };
  discovery: FeedDiscovery | null;
  feedback: Feedback;
  folders: Folder[];
  onChangeFolder: (folderId: number | null) => void;
  onChangeType: (mediaType: SourceMediaType) => void;
  onClose: () => void;
  onCreate: () => void;
  onDiscover: (event: FormEvent<HTMLFormElement>) => void;
  onInputChange: (value: string) => void;
  onKeyDown: (event: KeyboardEvent<HTMLDialogElement>) => void;
  onOpmlSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onOverlayMouseDown: (event: MouseEvent<HTMLDialogElement>) => void;
  previewType: SourceMediaType;
  setPreviewType: (mediaType: SourceMediaType) => void;
  url: string;
  urlRef: { current: HTMLInputElement | null };
}) {
  const folderOptions = folders.filter((folder) => folder.media_type === addType);
  const modalRef = useNativeModal();
  const candidate = discovery?.candidates[0];
  const discoveredName = discovery && candidate ? discovery.title || candidate.title || sourceNameFallback(candidate.url) : "";
  const folderName = addFolderId === null ? "未分类" : folderOptions.find((folder) => folder.id === addFolderId)?.name ?? "未分类";

  return (
    <dialog ref={modalRef} className="source-detail-overlay source-add-overlay" role="dialog" aria-modal="true" aria-labelledby="source-add-title" onCancel={(event) => { event.preventDefault(); onClose(); }} onKeyDown={onKeyDown} onMouseDown={onOverlayMouseDown}>
      <div ref={dialogRef} className="source-detail-dialog source-add-dialog">
        <header className="source-detail-header">
          <div><h2 id="source-add-title">添加订阅源</h2><p>{context.locked ? `已锁定到${mediaLabel(context.mediaType)} · ${context.folderId === null ? "未分类" : folders.find((folder) => folder.id === context.folderId)?.name ?? "未分类"}` : "仅支持直接可访问的 Feed 地址"}</p></div>
          <button aria-label="关闭添加订阅源" disabled={busy} type="button" onClick={onClose}><X size={18} /></button>
        </header>
        <div className="source-add-body">
          <form className="source-add-read-form" noValidate onSubmit={onDiscover}>
            <label>RSS/Atom/JSON/Newsletter Feed 地址<input ref={urlRef} name="feed_url" disabled={busy} inputMode="url" placeholder="https://example.com/feed.xml" required type="url" value={url} onChange={(event) => onInputChange(event.target.value)} /></label>
            <button className="primary" disabled={busy || !url.trim()} type="submit">读取 Feed</button>
          </form>

          <form className="source-add-opml" action="/actions/import-opml" method="post" encType="multipart/form-data" noValidate onSubmit={onOpmlSubmit}>
            <label>OPML 文件<input disabled={busy} name="file" type="file" accept=".opml,.xml,text/xml" required /></label>
            <button disabled={busy} type="submit"><Upload size={15} /> 导入 OPML</button>
          </form>

          {discovery && candidate ? (
            <section className="source-add-result" aria-label="Feed 发现结果">
              <dl className="source-add-values"><div><dt>发现名称</dt><dd>{discoveredName}</dd></div><div><dt>状态</dt><dd>考察</dd></div></dl>
              <div className="source-add-fields">
                {context.locked ? <p className="source-add-locked-field"><span>类型</span><strong>{mediaLabel(addType)}</strong></p> : <label>类型<select name="add_media_type" disabled={busy || addCreated} value={addType} onChange={(event) => onChangeType(event.target.value as SourceMediaType)}>{SOURCE_MEDIA_OPTIONS.map(({ value, label }) => <option key={value} value={value}>{label}</option>)}</select></label>}
                {context.locked ? <p className="source-add-locked-field"><span>文件夹</span><strong>{folderName}</strong></p> : <label>文件夹<select name="add_folder_id" disabled={busy || addCreated} value={addFolderId ?? ""} onChange={(event) => onChangeFolder(event.target.value ? Number(event.target.value) : null)}><option value="">未分类</option>{folderOptions.map((folder) => <option key={folder.id} value={folder.id}>{folder.name}</option>)}</select></label>}
              </div>
              <FeedPreview busy={busy} discovery={discovery} previewType={previewType} setPreviewType={setPreviewType} />
              <button className="source-add-create primary" disabled={busy || addCreated} type="button" onClick={onCreate}>确认添加</button>
            </section>
          ) : null}
          <footer className="source-add-footer">{feedback ? <p className={feedback.kind === "error" ? "error-line" : "status-line success-line"} role={feedback.kind === "error" ? "alert" : "status"}>{feedback.message}</p> : <span>输入地址后点击“读取 Feed”，不会自动请求。</span>}</footer>
        </div>
      </div>
    </dialog>
  );
}

function FeedPreview({ busy, discovery, previewType, setPreviewType }: { busy: boolean; discovery: FeedDiscovery; previewType: SourceMediaType; setPreviewType: (mediaType: SourceMediaType) => void }) {
  const sourceName = discovery.title || discovery.candidates[0]?.title || sourceNameFallback(discovery.candidates[0]?.url ?? "");
  const items: BrowseCardItem[] = discovery.entries.map((entry, index) => ({
    id: index + 1,
    source_name: sourceName,
    source_site_url: discovery.site_url,
    title: entry.title,
    title_translation: "",
    summary: entry.summary,
    summary_translation: "",
    image_url: entry.image_url,
    media_url: entry.media_url,
    media_kind: entry.media_kind,
    media_duration: entry.media_duration,
    content_text: entry.summary,
    url: entry.url,
    published_at: entry.published_at,
    read_status: "unread",
    read_later: false,
    starred: false,
    filtered: false,
    filter_rules: []
  }));
  return (
    <section className="source-add-preview" aria-label="展示预览">
      <div className="source-add-preview-heading"><h3>展示预览</h3><span>最近 {items.length} 条</span></div>
      <div className="source-add-preview-tabs" aria-label="预览类型">
        {SOURCE_MEDIA_OPTIONS.map(({ value: mediaType, label }) => <button key={mediaType} aria-pressed={previewType === mediaType} className={previewType === mediaType ? "active" : ""} disabled={busy} type="button" onClick={() => setPreviewType(mediaType)}>{label}</button>)}
      </div>
      {!items.length ? <p className="subscription-lane-empty">Feed 没有可预览的条目。</p> : <PreviewItems items={items} mediaType={previewType} />}
    </section>
  );
}

function PreviewItems({ items, mediaType }: { items: BrowseCardItem[]; mediaType: SourceMediaType }) {
  if (mediaType === "social") return <div className="browse-social-feed source-add-preview-social">{items.map((item) => <BrowseSocialCard key={item.id} emptyMediaLabel="无图片" item={item} staticPreview />)}</div>;
  if (mediaType === "image") return <div className="browse-image-grid layout-grid source-add-preview-grid">{items.map((item) => <BrowseImageCard key={item.id} item={item} staticPreview />)}</div>;
  if (mediaType === "video") return <div className="browse-video-grid source-add-preview-grid">{items.map((item) => <BrowseVideoCard key={item.id} item={item} staticPreview />)}</div>;
  return <div className="source-add-preview-list">{items.map((item) => <BrowseListRow key={item.id} emptyMediaLabel={mediaType === "podcast" ? "无音频" : undefined} item={item} requiredMediaKind={mediaType === "podcast" ? "audio" : undefined} showThumbnail={Boolean(item.image_url || item.media_kind === "image")} staticPreview />)}</div>;
}

function CardIndicators({ source }: { source: Source }) {
  const status = source.last_error
    ? <span className="subscription-card-indicator error"><AlertTriangle size={13} /> 抓取错误</span>
    : isSourcePaused(source)
      ? <span className="subscription-card-indicator paused">已暂停抓取</span>
      : source.status === "trial" ? <span className="subscription-card-indicator trial">考察</span> : null;
  const privateLock = source.privacy_class === "private" ? <Lock aria-label="私密来源" size={14} /> : null;
  return status || privateLock ? <span className="subscription-card-indicators">{status}{privateLock}</span> : null;
}

function cardStateClass(source: Source) {
  if (source.last_error) return "is-error";
  if (isSourcePaused(source)) return "is-paused";
  return source.status === "trial" ? "is-trial" : "";
}

async function requestJson<T = unknown>(path: string, init: RequestInit): Promise<T> {
  const response = await fetch(path, { ...init, headers: { "Content-Type": "application/json", ...init.headers } });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(typeof payload.detail === "string" ? payload.detail : typeof payload.error === "string" ? payload.error : "操作失败");
  return payload as T;
}

function mediaLabel(mediaType: SourceMediaType) {
  return SOURCE_MEDIA_OPTIONS.find(({ value }) => value === mediaType)?.label ?? "文章";
}

function domainLabel(value: string) {
  try {
    return new URL(value).hostname.replace(/^www\./, "");
  } catch {
    return value;
  }
}

function sourceNameFallback(value: string) {
  try {
    return new URL(value).hostname.replace(/^www\./, "") || "未命名来源";
  } catch {
    return "未命名来源";
  }
}

function isHttpUrl(value: string) {
  try {
    return ["http:", "https:"].includes(new URL(value).protocol);
  } catch {
    return false;
  }
}
