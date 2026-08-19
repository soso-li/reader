"use client";

import { ExternalLink, Pencil, RefreshCw, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { FormEvent, KeyboardEvent, MouseEvent } from "react";

import { useActionDialog, useNativeModal } from "./action-dialog";
import { isLegacySourceStatus, isSourcePaused } from "./source-lifecycle";
import { SOURCE_MEDIA_OPTIONS, type SourceMediaType } from "./source-media";
import { dialogControls, errorMessage, recentEntryLabel } from "./subscription-ui";
import { formatExactTime } from "./time-format";

const PRIVACY_OPTIONS = [["unclassified", "未分类"], ["public", "公开"], ["private", "私密"]] as const;
type SourcePrivacyClass = (typeof PRIVACY_OPTIONS)[number][0];

export type SourceDetailSource = {
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
  privacy_class: SourcePrivacyClass;
  external_generation_allowed: boolean;
  feed_trust_score: number;
  fetched_count: number;
  read_count: number;
  opened_count: number;
  starred_count: number;
  read_later_count: number;
  cluster_count: number;
  duplicate_count: number;
  recent_entry_count_30d: number;
  last_fetched_at: string | null;
  last_error: string;
};

export type SourceDetailFolder = { id: number; name: string; media_type: SourceMediaType };
type Draft = {
  name: string;
  url: string;
  folder_id: string;
  media_type: SourceMediaType;
  status: "active" | "trial";
  enabled: boolean;
  fetch_full_content: boolean;
  article_selector: string;
  remove_selector: string;
  privacy_class: SourcePrivacyClass;
  external_generation_allowed: boolean;
};
type Feedback = { kind: "error" | "success"; message: string } | null;
type PublicRulesStatus = {
  version: string;
  commit: string;
  activated_at: string | null;
  bundled: boolean;
};
type ArticlePreviewOptions = {
  entries: Array<{
    raw_entry_id: number;
    title: string;
    published_at: string | null;
  }>;
  public_rules: PublicRulesStatus;
};
type ExtractionPreview = {
  title: string;
  reading_html: string;
  rss_characters: number;
  webpage_characters: number;
  method: string;
  version: string;
  adopted_webpage: boolean;
  matched_elements: number;
  removed_elements: number;
  diagnostics: string[];
  fallback_reason: string;
};
type ArticlePreview = ExtractionPreview & {
  raw_entry_id: number;
  body_source: "rss" | "webpage";
  web_fetch_status: "not_requested" | "failed" | "succeeded";
};
type PublicRulesCheck = {
  current_version: string;
  current_commit: string;
  candidate_version: string;
  candidate_commit: string;
  rules_count: number;
  skipped_count: number;
  subscribed_domains: number;
  covered_subscribed_domains: number;
  changed_subscribed_domains: number;
  tested_subscribed_domains: number;
  invalid_subscribed_domains: string[];
  failed_subscribed_domains: string[];
  preview: (ExtractionPreview & { hostname: string; passed: boolean }) | null;
  passed: boolean;
  can_activate: boolean;
};

export function friendlyFetchError(raw: string) {
  const value = raw.toLowerCase();
  if (/dns|enotfound|getaddrinfo|name or service|nodename nor servname|temporary failure in name resolution/.test(value)) return "无法解析来源域名";
  if (/timeout|timed out|超时/.test(value)) return "抓取超时";
  if (/\b404\b|not found/.test(value)) return "来源地址不存在（404）";
  if (/certificate|cert |ssl|tls/.test(value)) return "来源证书验证失败";
  if (/feed|rss|atom|xml|parse|format|格式/.test(value)) return "Feed 格式无效";
  return "抓取失败";
}

export function selectorSyntaxError(selector: string) {
  const raw = selector.trim();
  if (!raw || typeof document === "undefined") return "";
  const xpath = raw.startsWith("xpath:");
  const value = raw.replace(/^(?:css:|xpath:)/, "").trim();
  if (!value) return "选择器不能为空";
  try {
    if (!xpath) {
      document.createElement("div").querySelector(value);
      return "";
    }
    const withoutStrings = value.replace(/'[^']*'|"[^"]*"/g, "");
    if (/\b[A-Za-z_][\w.-]*:(?!:)/.test(withoutStrings)) return "XPath 扩展与命名空间前缀不受支持";
    if (/^(?:boolean|count|false|number|string|string-length|sum|true)\s*\(/.test(withoutStrings)
      || /(?:\/@|::attribute|\/text\(\)|\/comment\(\)|\/processing-instruction\()/.test(withoutStrings)) {
      return "XPath 选择器只能返回元素节点";
    }
    document.evaluate(value, document, null, XPathResult.ANY_TYPE, null);
    return "";
  } catch {
    return `${xpath ? "XPath" : "CSS"} 选择器语法无效`;
  }
}

export default function SourceDetailDialog({
  source,
  folders,
  busy,
  onClose,
  onDeleted,
  onDirtyChange,
  onRefresh,
  onSaved,
  request
}: {
  source: SourceDetailSource;
  folders: SourceDetailFolder[];
  busy: boolean;
  onClose: (sourceId: number) => void;
  onDeleted: (source: SourceDetailSource) => void;
  onDirtyChange: (dirty: boolean) => void;
  onRefresh: () => void;
  onSaved: (source: SourceDetailSource) => void;
  request: <T>(path: string, init: RequestInit) => Promise<T>;
}) {
  const dialogRef = useRef<HTMLDivElement | null>(null);
  const modalRef = useNativeModal();
  const nameInputRef = useRef<HTMLInputElement | null>(null);
  const addressInputRef = useRef<HTMLInputElement | null>(null);
  const sourceIdRef = useRef(source.id);
  const draftRef = useRef(draftFromSource(source));
  const dirtyRef = useRef(false);
  const [draft, setDraft] = useState(() => draftFromSource(source));
  const [baseline, setBaseline] = useState(() => draftFromSource(source));
  const [feedback, setFeedback] = useState<Feedback>(null);
  const [previewOptions, setPreviewOptions] = useState<ArticlePreviewOptions | null>(null);
  const [previewRawEntryId, setPreviewRawEntryId] = useState("");
  const [articlePreview, setArticlePreview] = useState<ArticlePreview | null>(null);
  const [extractionError, setExtractionError] = useState("");
  const [rulesCheck, setRulesCheck] = useState<PublicRulesCheck | null>(null);
  const actionDialog = useActionDialog();
  const dirty = !sameDraft(draft, baseline);
  const folderOptions = folders.filter((folder) => folder.media_type === draft.media_type);
  const articleSelectorError = selectorSyntaxError(draft.article_selector);
  const removeSelectorError = selectorSyntaxError(draft.remove_selector);
  const selectorError = articleSelectorError || removeSelectorError;

  useEffect(() => {
    const sourceChanged = source.id !== sourceIdRef.current;
    if (sourceChanged || !dirtyRef.current) {
      const next = draftFromSource(source);
      sourceIdRef.current = source.id;
      draftRef.current = next;
      dirtyRef.current = false;
      setDraft(next);
      setBaseline(next);
      setFeedback(null);
      setPreviewOptions(null);
      setPreviewRawEntryId("");
      setArticlePreview(null);
      setExtractionError("");
      setRulesCheck(null);
      onDirtyChange(false);
      window.setTimeout(() => nameInputRef.current?.focus(), 0);
    }
  }, [onDirtyChange, source]);

  useEffect(() => {
    const nextDirty = !sameDraft(draft, baseline);
    dirtyRef.current = nextDirty;
    onDirtyChange(nextDirty);
  }, [baseline, draft, onDirtyChange]);

  function changeDraft(changes: Partial<Draft>) {
    if (busy) return;
    const next = { ...draftRef.current, ...changes };
    draftRef.current = next;
    setDraft(next);
    setFeedback(null);
    if ("fetch_full_content" in changes || "article_selector" in changes || "remove_selector" in changes) {
      setArticlePreview(null);
      setExtractionError("");
    }
  }

  async function close() {
    if (busy) return;
    if (dirtyRef.current && !(await actionDialog.confirm({
      title: "放弃更改",
      message: "放弃未保存的更改？",
      confirmLabel: "放弃更改",
      cancelLabel: "继续编辑",
      danger: true
    }))) return;
    onDirtyChange(false);
    onClose(source.id);
  }

  function handleKeyDown(event: KeyboardEvent<HTMLDialogElement>) {
    if (event.key === "Escape") {
      event.preventDefault();
      void close();
      return;
    }
    if (event.key !== "Tab") return;
    const controls = dialogControls(dialogRef.current);
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

  function handleOverlayMouseDown(event: MouseEvent<HTMLDialogElement>) {
    if (event.target === event.currentTarget) void close();
  }

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (busy) return;
    if (selectorError) {
      setFeedback({ kind: "error", message: selectorError });
      return;
    }
    setFeedback(null);
    try {
      const lifecycleChanged = draft.status !== baseline.status || draft.enabled !== baseline.enabled;
      const lifecycle = isLegacySourceStatus(source)
        ? (lifecycleChanged ? { status: draft.status, enabled: draft.enabled } : {})
        : {
            ...(draft.status !== baseline.status ? { status: draft.status } : {}),
            ...(draft.enabled !== baseline.enabled ? { enabled: draft.enabled } : {})
          };
      const sourceChanges: Partial<Draft> = { ...draft };
      delete sourceChanges.status;
      delete sourceChanges.enabled;
      const updated = await request<SourceDetailSource>(`/api/sources/${source.id}`, {
        method: "PATCH",
        body: JSON.stringify({
          ...sourceChanges,
          ...lifecycle,
          article_selector: draft.article_selector.trim() || null,
          remove_selector: draft.remove_selector.trim() || null,
          folder_id: draft.folder_id ? Number(draft.folder_id) : null
        })
      });
      const next = draftFromSource(updated);
      draftRef.current = next;
      dirtyRef.current = false;
      setDraft(next);
      setBaseline(next);
      onDirtyChange(false);
      setFeedback({ kind: "success", message: "已保存来源" });
      onSaved(updated);
      onRefresh();
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  }

  async function refetch() {
    if (busy) return;
    setFeedback(null);
    try {
      await request(`/api/sources/${source.id}/fetch`, { method: "POST" });
      setFeedback({ kind: "success", message: "已安排重新抓取" });
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  }

  async function loadPreviewOptions() {
    if (busy) return;
    setExtractionError("");
    try {
      const options = await request<ArticlePreviewOptions>(`/api/sources/${source.id}/article-preview`, {});
      setPreviewOptions(options);
      setPreviewRawEntryId(options.entries[0] ? String(options.entries[0].raw_entry_id) : "");
      setArticlePreview(null);
    } catch (error) {
      setExtractionError(errorMessage(error));
    }
  }

  async function testExtraction() {
    if (busy || !previewRawEntryId || selectorError) return;
    setExtractionError("");
    try {
      setArticlePreview(await request<ArticlePreview>(`/api/sources/${source.id}/article-preview`, {
        method: "POST",
        body: JSON.stringify({
          raw_entry_id: Number(previewRawEntryId),
          fetch_full_content: draft.fetch_full_content,
          article_selector: draft.article_selector.trim() || null,
          remove_selector: draft.remove_selector.trim() || null
        })
      }));
    } catch (error) {
      setExtractionError(errorMessage(error));
    }
  }

  async function checkRules() {
    if (busy) return;
    setExtractionError("");
    try {
      setRulesCheck(await request<PublicRulesCheck>("/api/article-rules/check", { method: "POST" }));
    } catch (error) {
      setExtractionError(errorMessage(error));
    }
  }

  async function activateRules() {
    if (busy || !rulesCheck?.can_activate) return;
    setExtractionError("");
    try {
      const active = await request<PublicRulesStatus>("/api/article-rules/activate", {
        method: "POST",
        body: JSON.stringify({ commit: rulesCheck.candidate_commit })
      });
      setPreviewOptions((current) => current ? { ...current, public_rules: active } : current);
      setRulesCheck(null);
      setFeedback({ kind: "success", message: "已采用公共规则更新" });
    } catch (error) {
      setExtractionError(errorMessage(error));
    }
  }

  async function deleteSource() {
    if (busy || !(await actionDialog.confirm({
      title: "永久删除订阅源",
      message: `永久删除“${draft.name}”？此操作不可撤销。`,
      confirmLabel: "永久删除",
      danger: true
    }))) return;
    setFeedback(null);
    try {
      await request(`/api/sources/${source.id}`, { method: "DELETE" });
      onDirtyChange(false);
      onDeleted(source);
    } catch (error) {
      setFeedback({ kind: "error", message: errorMessage(error) });
    }
  }

  return (
    <>
      <dialog ref={modalRef} className="source-detail-overlay" role="dialog" aria-modal="true" aria-labelledby="source-detail-title" onCancel={(event) => { event.preventDefault(); void close(); }} onKeyDown={handleKeyDown} onMouseDown={handleOverlayMouseDown}>
        <div ref={dialogRef} className="source-detail-dialog">
        <header className="source-detail-header">
          <div><h2 id="source-detail-title">编辑订阅源</h2><p>{draft.name}</p></div>
          <button aria-label="关闭来源详情" disabled={busy} type="button" onClick={() => void close()}><X size={18} /></button>
        </header>
        <form className="source-detail-form" onSubmit={save}>
          <section className="source-detail-management" aria-label="管理">
            <h3>管理</h3>
            <label>名称<input ref={nameInputRef} name="source_name" disabled={busy} value={draft.name} onChange={(event) => changeDraft({ name: event.target.value })} /></label>
            <label>RSS/Atom/Newsletter Feed 地址<input ref={addressInputRef} name="source_url" disabled={busy} value={draft.url} onChange={(event) => changeDraft({ url: event.target.value })} /></label>
            <a className="source-detail-site-link" href={source.site_url || draft.url} rel="noopener noreferrer" target="_blank"><ExternalLink size={15} /> 来源网站链接</a>
            <label>类型<select name="source_media_type" disabled={busy} value={draft.media_type} onChange={(event) => changeDraft({ media_type: event.target.value as SourceMediaType, folder_id: "" })}>{SOURCE_MEDIA_OPTIONS.map(({ value, label }) => <option key={value} value={value}>{label}</option>)}</select></label>
            <label>文件夹<select name="source_folder_id" disabled={busy} value={draft.folder_id} onChange={(event) => changeDraft({ folder_id: event.target.value })}><option value="">未分类</option>{folderOptions.map((folder) => <option key={folder.id} value={folder.id}>{folder.name}</option>)}</select></label>
            <label>收录状态<select name="source_status" disabled={busy} value={draft.status} onChange={(event) => changeDraft({ status: event.target.value as Draft["status"] })}><option value="active">正式</option><option value="trial">考察</option></select></label>
            <div className="source-detail-switch-row"><span>暂停抓取</span><button aria-checked={!draft.enabled} aria-label={draft.enabled ? "暂停抓取" : "恢复抓取"} className="source-detail-switch" disabled={busy} role="switch" type="button" onClick={() => changeDraft({ enabled: !draft.enabled })}><span aria-hidden="true" /></button></div>
            <label className="source-detail-check"><input checked={draft.fetch_full_content} disabled={busy} type="checkbox" onChange={(event) => changeDraft({ fetch_full_content: event.target.checked })} /> 抓取网页全文</label>
            <details className="source-extraction-settings">
              <summary>网页正文与公共规则</summary>
              <div className="source-extraction-body">
                <label>正文选择器（CSS / XPath）
                  <input aria-describedby={articleSelectorError ? "article-selector-error" : undefined} aria-invalid={Boolean(articleSelectorError)} disabled={busy} name="article_selector" placeholder="css:article 或 xpath://article" value={draft.article_selector} onChange={(event) => changeDraft({ article_selector: event.target.value })} />
                </label>
                {articleSelectorError ? <p className="source-extraction-error" id="article-selector-error" role="alert">{articleSelectorError}</p> : null}
                <label>删除选择器（可选）
                  <input aria-describedby={removeSelectorError ? "remove-selector-error" : undefined} aria-invalid={Boolean(removeSelectorError)} disabled={busy} name="remove_selector" placeholder="css:.advertisement" value={draft.remove_selector} onChange={(event) => changeDraft({ remove_selector: event.target.value })} />
                </label>
                {removeSelectorError ? <p className="source-extraction-error" id="remove-selector-error" role="alert">{removeSelectorError}</p> : null}
                {!previewOptions ? <button disabled={busy} type="button" onClick={() => void loadPreviewOptions()}>加载最近 6 篇</button> : (
                  <>
                    <label>测试文章
                      <select disabled={busy || !previewOptions.entries.length} name="preview_raw_entry_id" value={previewRawEntryId} onChange={(event) => { setPreviewRawEntryId(event.target.value); setArticlePreview(null); }}>
                        {previewOptions.entries.map((entry) => <option key={entry.raw_entry_id} value={entry.raw_entry_id}>{entry.title}</option>)}
                      </select>
                    </label>
                    {previewOptions.entries.length ? <button disabled={busy || Boolean(selectorError)} type="button" onClick={() => void testExtraction()}>测试正文提取</button> : <p className="source-extraction-note">最近没有可测试文章。</p>}
                    <p className="source-extraction-note">当前公共规则：<code title={previewOptions.public_rules.commit}>{previewOptions.public_rules.commit.slice(0, 12)}</code>{previewOptions.public_rules.activated_at ? ` · ${formatExactTime(previewOptions.public_rules.activated_at)}` : " · 随当前版本内置"}</p>
                  </>
                )}
                {articlePreview ? <ExtractionPreviewCard ariaLabel="正文提取预览" preview={articlePreview} warningSuffix="仍可保存当前规则。" /> : null}
                <div className="source-extraction-rule-actions">
                  <button disabled={busy} type="button" onClick={() => void checkRules()}>检查公共规则更新</button>
                  {rulesCheck?.can_activate ? <button className="primary" disabled={busy} type="button" onClick={() => void activateRules()}>采用此更新</button> : null}
                </div>
                {rulesCheck ? (
                  <>
                    <p className="source-extraction-note">
                      <code title={rulesCheck.current_version}>{rulesCheck.current_commit.slice(0, 12)}</code>
                      {" → "}
                      <code title={rulesCheck.candidate_version}>{rulesCheck.candidate_commit.slice(0, 12)}</code>
                      ：{rulesCheck.rules_count} 条安全规则，覆盖 {rulesCheck.covered_subscribed_domains} / {rulesCheck.subscribed_domains} 个已订阅域名，{rulesCheck.changed_subscribed_domains} 个变化，实测 {rulesCheck.tested_subscribed_domains} 个。
                      {rulesCheck.can_activate ? "测试通过，可采用。" : rulesCheck.passed ? "当前已是此版本。" : `未通过：${[...rulesCheck.invalid_subscribed_domains, ...rulesCheck.failed_subscribed_domains].join("、")}`}
                    </p>
                    {rulesCheck.preview ? <ExtractionPreviewCard ariaLabel="候选公共规则预览" heading={`候选示例 · ${rulesCheck.preview.hostname}`} preview={rulesCheck.preview} warningSuffix={rulesCheck.preview.passed ? "" : "候选测试未通过，不能采用。"} /> : null}
                  </>
                ) : null}
                {extractionError ? <p className="source-extraction-error" role="alert">{extractionError}</p> : null}
              </div>
            </details>
            <label>隐私分类<select name="privacy_class" disabled={busy} value={draft.privacy_class} onChange={(event) => { const privacy = event.target.value as SourcePrivacyClass; changeDraft({ privacy_class: privacy, external_generation_allowed: privacy === "public" && draft.external_generation_allowed }); }}>{PRIVACY_OPTIONS.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
            <label className="source-detail-check"><input checked={draft.external_generation_allowed} disabled={busy || draft.privacy_class !== "public"} name="external_generation_allowed" type="checkbox" onChange={(event) => changeDraft({ external_generation_allowed: event.target.checked })} /> 允许把该来源的内容发送到外部 AI 服务</label>
            <div className="source-detail-danger"><div><strong>永久删除订阅源</strong><span>删除后不可恢复。</span></div><button className="danger" disabled={busy} type="button" onClick={() => void deleteSource()}>永久删除</button></div>
          </section>
          <aside className="source-detail-side">
            <section aria-label="监控"><h3>监控</h3><dl className="source-detail-values"><Value label="最近抓取" value={formatExactTime(source.last_fetched_at)} /><Value label="近 30 天" value={recentEntryLabel(source.recent_entry_count_30d)} /><Value label="Feed 地址" value={draft.url} /><Value label="来源 ID" value={String(source.id)} /></dl>{source.last_error ? <div className="source-detail-error"><strong>{friendlyFetchError(source.last_error)}</strong></div> : <p className="source-detail-healthy">当前没有抓取错误。</p>}<div className="source-detail-monitor-actions"><button disabled={busy} type="button" onClick={() => void refetch()}><RefreshCw size={15} /> 重新抓取</button><button disabled={busy} type="button" onClick={() => addressInputRef.current?.focus()}><Pencil size={15} /> 修改链接</button></div></section>
            <section aria-label="价值"><h3>价值</h3><dl className="source-detail-values"><Value label="星标" value={String(source.starred_count)} /><Value label="打开原文" value={String(source.opened_count)} /><Value label="入簇" value={String(source.cluster_count)} /><Value label="重复" value={String(source.duplicate_count)} /><Value label="信任分" value={`${Number(source.feed_trust_score ?? 0).toFixed(1)} / 100`} /></dl><p className="source-detail-trust">信任分范围 0–100，按（已读 + 2×打开原文 + 3×星标 + 稍后读 + 入簇 - 重复）×100 / max（抓取数，1）计算。</p></section>
          </aside>
          <footer className="source-detail-footer">{feedback ? <p className={feedback.kind === "error" ? "error-line" : "status-line success-line"} role={feedback.kind === "error" ? "alert" : "status"}>{feedback.message}</p> : <span>{dirty ? "有未保存更改" : ""}</span>}<button className="primary" disabled={busy || Boolean(selectorError)} type="submit">保存</button></footer>
        </form>
        </div>
      </dialog>
      {actionDialog.dialog}
    </>
  );
}

function Value({ label, value }: { label: string; value: string }) {
  return <div><dt>{label}</dt><dd title={value}>{value}</dd></div>;
}

function ExtractionPreviewCard({
  ariaLabel,
  heading,
  preview,
  warningSuffix
}: {
  ariaLabel: string;
  heading?: string;
  preview: ExtractionPreview;
  warningSuffix: string;
}) {
  return (
    <section className="source-article-preview" aria-label={ariaLabel}>
      <strong>{heading ? `${heading} · ` : ""}{methodLabel(preview.method)} · {preview.adopted_webpage ? "采用网页正文" : "采用 RSS 正文"}</strong>
      <p>RSS {preview.rss_characters} 字 · 网页 {preview.webpage_characters} 字</p>
      <p>匹配 {preview.matched_elements} 个 · 删除 {preview.removed_elements} 个 · {preview.version}</p>
      {preview.fallback_reason ? <p className="source-extraction-warning" role="status">{preview.fallback_reason}{warningSuffix ? `；${warningSuffix}` : "。"}</p> : null}
      <div className="source-article-preview-body" dangerouslySetInnerHTML={{ __html: preview.reading_html }} />
    </section>
  );
}

function methodLabel(method: string) {
  return {
    fivefilters: "公共规则",
    manual: "手工规则",
    rss: "RSS",
    trafilatura: "Trafilatura"
  }[method] || method;
}

function draftFromSource(source: SourceDetailSource): Draft {
  return { name: source.name, url: source.url, folder_id: source.folder_id ? String(source.folder_id) : "", media_type: source.media_type, status: source.status === "trial" ? "trial" : "active", enabled: !isSourcePaused(source), fetch_full_content: source.fetch_full_content, article_selector: source.article_selector || "", remove_selector: source.remove_selector || "", privacy_class: source.privacy_class, external_generation_allowed: source.external_generation_allowed };
}

function sameDraft(left: Draft, right: Draft) {
  return left.name === right.name && left.url === right.url && left.folder_id === right.folder_id && left.media_type === right.media_type && left.status === right.status && left.enabled === right.enabled && left.fetch_full_content === right.fetch_full_content && left.article_selector === right.article_selector && left.remove_selector === right.remove_selector && left.privacy_class === right.privacy_class && left.external_generation_allowed === right.external_generation_allowed;
}
