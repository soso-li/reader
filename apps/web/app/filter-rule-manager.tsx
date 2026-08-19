"use client";

import { Pencil, Play, Plus, Power, Trash2, X } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import type { KeyboardEvent as ReactKeyboardEvent } from "react";
import { useRouter } from "next/navigation";

import { useActionDialog, useNativeModal } from "./action-dialog";
import { userFacingErrorMessage } from "./lib/api";
import { TimeText } from "./time-text";

type Source = { id: number; name: string };
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
type PreviewItem = {
  id: number;
  source_name: string;
  title: string;
  summary: string;
  content_text: string;
  published_at: string | null;
};
type Preview = { count: number; items: PreviewItem[] };
type Editor = {
  id: number | null;
  enabled: boolean;
  sourceId: string;
  matchType: "literal" | "regex";
  pattern: string;
};

const EMPTY_EDITOR: Editor = { id: null, enabled: true, sourceId: "", matchType: "literal", pattern: "" };
const CHOOSE_SOURCE = "__choose__";

export default function FilterRuleManager({
  autoCreate = false,
  initialRules,
  initialSourceIds = [],
  sources
}: {
  autoCreate?: boolean;
  initialRules: FilterRule[];
  initialSourceIds?: number[];
  sources: Source[];
}) {
  const router = useRouter();
  const editorDialogRef = useRef<HTMLDivElement | null>(null);
  const editorReturnFocusRef = useRef<HTMLElement | null>(null);
  const [rules, setRules] = useState(initialRules);
  const [editor, setEditor] = useState<Editor>(EMPTY_EDITOR);
  const [editorOpen, setEditorOpen] = useState(false);
  const editorModalRef = useNativeModal(editorOpen);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [previewSignature, setPreviewSignature] = useState("");
  const [pending, setPending] = useState("");
  const [error, setError] = useState("");
  const actionDialog = useActionDialog();
  const signature = useMemo(() => editorSignature(editor), [editor]);
  const canApply = Boolean(editor.pattern.trim()) && editor.sourceId !== CHOOSE_SOURCE && previewSignature === signature && preview !== null;
  const proposedSources = sources.filter((source) => initialSourceIds.includes(source.id));
  const autoOpened = useRef(false);

  useEffect(() => {
    if (!autoCreate || autoOpened.current) return;
    autoOpened.current = true;
    openCreate(true);
  }, [autoCreate]);

  useEffect(() => {
    if (!editorOpen) return;
    const timer = window.setTimeout(() => editorDialogRef.current?.querySelector<HTMLElement>("select, textarea, button")?.focus(), 0);
    return () => window.clearTimeout(timer);
  }, [editorOpen]);

  function changeEditor(changes: Partial<Editor>) {
    setEditor((current) => ({ ...current, ...changes }));
    setPreview(null);
    setPreviewSignature("");
    setError("");
  }

  function openCreate(fromSample = false) {
    editorReturnFocusRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    setEditor({
      ...EMPTY_EDITOR,
      sourceId: fromSample
        ? initialSourceIds.length > 1
          ? CHOOSE_SOURCE
          : proposedSources.length === 1
          ? String(proposedSources[0].id)
          : ""
        : ""
    });
    setPreview(null);
    setPreviewSignature("");
    setError("");
    setEditorOpen(true);
  }

  function openEdit(rule: FilterRule) {
    editorReturnFocusRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    setEditor({ id: rule.id, enabled: rule.enabled, sourceId: rule.source_id ? String(rule.source_id) : "", matchType: rule.match_type, pattern: rule.pattern });
    setPreview(null);
    setPreviewSignature("");
    setError("");
    setEditorOpen(true);
  }

  function closeEditor() {
    setEditorOpen(false);
    window.setTimeout(() => editorReturnFocusRef.current?.focus(), 0);
  }

  function handleEditorKeyDown(event: ReactKeyboardEvent<HTMLDialogElement>) {
    if (event.key === "Escape" && !pending) {
      event.preventDefault();
      closeEditor();
      return;
    }
    if (event.key !== "Tab") return;
    const focusable = filterFocusableElements(editorDialogRef.current);
    if (!focusable.length) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }

  async function runPreview() {
    setPending("preview");
    setError("");
    try {
      const result = await filterRuleAction<Preview>({ action: "preview", ...editorPayload(editor) });
      setPreview(result);
      setPreviewSignature(signature);
    } catch (cause) {
      setPreview(null);
      setPreviewSignature("");
      setError(errorMessage(cause));
    } finally {
      setPending("");
    }
  }

  async function applyRule() {
    if (!canApply) return;
    setPending("apply");
    setError("");
    try {
      const rule = await filterRuleAction<FilterRule>({ action: editor.id ? "update" : "create", id: editor.id ?? undefined, ...editorPayload(editor), enabled: editor.enabled });
      setRules((current) => editor.id ? current.map((item) => item.id === rule.id ? rule : item) : [rule, ...current]);
      closeEditor();
      setPreview(null);
      setPreviewSignature("");
      router.refresh();
    } catch (cause) {
      setError(errorMessage(cause));
    } finally {
      setPending("");
    }
  }

  async function toggleRule(rule: FilterRule) {
    setPending(`toggle-${rule.id}`);
    setError("");
    try {
      const updated = await filterRuleAction<FilterRule>({ action: "update", id: rule.id, enabled: !rule.enabled });
      setRules((current) => current.map((item) => item.id === updated.id ? updated : item));
      router.refresh();
    } catch (cause) {
      setError(errorMessage(cause));
    } finally {
      setPending("");
    }
  }

  async function deleteRule(rule: FilterRule) {
    if (!(await actionDialog.confirm({
      title: "永久删除过滤规则",
      message: `永久删除过滤规则“${rule.pattern}”？匹配记录会立即清除。`,
      confirmLabel: "永久删除",
      danger: true
    }))) return;
    setPending(`delete-${rule.id}`);
    setError("");
    try {
      await filterRuleAction({ action: "delete", id: rule.id });
      setRules((current) => current.filter((item) => item.id !== rule.id));
      if (editor.id === rule.id) closeEditor();
      router.refresh();
    } catch (cause) {
      setError(errorMessage(cause));
    } finally {
      setPending("");
    }
  }

  return (
    <section id="settings-filters" className="settings-block filter-rule-manager">
      <div className="filter-rule-heading">
        <div>
          <h3>关键词过滤</h3>
          <p className="source-meta">命中原始标题、RSS 摘要或原始正文的条目会退出自动流；搜索、收藏、稍后读和“已过滤”列表仍可排查。</p>
        </div>
        <button className="action-link" type="button" onClick={() => openCreate()}><Plus size={14} /> 新建规则</button>
      </div>
      {error && !editorOpen ? <p className="error-line" role="alert">{error}</p> : null}
      <div className="filter-rule-list">
        {rules.map((rule) => (
          <article key={rule.id} className={`filter-rule-row ${rule.enabled ? "" : "is-paused"}`}>
            <div className="filter-rule-main">
              <div className="filter-rule-pattern"><code>{rule.pattern}</code></div>
              <div className="source-meta">
                {rule.match_type === "regex" ? "正则" : "包含关键词"} · {rule.source_id ? rule.source_name : "全部来源"} · 当前匹配 {rule.match_count} 条
              </div>
            </div>
            <span className={`filter-rule-status ${rule.enabled ? "enabled" : "paused"}`}>{rule.enabled ? "生效中" : "已暂停"}</span>
            <div className="filter-rule-actions">
              <button type="button" disabled={Boolean(pending)} onClick={() => void toggleRule(rule)}>
                {rule.enabled ? <Power size={14} /> : <Play size={14} />} {rule.enabled ? "暂停" : "恢复"}
              </button>
              <button type="button" disabled={Boolean(pending)} onClick={() => openEdit(rule)}><Pencil size={14} /> 编辑</button>
              <button className="danger" type="button" disabled={Boolean(pending)} onClick={() => void deleteRule(rule)}><Trash2 size={14} /> 删除</button>
            </div>
          </article>
        ))}
        {!rules.length ? <div className="placeholder">还没有过滤规则。新建规则后先预览匹配，再确认生效。</div> : null}
      </div>

      {editorOpen ? (
        <dialog ref={editorModalRef} className="toolbar-modal-backdrop" role="dialog" aria-modal="true" aria-labelledby="filter-rule-editor-title" onCancel={(event) => { event.preventDefault(); if (!pending) closeEditor(); }} onKeyDown={handleEditorKeyDown} onMouseDown={() => !pending && closeEditor()}>
          <div ref={editorDialogRef} className="toolbar-modal filter-rule-modal" onMouseDown={(event) => event.stopPropagation()}>
            <div className="toolbar-modal-header">
              <h3 id="filter-rule-editor-title">{editor.id ? "编辑过滤规则" : "新建过滤规则"}</h3>
              <button className="icon" aria-label="关闭过滤规则编辑器" type="button" disabled={Boolean(pending)} onClick={closeEditor}><X size={16} /></button>
            </div>
            <div className="form-stack">
              <label>范围
                <select name="filter_source_scope" value={editor.sourceId} onChange={(event) => changeEditor({ sourceId: event.target.value })}>
                  {editor.sourceId === CHOOSE_SOURCE ? <option value={CHOOSE_SOURCE} disabled>请选择范围</option> : null}
                  <option value="">全部来源</option>
                  {(initialSourceIds.length > 1 && editor.id === null ? proposedSources : sources).map((source) => <option key={source.id} value={source.id}>{source.name}</option>)}
                </select>
              </label>
              <label>匹配方式
                <select name="filter_match_type" value={editor.matchType} onChange={(event) => changeEditor({ matchType: event.target.value as Editor["matchType"] })}>
                  <option value="literal">包含关键词（不区分大小写）</option>
                  <option value="regex">正则表达式（不区分大小写）</option>
                </select>
              </label>
              <label>表达式
                <textarea name="filter_pattern" rows={3} maxLength={500} value={editor.pattern} onChange={(event) => changeEditor({ pattern: event.target.value })} placeholder={editor.matchType === "regex" ? "例如：赞助|广告|推广" : "例如：sponsored"} />
              </label>
            </div>
            <div className="filter-preview-actions">
              <button type="button" disabled={Boolean(pending) || !editor.pattern.trim() || editor.sourceId === CHOOSE_SOURCE} onClick={() => void runPreview()}>{pending === "preview" ? "正在匹配…" : "预览匹配"}</button>
              <button type="button" disabled={Boolean(pending) || !canApply} onClick={() => void applyRule()}>{pending === "apply" ? "正在应用…" : editor.id ? "确认更新" : "创建并启用"}</button>
            </div>
            {error ? <p className="error-line" role="alert">{error}</p> : null}
            {preview ? (
              <div className="filter-preview">
                <div className="filter-preview-summary">匹配 {preview.count} 条，以下为最新 {preview.items.length} 条</div>
                {preview.items.map((item) => (
                  <article key={item.id} className="filter-preview-item">
                    <strong>{item.title || "无标题"}</strong>
                    <span className="source-meta">{item.source_name} · <TimeText value={item.published_at} /></span>
                    <p>{previewText(item)}</p>
                  </article>
                ))}
                {!preview.items.length ? <div className="placeholder">当前没有匹配条目。仍可创建规则，以后新内容会自动匹配。</div> : null}
              </div>
            ) : <p className="source-meta">必须先用当前表达式预览；编辑失败时旧规则继续生效。</p>}
          </div>
        </dialog>
      ) : null}
      {actionDialog.dialog}
    </section>
  );
}

function filterFocusableElements(container: HTMLElement | null) {
  if (!container) return [];
  return Array.from(
    container.querySelectorAll<HTMLElement>('a[href], button:not([disabled]), input:not([disabled]):not([type="hidden"]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])')
  ).filter((element) => element.tabIndex !== -1);
}

function editorPayload(editor: Editor) {
  return {
    source_id: editor.sourceId && editor.sourceId !== CHOOSE_SOURCE ? Number(editor.sourceId) : null,
    match_type: editor.matchType,
    pattern: editor.pattern.trim()
  };
}

function editorSignature(editor: Editor) {
  return JSON.stringify(editorPayload(editor));
}

function previewText(item: PreviewItem) {
  const value = (item.summary || item.content_text || "").replace(/\s+/g, " ").trim();
  return value.length > 180 ? `${value.slice(0, 180)}…` : value;
}

async function filterRuleAction<T = { ok: boolean }>(payload: object): Promise<T> {
  const response = await fetch("/actions/filter-rules", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const result = await response.json().catch(() => ({})) as T & { error?: string };
  if (!response.ok) throw new Error(result.error || "过滤规则操作失败");
  return result;
}

function errorMessage(error: unknown) {
  return userFacingErrorMessage(error, "过滤规则操作失败");
}
