"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import type { MutableRefObject, ReactNode } from "react";
import { Ellipsis, GripVertical, Settings, X } from "lucide-react";

export type ToolbarAction = { id: string; label: string; node: ReactNode };
type ToolbarLayout = { primary: string[]; more: string[] };
type Layer = keyof ToolbarLayout;
const DEFAULT_PRIMARY_IDS = new Set(["read-toggle", "star", "summary", "bionic", "share"]);

export default function CustomToolbar({ actions, leading, storageKey }: { actions: ToolbarAction[]; leading?: ReactNode; storageKey: string }) {
  const [layout, setLayout] = useState<ToolbarLayout>(() => defaultLayout(actions));
  const [loaded, setLoaded] = useState(false);
  const [customizing, setCustomizing] = useState(false);
  const dragging = useRef<string | null>(null);
  const menu = useRef<HTMLDetailsElement | null>(null);
  const dialog = useRef<HTMLDialogElement | null>(null);

  useEffect(() => {
    const saved = localStorage.getItem(storageKey);
    if (!saved) {
      setLoaded(true);
      return;
    }
    try {
      const data = JSON.parse(saved) as Partial<ToolbarLayout> & { order?: string[]; limit?: number };
      setLayout(savedLayout(actions, data));
    } catch {
      localStorage.removeItem(storageKey);
    }
    setLoaded(true);
  }, [storageKey]);

  useEffect(() => {
    if (!loaded) return;
    localStorage.setItem(storageKey, JSON.stringify(layout));
  }, [layout, loaded, storageKey]);

  useEffect(() => {
    const modal = dialog.current;
    if (!modal) return;
    if (customizing && !modal.open) modal.showModal();
    if (!customizing && modal.open) modal.close();
  }, [customizing]);

  const normalized = useMemo(() => normalizeLayout(actions, layout), [actions, layout]);
  const primary = useMemo(() => actionsFor(actions, normalized.primary), [actions, normalized.primary]);
  const more = useMemo(() => actionsFor(actions, normalized.more), [actions, normalized.more]);

  function drop(targetLayer: Layer, targetId?: string) {
    const source = dragging.current;
    if (!source) return;
    setLayout((current) => {
      const next = normalizeLayout(actions, current);
      next.primary = next.primary.filter((id) => id !== source);
      next.more = next.more.filter((id) => id !== source);
      const target = next[targetLayer];
      const targetIndex = targetId ? target.indexOf(targetId) : -1;
      target.splice(targetIndex >= 0 ? targetIndex : target.length, 0, source);
      return next;
    });
  }

  return (
    <div className="custom-toolbar">
      {leading ? <div className="toolbar-leading">{leading}</div> : null}
      <div className="toolbar">{primary.map((action) => <span key={action.id}>{action.node}</span>)}</div>
      <details className="toolbar-more" ref={menu}>
        <summary className="icon-link" title="更多工具" aria-label="更多工具">
          <Ellipsis size={16} />
        </summary>
        <div className="toolbar-more-panel">
          {more.map((action) => (
            <div key={action.id} className="toolbar-more-row">
              <span>{action.node}</span>
              <span>{action.label}</span>
            </div>
          ))}
          <button
            type="button"
            className="toolbar-more-row toolbar-customize-entry"
            onClick={() => {
              menu.current?.removeAttribute("open");
              setCustomizing(true);
            }}
          >
            <span>
              <Settings size={16} />
            </span>
            <span>自定义工具栏</span>
          </button>
        </div>
      </details>
      <dialog
        ref={dialog}
        className="toolbar-customize-dialog"
        aria-label="自定义工具栏"
        onCancel={() => setCustomizing(false)}
        onClose={() => {
          setCustomizing(false);
          window.setTimeout(() => menu.current?.querySelector<HTMLElement>("summary")?.focus(), 0);
        }}
        onClick={(event) => {
          if (event.target === event.currentTarget) setCustomizing(false);
        }}
      >
          <div className="toolbar-modal">
            <div className="toolbar-modal-header">
              <h3>自定义工具栏</h3>
              <button className="icon" type="button" title="关闭" aria-label="关闭" onClick={() => setCustomizing(false)}>
                <X size={16} />
              </button>
            </div>
            <div className="toolbar-modal-body">
              <ToolbarDropList title="当前工具栏" actions={primary} layer="primary" dragging={dragging} onDrop={drop} />
              <ToolbarDropList title="更多工具栏" actions={more} layer="more" dragging={dragging} onDrop={drop} />
            </div>
          </div>
      </dialog>
    </div>
  );
}

function ToolbarDropList({
  actions,
  dragging,
  layer,
  onDrop,
  title
}: {
  actions: ToolbarAction[];
  dragging: MutableRefObject<string | null>;
  layer: Layer;
  onDrop: (layer: Layer, targetId?: string) => void;
  title: string;
}) {
  return (
    <section className="toolbar-drop-list" onDragOver={(event) => event.preventDefault()} onDrop={() => onDrop(layer)}>
      <h4>{title}</h4>
      {actions.length ? (
        actions.map((action) => (
          <button
            key={action.id}
            draggable
            type="button"
            onDragStart={() => {
              dragging.current = action.id;
            }}
            onDragOver={(event) => event.preventDefault()}
            onDrop={(event) => {
              event.stopPropagation();
              onDrop(layer, action.id);
            }}
          >
            <GripVertical size={14} />
            {action.label}
          </button>
        ))
      ) : (
        <p>拖到这里</p>
      )}
    </section>
  );
}

function savedLayout(actions: ToolbarAction[], data: Partial<ToolbarLayout> & { order?: string[]; limit?: number }) {
  if (Array.isArray(data.primary) || Array.isArray(data.more)) return normalizeLayout(actions, { primary: data.primary ?? [], more: data.more ?? [] });
  if (Array.isArray(data.order)) {
    const sorted = actionsFor(actions, data.order).map((action) => action.id);
    const limit = Number.isFinite(data.limit) ? Math.max(1, Number(data.limit)) : actions.length;
    return normalizeLayout(actions, { primary: sorted.slice(0, limit), more: sorted.slice(limit) });
  }
  return defaultLayout(actions);
}

function defaultLayout(actions: ToolbarAction[]) {
  return {
    primary: actions.filter((action) => DEFAULT_PRIMARY_IDS.has(action.id)).map((action) => action.id),
    more: actions.filter((action) => !DEFAULT_PRIMARY_IDS.has(action.id)).map((action) => action.id)
  };
}

function normalizeLayout(actions: ToolbarAction[], layout: ToolbarLayout): ToolbarLayout {
  const valid = new Set(actions.map((action) => action.id));
  const hadRemovedPrimaryAction = layout.primary.some((id) => !valid.has(id));
  const defaultPrimary = defaultLayout(actions).primary;
  const primary = layout.primary.filter((id) => valid.has(id));
  const primarySet = new Set(primary);
  const more = layout.more.filter((id) => valid.has(id) && !primarySet.has(id));
  const seen = new Set([...primary, ...more]);
  for (const action of actions) {
    if (seen.has(action.id)) continue;
    const shouldPromote = DEFAULT_PRIMARY_IDS.has(action.id) && (hadRemovedPrimaryAction || primary.length < defaultPrimary.length);
    if (shouldPromote) {
      primary.push(action.id);
      primarySet.add(action.id);
    } else {
      more.push(action.id);
    }
    seen.add(action.id);
  }
  const targetPrimaryCount = hadRemovedPrimaryAction ? defaultPrimary.length : Math.min(layout.primary.length, actions.length);
  while (primary.length < targetPrimaryCount && more.length) {
    const id = more.shift();
    if (id) primary.push(id);
  }
  if (valid.has("read-toggle")) {
    const limit = Math.max(1, targetPrimaryCount || defaultPrimary.length);
    const pinnedPrimary = ["read-toggle", ...primary.filter((id) => id !== "read-toggle")];
    const pinnedMore = more.filter((id) => id !== "read-toggle");
    while (pinnedPrimary.length > limit) {
      const id = pinnedPrimary.pop();
      if (id) pinnedMore.unshift(id);
    }
    return { primary: pinnedPrimary, more: pinnedMore };
  }
  return { primary, more };
}

function actionsFor(actions: ToolbarAction[], order: string[]) {
  const byId = new Map(actions.map((action) => [action.id, action]));
  return order.map((id) => byId.get(id)).filter(Boolean) as ToolbarAction[];
}
