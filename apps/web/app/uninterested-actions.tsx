"use client";

import { CircleMinus, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { KeyboardEvent as ReactKeyboardEvent } from "react";

import { createOperationId } from "./event-user-state";
import { useNativeModal } from "./action-dialog";
import { userFacingErrorMessage } from "./lib/api";
import { TimeText } from "./time-text";
import { queryString } from "./url-state";

export type UninterestedReason =
  | "promotion"
  | "repetitive"
  | "topic"
  | "low_quality"
  | "other";

export type UninterestedTarget = {
  target_kind: "event" | "item";
  event_uid: string | null;
  current_revision_uid: string | null;
  cluster_id: number | null;
  item_id: number | null;
  item_ids: number[];
  title: string;
  summary: string;
  source_ids: number[];
  source_names: string[];
  media_type: string;
  item_count: number;
  reason: UninterestedReason | null;
  note: string | null;
  marked_at: string;
};

type MutationTarget =
  | { target_type: "event"; event_uid: string; observed_revision_uid: string }
  | { target_type: "item" | "article"; item_id: number };

type MutationResult = {
  target_kind: "event" | "item";
  event_uid: string | null;
  observed_revision_uid: string | null;
  cluster_id: number | null;
  item_id: number | null;
  affected_item_ids: number[];
  uninterested: boolean;
  reason: UninterestedReason | null;
  note: string | null;
  marked_at: string | null;
};

const REASONS: Array<[UninterestedReason, string]> = [
  ["promotion", "广告 / 推广"],
  ["repetitive", "没有新信息 / 重复炒作"],
  ["topic", "这个主题不感兴趣"],
  ["low_quality", "标题党 / 内容质量差"],
  ["other", "其他"]
];

export function ReduceSimilarButton({
  compact = false,
  dismissIcon = false,
  initialFeedback,
  target,
  onHidden,
  onRestored
}: {
  compact?: boolean;
  dismissIcon?: boolean;
  initialFeedback?: { reason: UninterestedReason | null; note: string | null };
  target: MutationTarget;
  onHidden?: (result: MutationResult) => void;
  onRestored?: (result: MutationResult) => void;
}) {
  const [feedback, setFeedback] = useState(initialFeedback);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");

  async function mark() {
    if (feedback) {
      setDialogOpen(true);
      return;
    }
    setPending(true);
    setError("");
    try {
      const next = await mutate({ ...target, value: true });
      setFeedback({ reason: next.reason, note: next.note });
      setDialogOpen(true);
      onHidden?.(next);
    } catch (cause) {
      setError(errorMessage(cause));
    } finally {
      setPending(false);
    }
  }

  const label = pending ? "移动中…" : feedback ? "管理不感兴趣" : "减少此类";

  return (
    <>
      <button
        className={compact ? `icon ${feedback ? "active" : ""}` : "uninterested-button"}
        type="button"
        title={compact ? label : undefined}
        aria-label={compact ? label : undefined}
        disabled={pending}
        onClick={() => void mark()}
      >
        {compact ? (dismissIcon ? <X size={17} /> : <CircleMinus size={17} />) : label}
      </button>
      {error && !dialogOpen ? <span className="error-line" role="alert">{error}</span> : null}
      {dialogOpen && feedback ? (
        <ReasonDialog
          target={target}
          reason={feedback.reason}
          note={feedback.note}
          onClose={() => setDialogOpen(false)}
          onRestored={(next) => {
            setFeedback(undefined);
            setDialogOpen(false);
            onRestored?.(next);
          }}
          onSaved={(next) => setFeedback({ reason: next.reason, note: next.note })}
        />
      ) : null}
    </>
  );
}

export function UninterestedList({
  initialCount,
  initialTargets
}: {
  initialCount: number;
  initialTargets: UninterestedTarget[];
}) {
  const [count, setCount] = useState(initialCount);
  const [targets, setTargets] = useState(initialTargets);
  return (
    <>
      <p className="uninterested-count">共 {count} 项，按标记时间倒序</p>
      <div className="uninterested-list">
        {targets.map((target) => (
          <UninterestedRow
            key={`${target.target_kind}-${target.event_uid ?? target.item_id}`}
            target={target}
            onRemove={() => {
              setTargets((current) => current.filter((item) => item !== target));
              setCount((current) => Math.max(0, current - 1));
            }}
            onUpdate={(next) => setTargets((current) => current.map((item) => item === target ? next : item))}
          />
        ))}
        {!targets.length ? <div className="placeholder">这里暂时没有内容。</div> : null}
      </div>
    </>
  );
}

function UninterestedRow({
  target,
  onRemove,
  onUpdate
}: {
  target: UninterestedTarget;
  onRemove: () => void;
  onUpdate: (target: UninterestedTarget) => void;
}) {
  const [editing, setEditing] = useState(false);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  const mutationTarget = targetMutation(target);
  const viewHref = target.target_kind === "event"
    ? `/?${queryString({ view: "clusters", cluster_id: target.cluster_id, filter: "all", pane: "detail" })}`
    : `/?${queryString({ view: "browse", media: target.media_type, item_id: target.item_id, filter: "all", pane: "detail" })}`;
  const ruleHref = `/?${queryString({
    view: "settings",
    settings_section: "filters",
    new_filter: "1",
    filter_source_ids: target.source_ids.join(",")
  })}`;

  async function restore() {
    setPending(true);
    setError("");
    try {
      await mutate({ ...mutationTarget, value: false });
      onRemove();
    } catch (cause) {
      setError(errorMessage(cause));
    } finally {
      setPending(false);
    }
  }

  return (
    <article className="uninterested-row">
      <div>
        <div className="item-meta">
          {target.target_kind === "event" ? `事件 · ${target.item_count} 篇原始文章` : "独立条目"}
          {" · "}<TimeText value={target.marked_at} />
        </div>
        <h2><a href={viewHref}>{target.title || "无标题"}</a></h2>
        <p className="item-meta">{target.source_names.join(" · ")}</p>
        {target.summary ? <p>{target.summary}</p> : null}
        <p className="uninterested-reason">
          原因：{target.reason ? uninterestedReasonLabel(target.reason) : "未选择"}
          {target.note ? ` · ${target.note}` : ""}
        </p>
      </div>
      <div className="uninterested-row-actions">
        <a href={viewHref}>查看</a>
        <button type="button" disabled={pending} onClick={() => setEditing(true)}>编辑原因</button>
        <button type="button" disabled={pending} onClick={() => void restore()}>恢复</button>
        <a href={ruleHref}>建立规则</a>
      </div>
      {error ? <p className="error-line" role="alert">{error}</p> : null}
      {editing ? (
        <ReasonDialog
          target={mutationTarget}
          reason={target.reason}
          note={target.note}
          editOnly
          onClose={() => setEditing(false)}
          onSaved={(result) => {
            setEditing(false);
            onUpdate({ ...target, reason: result.reason, note: result.note });
          }}
        />
      ) : null}
    </article>
  );
}

function ReasonDialog({
  target,
  reason: initialReason,
  note: initialNote,
  editOnly = false,
  onClose,
  onRestored,
  onSaved
}: {
  target: MutationTarget;
  reason: UninterestedReason | null;
  note: string | null;
  editOnly?: boolean;
  onClose: () => void;
  onRestored?: (result: MutationResult) => void;
  onSaved?: (result: MutationResult) => void;
}) {
  const dialogRef = useRef<HTMLDivElement | null>(null);
  const modalRef = useNativeModal();
  const returnFocusRef = useRef<HTMLElement | null>(null);
  const [reason, setReason] = useState<UninterestedReason | null>(initialReason);
  const [note, setNote] = useState(initialNote ?? "");
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    returnFocusRef.current = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    const timer = window.setTimeout(
      () => dialogRef.current?.querySelector<HTMLElement>("button, input")?.focus(),
      0
    );
    return () => {
      window.clearTimeout(timer);
      const target = returnFocusRef.current;
      window.setTimeout(() => target?.isConnected && target.focus(), 0);
    };
  }, []);

  async function save(nextReason: UninterestedReason | null = reason) {
    if (nextReason === "other" && !note.trim()) return;
    setPending(true);
    setError("");
    try {
      const next = await mutate({
        ...target,
        value: true,
        ...(nextReason ? { reason: nextReason } : {}),
        ...(nextReason === "other" ? { note: note.trim() } : {})
      });
      setReason(next.reason);
      onSaved?.(next);
    } catch (cause) {
      setError(errorMessage(cause));
    } finally {
      setPending(false);
    }
  }

  async function restore() {
    setPending(true);
    setError("");
    try {
      onRestored?.(await mutate({ ...target, value: false }));
    } catch (cause) {
      setError(errorMessage(cause));
    } finally {
      setPending(false);
    }
  }

  function handleKeyDown(event: ReactKeyboardEvent<HTMLDialogElement>) {
    if (event.key === "Escape" && !pending) {
      event.preventDefault();
      onClose();
      return;
    }
    if (event.key !== "Tab") return;
    const focusable = dialogFocusableElements(dialogRef.current);
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

  return (
    <dialog ref={modalRef} className="toolbar-modal-backdrop" role="dialog" aria-modal="true" aria-labelledby="uninterested-dialog-title" onCancel={(event) => { event.preventDefault(); if (!pending) onClose(); }} onKeyDown={handleKeyDown} onMouseDown={() => !pending && onClose()}>
      <div ref={dialogRef} className="toolbar-modal uninterested-modal" onMouseDown={(event) => event.stopPropagation()}>
        <div className="toolbar-modal-header">
          <h3 id="uninterested-dialog-title">{editOnly ? "编辑原因" : "已移入“不感兴趣”"}</h3>
          <button className="icon" type="button" aria-label="关闭" disabled={pending} onClick={onClose}>×</button>
        </div>
        {!editOnly ? <p>原因只用于记录；不会自动建立规则。</p> : null}
        <div className="uninterested-reason-options">
          <button
            className={reason === null ? "active" : ""}
            type="button"
            aria-pressed={reason === null}
            disabled={pending}
            onClick={() => {
              setReason(null);
              setNote("");
              void save(null);
            }}
          >
            不填写原因
          </button>
          {REASONS.map(([value, label]) => (
            <button
              key={value}
              className={reason === value ? "active" : ""}
              type="button"
              aria-pressed={reason === value}
              disabled={pending}
              onClick={() => {
                setReason(value);
                if (value !== "other") void save(value);
              }}
            >
              {label}
            </button>
          ))}
        </div>
        {reason === "other" ? (
          <label>简短说明
            <input value={note} maxLength={240} onChange={(event) => setNote(event.target.value)} />
          </label>
        ) : null}
        <div className="uninterested-dialog-actions">
          {reason === "other" || editOnly ? (
            <button type="button" disabled={pending || !reason || (reason === "other" && !note.trim())} onClick={() => void save()}>
              {pending ? "保存中…" : "保存原因"}
            </button>
          ) : null}
          {!editOnly ? <button type="button" disabled={pending} onClick={() => void restore()}>撤销</button> : null}
          <button type="button" disabled={pending} onClick={onClose}>完成</button>
        </div>
        {error ? <p className="error-line" role="alert">{error}</p> : null}
      </div>
    </dialog>
  );
}

function targetMutation(target: UninterestedTarget): MutationTarget {
  if (target.target_kind === "event" && target.event_uid && target.current_revision_uid) {
    return {
      target_type: "event",
      event_uid: target.event_uid,
      observed_revision_uid: target.current_revision_uid
    };
  }
  return { target_type: "item", item_id: Number(target.item_id) };
}

async function mutate(payload: MutationTarget & {
  value: boolean;
  reason?: UninterestedReason;
  note?: string;
}) {
  const response = await fetch("/actions/uninterested", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ operation_id: createOperationId(), ...payload })
  });
  const result = await response.json().catch(() => ({})) as MutationResult & { error?: string };
  if (!response.ok) throw new Error(result.error || "不感兴趣操作失败");
  return result;
}

export function uninterestedReasonLabel(reason: UninterestedReason | string) {
  return REASONS.find(([value]) => value === reason)?.[1] ?? reason;
}

function errorMessage(error: unknown) {
  return userFacingErrorMessage(error, "不感兴趣操作失败");
}

function dialogFocusableElements(container: HTMLElement | null) {
  if (!container) return [];
  return Array.from(
    container.querySelectorAll<HTMLElement>(
      'button:not([disabled]), input:not([disabled]), [tabindex]:not([tabindex="-1"])'
    )
  ).filter((element) => element.tabIndex !== -1);
}
