"use client";

import { X } from "lucide-react";
import { type FormEvent, type ReactNode, useCallback, useEffect, useId, useRef, useState } from "react";

type ConfirmOptions = {
  title: string;
  message: string;
  confirmLabel?: string;
  cancelLabel?: string;
  danger?: boolean;
};

type PromptOptions = ConfirmOptions & {
  defaultValue?: string;
  inputLabel: string;
};

type PendingDialog =
  | (ConfirmOptions & { kind: "confirm"; resolve: (value: boolean) => void })
  | (PromptOptions & { kind: "prompt"; resolve: (value: string | null) => void });

export function useNativeModal(open = true) {
  const dialogRef = useRef<HTMLDialogElement | null>(null);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (open && !dialog.open) {
      if (typeof dialog.showModal === "function") dialog.showModal();
      else dialog.setAttribute("open", "");
    } else if (!open && dialog.open) {
      if (typeof dialog.close === "function") dialog.close();
      else dialog.removeAttribute("open");
    }
  }, [open]);

  return dialogRef;
}

export function useActionDialog(): {
  confirm: (options: ConfirmOptions) => Promise<boolean>;
  prompt: (options: PromptOptions) => Promise<string | null>;
  dialog: ReactNode;
} {
  const [pending, setPending] = useState<PendingDialog | null>(null);
  const [inputValue, setInputValue] = useState("");
  const pendingRef = useRef<PendingDialog | null>(null);
  const dialogRef = useRef<HTMLDialogElement | null>(null);
  const cancelRef = useRef<HTMLButtonElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const returnFocusRef = useRef<HTMLElement | null>(null);
  const id = useId();

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (pending) {
      if (!dialog.open) {
        if (typeof dialog.showModal === "function") dialog.showModal();
        else dialog.setAttribute("open", "");
      }
      window.setTimeout(() => {
        if (pending.kind === "prompt") {
          inputRef.current?.focus();
          inputRef.current?.select();
        } else {
          cancelRef.current?.focus();
        }
      }, 0);
    } else if (dialog.open) {
      if (typeof dialog.close === "function") dialog.close();
      else dialog.removeAttribute("open");
    }
  }, [pending]);

  useEffect(() => () => {
    const current = pendingRef.current;
    pendingRef.current = null;
    if (current?.kind === "prompt") current.resolve(null);
    else current?.resolve(false);
  }, []);

  const open = useCallback(<T,>(request: PendingDialog, fallback: T) => {
    if (pendingRef.current) return Promise.resolve(fallback);
    returnFocusRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    pendingRef.current = request;
    setInputValue(request.kind === "prompt" ? request.defaultValue ?? "" : "");
    setPending(request);
    return null;
  }, []);

  const confirm = useCallback((options: ConfirmOptions) => new Promise<boolean>((resolve) => {
    const fallback = open({ ...options, kind: "confirm", resolve }, false);
    if (fallback) void fallback.then(resolve);
  }), [open]);

  const prompt = useCallback((options: PromptOptions) => new Promise<string | null>((resolve) => {
    const fallback = open({ ...options, kind: "prompt", resolve }, null);
    if (fallback) void fallback.then(resolve);
  }), [open]);

  function finish(value: boolean | string | null) {
    const current = pendingRef.current;
    if (!current) return;
    pendingRef.current = null;
    setPending(null);
    if (current.kind === "prompt") current.resolve(typeof value === "string" ? value : null);
    else current.resolve(value === true);
    window.setTimeout(() => {
      if (returnFocusRef.current?.isConnected) returnFocusRef.current.focus();
    }, 0);
  }

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!pending) return;
    if (pending.kind === "prompt") {
      const value = inputValue.trim();
      if (value) finish(value);
      return;
    }
    finish(true);
  }

  const titleId = `${id}-title`;
  const messageId = `${id}-message`;
  const dialog = (
    <dialog
      ref={dialogRef}
      aria-describedby={messageId}
      aria-labelledby={titleId}
      aria-modal="true"
      className="toolbar-customize-dialog action-dialog"
      onCancel={(event) => {
        event.preventDefault();
        finish(false);
      }}
      onClick={(event) => {
        if (event.target === event.currentTarget) finish(false);
      }}
    >
      {pending ? (
        <form className="toolbar-modal action-dialog-panel" noValidate onSubmit={submit}>
          <div className="toolbar-modal-header">
            <h3 id={titleId}>{pending.title}</h3>
            <button aria-label={`关闭${pending.title}`} className="icon" type="button" onClick={() => finish(false)}><X size={16} /></button>
          </div>
          <div className="action-dialog-body">
            <p id={messageId}>{pending.message}</p>
            {pending.kind === "prompt" ? (
              <label>{pending.inputLabel}<input ref={inputRef} name="action_dialog_value" required value={inputValue} onChange={(event) => setInputValue(event.target.value)} /></label>
            ) : null}
          </div>
          <div className="action-dialog-actions">
            <button ref={cancelRef} type="button" onClick={() => finish(false)}>{pending.cancelLabel ?? "取消"}</button>
            <button className={pending.danger ? "danger" : "primary"} disabled={pending.kind === "prompt" && !inputValue.trim()} type="submit">{pending.confirmLabel ?? "确认"}</button>
          </div>
        </form>
      ) : null}
    </dialog>
  );

  return { confirm, prompt, dialog };
}
