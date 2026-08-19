"use client";

import { useCallback, useEffect, useRef } from "react";
import { useRouter } from "next/navigation";
import { X } from "lucide-react";

import ArticleContent from "./article-content";
import { TimeText } from "./time-text";

const FOCUSABLE_SELECTOR = 'a[href], button:not([disabled]), input:not([disabled]):not([type="hidden"]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

export type AssistantCitation = { id: number; title: string; source_name: string; published_at: string | null; url: string };
export type AssistantResult = { query: string; answer: string; citations: AssistantCitation[] };

type AssistantContext = {
  id: number;
  title: string;
  closeHref: string;
  formParams: Record<string, string | number | null | undefined>;
};

export function restoreAssistantTriggerFocus() {
  window.setTimeout(() => {
    document.querySelector<HTMLElement>('[data-assistant-trigger="cluster"]')?.focus();
  }, 0);
}

export default function AssistantChatWindow({ context, error, question, result }: { context: AssistantContext; error: string; question: string; result: AssistantResult | null }) {
  const router = useRouter();
  const dialogRef = useRef<HTMLDialogElement | null>(null);
  const closeRef = useRef<HTMLAnchorElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const closeAssistant = useCallback(() => {
    router.push(context.closeHref);
    restoreAssistantTriggerFocus();
  }, [context.closeHref, router]);

  useEffect(() => {
    const focusTimer = window.setTimeout(() => {
      if (inputRef.current) inputRef.current.focus();
      else closeRef.current?.focus();
    }, 0);

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        closeAssistant();
        return;
      }
      if (event.key !== "Tab") return;
      const dialog = dialogRef.current;
      if (!dialog) return;
      const focusable = Array.from(dialog.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR)).filter((element) => element.tabIndex !== -1);
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

    document.addEventListener("keydown", onKeyDown);
    return () => {
      window.clearTimeout(focusTimer);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [closeAssistant]);

  return (
    <dialog ref={dialogRef} open data-testid="assistant-chat" className="assistant-chat" aria-label="聚类助手">
      <div className="assistant-chat-header">
        <div className="assistant-chat-title">
          <strong>聚类助手</strong>
          <div className="item-meta">{context.title}</div>
        </div>
        <a ref={closeRef} className="icon-link" title="关闭助手" href={context.closeHref} onClick={(event) => { event.preventDefault(); closeAssistant(); }}>
          <X size={16} />
        </a>
      </div>
      <div className="assistant-chat-body">
        {question ? (
          <div className="assistant-message-row user">
            <div className="assistant-message">{question}</div>
          </div>
        ) : (
          <div className="placeholder">围绕当前聚类提问，助手会只基于这里的 RSS 内容回答并保留引用。</div>
        )}
        {error ? <p className="error-line">{error}</p> : null}
        {result ? (
          <div className="assistant-answer">
            <ArticleContent text={result.answer} />
            <div className="cluster-sources">
              <h3>引用条目</h3>
              {result.citations.length ? (
                result.citations.map((citation, index) => (
                  <a key={citation.id} className="cluster-source" href={citation.url || "#"}>
                    <div className="item-title">[{index + 1}] {citation.title}</div>
                    <div className="item-meta">
                      {citation.source_name} · <TimeText value={citation.published_at} />
                    </div>
                  </a>
                ))
              ) : (
                <div className="placeholder">没有可引用条目。</div>
              )}
            </div>
          </div>
        ) : null}
      </div>
      <form action="/" method="get" className="assistant-chat-form">
        <HiddenFields values={context.formParams} />
        <input ref={inputRef} aria-label="向 Assistant 提问" name="assistant_ask" defaultValue={question} placeholder="问这个聚类…" />
        <button type="submit">发送</button>
      </form>
    </dialog>
  );
}

function HiddenFields({ values }: { values: Record<string, string | number | null | undefined> }) {
  return (
    <>
      {Object.entries(values).map(([key, value]) => (value !== null && value !== undefined && value !== "" ? <input key={key} type="hidden" name={key} value={String(value)} /> : null))}
    </>
  );
}
