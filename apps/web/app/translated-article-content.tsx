"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import ArticleContent, {
  extractTranslationBlocks,
  type ReadingTranslationBlock
} from "./article-content";

type TranslationState = {
  status: string;
  translation: string;
  blocks?: ReadingTranslationBlock[];
  model_version: string;
  updated_at: string | null;
};

type TranslatedArticleContentProps = {
  apiUrl: string;
  sourceId?: number;
  bionic?: boolean;
  html?: string | null;
  text: string;
  deferMs?: number;
  initialTranslation?: string;
  translationNeeded?: boolean;
  /** 轮询间隔，仅测试注入；生产走默认值。 */
  pollIntervalMs?: number;
};

// 翻译异步化（#106）：POST 返回 pending 时以同一请求轮询直至终态。
const TRANSLATION_POLL_MS = 2000;
const TRANSLATION_POLL_LIMIT = 150;

export default function TranslatedArticleContent(props: TranslatedArticleContentProps) {
  return (
    <TargetedTranslatedArticleContent
      key={`${props.sourceId ?? "local"}:${readingTarget(props.html ?? null, props.text)}`}
      {...props}
    />
  );
}

function TargetedTranslatedArticleContent({
  apiUrl,
  sourceId,
  bionic = false,
  html = null,
  text,
  deferMs = 0,
  initialTranslation = "",
  translationNeeded,
  pollIntervalMs = TRANSLATION_POLL_MS
}: TranslatedArticleContentProps) {
  const shouldTranslate = translationNeeded !== false;
  const [translation, setTranslation] = useState<TranslationState | null>(() => !html?.trim() && initialTranslation.trim() ? readyTranslation(initialTranslation) : null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const currentTarget = useRef(readingTarget(html, text));
  currentTarget.current = readingTarget(html, text);
  const pollTimer = useRef<number | null>(null);
  const requestController = useRef<AbortController | null>(null);

  const clearPollTimer = useCallback(() => {
    if (pollTimer.current === null) return;
    window.clearTimeout(pollTimer.current);
    pollTimer.current = null;
  }, []);

  const cancelTranslation = useCallback(() => {
    clearPollTimer();
    requestController.current?.abort();
    requestController.current = null;
  }, [clearPollTimer]);

  useEffect(() => {
    cancelTranslation();
    setTranslation(!html?.trim() && initialTranslation.trim() ? readyTranslation(initialTranslation) : null);
    setLoading(false);
    setError("");
    return cancelTranslation;
  }, [apiUrl, cancelTranslation, html, initialTranslation, sourceId, text, translationNeeded]);

  const performRequest = useCallback((retry: boolean, attempt: number) => {
    if (!shouldTranslate) return;
    const requestedTarget = readingTarget(html, text);
    const blocks = html ? translationBlocksFromHtml(html) : [];
    if (html?.trim() && !blocks.length) {
      setTranslation({ status: "skipped", translation: "", model_version: "", updated_at: null });
      setError("");
      return;
    }
    setLoading(true);
    setError("");
    const controller = new AbortController();
    requestController.current?.abort();
    requestController.current = controller;
    const payload = blocks.length ? { blocks, source_id: sourceId } : { text, source_id: sourceId };
    fetch(`${apiUrl.replace(/\/$/, "")}/translations`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(retry ? { ...payload, retry: true } : payload),
      signal: controller.signal
    })
      .then((response) => {
        if (!response.ok) throw new Error("翻译失败");
        return response.json();
      })
      .then((data: TranslationState) => {
        if (controller.signal.aborted || currentTarget.current !== requestedTarget) return;
        if (data.status === "pending") {
          if (attempt + 1 >= TRANSLATION_POLL_LIMIT) {
            setError("翻译超时，请重试");
            setLoading(false);
            return;
          }
          clearPollTimer();
          pollTimer.current = window.setTimeout(() => {
            pollTimer.current = null;
            performRequest(false, attempt + 1);
          }, pollIntervalMs);
          return;
        }
        if (data.status === "error") {
          setError("翻译失败，请重试");
          setLoading(false);
          return;
        }
        setTranslation(data);
        setLoading(false);
      })
      .catch((reason: Error) => {
        if (controller.signal.aborted || currentTarget.current !== requestedTarget) return;
        setError(reason.message || "翻译失败");
        setLoading(false);
      });
  }, [apiUrl, clearPollTimer, html, pollIntervalMs, shouldTranslate, sourceId, text]);

  const requestTranslation = useCallback(() => {
    if (loading) return;
    // 手动触发（含错误后的重试按钮）明确要求重新入队。
    performRequest(Boolean(error), 0);
  }, [error, loading, performRequest]);

  useEffect(() => {
    if (!shouldTranslate || translation || loading || error) return;
    if (deferMs <= 0) {
      performRequest(false, 0);
      return;
    }
    const timer = window.setTimeout(() => performRequest(false, 0), deferMs);
    return () => window.clearTimeout(timer);
  }, [deferMs, error, loading, performRequest, shouldTranslate, translation]);

  if (!shouldTranslate || translation?.status === "skipped" || translation?.status === "empty") {
    return <ArticleContent bionic={bionic} html={html} text={text} />;
  }
  if (
    translation?.status === "ready" &&
    html &&
    validBlockMapping(html, translation.blocks)
  ) {
    return (
      <ArticleContent
        bionic={bionic}
        html={html}
        text={text}
        translations={translation.blocks}
      />
    );
  }
  if (translation?.status === "ready" && translation.translation.trim()) {
    if (html) {
      return (
        <div className="bilingual-content rich-translation-fallback">
          <ArticleContent bionic={bionic} html={html} text={text} />
          <div className="bilingual-translation">
            <ArticleContent text={translation.translation} />
          </div>
        </div>
      );
    }
    return <BilingualBlocks bionic={bionic} original={text} translation={translation.translation} />;
  }
  return (
    <>
      <div className="translation-toolbar">
        <button type="button" disabled={loading} onClick={requestTranslation}>
          {loading ? "翻译中..." : "翻译当前内容"}
        </button>
        {error ? <span className="translation-status">{error}</span> : null}
      </div>
      <ArticleContent bionic={bionic} html={html} text={text} />
    </>
  );
}

export function TranslatedTitle({ text, initialTranslation = "" }: { text: string; initialTranslation?: string }) {
  const original = cleanTitleMarkup(text);
  const translation = cleanTitleMarkup(initialTranslation);
  if (!translation) return <>{original}</>;
  return (
    <span className="bilingual-title">
      <span>{original}</span>
      <span className="title-translation">{translation}</span>
    </span>
  );
}

export function cleanTitleMarkup(value: string) {
  const normalized = value.trim().replace(/\r\n?/g, "\n").replace(/\\n/g, "\n");
  const transportParts = normalized.split(/\n\s*(?:-|=)>\s*(?:\n+|$)/);
  const title = transportParts.at(-1)?.trim() || transportParts[0]?.trim() || "";
  return title
    .replace(/(\*\*|__)(?=\S)([\s\S]*?\S)\1/g, "$2")
    .trim();
}

function BilingualBlocks({ bionic, original, translation }: { bionic: boolean; original: string; translation: string }) {
  const translatedBlocks = splitBlocks(translation).filter((block) => !markdownImage(block));
  let translationIndex = 0;
  return (
    <div className="bilingual-content">
      {splitBlocks(original).map((block, index) => {
        if (markdownImage(block)) return <ArticleContent key={`${block}-${index}`} bionic={bionic} text={block} />;
        const translated = translatedBlocks[translationIndex++] || "";
        return (
          <div key={`${block}-${index}`} className="bilingual-block">
            <div className="bilingual-original">
              <ArticleContent bionic={bionic} text={block} />
            </div>
            {translated ? (
              <div className="bilingual-translation">
                <ArticleContent bionic={bionic} text={translated} />
              </div>
            ) : null}
          </div>
        );
      })}
    </div>
  );
}

function readyTranslation(translation: string): TranslationState {
  return { status: "ready", translation, model_version: "", updated_at: null };
}

function translationBlocksFromHtml(html: string): ReadingTranslationBlock[] {
  if (typeof document === "undefined") return [];
  const template = document.createElement("template");
  template.innerHTML = html;
  return extractTranslationBlocks(template.content);
}

function validBlockMapping(
  html: string,
  blocks: ReadingTranslationBlock[] | undefined
): blocks is ReadingTranslationBlock[] {
  if (!blocks?.length) return false;
  const source = translationBlocksFromHtml(html);
  return (
    source.length === blocks.length &&
    source.every(
      (block, index) =>
        block.id === blocks[index]?.id && Boolean(blocks[index]?.text.trim())
    )
  );
}

function readingTarget(html: string | null, text: string) {
  return `${html || ""}\0${text}`;
}

function splitBlocks(text: string) {
  return text.split(/\n+/).map((line) => line.trim()).filter(Boolean);
}

function markdownImage(line: string) {
  return /^!\[[^\]]*]\((.+)\)$/.test(line.trim());
}
