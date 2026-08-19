"use client";

import { memo, useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";

import { rssImageSrc } from "./rss-image";

export type ReadingTranslationBlock = { id: string; text: string };
const emptyTranslations: ReadingTranslationBlock[] = [];

export default function ArticleContent({
  bionic = false,
  html = null,
  text,
  translations = emptyTranslations
}: {
  bionic?: boolean;
  html?: string | null;
  text: string;
  translations?: ReadingTranslationBlock[];
}) {
  if (html?.trim()) {
    return (
      <RichArticleContent
        bionic={bionic}
        html={html}
        translations={translations}
      />
    );
  }
  return (
    <div className={bionic ? "article-text bionic-text" : "article-text"}>
      {text.split(/\n+/).map((line, index) => {
        const image = markdownImage(line);
        if (image) {
          return <ArticleImage key={`${image.src}-${index}`} alt={image.alt} src={image.src} />;
        }
        const heading = line.trim().match(/^(#{1,6})\s+(.+)$/);
        if (heading) {
          const content = bionic ? bionicInline(heading[2]) : heading[2];
          return heading[1].length <= 2 ? <h2 key={`${line}-${index}`}>{content}</h2> : <h3 key={`${line}-${index}`}>{content}</h3>;
        }
        return <p key={`${line}-${index}`}>{bionic ? bionicInline(line) : line}</p>;
      })}
    </div>
  );
}

const RichArticleContent = memo(function RichArticleContent({
  bionic,
  html,
  translations
}: {
  bionic: boolean;
  html: string;
  translations: ReadingTranslationBlock[];
}) {
  const rootRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const root = rootRef.current;
    if (!root) return;
    if (root.innerHTML !== html) root.innerHTML = html;
    const removeImageListeners = prepareArticleImages(root);
    if (translations.length) insertBlockTranslations(root, translations);
    if (bionic) applyBionicReading(root);
    return removeImageListeners;
  }, [bionic, html, translations]);

  return (
    <div
      ref={rootRef}
      className={bionic ? "article-text article-rich-content bionic-text" : "article-text article-rich-content"}
      dangerouslySetInnerHTML={{ __html: html }}
      onErrorCapture={(event) => {
        const image = event.target;
        if (!(image instanceof HTMLImageElement)) return;
        replaceFailedArticleImage(image);
      }}
    />
  );
});

function prepareArticleImages(root: HTMLElement) {
  const cleanups: Array<() => void> = [];
  for (const image of root.querySelectorAll<HTMLImageElement>("img")) {
    if (image.parentElement?.classList.contains("article-image-frame")) continue;
    if (image.complete && image.naturalWidth === 0) {
      replaceFailedArticleImage(image);
      continue;
    }
    const frame = root.ownerDocument.createElement("span");
    const loading = root.ownerDocument.createElement("span");
    frame.className = "article-image-frame";
    frame.setAttribute("aria-busy", "true");
    loading.className = "article-image-loading";
    loading.textContent = "图片正在加载…";
    image.before(frame);
    frame.append(loading, image);
    const loaded = () => {
      frame.classList.add("is-loaded");
      frame.removeAttribute("aria-busy");
      loading.remove();
    };
    if (image.complete && image.naturalWidth > 0) loaded();
    else {
      image.addEventListener("load", loaded, { once: true });
      cleanups.push(() => image.removeEventListener("load", loaded));
    }
  }
  return () => cleanups.forEach((cleanup) => cleanup());
}

function replaceFailedArticleImage(image: HTMLImageElement) {
  const fallback = image.ownerDocument.createElement("p");
  fallback.className = "article-image-alt";
  fallback.textContent = imageFailureText(image.alt);
  (image.closest(".article-image-frame") || image).replaceWith(fallback);
}

export function extractTranslationBlocks(root: ParentNode): ReadingTranslationBlock[] {
  const blocks: ReadingTranslationBlock[] = [];
  const seen = new Set<string>();
  for (const element of root.querySelectorAll<HTMLElement>("[data-reader-block-id]")) {
    const id = element.dataset.readerBlockId || "";
    if (
      !/^block-[0-9a-f]{16}$/.test(id) ||
      seen.has(id) ||
      element.matches("pre, code")
    ) {
      if (seen.has(id)) return [];
      continue;
    }
    const clone = element.cloneNode(true) as HTMLElement;
    clone.querySelectorAll("[data-reader-block-id]").forEach((node) => node.replaceWith(" "));
    clone.querySelectorAll("pre, code, .bilingual-translation").forEach((node) => node.remove());
    const text = stripUrls(clone.textContent || "").replace(/\s+/g, " ").trim();
    if (!text) continue;
    seen.add(id);
    blocks.push({ id, text });
  }
  return blocks;
}

export function insertBlockTranslations(
  root: ParentNode,
  translations: ReadingTranslationBlock[]
): boolean {
  const sourceBlocks = extractTranslationBlocks(root);
  const translatedById = new Map<string, string>();
  for (const block of translations) {
    const text = block.text.trim();
    if (!text || translatedById.has(block.id)) return false;
    translatedById.set(block.id, text);
  }
  if (
    sourceBlocks.length !== translations.length ||
    sourceBlocks.some((block) => !translatedById.has(block.id))
  ) {
    return false;
  }
  const elements = Array.from(
    root.querySelectorAll<HTMLElement>("[data-reader-block-id]")
  );
  for (const block of sourceBlocks) {
    const element = elements.find(
      (candidate) => candidate.dataset.readerBlockId === block.id
    );
    if (!element) return false;
    const translated = element.ownerDocument.createElement("span");
    translated.className = "bilingual-translation rich-block-translation";
    translated.lang = "zh-Hans";
    translated.textContent = translatedById.get(block.id) || "";
    const nestedBlock = Array.from(element.children).find(
      (child) =>
        child.matches("[data-reader-block-id]") ||
        child.querySelector("[data-reader-block-id]")
    );
    element.insertBefore(translated, nestedBlock || null);
  }
  return true;
}

export function applyBionicReading(root: ParentNode): void {
  const document = root.ownerDocument;
  if (!document) return;
  const walker = document.createTreeWalker(root, document.defaultView?.NodeFilter.SHOW_TEXT ?? 4);
  const textNodes: Text[] = [];
  for (let node = walker.nextNode(); node; node = walker.nextNode()) {
    textNodes.push(node as Text);
  }
  for (const textNode of textNodes) {
    const parent = textNode.parentElement;
    if (
      !parent ||
      parent.closest("pre, code, .bilingual-translation") ||
      !/[A-Za-z]/.test(textNode.data)
    ) {
      continue;
    }
    const fragment = document.createDocumentFragment();
    let last = 0;
    for (const match of textNode.data.matchAll(
      /(?:https?:\/\/|www\.)[^\s<]+|(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?:[/:?#][^\s<]*)?|[A-Za-z][A-Za-z']*/g
    )) {
      const index = match.index ?? 0;
      const value = match[0];
      if (index > last) fragment.append(textNode.data.slice(last, index));
      if (looksLikeUrl(value)) {
        fragment.append(value);
      } else {
        const word = document.createElement("span");
        const strong = document.createElement("strong");
        const split = Math.max(1, Math.ceil(value.length * 0.45));
        word.className = "bionic-word";
        strong.textContent = value.slice(0, split);
        word.append(strong, value.slice(split));
        fragment.append(word);
      }
      last = index + value.length;
    }
    if (!last) continue;
    if (last < textNode.data.length) fragment.append(textNode.data.slice(last));
    textNode.replaceWith(fragment);
  }
}

function stripUrls(text: string) {
  return text.replace(
    /(?:https?:\/\/|www\.)\S+|(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?:[/:?#]\S*)?/gi,
    " "
  );
}

function looksLikeUrl(value: string) {
  return /^(?:https?:\/\/|www\.|(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,})/i.test(value);
}

function ArticleImage({ alt, src }: { alt: string; src: string }) {
  const [failed, setFailed] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const imageRef = useRef<HTMLImageElement>(null);
  useEffect(() => {
    const image = imageRef.current;
    if (image?.complete && image.naturalWidth === 0) setFailed(true);
    else if (image?.complete) setLoaded(true);
  }, []);
  if (failed) {
    return <p className="article-image-alt">{imageFailureText(alt)}</p>;
  }
  return (
    <span className={`article-image-frame${loaded ? " is-loaded" : ""}`} aria-busy={loaded ? undefined : "true"}>
      {!loaded ? <span className="article-image-loading">图片正在加载…</span> : null}
      <img
        ref={imageRef}
        className="article-image"
        src={rssImageSrc(src)}
        data-original-src={src}
        alt={alt || "RSS 图片"}
        loading="lazy"
        decoding="async"
        onLoad={() => setLoaded(true)}
        onError={() => setFailed(true)}
      />
    </span>
  );
}

function imageFailureText(alt: string) {
  return alt.trim() ? `图片不可用：${alt.trim()}` : "图片不可用";
}

function markdownImage(line: string): { alt: string; src: string } | null {
  const image = line.trim().match(/^!\[([^\]]*)\]\((.+)\)$/);
  if (!image) return null;
  const src = image[2].trim().replace(/^<(.+)>$/, "$1");
  if (!/^https?:\/\//.test(src)) return null;
  return { alt: image[1].trim(), src };
}

function bionicInline(text: string) {
  const nodes: ReactNode[] = [];
  let last = 0;
  for (const match of text.matchAll(/[A-Za-z][A-Za-z']*/g)) {
    const index = match.index ?? 0;
    const word = match[0];
    if (index > last) nodes.push(text.slice(last, index));
    const split = Math.max(1, Math.ceil(word.length * 0.45));
    nodes.push(
      <span key={`${word}-${index}`}>
        <strong>{word.slice(0, split)}</strong>
        {word.slice(split)}
      </span>
    );
    last = index + word.length;
  }
  if (last < text.length) nodes.push(text.slice(last));
  return nodes;
}
