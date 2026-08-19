"use client";

import { Check, Copy, Download, Share2 } from "lucide-react";
import { useState } from "react";
import { markdownQuote } from "./markdown-quote";

export function ShareButton({ sourceUrl, title }: { sourceUrl: string; title: string }) {
  return (
    <button className="icon" type="button" title="分享" aria-label="分享" onClick={() => share(title, sourceUrl)}>
      <Share2 size={17} />
    </button>
  );
}

export function CopyLinkButton({ sourceUrl }: { sourceUrl: string }) {
  return (
    <button className="icon" type="button" title="复制链接" aria-label="复制链接" onClick={() => copyText(sourceUrl || window.location.href)}>
      <Copy size={17} />
    </button>
  );
}

export function CopyMarkdownButton({ publishedAt, sourceName, summary, title, url }: { publishedAt: string | null; sourceName: string; summary: string; title: string; url: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      className={`icon ${copied ? "active" : ""}`}
      type="button"
      title={copied ? "已复制 Markdown 引用" : "复制 Markdown 引用"}
      aria-label={copied ? "已复制 Markdown 引用" : "复制 Markdown 引用"}
      onClick={async () => {
        await copyText(markdownQuote({ publishedAt, sourceName, summary, title, url: url || window.location.href }));
        setCopied(true);
        window.setTimeout(() => setCopied(false), 1200);
      }}
    >
      {copied ? <Check size={17} /> : <Copy size={17} />}
    </button>
  );
}

export function PrintButton() {
  return (
    <button className="icon" type="button" title="导出 PDF" aria-label="导出 PDF" onClick={() => window.print()}>
      <Download size={17} />
    </button>
  );
}

async function share(title: string, sourceUrl: string) {
  const url = sourceUrl || window.location.href;
  if (navigator.share) {
    await navigator.share({ title, url }).catch(() => undefined);
    return;
  }
  await copyText(url);
}

export async function copyText(value: string) {
  if (navigator.clipboard) {
    try {
      await navigator.clipboard.writeText(value);
      return;
    } catch {
      // Fall back for non-HTTPS self-hosted installs where Clipboard API is blocked.
    }
  }
  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.top = "0";
  textarea.style.left = "0";
  textarea.style.width = "2em";
  textarea.style.height = "2em";
  textarea.style.padding = "0";
  textarea.style.border = "0";
  textarea.style.outline = "0";
  textarea.style.boxShadow = "none";
  textarea.style.color = "transparent";
  textarea.style.background = "transparent";
  document.body.appendChild(textarea);
  try {
    textarea.focus();
    textarea.select();
    textarea.setSelectionRange(0, textarea.value.length);
    if (typeof document.execCommand !== "function" || !document.execCommand("copy")) throw new Error("copy failed");
  } finally {
    textarea.remove();
  }
}
