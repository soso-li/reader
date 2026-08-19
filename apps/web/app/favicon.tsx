"use client";

import { useState } from "react";

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "";
const FAVICON_SRC_VERSION = "20260719";

export default function Favicon({ eager = false, url, label = "" }: { eager?: boolean; url: string; label?: string }) {
  const [failedSrc, setFailedSrc] = useState("");
  const src = faviconSrc(url);
  const initial = sourceInitial(label || url);
  const showFallback = !src || failedSrc === src;
  if (!src && !initial) return <span className="favicon-spacer" aria-hidden="true" />;
  return (
    <span className="favicon" title={label} style={showFallback ? { backgroundColor: colorFor(initial || label || url) } : undefined}>
      {showFallback ? initial : null}
      {!showFallback ? <img ref={(image) => { if (image?.complete && image.naturalWidth === 0) setFailedSrc(src); }} src={src} alt="" width="14" height="14" loading={eager ? "eager" : "lazy"} fetchPriority={eager ? "high" : undefined} decoding="async" onError={() => setFailedSrc(src)} /> : null}
    </span>
  );
}

export function faviconSrc(value: string) {
  try {
    const url = new URL(value);
    if (isLocalHost(url.hostname)) return "";
    const base = API_URL.replace(/\/$/, "");
    return `${base}/images/favicon?${new URLSearchParams({ domain: url.hostname, v: FAVICON_SRC_VERSION }).toString()}`;
  } catch {
    return "";
  }
}

function isLocalHost(hostname: string) {
  const host = hostname.toLowerCase();
  if (host === "localhost" || host === "::1" || host === "[::1]" || host.endsWith(".local")) return true;
  const parts = host.split(".").map((part) => Number(part));
  if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part))) return false;
  const [a, b] = parts;
  return a === 10 || a === 127 || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168) || (a === 169 && b === 254);
}

function sourceInitial(value: string) {
  const text = value.trim();
  return text ? text.slice(0, 1).toUpperCase() : "";
}

function colorFor(value: string) {
  const palette = ["#2563eb", "#0f766e", "#7c3aed", "#be123c", "#b45309", "#4d7c0f"];
  const seed = value.charCodeAt(0) || 0;
  return palette[seed % palette.length];
}
