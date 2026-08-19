"use client";

import { type MouseEvent, type ReactNode, useEffect, useRef, useState } from "react";

import Favicon from "./favicon";
import { rssImageSrc } from "./rss-image";
import { displaySourceName } from "./source-name";
import { activateStretchedRowLink } from "./stretched-row-link";
import { previewText } from "./text-preview";
import { TimeText } from "./time-text";
import { cleanTitleMarkup, TranslatedTitle } from "./translated-article-content";

export type BrowseCardItem = {
  id: number;
  source_name: string;
  source_site_url: string;
  title: string;
  title_translation: string;
  summary: string;
  summary_translation: string;
  image_url: string;
  media_url: string;
  media_kind: string;
  media_duration: number;
  content_text: string;
  reading_html?: string | null;
  reading_translation_needed?: boolean;
  url: string;
  published_at: string | null;
  read_status: string;
  read_later: boolean;
  starred: boolean;
  filtered: boolean;
  filter_rules: string[];
};

type CardProps = {
  item: BrowseCardItem;
  active?: boolean;
  eagerFavicon?: boolean;
  href?: string;
  onNavigate?: (event: MouseEvent<HTMLAnchorElement>) => void;
  actions?: ReactNode;
  staticPreview?: boolean;
  emptyMediaLabel?: string;
  requiredMediaKind?: "audio";
};

export function BrowseListRow({ item, active = false, eagerFavicon = false, href, onNavigate, staticPreview = false, emptyMediaLabel, requiredMediaKind, showThumbnail }: CardProps & { showThumbnail: boolean }) {
  const imageUrl = browseImageUrl(item);
  const content = (
    <>
      <div className="item-title">
        {!staticPreview && item.read_status === "unread" ? <span className="unread-dot" aria-hidden="true" /> : null}
        <BrowseTitle href={href} onNavigate={onNavigate}><BrowseTranslatedTitle item={item} /></BrowseTitle>
      </div>
      <BrowseMeta eager={eagerFavicon} item={item} staticPreview={staticPreview} />
      <div className="item-summary">{browseSummary(item)}</div>
      <div className="badges">
        {browseMediaBadge(item) ? <span className="badge">{browseMediaBadge(item)}</span> : null}
        {emptyMediaLabel && (requiredMediaKind ? item.media_kind !== requiredMediaKind : !item.media_url) ? <span className="badge">{emptyMediaLabel}</span> : null}
        {!staticPreview && item.read_status === "original_opened" ? <span className="badge">已打开原文</span> : null}
        {!staticPreview && item.read_later ? <span className="badge">稍后</span> : null}
        {!staticPreview && item.starred ? <span className="badge">星标</span> : null}
        {!staticPreview && item.filtered ? <span className="badge filtered-badge" title={item.filter_rules.join("；")}>已过滤</span> : null}
      </div>
    </>
  );
  return (
    <div className={`item-row ${href ? "clickable-item-row" : ""} ${active ? "active" : ""} ${!staticPreview && isSeenStatus(item.read_status) ? "is-seen" : ""}`} data-scroll-seen-id={staticPreview ? undefined : item.id} onClick={href ? activateStretchedRowLink : undefined}>
      {showThumbnail ? (
        <div className="item-row-main with-thumb">
          <div>{content}</div>
          <BrowseRowThumbnail label={browseTitle(item)} url={imageUrl} />
        </div>
      ) : content}
    </div>
  );
}

export function BrowseImageCard({ item, active = false, eagerFavicon = false, href, onNavigate, actions, staticPreview = false }: CardProps) {
  const imageUrl = browseImageUrl(item);
  return (
    <div className={`browse-card-shell browse-image-card-shell ${active ? "is-active" : ""}`} data-scroll-seen-id={staticPreview ? undefined : item.id}>
      <div className={`browse-image-card ${href ? "clickable-item-row" : ""} ${active ? "active" : ""} ${!staticPreview && isSeenStatus(item.read_status) ? "is-seen" : ""}`} onClick={href ? activateStretchedRowLink : undefined}>
        <div className="browse-image-thumb">
          {imageUrl ? <BrowseImageTileImage url={imageUrl} /> : <span>无图片</span>}
          {!staticPreview && item.read_status === "unread" ? <span className="unread-dot" aria-hidden="true" /> : null}
        </div>
        <strong><BrowseTitle href={href} onNavigate={onNavigate}><BrowseTranslatedTitle item={item} /></BrowseTitle></strong>
        <BrowseMeta eager={eagerFavicon} item={item} staticPreview={staticPreview} />
      </div>
      {actions}
    </div>
  );
}

export function BrowseSocialCard({ item, active = false, eagerFavicon = false, href, onNavigate, staticPreview = false, emptyMediaLabel }: CardProps) {
  const imageUrl = browseImageUrl(item);
  return (
    <div className={`browse-social-card ${href ? "clickable-item-row" : ""} ${active ? "active" : ""} ${!staticPreview && isSeenStatus(item.read_status) ? "is-seen" : ""}`} data-scroll-seen-id={staticPreview ? undefined : item.id} onClick={href ? activateStretchedRowLink : undefined}>
      {!staticPreview && item.read_status === "unread" ? <span className="browse-feed-dot" aria-hidden="true" /> : null}
      <div className="browse-social-avatar"><Favicon eager={eagerFavicon} url={browseSourceIconUrl(item)} label={browseSourceName(item)} /></div>
      <div className="browse-social-content">
        <div className="browse-social-meta"><strong>{browseSourceName(item)}</strong><span>·</span><TimeText interactive={!staticPreview} value={item.published_at} /></div>
        <strong className="browse-social-title"><BrowseTitle href={href} onNavigate={onNavigate}><BrowseTranslatedTitle item={item} /></BrowseTitle></strong>
        <p>{browseSummary(item)}</p>
        {imageUrl ? <div className="browse-social-image"><BrowseImageTileImage url={imageUrl} /></div> : emptyMediaLabel ? <span className="browse-preview-missing-media">{emptyMediaLabel}</span> : null}
      </div>
    </div>
  );
}

export function BrowseVideoCard({ item, active = false, eagerFavicon = false, href, onNavigate, actions, staticPreview = false }: CardProps) {
  const imageUrl = browseImageUrl(item);
  return (
    <div className={`browse-card-shell browse-video-card-shell ${active ? "is-active" : ""}`} data-scroll-seen-id={staticPreview ? undefined : item.id}>
      <div className={`browse-video-card ${href ? "clickable-item-row" : ""} ${active ? "active" : ""} ${!staticPreview && isSeenStatus(item.read_status) ? "is-seen" : ""}`} onClick={href ? activateStretchedRowLink : undefined}>
        <div className="browse-video-thumb">
          {imageUrl ? <BrowseImageTileImage url={imageUrl} /> : <span>无缩略图</span>}
          {item.media_duration ? <span className="browse-duration">{formatBrowseDuration(item.media_duration)}</span> : null}
        </div>
        <strong className="browse-card-title"><BrowseTitle href={href} onNavigate={onNavigate}>{!staticPreview && item.read_status === "unread" ? <span className="browse-inline-dot" aria-hidden="true" /> : null}<BrowseTranslatedTitle item={item} /></BrowseTitle></strong>
        <BrowseMeta eager={eagerFavicon} item={item} staticPreview={staticPreview} />
      </div>
      {actions}
    </div>
  );
}

export function browseTitle(item: BrowseCardItem) {
  return cleanTitleMarkup(item.title || "无标题");
}

export function browseSummary(item: BrowseCardItem) {
  return previewText(item.summary_translation || item.summary || item.content_text || item.title);
}

export function browseImageUrl(item: Pick<BrowseCardItem, "image_url" | "media_kind" | "media_url">) {
  return item.image_url || (item.media_kind === "image" ? item.media_url : "");
}

export function formatBrowseDuration(seconds: number) {
  const value = Math.max(Math.floor(seconds), 0);
  const hours = Math.floor(value / 3600);
  const minutes = Math.floor((value % 3600) / 60);
  const rest = value % 60;
  if (hours) return `${hours}:${String(minutes).padStart(2, "0")}:${String(rest).padStart(2, "0")}`;
  return `${minutes}:${String(rest).padStart(2, "0")}`;
}

function BrowseTitle({ href, onNavigate, children }: { href?: string; onNavigate?: (event: MouseEvent<HTMLAnchorElement>) => void; children: ReactNode }) {
  return href ? <a className="stretched-row-link" href={href} onClick={onNavigate}>{children}</a> : <span>{children}</span>;
}

function BrowseTranslatedTitle({ item }: { item: BrowseCardItem }) {
  return <TranslatedTitle text={item.title || "无标题"} initialTranslation={item.title_translation} />;
}

function BrowseMeta({ item, eager, staticPreview }: { item: BrowseCardItem; eager: boolean; staticPreview: boolean }) {
  return <span className="item-meta item-meta-source"><Favicon eager={eager} url={browseSourceIconUrl(item)} label={browseSourceName(item)} /><span>{browseSourceName(item)}</span><span>·</span><TimeText interactive={!staticPreview} value={item.published_at} /></span>;
}

function BrowseRowThumbnail({ label, url }: { label: string; url: string }) {
  const [failed, setFailed] = useState(false);
  const imageRef = useRef<HTMLImageElement>(null);
  useEffect(() => {
    const image = imageRef.current;
    if (image?.complete && image.naturalWidth === 0) setFailed(true);
  }, []);
  return url && !failed ? <img ref={imageRef} className="item-thumb" src={rssImageSrc(url)} alt="" loading="lazy" fetchPriority="low" decoding="async" onError={() => setFailed(true)} /> : <span className="item-thumb item-thumb-placeholder">{sourceInitial(label)}</span>;
}

function BrowseImageTileImage({ url }: { url: string }) {
  const [failed, setFailed] = useState(false);
  const imageRef = useRef<HTMLImageElement>(null);
  useEffect(() => {
    const image = imageRef.current;
    if (image?.complete && image.naturalWidth === 0) setFailed(true);
  }, []);
  return failed ? <span>图片不可用</span> : <img ref={imageRef} src={rssImageSrc(url)} alt="" loading="lazy" fetchPriority="low" decoding="async" onError={() => setFailed(true)} />;
}

export function browseSourceIconUrl(item: Pick<BrowseCardItem, "source_site_url" | "url">) {
  return item.source_site_url || item.url;
}

export function browseSourceName(item: Pick<BrowseCardItem, "source_name">) {
  return displaySourceName(item.source_name);
}

function browseMediaBadge(item: BrowseCardItem) {
  const label = item.media_kind === "image" ? "图片" : item.media_kind === "video" ? "视频" : item.media_kind === "audio" ? "音频" : "";
  const duration = item.media_duration ? formatBrowseDuration(item.media_duration) : "";
  if (label && duration) return `${label} · ${duration}`;
  return label || duration;
}

function sourceInitial(value: string) {
  const text = value.trim();
  return text ? text.slice(0, 1).toUpperCase() : "?";
}

function isSeenStatus(status: string) {
  return status === "summary_seen" || status === "original_opened" || status === "dismissed";
}
