"use client";

import { MouseEvent, useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";

import { sendClientUserState } from "./client-user-state";
import Favicon from "./favicon";
import { rssImageSrc } from "./rss-image";
import { activateStretchedRowLink } from "./stretched-row-link";
import { TimeText } from "./time-text";
import { TranslatedTitle } from "./translated-article-content";

type ThumbnailMode = "always" | "auto" | "never";
type Props = {
  active: boolean;
  apiUrl?: string;
  href: string;
  id: number;
  meta: ReactNode;
  objectType?: "topic";
  readLater: boolean;
  readStatus: string;
  starred: boolean;
  summary: string;
  thumbnailMode?: ThumbnailMode;
  thumbnailUrl?: string;
  title: string;
  titleTranslation?: string;
  sources?: Array<{ id: number; source_name: string; source_site_url?: string; title: string; title_translation?: string; published_at: string | null; url?: string }>;
  onSelect?: (event: MouseEvent<HTMLElement>) => void;
};

export default function ClusterRowLink({ active, apiUrl, href, id, meta, objectType, onSelect, readLater, readStatus, starred, sources = [], summary, thumbnailMode = "never", thumbnailUrl = "", title, titleTranslation = "" }: Props) {
  const followupSources = sources.slice(1);
  const showThumbnail = Boolean(thumbnailUrl && (thumbnailMode === "always" || thumbnailMode === "auto"));

  function markSeen() {
    if (readStatus !== "unread" || !objectType) return;
    void sendClientUserState({ object_type: objectType, object_id: id, read_status: "summary_seen" }).catch(() => undefined);
  }

  function handleClick(event: MouseEvent<HTMLElement>) {
    if (!onSelect) markSeen();
    onSelect?.(event);
  }

  const content = (
    <>
      <div className="item-title">
        {readStatus === "unread" ? <span className="unread-dot" aria-hidden="true" /> : null}
        <a className="stretched-row-link" href={href} onClick={handleClick}>
          {apiUrl ? <TranslatedTitle text={title} initialTranslation={titleTranslation} /> : title}
        </a>
      </div>
      <div className="item-meta">{meta}</div>
      <div className="item-summary">{summary}</div>
      {followupSources.length ? (
        <div className="cluster-row-followups" aria-label="后续来源报道">
          <div className="cluster-followup-rail" aria-hidden="true">
            {followupSources.map((source, index) => (
              <span key={`${source.id}-${index}`} className={`cluster-followup-segment source-tone-${index % 6}`} />
            ))}
          </div>
          <div className="cluster-followup-list">
            {followupSources.map((source, index) => (
              <div key={source.id} className="cluster-followup-item">
                <span className={`cluster-followup-logo source-tone-${index % 6}`}>
                  <Favicon url={source.source_site_url || source.url || ""} label={source.source_name} />
                </span>
                <span className="cluster-followup-title" title={source.source_name}>
                  {apiUrl ? <TranslatedTitle text={source.title || source.source_name || "无标题来源"} initialTranslation={source.title_translation || ""} /> : source.title || source.source_name || "无标题来源"}
                </span>
                <span className="cluster-followup-time">
                  <TimeText value={source.published_at} />
                  收到
                </span>
              </div>
            ))}
          </div>
        </div>
      ) : null}
      <div className="badges">
        {readStatus === "dismissed" ? <span className="badge">忽略</span> : null}
        {readLater ? <span className="badge">稍后</span> : null}
        {starred ? <span className="badge">星标</span> : null}
      </div>
    </>
  );

  return (
    <div className={`item-row clickable-item-row ${active ? "active" : ""} ${isSeenStatus(readStatus) ? "is-seen" : ""}`} onClick={activateStretchedRowLink}>
      {showThumbnail ? (
        <div className="item-row-main with-thumb">
          <div className="item-row-content">{content}</div>
          <RowThumbnail title={title} url={thumbnailUrl} />
        </div>
      ) : (
        content
      )}
    </div>
  );
}

function isSeenStatus(status: string) {
  return status === "summary_seen" || status === "original_opened" || status === "dismissed";
}

function RowThumbnail({ title, url }: { title: string; url: string }) {
  const [failed, setFailed] = useState(false);
  const imageRef = useRef<HTMLImageElement>(null);
  useEffect(() => {
    const image = imageRef.current;
    if (image?.complete && image.naturalWidth === 0) setFailed(true);
  }, []);
  return (
    <span className="item-thumb item-thumb-frame">
      {url && !failed ? <img ref={imageRef} className="item-thumb-image" src={rssImageSrc(url)} alt="" loading="lazy" fetchPriority="low" decoding="async" onError={() => setFailed(true)} /> : <span>{sourceInitial(title)}</span>}
    </span>
  );
}

function sourceInitial(value: string) {
  const text = value.trim();
  if (!text) return "?";
  return text.slice(0, 1).toUpperCase();
}
