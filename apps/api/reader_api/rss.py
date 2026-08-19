from __future__ import annotations

import hashlib
import logging
import re
from calendar import timegm
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone, tzinfo
from email.utils import parsedate_to_datetime
from html import escape, unescape
from time import perf_counter, struct_time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import feedparser
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .article_fetch import (
    FiveFiltersRules,
    PageFetchResult,
    fetch_article,
    fetch_html as fetch_public_html,
)
from .article_image_cache import (
    configured_article_image_cache,
    download_image,
    prepare_reading_images,
)
from .cluster import assign_cluster, prune_clusters
from .clustering_run import (
    ClusteringRule,
    clustering_run,
    clustering_run_execution_lock,
)
from .content_filters import refresh_filter_matches_for_items
from .digest import (
    canonical_url,
    clean_preview,
    clean_title,
    content_hash,
    digest_score,
    document_type,
    first_markdown_image_url,
    lsh_signature,
    merge_missing_markdown_images,
    normalize_space,
    normalize_title,
    split_digest_items,
    strip_html,
)
from .models import Cluster, ClusterItem, ContentItem, Document, RawEntry, Source, UserState
from .public_rules import active_public_rules
from .reading_content import normalize_reading_content, safe_absolute_url
from .source_entry_relations import record_duplicate_feed_relations
from .source_ingest import IngestEntry, ingest_source_entries, new_revision_indices_for_ingest_entries, refresh_cluster_dates_from_items, source_entries_clustering_item_ids

# Default for feeds that publish local dates without a timezone.
DEFAULT_FEED_TIMEZONE = timezone(timedelta(hours=8))
BIBIGPT_DAILY_FEED_URL = "https://bibigpt.co/popular/rss.xml"
logger = logging.getLogger(__name__)
MAX_FEED_BYTES = 10 * 1024 * 1024
SHORT_RSS_CONTENT_LENGTH = 800
SHORT_NUMERIC_TZ_RE = re.compile(r"([ T]\d{1,2}:\d{2}(?::\d{2})?)\s*([+-])(\d{2})$")
YOUTUBE_VIDEO_ID_RE = re.compile(r"(?:youtube\.com/(?:watch\?v=|shorts/|v/)|youtu\.be/)([A-Za-z0-9_-]{6,})", re.I)
DURATION_FIELD_RE = re.compile(r"""["']?(?:duration|lengthSeconds)["']?\s*:\s*["']?(?P<value>P(?:\d+(?:\.\d+)?D)?(?:T(?:\d+(?:\.\d+)?H)?(?:\d+(?:\.\d+)?M)?(?:\d+(?:\.\d+)?S)?)?|\d+(?:\.\d+)?|\d{1,2}:\d{2}(?::\d{2})?)""", re.I)
TIMELENGTH_FIELD_RE = re.compile(r"""["']?timelength["']?\s*:\s*["']?(?P<milliseconds>\d+(?:\.\d+)?)""", re.I)
ISO_DURATION_RE = re.compile(r"^P(?:(?P<days>\d+(?:\.\d+)?)D)?(?:T(?:(?P<hours>\d+(?:\.\d+)?)H)?(?:(?P<minutes>\d+(?:\.\d+)?)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$", re.I)
MEDIA_URL_EXTENSIONS = {
    "image": (".avif", ".gif", ".jpeg", ".jpg", ".png", ".webp"),
    "video": (".m4v", ".mov", ".mp4", ".webm"),
    "audio": (".aac", ".flac", ".m4a", ".mp3", ".ogg", ".opus", ".wav"),
}
INTERNAL_SMOKE_SOURCE_URL_PREFIXES = (
    "https://deployment-smoke.invalid/",
    "https://p03-smoke.invalid/",
)
RSS_RUN_COUNTS_SESSION_KEY = "reader_rss_run_counts"


@dataclass(frozen=True)
class PreparedReadingBody:
    reading_html: str
    content_text: str
    body_source: str
    web_fetch_status: str
    method: str
    version: str
    diagnostics: tuple[str, ...]
    matched_elements: int
    removed_elements: int
    rss_characters: int
    webpage_characters: int


def fetch_source(session: Session, source: Source, imported_item_ids: list[int] | None = None) -> int:
    started_at = perf_counter()
    fetched_url = source.url
    failure_info = ""
    feed_bytes: bytes | None = None
    response_etag = ""
    response_last_modified = ""
    not_modified = False
    headers = {"User-Agent": "Reader/0.1 (+personal RSS reader)"}
    if source.fetch_etag:
        headers["If-None-Match"] = source.fetch_etag
    if source.fetch_last_modified:
        headers["If-Modified-Since"] = source.fetch_last_modified
    sent_conditional = bool(
        headers.get("If-None-Match") or headers.get("If-Modified-Since")
    )
    download_started_at = perf_counter()
    try:
        request = Request(fetched_url, headers=headers)
        with urlopen(request, timeout=12) as response:
            feed_bytes = response.read(MAX_FEED_BYTES + 1)
            if len(feed_bytes) > MAX_FEED_BYTES:
                feed_bytes = None
                failure_info = "RSS 抓取失败: 响应超过 10 MiB"
            response_headers = getattr(response, "headers", None)
            response_etag = response_header(response_headers, "ETag")
            response_last_modified = response_header(
                response_headers, "Last-Modified"
            )
    except HTTPError as exc:
        if exc.code == 304 and sent_conditional:
            not_modified = True
            response_etag = response_header(exc.headers, "ETag")
            response_last_modified = response_header(exc.headers, "Last-Modified")
        else:
            failure_info = fetch_error_message("RSS 抓取失败", exc)
    except (URLError, TimeoutError, OSError) as exc:
        failure_info = fetch_error_message("RSS 抓取失败", exc)
    download_seconds = perf_counter() - download_started_at

    with locked_current_fetch_source(session, source, fetched_url) as is_current:
        if not is_current:
            return 0
        if failure_info:
            source.last_error = failure_info
            source.last_fetched_at = datetime.now(timezone.utc)
            session.commit()
            logger.info(
                "RSS 源阶段：id=%s mode=error download=%.3fs parse=0.000s ingest=0.000s total=%.3fs",
                source.id,
                download_seconds,
                perf_counter() - started_at,
            )
            return 0
        if not_modified:
            source.fetch_etag = response_etag or source.fetch_etag
            source.fetch_last_modified = (
                response_last_modified or source.fetch_last_modified
            )
            mark_source_fetch_success(session, source)
            logger.info(
                "RSS 源阶段：id=%s mode=not-modified download=%.3fs parse=0.000s ingest=0.000s total=%.3fs",
                source.id,
                download_seconds,
                perf_counter() - started_at,
            )
            return 0

        assert feed_bytes is not None
        payload_hash = hashlib.sha256(feed_bytes).hexdigest()
        if source.last_successful_payload_hash == payload_hash:
            source.fetch_etag = response_etag or None
            source.fetch_last_modified = response_last_modified or None
            mark_source_fetch_success(session, source)
            logger.info(
                "RSS 源阶段：id=%s mode=same-payload download=%.3fs parse=0.000s ingest=0.000s total=%.3fs",
                source.id,
                download_seconds,
                perf_counter() - started_at,
            )
            return 0

        parse_started_at = perf_counter()
        parsed = feedparser.parse(feed_bytes)
        parse_seconds = perf_counter() - parse_started_at
        if parsed.bozo and not parsed.entries:
            source.last_error = fetch_error_message(
                "RSS 解析失败", parsed.bozo_exception
            )
            source.last_fetched_at = datetime.now(timezone.utc)
            session.commit()
            logger.info(
                "RSS 源阶段：id=%s mode=parse-error download=%.3fs parse=%.3fs ingest=0.000s total=%.3fs",
                source.id,
                download_seconds,
                parse_seconds,
                perf_counter() - started_at,
            )
            return 0

        ingest_started_at = perf_counter()
        try:
            imported = ingest_parsed_source(
                session,
                source,
                parsed,
                imported_item_ids=imported_item_ids,
                existing_media_durations=existing_media_durations(
                    session, source
                ),
                fetch_etag=response_etag,
                fetch_last_modified=response_last_modified,
                payload_hash=payload_hash,
            )
        except BaseException:
            session.rollback()
            raise
        logger.info(
            "RSS 源阶段：id=%s mode=ingested download=%.3fs parse=%.3fs ingest=%.3fs total=%.3fs",
            source.id,
            download_seconds,
            parse_seconds,
            perf_counter() - ingest_started_at,
            perf_counter() - started_at,
        )
        return imported


def response_header(headers: object, name: str) -> str:
    getter = getattr(headers, "get", None)
    if not callable(getter):
        return ""
    value = str(getter(name, "") or "").strip()
    return "" if "\r" in value or "\n" in value else value


def mark_source_fetch_success(session: Session, source: Source) -> None:
    source.last_error = ""
    source.last_fetched_at = datetime.now(timezone.utc)
    session.commit()


def existing_media_durations(
    session: Session,
    source: Source,
) -> dict[str, int]:
    if source.media_type != "video":
        return {}
    return {
        url: int(duration)
        for url, duration in session.execute(
            select(ContentItem.canonical_url, ContentItem.media_duration).where(
                ContentItem.source_id == source.id,
                ContentItem.media_duration > 0,
                ContentItem.canonical_url != "",
            )
        )
    }


def source_is_fetch_eligible(source: Source) -> bool:
    return bool(source.enabled) and source.status in ("active", "trial")


def source_is_due_for_scheduled_fetch(source: Source, now: datetime) -> bool:
    if source.url != BIBIGPT_DAILY_FEED_URL:
        return True
    local_now = now.astimezone(DEFAULT_FEED_TIMEZONE)
    if local_now.hour < 22:
        return False
    last_fetched_at = source.last_fetched_at
    if last_fetched_at is None or source.last_error:
        return True
    if last_fetched_at.tzinfo is None:
        last_fetched_at = last_fetched_at.replace(tzinfo=timezone.utc)
    return (
        last_fetched_at.astimezone(DEFAULT_FEED_TIMEZONE).date()
        != local_now.date()
    )


@contextmanager
def locked_current_fetch_source(
    session: Session,
    source: Source,
    attempted_url: str,
) -> Iterator[bool]:
    with clustering_run_execution_lock(session):
        session.refresh(source, with_for_update=True)
        is_current = (
            source.url == attempted_url and source_is_fetch_eligible(source)
        )
        if not is_current:
            session.rollback()
        yield is_current


def ingest_parsed_source(
    session: Session,
    source: Source,
    parsed: Any,
    *,
    imported_item_ids: list[int] | None,
    existing_media_durations: dict[str, int] | None = None,
    fetch_etag: str = "",
    fetch_last_modified: str = "",
    payload_hash: str = "",
) -> int:
    feed_title = normalize_space(parsed.feed.get("title", ""))
    feed_site_url = safe_absolute_url(
        normalize_space(parsed.feed.get("link", "")),
        source.url,
    )
    parsed_entries = list(parsed.entries)
    source_id = source.id
    fetched_url = source.url
    fetch_config = (
        source.media_type,
        source.fetch_full_content,
        source.article_selector,
        source.remove_selector,
        source.status,
        source.enabled,
    )
    source_media_type = source.media_type
    fetch_full_content = source.fetch_full_content
    article_selector = source.article_selector
    remove_selector = source.remove_selector
    public_rules = active_public_rules(session).rules
    session.commit()

    entries = [
        rss_ingest_entry(
            entry,
            source_media_type,
            existing_media_durations or {},
        )
        for entry in parsed_entries
    ]
    initial_source_fetch = not bool(source.last_successful_payload_hash)
    new_revision_indices = new_revision_indices_for_ingest_entries(
        session,
        source_id,
        entries,
    )
    session.rollback()

    prefetch_indices = (
        latest_entry_indices(parsed_entries, limit=20)
        & set(new_revision_indices)
        if initial_source_fetch
        else set(new_revision_indices)
    )
    try:
        image_cache = (
            configured_article_image_cache()
            if new_revision_indices
            else None
        )
    except OSError:
        logger.exception("文章图片缓存不可用，继续保存安全代理地址")
        image_cache = None

    for index in new_revision_indices:
        entry = entries[index]
        prepared_body = prepare_entry_reading_body(
            entry.raw_content or entry.raw_summary,
            entry.content_text,
            entry.url,
            fetch_web=(
                fetch_full_content
                and index in prefetch_indices
            ),
            article_selector=article_selector,
            remove_selector=remove_selector,
            rules=public_rules,
        )
        reading_html = prepared_body.reading_html
        final_text = prepared_body.content_text
        body_source = prepared_body.body_source
        web_fetch_status = prepared_body.web_fetch_status
        try:
            reading_html = prepare_reading_images(
                reading_html,
                image_cache,
                additional_url=(
                    first_markdown_image_url(final_text)
                    or first_markdown_image_url(entry.content_text)
                    or (entry.media_url if entry.media_kind == "image" else "")
                ),
                downloader=download_image,
                prefetch=index in prefetch_indices,
            )
        except Exception:
            logger.exception("文章图片准备失败，正文继续落地")
        entries[index] = replace(
            entry,
            content_text=final_text,
            reading_html=reading_html,
            body_source=body_source,
            web_fetch_status=web_fetch_status,
        )

    with locked_current_fetch_source(
        session,
        source,
        fetched_url,
    ) as is_current:
        current_config = (
            source.media_type,
            source.fetch_full_content,
            source.article_selector,
            source.remove_selector,
            source.status,
            source.enabled,
        )
        if not is_current or current_config != fetch_config:
            session.rollback()
            return 0
        if feed_title and (not source.name or source.name == source.url):
            source.name = feed_title
        elif not source.name:
            source.name = source.url
        source.site_url = source.site_url or feed_site_url
        source.last_error = ""
        source.last_fetched_at = datetime.now(timezone.utc)

        clusterable_source = (
            source.status == "active"
            and source.enabled
            and source.media_type == "article"
        )
        clustering_item_ids = (
            source_entries_clustering_item_ids(session, source, entries)
            if clusterable_source
            else ()
        )
        requires_clustering_run = bool(clustering_item_ids)
        if clusterable_source:
            counters = session.info.get(RSS_RUN_COUNTS_SESSION_KEY)
            if isinstance(counters, dict):
                key = (
                    "clustering_runs"
                    if requires_clustering_run
                    else "skipped_runs"
                )
                counters[key] = int(counters.get(key, 0)) + 1
        failed_entry_ids: list[str] = []
        if requires_clustering_run:
            with clustering_run(
                session,
                scope_type="rss-source-ingest",
                item_ids=clustering_item_ids,
                rule_version=ClusteringRule("rss-source-ingest-v1").version,
            ):
                imported = ingest_source_entries(
                    session,
                    source,
                    entries,
                    imported_item_ids=imported_item_ids,
                    failed_entry_ids=failed_entry_ids,
                )
                apply_fetch_projection_outcome(
                    source,
                    failed_entry_ids,
                    fetch_etag=fetch_etag,
                    fetch_last_modified=fetch_last_modified,
                    payload_hash=payload_hash,
                )
        else:
            imported = ingest_source_entries(
                session,
                source,
                entries,
                imported_item_ids=imported_item_ids,
                failed_entry_ids=failed_entry_ids,
            )
            apply_fetch_projection_outcome(
                source,
                failed_entry_ids,
                fetch_etag=fetch_etag,
                fetch_last_modified=fetch_last_modified,
                payload_hash=payload_hash,
            )
            session.commit()
    return imported


def apply_fetch_projection_outcome(
    source: Source,
    failed_entry_ids: list[str],
    *,
    fetch_etag: str,
    fetch_last_modified: str,
    payload_hash: str,
) -> None:
    if failed_entry_ids:
        source.last_error = (
            f"RSS 部分条目投影失败: {len(failed_entry_ids)}"
        )
        return
    source.fetch_etag = fetch_etag or None
    source.fetch_last_modified = fetch_last_modified or None
    source.last_successful_payload_hash = payload_hash or None


def rss_ingest_entry(
    entry: object,
    source_media_type: str,
    existing_media_durations: dict[str, int],
) -> IngestEntry:
    title = clean_title(entry.get("title", ""))  # type: ignore[attr-defined]
    link = normalize_space(entry.get("link", ""))  # type: ignore[attr-defined]
    summary_html = entry.get("summary", "") or ""  # type: ignore[attr-defined]
    content_html = ""
    if entry.get("content"):  # type: ignore[attr-defined]
        content_html = entry.content[0].get("value", "") or ""  # type: ignore[attr-defined]
    raw_text = strip_html(content_html or summary_html, link)
    normalized = normalize_reading_content(
        content_html or summary_html or f"<p>{escape(raw_text)}</p>",
        link,
    )
    summary = clean_preview(strip_html(summary_html, link), 500)
    media = media_payload_for_entry(
        entry,
        source_media_type,
        known_duration=existing_media_durations.get(canonical_url(link), 0),
    )
    entry_hash = content_hash(title, raw_text or summary, link)
    source_guid = normalize_space(
        entry.get("id") or entry.get("guid") or ""  # type: ignore[attr-defined]
    )
    return IngestEntry(
        external_id=source_guid or (
            content_hash(title, link) if link else entry_hash
        ),
        title=title,
        url=link,
        source_guid=source_guid,
        author=normalize_space(entry.get("author", "")),  # type: ignore[attr-defined]
        published_at=entry_published_at(entry),
        raw_summary=summary_html,
        raw_content=content_html,
        content_text=normalized.content_text,
        reading_html=normalized.reading_html,
        summary=summary,
        content_hash=entry_hash,
        media_url=str(media["url"]),
        media_kind=str(media["kind"]),
        media_duration=int(media["duration"]),
    )


def latest_entry_indices(
    entries: list[object],
    *,
    limit: int,
) -> set[int]:
    indices = list(range(len(entries)))
    published = [entry_published_at(entry) for entry in entries]
    if published and all(value is not None for value in published):
        indices.sort(
            key=lambda index: published[index],
            reverse=True,
        )
    return set(indices[:limit])


def prepare_entry_reading_body(
    rss_html: str,
    rss_text: str,
    link: str,
    *,
    fetch_web: bool,
    article_selector: str | None,
    remove_selector: str | None,
    rules: FiveFiltersRules | None = None,
) -> PreparedReadingBody:
    normalized_rss = normalize_reading_content(
        rss_html or f"<p>{escape(rss_text)}</p>",
        link,
    )
    if not fetch_web or not link:
        return PreparedReadingBody(
            reading_html=normalized_rss.reading_html,
            content_text=normalized_rss.content_text,
            body_source="rss",
            web_fetch_status="not_requested",
            method="rss",
            version="rss-v1",
            diagnostics=(),
            matched_elements=0,
            removed_elements=0,
            rss_characters=len(normalized_rss.content_text),
            webpage_characters=0,
        )
    try:
        result = fetch_article(
            link,
            rss_text,
            article_selector=article_selector,
            remove_selector=remove_selector,
            rules=rules,
        )
    except Exception:
        logger.exception("网页正文抓取失败，采用 RSS")
        result = None
    if result is not None:
        logger.info(
            "网页正文抓取：method=%s version=%s diagnostics=%s",
            result.method,
            result.version,
            ",".join(result.diagnostics),
        )
        if result.body_source == "webpage" and result.content_html:
            normalized_web = normalize_reading_content(
                result.content_html,
                result.final_url or link,
            )
            return PreparedReadingBody(
                reading_html=normalized_web.reading_html,
                content_text=normalized_web.content_text,
                body_source="webpage",
                web_fetch_status="succeeded",
                method=result.method,
                version=result.version,
                diagnostics=result.diagnostics,
                matched_elements=result.matched_elements,
                removed_elements=result.removed_elements,
                rss_characters=len(normalized_rss.content_text),
                webpage_characters=len(normalized_web.content_text),
            )
    return PreparedReadingBody(
        reading_html=normalized_rss.reading_html,
        content_text=normalized_rss.content_text,
        body_source="rss",
        web_fetch_status="failed",
        method=result.method if result is not None else "rss",
        version=result.version if result is not None else "rss-v1",
        diagnostics=(
            result.diagnostics
            if result is not None
            else ("article_fetch_failed",)
        ),
        matched_elements=result.matched_elements if result is not None else 0,
        removed_elements=result.removed_elements if result is not None else 0,
        rss_characters=len(normalized_rss.content_text),
        webpage_characters=(
            result.candidate_characters if result is not None else 0
        ),
    )


def fetch_article_text(
    url: str,
    *,
    article_selector: str | None = None,
    remove_selector: str | None = None,
) -> str:
    result = fetch_article(
        url,
        "",
        article_selector=article_selector,
        remove_selector=remove_selector,
        page_fetcher=fetch_public_html,
    )
    logger.info(
        "网页正文抓取：method=%s version=%s diagnostics=%s",
        result.method,
        result.version,
        ",".join(result.diagnostics),
    )
    return result.content_text if result.body_source == "webpage" else ""


def full_text_for_short_rss(
    raw_text: str,
    link: str,
    *,
    article_selector: str | None = None,
    remove_selector: str | None = None,
) -> str:
    if not link or len(normalize_space(raw_text)) >= SHORT_RSS_CONTENT_LENGTH:
        return ""
    try:
        fetched = (
            fetch_article_text(
                link,
                article_selector=article_selector,
                remove_selector=remove_selector,
            )
            if article_selector or remove_selector
            else fetch_article_text(link)
        )
    except Exception:
        # ponytail: article extraction is opportunistic; a bad page must not break RSS ingest.
        return ""
    return merge_missing_markdown_images(raw_text, fetched) if fetched else ""


def media_payload_for_entry(
    entry: object,
    source_media_type: str,
    *,
    known_duration: int = 0,
) -> dict[str, str | int]:
    candidates: list[dict[str, str | int]] = []
    for enclosure in entry.get("enclosures", []) or []:  # type: ignore[attr-defined]
        candidates.append(media_payload_candidate(enclosure, entry))
    for media in entry.get("media_content", []) or []:  # type: ignore[attr-defined]
        candidates.append(media_payload_candidate(media, entry))
    if source_media_type == "image":
        for thumbnail in entry.get("media_thumbnail", []) or []:  # type: ignore[attr-defined]
            candidates.append(media_payload_candidate(thumbnail, entry, fallback_kind="image"))
    candidates = [candidate for candidate in candidates if candidate["url"] and candidate["kind"]]
    if not candidates:
        duration = entry_media_duration(entry)
        if source_media_type == "video" and not duration:
            duration = known_duration or page_duration_for_entry(entry)
        return {"url": "", "kind": "", "duration": duration}
    media = candidates[0]
    if (media["kind"] == "video" or source_media_type == "video") and not media["duration"]:
        media["duration"] = known_duration or page_duration_for_entry(
            entry, str(media["url"])
        )
    return media


def media_payload_candidate(payload: object, entry: object, fallback_kind: str = "") -> dict[str, str | int]:
    values = payload if isinstance(payload, dict) else {}
    media_url = normalize_space(str(values.get("href") or values.get("url") or ""))
    media_type = normalize_space(str(values.get("type") or ""))
    medium = normalize_space(str(values.get("medium") or ""))
    kind = media_kind_for(media_type, medium, media_url) or fallback_kind
    duration = media_duration_seconds(values.get("duration")) or entry_media_duration(entry)
    return {"url": media_url, "kind": kind, "duration": duration}


def entry_media_duration(entry: object) -> int:
    for key in ("itunes_duration", "itunes:duration", "duration", "media_duration"):
        duration = media_duration_seconds(entry.get(key))  # type: ignore[attr-defined]
        if duration:
            return duration
    for fragment in entry_html_fragments(entry):
        duration = structured_duration_from_text(fragment)
        if duration:
            return duration
    return 0


def media_kind_for(content_type: str, medium: str, media_url: str) -> str:
    value = f"{content_type} {medium}".lower()
    if "image" in value:
        return "image"
    if "video" in value or "shockwave" in value:
        return "video"
    if "audio" in value or "podcast" in value:
        return "audio"
    url_path = media_url.lower().split("?", 1)[0]
    for kind, extensions in MEDIA_URL_EXTENSIONS.items():
        if url_path.endswith(extensions):
            return kind
    if "youtube.com/v/" in media_url.lower() or "youtu.be/" in media_url.lower():
        return "video"
    return ""


def media_duration_seconds(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return max(int(value), 0)
    text = normalize_space(str(value))
    if not text:
        return 0
    if text.isdigit():
        return int(text)
    if re.fullmatch(r"\d+\.\d+", text):
        return max(int(float(text)), 0)
    iso_duration = iso_duration_seconds(text)
    if iso_duration:
        return iso_duration
    parts = text.split(":")
    if 2 <= len(parts) <= 3 and all(part.isdigit() for part in parts):
        seconds = 0
        for part in parts:
            seconds = seconds * 60 + int(part)
        return seconds
    return 0


def iso_duration_seconds(value: str) -> int:
    match = ISO_DURATION_RE.match(value)
    if not match:
        return 0
    total = 0.0
    for key, multiplier in (("days", 86400), ("hours", 3600), ("minutes", 60), ("seconds", 1)):
        if match.group(key):
            total += float(match.group(key)) * multiplier
    return max(int(total), 0)


def entry_html_fragments(entry: object) -> list[str]:
    fragments = [str(entry.get(key) or "") for key in ("summary", "description")]  # type: ignore[attr-defined]
    for content in entry.get("content", []) or []:  # type: ignore[attr-defined]
        if isinstance(content, dict):
            fragments.append(str(content.get("value") or ""))
    return [fragment for fragment in fragments if fragment]


def structured_duration_from_text(value: str) -> int:
    text = unescape(value or "")
    for match in DURATION_FIELD_RE.finditer(text):
        duration = media_duration_seconds(match.group("value"))
        if duration:
            return duration
    for match in TIMELENGTH_FIELD_RE.finditer(text):
        milliseconds = float(match.group("milliseconds"))
        if milliseconds:
            return max(int(milliseconds / 1000), 0)
    return 0


def page_duration_for_entry(entry: object, media_url: str = "") -> int:
    seen: set[str] = set()
    for url in (entry.get("link", ""), media_url):  # type: ignore[attr-defined]
        page_url = duration_page_url(str(url))
        if not page_url or page_url in seen:
            continue
        seen.add(page_url)
        duration = duration_from_page(page_url)
        if duration:
            return duration
    return 0


def youtube_watch_url(url: str) -> str:
    match = YOUTUBE_VIDEO_ID_RE.search(url)
    return f"https://www.youtube.com/watch?v={match.group(1)}" if match else ""


def duration_page_url(url: str) -> str:
    watch_url = youtube_watch_url(url)
    if watch_url:
        return watch_url
    if not url.startswith(("http://", "https://")):
        return ""
    path = url.lower().split("?", 1)[0]
    if path.endswith(MEDIA_URL_EXTENSIONS["video"]):
        return ""
    return url


def duration_from_page(url: str) -> int:
    html = fetch_article_html(url)
    if not html:
        return 0
    return structured_duration_from_text(html)


def fetch_article_html(url: str) -> str:
    result = fetch_public_html(url)
    return result.html if result.succeeded else ""


def extract_article_text(html: str, url: str) -> str:
    page = PageFetchResult(
        succeeded=True,
        html=html,
        final_url=url,
        diagnostic="",
        attempts=0,
        redirects=0,
    )
    result = fetch_article(
        url,
        "",
        page_fetcher=lambda _url: page,
        rules=FiveFiltersRules("compat", {}, {}),
    )
    return result.content_text if result.body_source == "webpage" else ""


def repair_duplicate_feed_entries(session: Session) -> int:
    return record_duplicate_feed_relations(session)


def fetch_enabled_sources(
    session: Session,
    imported_item_ids: list[int] | None = None,
    run_result: dict[str, int] | None = None,
) -> int:
    total = 0
    run_counts = {"clustering_runs": 0, "skipped_runs": 0}
    session.info[RSS_RUN_COUNTS_SESSION_KEY] = run_counts
    sources = session.scalars(
        select(Source).where(
            Source.enabled.is_(True),
            Source.status.in_(("active", "trial")),
            *(
                ~Source.url.startswith(prefix)
                for prefix in INTERNAL_SMOKE_SOURCE_URL_PREFIXES
            ),
        )
    ).all()
    fetch_time = datetime.now(timezone.utc)
    sources = [
        source
        for source in sources
        if source_is_due_for_scheduled_fetch(source, fetch_time)
    ]
    if run_result is not None:
        run_result.update(
            attempted_sources=len(sources),
            successful_sources=0,
        )
    logger.info("RSS 抓取开始：sources=%s", len(sources))
    for source in sources:
        attempted_url = source.url
        before_count = len(imported_item_ids) if imported_item_ids is not None else 0
        logger.info("RSS 源开始：id=%s name=%s", source.id, source.name)
        try:
            imported = fetch_source(session, source, imported_item_ids=imported_item_ids)
            total += imported
            if run_result is not None and not source.last_error:
                run_result["successful_sources"] += 1
            item_delta = (len(imported_item_ids) - before_count) if imported_item_ids is not None else 0
            logger.info("RSS 源完成：id=%s name=%s imported=%s item_ids=%s error=%s", source.id, source.name, imported, item_delta, source.last_error or "")
        except Exception as exc:
            session.rollback()
            try:
                with locked_current_fetch_source(
                    session,
                    source,
                    attempted_url,
                ) as is_current:
                    if not is_current:
                        logger.info(
                            "RSS 源 URL 已变化或失去抓取资格，丢弃异常结果：id=%s name=%s",
                            source.id,
                            source.name,
                        )
                        continue
                    source.last_error = fetch_error_message("RSS 抓取失败", exc)
                    source.last_fetched_at = datetime.now(timezone.utc)
                    session.commit()
                    logger.exception("RSS 源异常：id=%s name=%s", source.id, source.name)
            except Exception:
                session.rollback()
                logger.exception(
                    "RSS 源错误状态记录失败，继续处理其余来源：id=%s name=%s",
                    source.id,
                    source.name,
                )
    session.info.pop(RSS_RUN_COUNTS_SESSION_KEY, None)
    logger.info(
        "RSS 抓取完成：imported=%s item_ids=%s clustering_runs=%s skipped_runs=%s",
        total,
        len(imported_item_ids) if imported_item_ids is not None else 0,
        run_counts["clustering_runs"],
        run_counts["skipped_runs"],
    )
    return total


def repair_over_split_documents(session: Session) -> int:
    document_ids = [
        row[0]
        for row in session.execute(
            select(Document.id)
            .join(ContentItem, ContentItem.document_id == Document.id)
            .group_by(Document.id)
            .having(func.count(ContentItem.id) > 1)
        )
    ]
    repaired = 0
    for document_id in document_ids:
        document = session.get(Document, document_id)
        if document is None:
            continue
        raw_html = ""
        if document.raw_entry is not None:
            raw_html = document.raw_entry.raw_content or document.raw_entry.raw_summary or ""
        score = digest_score(document.title, document.content_text, raw_html)
        if len(split_digest_items(document.title, document.content_text, score)) > 1:
            document.digest_score = score
            document.document_type = document_type(score, document.title)
            continue
        items = sorted(document.content_items, key=lambda item: item.id)
        if len(items) <= 1:
            continue
        keep = original_content_item(document, items)
        delete_ids = [item.id for item in items if item.id != keep.id]
        old_cluster_ids = {
            row[0]
            for row in session.execute(
                select(ClusterItem.cluster_id).where(
                    ClusterItem.content_item_id.in_([item.id for item in items]),
                )
            )
        }
        session.query(ClusterItem).filter(ClusterItem.content_item_id.in_(delete_ids)).delete(synchronize_session=False)
        session.query(UserState).filter(UserState.object_type == "item", UserState.object_id.in_(delete_ids)).delete(synchronize_session=False)
        for item in items:
            if item.id != keep.id:
                session.delete(item)

        keep.title = clean_title(document.title)
        keep.summary = clean_preview(document.summary or document.content_text, 500)
        keep.content_text = document.content_text
        keep.content_hash = content_hash(keep.title, keep.content_text, keep.url)
        keep.canonical_url = canonical_url(keep.url)
        keep.normalized_title = normalize_title(keep.title)
        keep.lsh_signature = lsh_signature(keep.title, keep.content_text)
        keep.embedding_vector = None
        keep.embedding_model = ""
        keep.cluster_score = 0.0
        refresh_filter_matches_for_items(session, [keep.id])
        document.digest_score = score
        document.document_type = document_type(score, document.title)

        current_cluster_ids = {
            row[0]
            for row in session.execute(
                select(ClusterItem.cluster_id).where(ClusterItem.content_item_id == keep.id),
            )
        }
        if keep.source.status == "active" and keep.source.media_type == "article":
            if current_cluster_ids:
                for cluster_id in current_cluster_ids:
                    cluster = session.get(Cluster, cluster_id)
                    if cluster is not None:
                        cluster.title = keep.title
                        refresh_cluster_dates_from_items(session, cluster)
            else:
                cluster = assign_cluster(session, keep)
                current_cluster_ids = {cluster.id}
        else:
            session.query(ClusterItem).filter(ClusterItem.content_item_id == keep.id).delete(synchronize_session=False)
            current_cluster_ids = set()
        prune_clusters(session, old_cluster_ids - current_cluster_ids)
        repaired += 1
    session.commit()
    return repaired


def original_content_item(document: Document, items: list[ContentItem]) -> ContentItem:
    document_title = clean_title(document.title)
    document_text = normalize_space(document.content_text)
    for item in items:
        if clean_title(item.title) == document_title:
            return item
    for item in items:
        if normalize_space(item.content_text) == document_text:
            return item
    return max(items, key=lambda item: len(item.content_text or ""))


def fetch_error_message(prefix: str, exc: object) -> str:
    detail = str(exc) or exc.__class__.__name__
    return f"{prefix}: {detail}"


def entry_published_at(entry: object) -> datetime | None:
    for key in ("published", "updated", "created"):
        published_at = parse_date(entry.get(key))  # type: ignore[attr-defined]
        if published_at is not None:
            return published_at
        published_at = parse_date(entry.get(f"{key}_parsed"))  # type: ignore[attr-defined]
        if published_at is not None:
            return published_at
    return None


def parse_date(value: str | struct_time | None, default_timezone: tzinfo = DEFAULT_FEED_TIMEZONE) -> datetime | None:
    if not value:
        return None
    if isinstance(value, struct_time):
        return datetime.fromtimestamp(timegm(value), timezone.utc)
    value = " ".join(value.strip().split())
    if not value:
        return None
    value = normalize_short_numeric_timezone(value)
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=default_timezone).astimezone(timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_short_numeric_timezone(value: str) -> str:
    return SHORT_NUMERIC_TZ_RE.sub(lambda match: f"{match.group(1)}{match.group(2)}{match.group(3)}00", value)
