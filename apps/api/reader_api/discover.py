from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from itertools import islice

import feedparser

from .config import settings
from .digest import (
    clean_preview,
    clean_title,
    first_markdown_image_url,
    normalize_space,
    strip_html,
)
from .rss import (
    MAX_FEED_BYTES,
    entry_media_duration,
    entry_published_at,
    media_payload_candidate,
    parse_date,
)
from .public_fetch import fetch_public_bytes
from .reading_content import safe_absolute_url
from .source_url import clean_source_url


FEED_CONTENT_TYPES = {
    "application/rss+xml",
    "application/atom+xml",
    "application/feed+json",
    "application/json",
    "text/xml",
    "application/xml",
}


@dataclass(frozen=True)
class FeedCandidate:
    title: str
    url: str


@dataclass(frozen=True)
class FeedPreviewEntry:
    title: str
    summary: str
    image_url: str
    media_url: str
    media_kind: str
    media_duration: int
    url: str
    published_at: datetime | None


@dataclass(frozen=True)
class FeedDiscovery:
    candidates: list[FeedCandidate]
    entries: list[FeedPreviewEntry]
    site_url: str
    title: str


def discover_feed_candidates(url: str) -> FeedDiscovery:
    source_url = clean_source_url(url)
    fetched = fetch_public_bytes(
        source_url,
        max_bytes=MAX_FEED_BYTES,
        trusted_origin=settings.rsshub_base_url,
    )
    if fetched.diagnostic == "body_too_large":
        raise ValueError("Feed 响应超过 10 MiB 限制")
    if not fetched.succeeded:
        raise ValueError("无法读取 Feed，请检查地址和网络连接")
    content_type = fetched.content_type
    body = fetched.body
    if not is_feed_response(content_type, body):
        raise ValueError("请输入直接可访问的 RSS、Atom、JSON Feed 或 Newsletter Feed 地址")

    if is_json_feed(content_type, body):
        title, site_url, entries = json_feed_preview(body)
    else:
        title, site_url, entries = parsed_feed_preview(body)
    return FeedDiscovery(
        candidates=[FeedCandidate(title=title, url=source_url)],
        entries=entries,
        site_url=safe_absolute_url(site_url, source_url) or source_url,
        title=title,
    )


def is_feed_response(content_type: str, body: bytes) -> bool:
    prefix = body[:512].lstrip().lower()
    return (
        content_type in FEED_CONTENT_TYPES
        or prefix.startswith((b"<?xml", b"<rss", b"<feed", b"{"))
    )


def is_json_feed(content_type: str, body: bytes) -> bool:
    return content_type in {"application/feed+json", "application/json"} or body.lstrip().startswith(b"{")


def parsed_feed_preview(body: bytes) -> tuple[str, str, list[FeedPreviewEntry]]:
    parsed = feedparser.parse(body)
    if parsed.bozo and not parsed.entries:
        raise ValueError("Feed 内容无法解析，请确认这是有效的 Feed 地址")
    title = clean_title(parsed.feed.get("title", ""))
    site_url = normalize_space(parsed.feed.get("link", ""))
    return title, site_url, [
        preview_entry(entry, published_at)
        for entry, published_at in islice(
            newest_entries(parsed.entries, entry_published_at),
            6,
        )
    ]


def json_feed_preview(body: bytes) -> tuple[str, str, list[FeedPreviewEntry]]:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Feed 内容无法解析，请确认这是有效的 Feed 地址") from exc
    version = payload.get("version") if isinstance(payload, dict) else ""
    if (
        not isinstance(payload, dict)
        or not isinstance(version, str)
        or not version.startswith("https://jsonfeed.org/version/")
        or not isinstance(payload.get("items", []), list)
    ):
        raise ValueError("Feed 内容无法解析，请确认这是有效的 Feed 地址")
    title = clean_title(str(payload.get("title") or ""))
    site_url = normalize_space(str(payload.get("home_page_url") or ""))
    valid_items = (item for item in payload["items"] if isinstance(item, dict))
    return title, site_url, [
        preview_json_entry(item, published_at)
        for item, published_at in islice(
            newest_entries(valid_items, json_entry_published_at),
            6,
        )
    ]


def newest_entries(entries, published_at_for):
    dated: list[tuple[object, datetime]] = []
    undated: list[tuple[object, None]] = []
    for entry in entries:
        published_at = published_at_for(entry)
        if published_at is None:
            undated.append((entry, None))
        else:
            dated.append((entry, published_at))
    dated.sort(key=lambda value: value[1], reverse=True)
    return iter([*dated, *undated])


def preview_entry(
    entry: object,
    published_at: datetime | None = None,
) -> FeedPreviewEntry:
    link = normalize_space(entry.get("link", ""))  # type: ignore[attr-defined]
    summary_html = entry.get("summary", "") or ""  # type: ignore[attr-defined]
    content_html = ""
    if entry.get("content"):  # type: ignore[attr-defined]
        content_html = entry.content[0].get("value", "") or ""  # type: ignore[attr-defined]
    preview_html = summary_html or content_html
    media = preview_media_payload(entry)
    return FeedPreviewEntry(
        title=clean_title(entry.get("title", "")),  # type: ignore[attr-defined]
        summary=clean_preview(strip_html(preview_html, link), 500),
        image_url=preview_image_url(entry)
        or preview_content_image_url(link, summary_html, content_html),
        media_url=str(media["url"]),
        media_kind=str(media["kind"]),
        media_duration=int(media["duration"]),
        url=link,
        published_at=published_at if published_at is not None else entry_published_at(entry),
    )


def json_entry_published_at(entry: object) -> datetime | None:
    values = entry if isinstance(entry, dict) else {}
    return parse_date(
        str(values.get("date_published") or values.get("date_modified") or "")
    )


def preview_json_entry(
    entry: dict[str, object],
    published_at: datetime | None = None,
) -> FeedPreviewEntry:
    link = normalize_space(str(entry.get("url") or entry.get("external_url") or ""))
    summary_html = str(entry.get("summary") or "")
    content_html = str(entry.get("content_html") or "")
    content_text = str(entry.get("content_text") or "")
    preview_html = summary_html or content_html or content_text
    attachments = entry.get("attachments")
    enclosures = [
        {
            "url": attachment.get("url"),
            "type": attachment.get("mime_type"),
            "duration": attachment.get("duration_in_seconds"),
        }
        for attachment in attachments or []
        if isinstance(attachment, dict)
    ]
    media = preview_media_payload({"enclosures": enclosures})
    image_url = normalize_space(
        str(entry.get("image") or entry.get("banner_image") or "")
    ) or preview_content_image_url(
        link,
        summary_html,
        content_html,
        content_text,
    )
    return FeedPreviewEntry(
        title=clean_title(str(entry.get("title") or "")),
        summary=clean_preview(strip_html(preview_html, link), 500),
        image_url=image_url,
        media_url=str(media["url"]),
        media_kind=str(media["kind"]),
        media_duration=int(media["duration"]),
        url=link,
        published_at=(
            published_at
            if published_at is not None
            else json_entry_published_at(entry)
        ),
    )


def preview_media_payload(entry: object) -> dict[str, str | int]:
    candidates: list[dict[str, str | int]] = []
    for enclosure in entry.get("enclosures", []) or []:  # type: ignore[attr-defined]
        candidates.append(media_payload_candidate(enclosure, entry))
    for media in entry.get("media_content", []) or []:  # type: ignore[attr-defined]
        candidates.append(media_payload_candidate(media, entry))
    for thumbnail in entry.get("media_thumbnail", []) or []:  # type: ignore[attr-defined]
        candidates.append(media_payload_candidate(thumbnail, entry, fallback_kind="image"))
    for candidate in candidates:
        if candidate["url"] and candidate["kind"]:
            return candidate
    return {"url": "", "kind": "", "duration": entry_media_duration(entry)}


def preview_image_url(entry: object) -> str:
    for thumbnail in entry.get("media_thumbnail", []) or []:  # type: ignore[attr-defined]
        candidate = media_payload_candidate(thumbnail, entry, fallback_kind="image")
        if candidate["url"]:
            return str(candidate["url"])
    media = preview_media_payload(entry)
    return str(media["url"]) if media["kind"] == "image" else ""


def preview_content_image_url(link: str, *content_values: str) -> str:
    """Return the first non-placeholder image embedded in feed content.

    ``strip_html`` preserves and resolves images as Markdown, so the existing
    extraction and placeholder rules apply consistently to RSS, Atom and JSON
    Feed without another network request.
    """
    for value in content_values:
        image_url = first_markdown_image_url(strip_html(value, link))
        if image_url:
            return image_url
    return ""
