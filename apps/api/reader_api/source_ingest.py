from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html import escape
import logging

from sqlalchemy import exists, func, select, tuple_
from sqlalchemy.orm import Session, selectinload

from .digest import canonical_url, clean_preview, clean_title, content_hash, digest_score, document_type, lsh_signature, normalize_space, normalize_title, split_digest_items, strip_html
from .content_filters import refresh_filter_matches_for_items
from .models import (
    Cluster,
    ClusterItem,
    ContentEmbedding,
    ContentItem,
    Document,
    FeedMetric,
    RawEntry,
    Source,
    SourceEntryIdentity,
    SourceEntryKey,
)
from .reading_content import safe_absolute_url
from .source_entry_identity import (
    SourceEntryIdentityInput,
    SourceEntryNaturalKey,
    natural_source_entry_key,
    resolve_source_entry_identity,
)
from .source_entry_revision import (
    RawEntryRevisionInput,
    RawEntryRevisionOutcome,
    allocate_raw_entry_revision,
    calculate_payload_fingerprint,
    raw_entry_revision_values,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestEntry:
    external_id: str
    title: str
    url: str
    author: str = ""
    published_at: datetime | None = None
    raw_summary: str = ""
    raw_content: str = ""
    content_text: str = ""
    reading_html: str = ""
    summary: str = ""
    content_hash: str = ""
    media_url: str = ""
    media_kind: str = ""
    media_duration: int = 0
    source_guid: str = ""
    body_source: str = "rss"
    web_fetch_status: str = "not_requested"


@dataclass(frozen=True)
class _PreparedIngestEntry:
    source_guid: str
    revision: RawEntryRevisionInput
    raw_text: str
    reading_html: str
    summary: str
    media_url: str
    media_kind: str
    media_duration: int
    body_source: str
    web_fetch_status: str


def create_source_entry_with_initial_revision(
    session: Session,
    source: Source,
    natural_key: SourceEntryNaturalKey,
    prepared: _PreparedIngestEntry,
) -> RawEntry:
    identity = SourceEntryIdentity(
        source_id=source.id,
        current_revision_no=1,
        projection_pending=False,
    )
    session.add(identity)
    session.flush()
    session.add(
        SourceEntryKey(
            source_entry_id=identity.id,
            source_id=source.id,
            identity_kind=natural_key.identity_kind,
            identity_key=natural_key.identity_key,
        )
    )
    revision = prepared.revision
    raw = RawEntry(
        **raw_entry_revision_values(
            source_id=source.id,
            source_entry_id=identity.id,
            revision_no=1,
            revision=revision,
        )
    )
    session.add(raw)
    session.flush()
    return raw


def ingest_source_entries(
    session: Session,
    source: Source,
    entries: list[IngestEntry],
    imported_item_ids: list[int] | None = None,
    failed_entry_ids: list[str] | None = None,
) -> int:
    """Write adapter-normalized entries into the canonical RawEntry -> Document -> ContentItem path."""
    _ensure_sqlite_outer_transaction(session)
    imported = 0
    for entry in entries:
        try:
            with session.begin_nested():
                imported_delta, queued_item_ids = _ingest_source_entry(
                    session,
                    source,
                    entry,
                )
        except Exception:
            if failed_entry_ids is None:
                raise
            failed_entry_ids.append(entry.external_id)
            logger.exception(
                "来源条目投影失败，继续处理后续条目：source_id=%s external_id=%s",
                source.id,
                entry.external_id,
            )
            continue
        imported += imported_delta
        if imported_item_ids is not None:
            imported_item_ids.extend(queued_item_ids)

    if imported:
        metric = session.scalar(select(FeedMetric).where(FeedMetric.source_id == source.id))
        if metric is None:
            metric = FeedMetric(source_id=source.id)
            session.add(metric)
        metric.fetched_count = (metric.fetched_count or 0) + imported
    return imported


def _ensure_sqlite_outer_transaction(session: Session) -> None:
    connection = session.connection()
    if connection.dialect.name != "sqlite":
        return
    driver_connection = connection.connection.driver_connection
    if not driver_connection.in_transaction:
        connection.exec_driver_sql("BEGIN")


def _ingest_source_entry(
    session: Session,
    source: Source,
    entry: IngestEntry,
) -> tuple[int, list[int]]:
    prepared = prepare_ingest_entry(entry)
    fingerprint = calculate_payload_fingerprint(prepared.revision)
    resolution = resolve_source_entry_identity(
        session,
        source.id,
        _source_entry_identity_input(prepared, fingerprint),
    )
    if resolution.source_entry is None:
        raw = create_source_entry_with_initial_revision(
            session,
            source,
            resolution.natural_key,
            prepared,
        )
        return 1, create_initial_projection(
            session,
            source,
            raw,
            prepared,
        )

    current_raw = current_raw_entry(
        session,
        resolution.source_entry,
    )
    document = projected_document(
        session,
        resolution.source_entry.id,
        current_raw,
    )
    if document is not None and document.document_type in {"digest", "mixed"}:
        allocation = allocate_raw_entry_revision(
            session,
            resolution.source_entry.id,
            prepared.revision,
        )
        if (
            allocation.raw_entry.revision_no
            == resolution.source_entry.current_revision_no
            and _project_missing_publication_date(
                session,
                resolution.source_entry,
                document,
                allocation.raw_entry,
            )
        ):
            return 0, []
        if allocation.outcome is RawEntryRevisionOutcome.CREATED:
            resolution.source_entry.projection_pending = True
            session.flush()
        return 0, []
    if (
        document is None
        or document.document_type != "normal_article"
        or len(document.content_items) != 1
    ):
        return 0, []
    projection_incomplete = _normal_article_projection_incomplete(
        resolution.source_entry,
        current_raw,
        document,
    )

    allocation = allocate_raw_entry_revision(
        session,
        resolution.source_entry.id,
        prepared.revision,
    )
    if allocation.outcome is RawEntryRevisionOutcome.EXISTING:
        item = document.content_items[0]
        # Repeating an older known payload must never roll the projection
        # backwards. A current payload may still enrich media metadata that
        # is deliberately outside the immutable raw fingerprint.
        if allocation.raw_entry.revision_no != resolution.source_entry.current_revision_no:
            return 0, []
        if not projection_incomplete:
            if media_projection_changed(item, prepared):
                item.media_url = prepared.media_url
                item.media_kind = prepared.media_kind
                item.media_duration = prepared.media_duration
                session.flush()
            return 0, []
    text_changed = update_normal_article_projection(
        session,
        source,
        document,
        document.content_items[0],
        allocation.raw_entry,
        prepared,
    )
    resolution.source_entry.projection_pending = False
    return 0, [document.content_items[0].id] if text_changed else []


def _project_missing_publication_date(
    session: Session,
    identity: SourceEntryIdentity,
    document: Document,
    raw: RawEntry,
) -> bool:
    projected = document.raw_entry
    if not (
        projected.published_at is None
        and raw.published_at is not None
        and (
            projected.external_id,
            projected.title,
            projected.url,
            projected.author,
            projected.raw_summary,
            projected.raw_content.replace("\u200b", ""),
        )
        == (
            raw.external_id,
            raw.title,
            raw.url,
            raw.author,
            raw.raw_summary,
            raw.raw_content.replace("\u200b", ""),
        )
    ):
        return False

    document.raw_entry = raw
    for item in document.content_items:
        item.published_at = raw.published_at
    for cluster_id in {
        cluster_item.cluster_id
        for item in document.content_items
        for cluster_item in item.cluster_items
    }:
        cluster = session.get(Cluster, cluster_id)
        if cluster is not None:
            refresh_cluster_dates_from_items(session, cluster)
    identity.projection_pending = False
    session.flush()
    return True


def source_entries_clustering_item_ids(
    session: Session,
    source: Source,
    entries: list[IngestEntry],
) -> tuple[int, ...]:
    """Return existing clustered items whose evidence ingest can change."""
    incoming_versions: list[
        tuple[SourceEntryNaturalKey, str, SourceEntryIdentityInput, str]
    ] = []
    for entry in entries:
        prepared = prepare_ingest_entry(entry)
        fingerprint = calculate_payload_fingerprint(prepared.revision)
        identity_input = _source_entry_identity_input(prepared, fingerprint)
        incoming_versions.append(
            (
                natural_source_entry_key(identity_input),
                fingerprint,
                identity_input,
                prepared.raw_text,
            )
        )
    if not incoming_versions:
        return ()

    keys = {
        (key.identity_kind, key.identity_key)
        for key, _fingerprint, _identity_input, _text in incoming_versions
    }
    key_rows = session.scalars(
        select(SourceEntryKey)
        .options(
            selectinload(SourceEntryKey.source_entry)
            .selectinload(SourceEntryIdentity.raw_entries)
            .selectinload(RawEntry.document)
            .selectinload(Document.content_items)
            .selectinload(ContentItem.cluster_items)
        )
        .where(
            SourceEntryKey.source_id == source.id,
            tuple_(
                SourceEntryKey.identity_kind,
                SourceEntryKey.identity_key,
            ).in_(keys),
        )
    ).all()
    identities: dict[SourceEntryNaturalKey, SourceEntryIdentity] = {
        SourceEntryNaturalKey(row.identity_kind, row.identity_key): row.source_entry
        for row in key_rows
    }

    has_legacy_identities = bool(
        session.scalar(
            select(
                exists().where(
                    SourceEntryKey.source_id == source.id,
                    SourceEntryKey.identity_kind == "legacy",
                )
            )
        )
    )
    if has_legacy_identities:
        for key, _fingerprint, identity_input, _text in incoming_versions:
            if key in identities:
                continue
            resolution = resolve_source_entry_identity(
                session,
                source.id,
                identity_input,
            )
            if resolution.source_entry is not None:
                identities[key] = resolution.source_entry

    affected_item_ids: set[int] = set()
    for key, fingerprint, _identity_input, incoming_text in incoming_versions:
        identity = identities.get(key)
        if identity is None:
            continue
        current_raw = next(
            (
                raw
                for raw in identity.raw_entries
                if raw.revision_no == identity.current_revision_no
            ),
            None,
        )
        identity_documents = [
            raw.document for raw in identity.raw_entries if raw.document is not None
        ]
        clustered_item_ids = {
            item.id
            for document in identity_documents
            for item in document.content_items
            if item.cluster_items
        }
        if current_raw is None or len(identity_documents) != 1:
            affected_item_ids.update(clustered_item_ids)
            continue
        document = identity_documents[0]
        if document.document_type != "normal_article":
            continue
        if _normal_article_projection_incomplete(identity, current_raw, document):
            affected_item_ids.update(clustered_item_ids)
            continue
        if len(document.content_items) != 1:
            affected_item_ids.update(clustered_item_ids)
            continue
        if not document.content_items[0].cluster_items:
            continue
        if (
            fingerprint
            not in {
                raw.payload_fingerprint for raw in identity.raw_entries
            }
            and document.content_items[0].content_text != incoming_text
        ):
            affected_item_ids.add(document.content_items[0].id)
    return tuple(sorted(affected_item_ids))


def _source_entry_identity_input(
    prepared: _PreparedIngestEntry,
    fingerprint: str,
) -> SourceEntryIdentityInput:
    return SourceEntryIdentityInput(
        source_guid=prepared.source_guid,
        url=prepared.revision.url or "",
        title=prepared.revision.title or "",
        author=prepared.revision.author or "",
        published_at=prepared.revision.published_at,
        payload_fingerprint=fingerprint,
    )


def _normal_article_projection_incomplete(
    identity: SourceEntryIdentity,
    current_raw: RawEntry,
    document: Document,
) -> bool:
    return identity.projection_pending or document.raw_entry_id != current_raw.id


def prepare_ingest_entry(entry: IngestEntry) -> _PreparedIngestEntry:
    title = clean_title(entry.title)
    link = safe_absolute_url(normalize_space(entry.url), "")
    media_url = safe_absolute_url(normalize_space(entry.media_url), link)
    raw_text = entry.content_text or strip_html(
        entry.raw_content or entry.raw_summary,
        link,
    )
    summary = clean_preview(
        entry.summary or strip_html(entry.raw_summary, link) or raw_text,
        500,
    )
    entry_hash = entry.content_hash or content_hash(
        title,
        raw_text or summary,
        link,
    )
    external_id = normalize_space(entry.external_id) or (
        content_hash(title, link) if link else entry_hash
    )
    return _PreparedIngestEntry(
        source_guid=normalize_space(entry.source_guid),
        revision=RawEntryRevisionInput(
            external_id=external_id,
            title=title,
            url=link,
            author=normalize_space(entry.author),
            published_at=entry.published_at,
            raw_summary=entry.raw_summary,
            raw_content=entry.raw_content,
            content_hash=entry_hash,
        ),
        raw_text=raw_text,
        reading_html=entry.reading_html or reading_html_from_text(raw_text),
        summary=summary,
        media_url=media_url,
        media_kind=entry.media_kind if media_url else "",
        media_duration=entry.media_duration,
        body_source=entry.body_source,
        web_fetch_status=entry.web_fetch_status,
    )


def new_revision_indices_for_ingest_entries(
    session: Session,
    source_id: int,
    entries: list[IngestEntry],
) -> list[int]:
    versions: list[
        tuple[
            int,
            SourceEntryNaturalKey,
            str,
            SourceEntryIdentityInput,
        ]
    ] = []
    for index, entry in enumerate(entries):
        prepared = prepare_ingest_entry(entry)
        fingerprint = calculate_payload_fingerprint(prepared.revision)
        identity_input = _source_entry_identity_input(
            prepared,
            fingerprint,
        )
        versions.append(
            (
                index,
                natural_source_entry_key(identity_input),
                fingerprint,
                identity_input,
            )
        )
    if not versions:
        return []

    keys = {
        (key.identity_kind, key.identity_key)
        for _index, key, _fingerprint, _identity_input in versions
    }
    known = set(
        session.execute(
            select(
                SourceEntryKey.identity_kind,
                SourceEntryKey.identity_key,
                RawEntry.payload_fingerprint,
            )
            .join(
                RawEntry,
                RawEntry.source_entry_id == SourceEntryKey.source_entry_id,
            )
            .where(
                SourceEntryKey.source_id == source_id,
                tuple_(
                    SourceEntryKey.identity_kind,
                    SourceEntryKey.identity_key,
                ).in_(keys),
            )
        ).all()
    )
    mapped_keys = {
        (identity_kind, identity_key)
        for identity_kind, identity_key, _fingerprint in known
    }
    has_legacy_identities = bool(
        session.scalar(
            select(
                exists().where(
                    SourceEntryKey.source_id == source_id,
                    SourceEntryKey.identity_kind == "legacy",
                )
            )
        )
    )
    if has_legacy_identities:
        checked_markers: set[tuple[str, str, str]] = set()
        for _index, key, fingerprint, identity_input in versions:
            key_value = (key.identity_kind, key.identity_key)
            marker = (*key_value, fingerprint)
            if key_value in mapped_keys or marker in checked_markers:
                continue
            checked_markers.add(marker)
            resolution = resolve_source_entry_identity(
                session,
                source_id,
                identity_input,
            )
            if resolution.source_entry is None:
                continue
            mapped_keys.add(key_value)
            known.update(
                (
                    key.identity_kind,
                    key.identity_key,
                    stored_fingerprint,
                )
                for stored_fingerprint in session.scalars(
                    select(RawEntry.payload_fingerprint).where(
                        RawEntry.source_entry_id
                        == resolution.source_entry.id
                    )
                )
            )

    indices: list[int] = []
    seen: set[tuple[str, str, str]] = set()
    for index, key, fingerprint, _identity_input in versions:
        marker = (key.identity_kind, key.identity_key, fingerprint)
        if marker in seen or marker in known:
            continue
        seen.add(marker)
        indices.append(index)
    return indices


def current_raw_entry(
    session: Session,
    source_entry: SourceEntryIdentity,
) -> RawEntry:
    raw = session.scalar(
        select(RawEntry).where(
            RawEntry.source_entry_id == source_entry.id,
            RawEntry.revision_no == source_entry.current_revision_no,
        )
    )
    if raw is None:
        raise RuntimeError(
            "Source Entry current revision 缺失："
            f"identity={source_entry.id}, revision={source_entry.current_revision_no}"
        )
    return raw


def projected_document(
    session: Session,
    source_entry_id: int,
    current_raw: RawEntry,
) -> Document | None:
    if current_raw.document is not None:
        return current_raw.document
    return session.scalars(
        select(Document)
        .join(RawEntry, Document.raw_entry_id == RawEntry.id)
        .where(RawEntry.source_entry_id == source_entry_id)
    ).one_or_none()


def create_initial_projection(
    session: Session,
    source: Source,
    raw: RawEntry,
    prepared: _PreparedIngestEntry,
) -> list[int]:
    title = prepared.revision.title or ""
    raw_text = prepared.raw_text

    score = digest_score(
        title,
        raw_text,
        (prepared.revision.raw_content or prepared.revision.raw_summary or ""),
    )
    doc_type = document_type(score, title)
    document = Document(
        raw_entry_id=raw.id,
        document_type=doc_type,
        title=title,
        summary=prepared.summary,
        content_text=raw_text,
        reading_html=prepared.reading_html,
        body_source=prepared.body_source,
        web_fetch_status=prepared.web_fetch_status,
        digest_score=score,
    )
    session.add(document)
    session.flush()

    pieces = split_digest_items(title, raw_text, score)
    if doc_type in {"digest", "mixed"} and len(pieces) > 1:
        pieces.insert(
            0,
            {
                "title": title,
                "content_text": raw_text,
                "summary": prepared.summary,
            },
        )
    created_item_ids: list[int] = []
    for piece in pieces:
        item_title = piece["title"] or title
        item_text = piece["content_text"]
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            **content_item_projection_values(
                prepared,
                title=item_title,
                summary=piece.get("summary", ""),
                text=item_text,
            ),
        )
        session.add(item)
        session.flush()
        created_item_ids.append(item.id)
    refresh_filter_matches_for_items(session, created_item_ids)
    return created_item_ids


def update_normal_article_projection(
    session: Session,
    source: Source,
    document: Document,
    item: ContentItem,
    raw: RawEntry,
    prepared: _PreparedIngestEntry,
) -> bool:
    title = prepared.revision.title or ""
    text = prepared.raw_text
    text_changed = item.content_text != text
    document.raw_entry = raw
    document.title = title
    document.summary = prepared.summary
    document.content_text = text
    document.reading_html = prepared.reading_html
    document.body_source = prepared.body_source
    document.web_fetch_status = prepared.web_fetch_status
    for field, value in content_item_projection_values(
        prepared,
        title=title,
        summary=prepared.summary,
        text=text,
    ).items():
        setattr(item, field, value)
    if text_changed:
        item.embedding_vector = None
        item.embedding_model = ""
        item.cluster_score = 0.0
        session.query(ContentEmbedding).filter(
            ContentEmbedding.content_item_id == item.id
        ).delete(synchronize_session=False)

    cluster_ids = set(
        session.scalars(
            select(ClusterItem.cluster_id).where(
                ClusterItem.content_item_id == item.id
            )
        ).all()
    )
    if source.status == "active" and source.media_type == "article":
        if cluster_ids:
            for cluster_id in cluster_ids:
                cluster = session.get(Cluster, cluster_id)
                if cluster is not None:
                    refresh_cluster_dates_from_items(session, cluster)
    session.flush()
    if text_changed:
        refresh_filter_matches_for_items(session, [item.id])
    return text_changed


def reading_html_from_text(text: str) -> str:
    # ponytail: legacy adapters without structured HTML use this safe bridge.
    return "<p>" + escape(text).replace("\n", "<br>") + "</p>"


def content_item_projection_values(
    prepared: _PreparedIngestEntry,
    *,
    title: str,
    summary: str,
    text: str,
) -> dict[str, object]:
    link = prepared.revision.url or ""
    return {
        "title": title,
        "summary": clean_preview(summary, 500),
        "content_text": text,
        "url": link,
        "published_at": prepared.revision.published_at,
        "content_hash": content_hash(title, text, link),
        "canonical_url": canonical_url(link),
        "normalized_title": normalize_title(title),
        "lsh_signature": lsh_signature(title, text),
        "media_url": prepared.media_url,
        "media_kind": prepared.media_kind,
        "media_duration": prepared.media_duration,
    }


def media_projection_changed(
    item: ContentItem,
    prepared: _PreparedIngestEntry,
) -> bool:
    return (
        item.media_url != prepared.media_url
        or item.media_kind != prepared.media_kind
        or item.media_duration != prepared.media_duration
    )


def refresh_cluster_dates_from_items(session: Session, cluster: Cluster) -> None:
    first_seen_at, last_seen_at = session.execute(
        select(func.min(ContentItem.published_at), func.max(ContentItem.published_at))
        .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
        .where(ClusterItem.cluster_id == cluster.id)
    ).one()
    cluster.first_seen_at = first_seen_at
    cluster.last_seen_at = last_seen_at
