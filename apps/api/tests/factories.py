from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from typing import Any

from sqlalchemy.orm import Session

from reader_api.cluster import assign_cluster as assign_runtime_cluster
from reader_api.digest import content_hash as calculate_content_hash
from reader_api.models import (
    Cluster,
    ContentItem,
    RawEntry,
    Source,
    SourceEntryIdentity,
    SourceEntryKey,
)
from reader_api.source_entry_revision import (
    RawEntryRevisionInput,
    calculate_payload_fingerprint,
)


def legacy_source_entry_key(external_id: str) -> str:
    digest = hashlib.sha256(external_id.encode("utf-8")).hexdigest()
    return f"legacy:{digest}"


INVALID_SOURCE_ENTRY_KEYS: tuple[tuple[str, str], ...] = (
    ("other", "other:" + "a" * 64),
    ("guid", "url:" + "a" * 64),
    ("url", "url:not-a-sha256"),
)
POSTGRES_TEST_VECTOR = "[" + ",".join(["1.0", *(["0.0"] * 2559)]) + "]"


def assign_publishable_cluster(
    session: Session,
    item: ContentItem,
    *args: Any,
    **kwargs: Any,
) -> Cluster:
    """Make a test fixture explicitly publishable before exercising Event behavior."""
    has_explicit_vector = bool(item.embedding_vector)
    is_postgres = session.get_bind().dialect.name == "postgresql"
    if is_postgres:
        item.embedding_vector = POSTGRES_TEST_VECTOR
    elif not has_explicit_vector:
        item.embedding_vector = "[1.0,0.0]"
    if not item.embedding_model:
        item.embedding_model = (
            "test-fixture-embedding"
            if has_explicit_vector
            else f"test-fixture-{item.id or 'new'}"
        )
    return assign_runtime_cluster(session, item, *args, **kwargs)


def make_raw_entry(
    *,
    source: Source | None = None,
    source_id: int | None = None,
    external_id: str = "test-entry",
    title: str = "Test entry",
    content_hash: str | None = None,
    **overrides: Any,
) -> RawEntry:
    """Build Raw Entry test data without coupling every test to its full schema."""
    if (source is None) == (source_id is None):
        raise ValueError("Provide exactly one of source or source_id")

    url = str(overrides.get("url", ""))
    raw_text = str(overrides.get("raw_content") or overrides.get("raw_summary") or "")
    resolved_hash = content_hash if content_hash is not None else calculate_content_hash(title, raw_text, url)
    values: dict[str, Any] = {
        "external_id": external_id,
        "title": title,
        "content_hash": resolved_hash,
        "revision_no": 1,
        **overrides,
    }
    if values.get("payload_fingerprint") is None:
        values["payload_fingerprint"] = calculate_payload_fingerprint(
            RawEntryRevisionInput(
                external_id=external_id,
                title=title,
                url=values.get("url", ""),
                author=values.get("author", ""),
                published_at=values.get("published_at"),
                raw_summary=values.get("raw_summary", ""),
                raw_content=values.get("raw_content", ""),
                content_hash=resolved_hash,
                fetched_at=values.get("fetched_at"),
            )
        )
    identity = SourceEntryIdentity(
        current_revision_no=1,
        projection_pending=False,
    )
    if source is not None:
        values["source"] = source
        identity.source = source
    else:
        values["source_id"] = source_id
        identity.source_id = source_id
    identity.keys.append(
        SourceEntryKey(
            identity_kind="legacy",
            identity_key=legacy_source_entry_key(external_id),
        )
    )
    values["source_entry"] = identity
    return RawEntry(**values)


def make_revision_input(**overrides: object) -> RawEntryRevisionInput:
    values: dict[str, object] = {
        "external_id": "entry-guid",
        "title": "Café launch",
        "url": "https://example.com/launch",
        "author": "Reader Author",
        "published_at": datetime(2026, 7, 10, 8, 30, tzinfo=timezone.utc),
        "raw_summary": "First summary\nSecond line",
        "raw_content": "First body\nSecond line",
        "content_hash": "a" * 64,
        "fetched_at": datetime(2026, 7, 10, 9, 0, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return RawEntryRevisionInput(**values)


def add_raw_revision_seed(
    session: Session,
    *,
    revision: RawEntryRevisionInput | None = None,
    payload_fingerprint: str | None,
    source_name: str = "Revision source",
    source_url: str = "https://example.com/revision.xml",
) -> tuple[SourceEntryIdentity, RawEntry]:
    initial = revision or make_revision_input()
    source = Source(name=source_name, url=source_url, status="trial")
    session.add(source)
    session.flush()
    identity = SourceEntryIdentity(source_id=source.id, current_revision_no=1)
    session.add(identity)
    session.flush()
    session.add(
        SourceEntryKey(
            source_entry_id=identity.id,
            source_id=source.id,
            identity_kind="legacy",
            identity_key=legacy_source_entry_key(initial.external_id),
        )
    )
    resolved_fingerprint = (
        payload_fingerprint
        if payload_fingerprint is not None
        else calculate_payload_fingerprint(initial)
    )
    raw = RawEntry(
        source_id=source.id,
        source_entry_id=identity.id,
        revision_no=1,
        payload_fingerprint=resolved_fingerprint,
        external_id=initial.external_id,
        title=initial.title or "",
        url=initial.url or "",
        author=initial.author or "",
        published_at=initial.published_at,
        fetched_at=initial.fetched_at,
        raw_summary=initial.raw_summary or "",
        raw_content=initial.raw_content or "",
        content_hash=initial.content_hash,
    )
    session.add(raw)
    session.flush()
    return identity, raw
