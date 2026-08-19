from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import unicodedata

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .canonicalization import canonical_utc_timestamp
from .models import RawEntry, SourceEntryIdentity


@dataclass(frozen=True)
class RawEntryRevisionInput:
    external_id: str
    title: str | None
    url: str | None = ""
    author: str | None = ""
    published_at: datetime | None = None
    raw_summary: str | None = ""
    raw_content: str | None = ""
    content_hash: str = ""
    fetched_at: datetime | None = None

    @classmethod
    def from_raw_entry(cls, raw_entry: RawEntry) -> RawEntryRevisionInput:
        return cls(
            external_id=raw_entry.external_id,
            title=raw_entry.title,
            url=raw_entry.url,
            author=raw_entry.author,
            published_at=raw_entry.published_at,
            raw_summary=raw_entry.raw_summary,
            raw_content=raw_entry.raw_content,
            content_hash=raw_entry.content_hash,
            fetched_at=raw_entry.fetched_at,
        )


class RawEntryRevisionOutcome(Enum):
    EXISTING = "existing"
    CREATED = "created"


@dataclass(frozen=True)
class RawEntryRevisionAllocation:
    outcome: RawEntryRevisionOutcome
    raw_entry: RawEntry


class RawEntryRevisionStateError(RuntimeError):
    pass


def calculate_payload_fingerprint(revision: RawEntryRevisionInput) -> str:
    canonical_payload = json.dumps(
        {
            "author": _normalize_payload_text(revision.author),
            "published_at": canonical_utc_timestamp(revision.published_at),
            "raw_content": _normalize_payload_text(revision.raw_content),
            "raw_summary": _normalize_payload_text(revision.raw_summary),
            "title": _normalize_payload_text(revision.title),
            "url": _normalize_payload_text(revision.url),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


def raw_entry_revision_values(
    *,
    source_id: int,
    source_entry_id: int,
    revision_no: int,
    revision: RawEntryRevisionInput,
) -> dict[str, object]:
    return {
        "source_id": source_id,
        "source_entry_id": source_entry_id,
        "revision_no": revision_no,
        "payload_fingerprint": calculate_payload_fingerprint(revision),
        "external_id": revision.external_id,
        "title": _normalize_missing_text(revision.title),
        "url": _normalize_missing_text(revision.url),
        "author": _normalize_missing_text(revision.author),
        "published_at": revision.published_at,
        "fetched_at": revision.fetched_at or datetime.now(timezone.utc),
        "raw_summary": _normalize_missing_text(revision.raw_summary),
        "raw_content": _normalize_missing_text(revision.raw_content),
        "content_hash": revision.content_hash,
    }


def allocate_raw_entry_revision(
    session: Session,
    source_entry_id: int,
    revision: RawEntryRevisionInput,
) -> RawEntryRevisionAllocation:
    """Allocate one immutable revision inside the caller's transaction.

    Production ingestion calls this inside its projection transaction. The service
    deliberately does not commit. A row lock serializes compliant writers; the
    fingerprint unique constraint is still re-read after an insert conflict as a
    final idempotency guard.
    """
    source_entry = session.scalar(
        select(SourceEntryIdentity)
        .where(SourceEntryIdentity.id == source_entry_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if source_entry is None:
        raise RawEntryRevisionStateError(
            f"Source Entry Identity 不存在：{source_entry_id}"
        )

    current = session.scalar(
        select(RawEntry).where(
            RawEntry.source_entry_id == source_entry.id,
            RawEntry.revision_no == source_entry.current_revision_no,
        )
    )
    if current is None:
        raise RawEntryRevisionStateError(
            "Source Entry current revision 缺失："
            f"identity={source_entry.id}, revision={source_entry.current_revision_no}"
        )

    incoming_fingerprint = calculate_payload_fingerprint(revision)
    existing = _find_revision_by_fingerprint(
        session,
        source_entry.id,
        incoming_fingerprint,
    )
    if existing is not None:
        return RawEntryRevisionAllocation(
            outcome=RawEntryRevisionOutcome.EXISTING,
            raw_entry=existing,
        )

    next_revision_no = source_entry.current_revision_no + 1
    values = raw_entry_revision_values(
        source_id=source_entry.source_id,
        source_entry_id=source_entry.id,
        revision_no=next_revision_no,
        revision=revision,
    )
    dialect_name = session.get_bind().dialect.name
    if dialect_name == "postgresql":
        statement = postgresql_insert(RawEntry).values(**values)
    elif dialect_name == "sqlite":
        statement = sqlite_insert(RawEntry).values(**values)
    else:
        raise RawEntryRevisionStateError(
            f"Raw Entry revision allocator 不支持数据库方言：{dialect_name}"
        )
    inserted_id = session.scalar(
        statement.on_conflict_do_nothing().returning(RawEntry.id)
    )
    if inserted_id is None:
        existing = _find_revision_by_fingerprint(
            session,
            source_entry.id,
            incoming_fingerprint,
        )
        if existing is not None:
            return RawEntryRevisionAllocation(
                outcome=RawEntryRevisionOutcome.EXISTING,
                raw_entry=existing,
            )
        raise RawEntryRevisionStateError(
            "Raw Entry revision 唯一冲突无法归并到相同 payload："
            f"identity={source_entry.id}, revision={next_revision_no}"
        )

    raw_entry = session.get(RawEntry, inserted_id)
    if raw_entry is None:
        raise RawEntryRevisionStateError(
            f"新 Raw Entry revision 写入后无法读取：{inserted_id}"
        )

    source_entry.current_revision_no = next_revision_no
    session.flush()
    return RawEntryRevisionAllocation(
        outcome=RawEntryRevisionOutcome.CREATED,
        raw_entry=raw_entry,
    )


def _find_revision_by_fingerprint(
    session: Session,
    source_entry_id: int,
    payload_fingerprint: str,
) -> RawEntry | None:
    return session.scalar(
        select(RawEntry)
        .where(
            RawEntry.source_entry_id == source_entry_id,
            RawEntry.payload_fingerprint == payload_fingerprint,
        )
        .order_by(RawEntry.revision_no)
    )


def _normalize_payload_text(value: str | None) -> str:
    normalized = _normalize_missing_text(value).replace("\r\n", "\n")
    return unicodedata.normalize("NFC", normalized.replace("\r", "\n"))


def _normalize_missing_text(value: str | None) -> str:
    return value if value is not None else ""
