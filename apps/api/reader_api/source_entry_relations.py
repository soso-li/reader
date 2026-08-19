from __future__ import annotations

import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import (
    ContentItem,
    Document,
    RawEntry,
    SourceEntryRelation,
    now_utc,
)


DUPLICATE_RELATION_TYPE = "duplicate"
DUPLICATE_RELATION_RULE_VERSION = "legacy-external-id-hash-v1"
LEGACY_HASH_SUFFIX = re.compile(r"^(?P<base>.+):[0-9a-f]{12}$")


def record_duplicate_feed_relations(session: Session) -> int:
    """Record conservative legacy duplicate relations without changing reader data."""
    items = session.scalars(
        select(ContentItem)
        .join(Document, Document.id == ContentItem.document_id)
        .join(RawEntry, RawEntry.id == Document.raw_entry_id)
        .where(ContentItem.canonical_url != "")
        .order_by(ContentItem.source_id, ContentItem.canonical_url, ContentItem.id)
    ).all()
    eligible = [item for item in items if _is_single_normal_article(item)]
    candidates = {
        (
            item.source_id,
            item.canonical_url,
            item.document.raw_entry.external_id,
        ): item.document.raw_entry
        for item in eligible
    }
    existing_relations = {
        (relation.source_entry_id, relation.canonical_source_entry_id)
        for relation in session.scalars(
            select(SourceEntryRelation).where(
                SourceEntryRelation.relation_type == DUPLICATE_RELATION_TYPE,
                SourceEntryRelation.rule_version == DUPLICATE_RELATION_RULE_VERSION,
            )
        ).all()
    }

    created = 0
    for item in eligible:
        duplicate = item.document.raw_entry
        match = LEGACY_HASH_SUFFIX.fullmatch(duplicate.external_id)
        if match is None:
            continue
        canonical = candidates.get(
            (
                item.source_id,
                item.canonical_url,
                match.group("base"),
            )
        )
        if canonical is None or canonical.source_entry_id == duplicate.source_entry_id:
            continue
        relation_key = (duplicate.source_entry_id, canonical.source_entry_id)
        if relation_key in existing_relations:
            continue
        session.add(
            SourceEntryRelation(
                source_entry_id=duplicate.source_entry_id,
                canonical_source_entry_id=canonical.source_entry_id,
                relation_type=DUPLICATE_RELATION_TYPE,
                reason=(
                    "同一来源 canonical URL，且 external ID 匹配 legacy 12 位内容 hash 后缀；"
                    f"canonical external ID 为 {canonical.external_id}"
                ),
                detected_at=now_utc(),
                rule_version=DUPLICATE_RELATION_RULE_VERSION,
                active=True,
            )
        )
        existing_relations.add(relation_key)
        created += 1
    session.flush()
    return created


def list_duplicate_feed_relations(
    session: Session,
    *,
    include_revoked: bool = False,
) -> list[SourceEntryRelation]:
    statement = select(SourceEntryRelation).where(
        SourceEntryRelation.relation_type == DUPLICATE_RELATION_TYPE
    )
    if not include_revoked:
        statement = statement.where(SourceEntryRelation.active.is_(True))
    return list(session.scalars(statement.order_by(SourceEntryRelation.id)).all())


def revoke_duplicate_feed_relation(
    session: Session,
    relation_id: int,
) -> SourceEntryRelation:
    relation = session.get(SourceEntryRelation, relation_id)
    if relation is None or relation.relation_type != DUPLICATE_RELATION_TYPE:
        raise LookupError(f"Source Entry Relation 不存在：{relation_id}")
    if relation.active:
        relation.active = False
        relation.revoked_at = now_utc()
        session.flush()
    return relation


def _is_single_normal_article(item: ContentItem) -> bool:
    document = item.document
    return bool(
        document is not None
        and document.raw_entry is not None
        and document.document_type == "normal_article"
        and len(document.content_items) == 1
    )
