from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from reader_api.db import Base
from reader_api.digest import canonical_url, content_hash, normalize_title
from reader_api.models import (
    Cluster,
    ClusterItem,
    ContentItem,
    Document,
    RawEntry,
    Source,
    SourceEntryIdentity,
    SourceEntryRelation,
    UserState,
)
from reader_api.source_entry_relations import (
    DUPLICATE_RELATION_RULE_VERSION,
    DUPLICATE_RELATION_TYPE,
    list_duplicate_feed_relations,
    record_duplicate_feed_relations,
    revoke_duplicate_feed_relation,
)
from tests.factories import make_raw_entry


def relation_session_factory() -> sessionmaker[Session]:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def add_article(
    session: Session,
    *,
    source: Source,
    cluster: Cluster,
    external_id: str,
    title: str,
    url: str,
    document_type: str = "normal_article",
) -> tuple[RawEntry, ContentItem]:
    raw = make_raw_entry(
        source=source,
        external_id=external_id,
        title=title,
        url=url,
        raw_content=title,
        content_hash=content_hash(title, title, url),
    )
    session.add(raw)
    session.flush()
    document = Document(
        raw_entry_id=raw.id,
        document_type=document_type,
        title=title,
        content_text=title,
    )
    session.add(document)
    session.flush()
    item = ContentItem(
        document_id=document.id,
        source_id=source.id,
        title=title,
        summary=title,
        content_text=title,
        url=url,
        published_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        content_hash=content_hash(title, title, url),
        canonical_url=canonical_url(url),
        normalized_title=normalize_title(title),
    )
    session.add(item)
    session.flush()
    session.add(
        ClusterItem(
            cluster_id=cluster.id,
            content_item_id=item.id,
            duplicate_score=1.0,
        )
    )
    return raw, item


def seed_duplicate_entries(
    session: Session,
) -> tuple[int, int, int]:
    source = Source(name="Relations RSS", url="https://example.com/relations.xml")
    cluster = Cluster(cluster_key="relations", title="Relations")
    session.add_all([source, cluster])
    session.flush()
    canonical_raw, canonical_item = add_article(
        session,
        source=source,
        cluster=cluster,
        external_id="story-guid",
        title="Original story",
        url="https://example.com/story",
    )
    duplicate_raw, duplicate_item = add_article(
        session,
        source=source,
        cluster=cluster,
        external_id="story-guid:abcdef123456",
        title="Updated story",
        url="https://example.com/story",
    )
    add_article(
        session,
        source=source,
        cluster=cluster,
        external_id="another-guid",
        title="Another publication on the same URL",
        url="https://example.com/story",
    )
    session.add_all(
        [
            UserState(
                object_type="item",
                object_id=duplicate_item.id,
                read_status="summary_seen",
                read_later=True,
                starred=True,
            ),
        ]
    )
    session.commit()
    return (
        duplicate_raw.source_entry_id,
        canonical_raw.source_entry_id,
        canonical_item.id,
    )


def preserved_counts(session: Session) -> tuple[int, ...]:
    return tuple(
        int(session.scalar(select(func.count()).select_from(model)) or 0)
        for model in (
            RawEntry,
            Document,
            ContentItem,
            Cluster,
            ClusterItem,
            UserState,
        )
    )


def test_duplicate_detection_records_relation_without_mutating_reader_data() -> None:
    session_factory = relation_session_factory()
    with session_factory() as session:
        duplicate_id, canonical_id, _item_id = seed_duplicate_entries(session)
        before = preserved_counts(session)

        assert record_duplicate_feed_relations(session) == 1
        session.commit()

        assert preserved_counts(session) == before
        relation = session.scalars(select(SourceEntryRelation)).one()
        assert relation.source_entry_id == duplicate_id
        assert relation.canonical_source_entry_id == canonical_id
        assert relation.relation_type == DUPLICATE_RELATION_TYPE
        assert relation.rule_version == DUPLICATE_RELATION_RULE_VERSION
        assert "canonical URL" in relation.reason
        assert relation.detected_at is not None
        assert relation.active is True
        assert relation.revoked_at is None


def test_duplicate_detection_is_idempotent_and_does_not_reactivate_revocation() -> None:
    session_factory = relation_session_factory()
    with session_factory() as session:
        seed_duplicate_entries(session)
        assert record_duplicate_feed_relations(session) == 1
        session.commit()
        relation = list_duplicate_feed_relations(session)[0]

        assert record_duplicate_feed_relations(session) == 0
        session.commit()
        assert session.scalar(select(func.count()).select_from(SourceEntryRelation)) == 1

        revoked = revoke_duplicate_feed_relation(session, relation.id)
        session.commit()
        revoked_at = revoked.revoked_at
        assert revoked.active is False
        assert revoked_at is not None
        assert list_duplicate_feed_relations(session) == []
        assert [row.id for row in list_duplicate_feed_relations(session, include_revoked=True)] == [relation.id]

        assert record_duplicate_feed_relations(session) == 0
        revoked_again = revoke_duplicate_feed_relation(session, relation.id)
        session.commit()
        assert revoked_again.active is False
        assert revoked_again.revoked_at == revoked_at
        assert session.scalar(select(func.count()).select_from(SourceEntryRelation)) == 1


def test_duplicate_detection_does_not_guess_without_exact_legacy_base() -> None:
    session_factory = relation_session_factory()
    with session_factory() as session:
        source = Source(name="Ambiguous RSS", url="https://example.com/ambiguous.xml")
        cluster = Cluster(cluster_key="ambiguous", title="Ambiguous")
        session.add_all([source, cluster])
        session.flush()
        add_article(
            session,
            source=source,
            cluster=cluster,
            external_id="missing-base:abcdef123456",
            title="Suffix without base",
            url="https://example.com/story",
        )
        add_article(
            session,
            source=source,
            cluster=cluster,
            external_id="different-guid",
            title="Different publication",
            url="https://example.com/story",
        )
        session.commit()

        assert record_duplicate_feed_relations(session) == 0
        session.commit()
        assert session.scalar(select(func.count()).select_from(SourceEntryRelation)) == 0


def test_revoke_duplicate_relation_rejects_unknown_id() -> None:
    session_factory = relation_session_factory()
    with session_factory() as session:
        try:
            revoke_duplicate_feed_relation(session, 999)
        except LookupError as exc:
            assert str(exc) == "Source Entry Relation 不存在：999"
        else:
            raise AssertionError("unknown relation id must be rejected")
