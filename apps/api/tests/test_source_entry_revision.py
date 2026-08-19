from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session, sessionmaker

import reader_api.source_entry_revision as revision_module
from reader_api.db import Base, engine
from reader_api.models import (
    Cluster,
    ClusterItem,
    ContentItem,
    Document,
    RawEntry,
    SourceEntryIdentity,
    UserState,
)
from reader_api.source_entry_revision import (
    RawEntryRevisionOutcome,
    allocate_raw_entry_revision,
    calculate_payload_fingerprint,
)
from tests.factories import add_raw_revision_seed, make_revision_input


@pytest.fixture()
def session() -> Session:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as value:
        yield value
    Base.metadata.drop_all(engine)


def add_revision_one(
    session: Session,
    *,
    payload_fingerprint: str | None,
    with_derived_state: bool = False,
) -> tuple[SourceEntryIdentity, RawEntry]:
    identity, raw = add_raw_revision_seed(
        session,
        payload_fingerprint=payload_fingerprint,
    )
    source = raw.source

    if with_derived_state:
        document = Document(
            raw_entry_id=raw.id,
            document_type="normal_article",
            title=raw.title,
            summary="Derived summary",
            content_text="Derived body",
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            summary=document.summary,
            content_text=document.content_text,
            url=raw.url,
            content_hash=raw.content_hash,
        )
        session.add(item)
        session.flush()
        cluster = Cluster(cluster_key="revision-cluster", title=raw.title)
        session.add(cluster)
        session.flush()
        session.add(
            ClusterItem(
                cluster_id=cluster.id,
                content_item_id=item.id,
            )
        )
        session.add(
            UserState(
                object_type="item",
                object_id=item.id,
                read_status="summary_seen",
                starred=True,
            )
        )
    session.commit()
    return identity, raw


def derived_snapshot(session: Session) -> dict[str, list[tuple[object, ...]]]:
    return {
        "documents": list(
            session.execute(
                select(
                    Document.id,
                    Document.raw_entry_id,
                    Document.title,
                    Document.summary,
                    Document.content_text,
                ).order_by(Document.id)
            ).all()
        ),
        "items": list(
            session.execute(
                select(
                    ContentItem.id,
                    ContentItem.document_id,
                    ContentItem.title,
                    ContentItem.summary,
                    ContentItem.content_text,
                    ContentItem.content_hash,
                ).order_by(ContentItem.id)
            ).all()
        ),
        "clusters": list(
            session.execute(
                select(
                    Cluster.id,
                    Cluster.cluster_key,
                    Cluster.title,
                ).order_by(Cluster.id)
            ).all()
        ),
        "cluster_items": list(
            session.execute(
                select(
                    ClusterItem.id,
                    ClusterItem.cluster_id,
                    ClusterItem.content_item_id,
                ).order_by(ClusterItem.id)
            ).all()
        ),
        "user_states": list(
            session.execute(
                select(
                    UserState.id,
                    UserState.object_type,
                    UserState.object_id,
                    UserState.read_status,
                    UserState.starred,
                ).order_by(UserState.id)
            ).all()
        ),
    }


def test_payload_fingerprint_normalizes_unicode_line_endings_missing_and_utc() -> None:
    decomposed = make_revision_input(
        title="Cafe\u0301 launch",
        author=None,
        published_at=datetime(
            2026,
            7,
            10,
            16,
            30,
            tzinfo=timezone(timedelta(hours=8)),
        ),
        raw_summary="First summary\r\nSecond line",
        raw_content="First body\rSecond line",
    )
    normalized = make_revision_input(
        title="Café launch",
        author="",
        published_at=datetime(2026, 7, 10, 8, 30, tzinfo=timezone.utc),
        raw_summary="First summary\nSecond line",
        raw_content="First body\nSecond line",
    )

    fingerprint = calculate_payload_fingerprint(decomposed)

    assert fingerprint == calculate_payload_fingerprint(normalized)
    assert len(fingerprint) == 64


def test_payload_fingerprint_excludes_fetch_metadata_and_derived_hash() -> None:
    original = make_revision_input()
    changed_metadata = replace(
        original,
        fetched_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        content_hash="f" * 64,
    )

    assert calculate_payload_fingerprint(original) == calculate_payload_fingerprint(
        changed_metadata
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("title", "Changed title"),
        ("url", "https://example.com/changed"),
        ("author", "Changed author"),
        ("published_at", datetime(2026, 7, 11, tzinfo=timezone.utc)),
        ("raw_summary", "Changed summary"),
        ("raw_content", "Changed body"),
    ],
)
def test_payload_fingerprint_changes_for_every_evidence_field(
    field: str,
    value: object,
) -> None:
    original = make_revision_input()

    assert calculate_payload_fingerprint(original) != calculate_payload_fingerprint(
        replace(original, **{field: value})
    )


def test_same_payload_returns_existing_without_mutating_revision(
    session: Session,
) -> None:
    initial = make_revision_input()
    fingerprint = calculate_payload_fingerprint(initial)
    identity, raw = add_revision_one(
        session,
        payload_fingerprint=fingerprint,
    )
    incoming = replace(
        initial,
        fetched_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        content_hash="f" * 64,
    )

    allocation = allocate_raw_entry_revision(session, identity.id, incoming)

    assert allocation.outcome is RawEntryRevisionOutcome.EXISTING
    assert allocation.raw_entry.id == raw.id
    assert raw.payload_fingerprint == fingerprint
    assert identity.current_revision_no == 1
    assert session.scalar(select(func.count()).select_from(RawEntry)) == 1


def test_changed_payload_appends_revision_without_touching_derived_state(
    session: Session,
) -> None:
    initial = make_revision_input()
    identity, raw = add_revision_one(
        session,
        payload_fingerprint=calculate_payload_fingerprint(initial),
        with_derived_state=True,
    )
    raw_snapshot = (
        raw.external_id,
        raw.title,
        raw.url,
        raw.author,
        raw.published_at,
        raw.fetched_at,
        raw.raw_summary,
        raw.raw_content,
        raw.content_hash,
        raw.payload_fingerprint,
    )
    before_derived = derived_snapshot(session)
    commits: list[bool] = []
    event.listen(session, "after_commit", lambda _session: commits.append(True))
    changed = replace(
        initial,
        external_id="entry-guid-revision-2",
        title="Café launch corrected",
        raw_content="Corrected body",
        content_hash="b" * 64,
        fetched_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )

    allocation = allocate_raw_entry_revision(session, identity.id, changed)

    assert allocation.outcome is RawEntryRevisionOutcome.CREATED
    assert allocation.raw_entry.revision_no == 2
    assert allocation.raw_entry.external_id == changed.external_id
    assert allocation.raw_entry.payload_fingerprint == calculate_payload_fingerprint(
        changed
    )
    assert identity.current_revision_no == 2
    assert commits == []
    assert (
        raw.external_id,
        raw.title,
        raw.url,
        raw.author,
        raw.published_at,
        raw.fetched_at,
        raw.raw_summary,
        raw.raw_content,
        raw.content_hash,
        raw.payload_fingerprint,
    ) == raw_snapshot
    assert derived_snapshot(session) == before_derived


def test_original_payload_is_found_after_a_newer_revision(
    session: Session,
) -> None:
    initial = make_revision_input()
    original_fingerprint = calculate_payload_fingerprint(initial)
    identity, original = add_revision_one(
        session,
        payload_fingerprint=original_fingerprint,
    )
    changed = replace(
        initial,
        external_id="entry-guid-newer",
        title="Newer title",
        content_hash="b" * 64,
    )
    created = allocate_raw_entry_revision(session, identity.id, changed)
    session.commit()

    allocation = allocate_raw_entry_revision(session, identity.id, initial)

    assert allocation.outcome is RawEntryRevisionOutcome.EXISTING
    assert allocation.raw_entry.id == original.id
    assert original.payload_fingerprint == original_fingerprint
    assert identity.current_revision_no == created.raw_entry.revision_no == 2
    assert session.scalar(select(func.count()).select_from(RawEntry)) == 2


def test_allocator_rollback_reverts_revision_and_current_pointer(
    session: Session,
) -> None:
    initial = make_revision_input()
    identity, raw = add_revision_one(
        session,
        payload_fingerprint=calculate_payload_fingerprint(initial),
    )
    changed = replace(
        initial,
        external_id="entry-guid-rollback",
        title="Rolled back title",
        content_hash="c" * 64,
    )

    allocation = allocate_raw_entry_revision(session, identity.id, changed)
    assert allocation.raw_entry.revision_no == 2
    assert identity.current_revision_no == 2

    session.rollback()
    session.expire_all()

    assert session.get(SourceEntryIdentity, identity.id).current_revision_no == 1
    assert session.get(RawEntry, raw.id) is not None
    assert session.scalar(select(func.count()).select_from(RawEntry)) == 1


def test_unique_fingerprint_conflict_rereads_existing_revision(
    session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = make_revision_input()
    identity, _raw = add_revision_one(
        session,
        payload_fingerprint=calculate_payload_fingerprint(initial),
    )
    changed = replace(
        initial,
        external_id="entry-guid-conflict-winner",
        title="Conflict winner",
        content_hash="d" * 64,
    )
    winner = allocate_raw_entry_revision(session, identity.id, changed)
    session.commit()
    competing = replace(
        changed,
        external_id="entry-guid-conflict-loser",
        fetched_at=datetime(2031, 1, 1, tzinfo=timezone.utc),
        content_hash="e" * 64,
    )
    real_find = revision_module._find_revision_by_fingerprint
    calls = 0

    def miss_precheck_once(
        value: Session,
        source_entry_id: int,
        payload_fingerprint: str,
    ) -> RawEntry | None:
        nonlocal calls
        calls += 1
        if calls == 1:
            return None
        return real_find(value, source_entry_id, payload_fingerprint)

    monkeypatch.setattr(
        revision_module,
        "_find_revision_by_fingerprint",
        miss_precheck_once,
    )

    allocation = allocate_raw_entry_revision(session, identity.id, competing)

    assert calls == 2
    assert allocation.outcome is RawEntryRevisionOutcome.EXISTING
    assert allocation.raw_entry.id == winner.raw_entry.id
    assert allocation.raw_entry.revision_no == 2
    assert identity.current_revision_no == 2
    assert session.scalar(select(func.count()).select_from(RawEntry)) == 2
