from datetime import datetime, timezone

import pytest
from reader_api.cluster import assign_cluster  # noqa: E402
from reader_api.db import Base, engine  # noqa: E402
from reader_api.digest import content_hash as calculate_content_hash  # noqa: E402
from reader_api.models import (  # noqa: E402
    Cluster,
    ClusterItem,
    ContentEmbedding,
    ContentItem,
    Document,
    LLMTask,
    RawEntry,
    Source,
    SourceEntryIdentity,
    SourceEntryKey,
    SourceEntryRelation,
    UserState,
)
from reader_api.source_entry_revision import (  # noqa: E402
    RawEntryRevisionInput,
    calculate_payload_fingerprint,
)
from reader_api.source_ingest import IngestEntry, ingest_source_entries, prepare_ingest_entry  # noqa: E402
from tests.factories import INVALID_SOURCE_ENTRY_KEYS, make_raw_entry  # noqa: E402
from sqlalchemy import CheckConstraint, UniqueConstraint, func, select  # noqa: E402
from sqlalchemy.exc import IntegrityError  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402


def test_raw_entry_factory_preserves_explicit_source_fields() -> None:
    source = Source(name="Factory RSS", url="https://example.com/factory.xml")
    published_at = datetime(2026, 7, 10, 8, 30, tzinfo=timezone.utc)

    raw = make_raw_entry(
        source=source,
        external_id="factory-entry",
        title="Factory entry",
        url="https://example.com/factory-entry",
        author="Factory Author",
        published_at=published_at,
        raw_summary="<p>Summary</p>",
        raw_content="<p>Body</p>",
    )

    assert raw.source is source
    assert raw.external_id == "factory-entry"
    assert raw.title == "Factory entry"
    assert raw.url == "https://example.com/factory-entry"
    assert raw.author == "Factory Author"
    assert raw.published_at == published_at
    assert raw.raw_summary == "<p>Summary</p>"
    assert raw.raw_content == "<p>Body</p>"
    assert raw.content_hash == calculate_content_hash("Factory entry", "<p>Body</p>", "https://example.com/factory-entry")


def test_ingest_entry_preserves_legacy_positional_author() -> None:
    entry = IngestEntry(
        "legacy-entry",
        "Legacy title",
        "https://example.com/legacy-entry",
        "Legacy Author",
    )

    assert entry.author == "Legacy Author"
    assert entry.source_guid == ""


def test_ingest_rejects_executable_links_and_resolves_safe_relative_media() -> None:
    unsafe = prepare_ingest_entry(IngestEntry(
        external_id="unsafe",
        title="Unsafe",
        url="javascript:alert(1)",
        media_url="data:text/html,boom",
        media_kind="video",
    ))
    safe = prepare_ingest_entry(IngestEntry(
        external_id="safe",
        title="Safe",
        url="https://example.com/posts/1",
        media_url="../audio.mp3",
        media_kind="audio",
    ))

    assert unsafe.revision.url == ""
    assert unsafe.media_url == ""
    assert unsafe.media_kind == ""
    assert safe.revision.url == "https://example.com/posts/1"
    assert safe.media_url == "https://example.com/audio.mp3"
    assert safe.media_kind == "audio"


def test_raw_entry_revision_identity_is_required_at_schema_head() -> None:
    table = RawEntry.__table__

    assert table.c.source_entry_id.nullable is False
    assert table.c.revision_no.nullable is False
    assert table.c.payload_fingerprint.nullable is False
    assert {
        constraint.name
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    } >= {
        "uq_raw_source_entry_fingerprint",
        "uq_raw_source_entry_revision",
    }
    assert "uq_raw_source_external" not in {
        constraint.name
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert "ck_raw_payload_fingerprint_sha256" in {
        constraint.name
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert {
        constraint.name
        for constraint in SourceEntryKey.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    } >= {
        "ck_source_entry_key_kind",
        "ck_source_entry_key_format",
        "ck_source_entry_key_kind_prefix",
    }


def test_database_rejects_raw_entry_without_payload_fingerprint() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name="Fingerprint contract",
            url="https://example.com/fingerprint-contract.xml",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="missing-fingerprint",
            title="Missing fingerprint",
        )
        raw.payload_fingerprint = None
        session.add(raw)

        with pytest.raises(IntegrityError):
            session.commit()


@pytest.mark.parametrize("invalid_fingerprint", ["", "z" * 64, "A" * 64])
def test_database_rejects_malformed_payload_fingerprint(
    invalid_fingerprint: str,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name="Malformed fingerprint contract",
            url="https://example.com/malformed-fingerprint.xml",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="malformed-fingerprint",
            title="Malformed fingerprint",
        )
        raw.payload_fingerprint = invalid_fingerprint
        session.add(raw)

        with pytest.raises(IntegrityError):
            session.commit()


@pytest.mark.parametrize(
    ("identity_kind", "identity_key"),
    INVALID_SOURCE_ENTRY_KEYS,
)
def test_database_rejects_invalid_source_entry_key(
    identity_kind: str,
    identity_key: str,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name="Identity key contract",
            url="https://example.com/identity-key-contract.xml",
        )
        raw = make_raw_entry(
            source=source,
            external_id="identity-key-contract",
            title="Identity key contract",
        )
        session.add(raw)
        session.commit()
        session.add(
            SourceEntryKey(
                source_entry_id=raw.source_entry_id,
                source_id=source.id,
                identity_kind=identity_kind,
                identity_key=identity_key,
            )
        )

        with pytest.raises(IntegrityError):
            session.commit()


def test_adapter_entry_uses_canonical_reader_layers() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(name="Deal Adapter", url="adapter://deals", media_type="article", status="active")
        session.add(source)
        session.commit()

        entry = IngestEntry(
            source_guid="deal-source-guid",
            external_id="deal-2026-07-06",
            title="Portable monitor discount",
            url="https://example.com/deal/monitor",
            author="Deals API",
            published_at=datetime(2026, 7, 6, 3, 0, tzinfo=timezone.utc),
            raw_summary="<p>Twenty percent off.</p>",
            content_text="Portable monitor discount with enough context for a reader item.",
            media_url="https://cdn.example.com/monitor.jpg",
            media_kind="image",
        )
        imported_item_ids: list[int] = []
        imported = ingest_source_entries(
            session,
            source,
            [entry],
            imported_item_ids=imported_item_ids,
        )
        session.commit()

        raw = session.scalars(select(RawEntry)).one()
        document = session.scalars(select(Document)).one()
        item = session.scalars(select(ContentItem)).one()
        cluster_count = session.scalar(select(func.count()).select_from(ClusterItem))
        model_task_count = session.scalar(select(func.count()).select_from(LLMTask))
        source_id = source.id
        item_id = item.id
        reading_body = (
            document.reading_html,
            document.body_source,
            document.web_fetch_status,
        )

    assert imported == 1
    assert imported_item_ids == [item_id]
    assert entry.source_guid == "deal-source-guid"
    assert raw.external_id == "deal-2026-07-06"
    assert raw.raw_summary == "<p>Twenty percent off.</p>"
    assert document.raw_entry_id == raw.id
    assert reading_body[0] == (
        "<p>Portable monitor discount with enough context for a reader item.</p>"
    )
    assert reading_body[1:] == ("rss", "not_requested")
    assert item.document_id == document.id
    assert item.source_id == source_id
    assert item.media_kind == "image"
    assert item.media_url == "https://cdn.example.com/monitor.jpg"
    assert cluster_count == 0
    assert model_task_count == 0


def test_new_ingest_creates_natural_source_entry_revision_and_is_idempotent() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name="Natural RSS",
            url="https://example.com/natural.xml",
            status="trial",
        )
        session.add(source)
        session.commit()

        first = IngestEntry(
            source_guid="natural-guid",
            external_id="natural-guid",
            title="Natural title",
            url="https://example.com/natural",
            author="Natural author",
            published_at=datetime(2026, 7, 11, 8, 0, tzinfo=timezone.utc),
            raw_summary="<p>First summary</p>",
            raw_content="<p>First body</p>",
            content_text="First body",
        )
        assert ingest_source_entries(session, source, [first]) == 1
        session.commit()

        raw = session.scalars(select(RawEntry)).one()
        identity = session.scalars(select(SourceEntryIdentity)).one()
        key = session.scalars(select(SourceEntryKey)).one()
        original_raw_id = raw.id

        assert raw.source_entry_id == identity.id
        assert raw.revision_no == 1
        assert raw.payload_fingerprint == calculate_payload_fingerprint(
            RawEntryRevisionInput.from_raw_entry(raw)
        )
        assert identity.source_id == source.id
        assert identity.current_revision_no == 1
        assert identity.projection_pending is False
        assert key.source_entry_id == identity.id
        assert key.source_id == source.id
        assert key.identity_kind == "guid"
        assert key.identity_key.startswith("guid:")

        assert ingest_source_entries(session, source, [first]) == 0
        session.commit()

        raw = session.scalars(select(RawEntry)).one()
        assert raw.id == original_raw_id
        assert raw.title == "Natural title"
        assert raw.revision_no == 1
        assert session.scalar(select(func.count()).select_from(SourceEntryIdentity)) == 1
        assert session.scalar(select(func.count()).select_from(SourceEntryKey)) == 1


def test_legacy_normal_article_update_appends_revision_and_preserves_user_state() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name="Existing RSS",
            url="https://example.com/existing.xml",
            status="active",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="existing-guid",
            title="Existing title",
            url="https://example.com/existing",
            raw_content="<p>Existing body</p>",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            document_type="normal_article",
            title=raw.title,
            content_text="Existing body",
        )
        session.add(document)
        session.flush()
        session.add(
            item := ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=raw.title,
                content_text="Existing body",
                url=raw.url,
                content_hash=raw.content_hash,
                embedding_vector="[1.0,0.0]",
                embedding_model="old-model",
            )
        )
        session.flush()
        cluster = assign_cluster(session, item)
        session.add(
            ContentEmbedding(
                content_item_id=item.id,
                representation="zh_canonical",
                model="old-model",
                vector="[1.0,0.0]",
            )
        )
        session.add_all(
            [
                UserState(
                    object_type="item",
                    object_id=item.id,
                    read_status="summary_seen",
                    starred=True,
                ),
            ]
        )
        session.commit()
        old_raw_id = raw.id
        item_id = item.id
        cluster_id = cluster.id
        user_state_before = [
            (state.object_type, state.object_id, state.read_status, state.read_later, state.starred)
            for state in session.scalars(select(UserState).order_by(UserState.id)).all()
        ]

        updated = IngestEntry(
            source_guid="existing-guid",
            external_id="existing-guid",
            title="Existing title updated",
            url="https://example.com/existing",
            raw_content="<p>Existing body updated</p>",
            content_text="Existing body updated",
        )
        assert ingest_source_entries(session, source, [updated]) == 0
        session.commit()

        raws = session.scalars(select(RawEntry).order_by(RawEntry.revision_no)).all()
        identity = session.scalars(select(SourceEntryIdentity)).one()
        document = session.scalars(select(Document)).one()
        item = session.get(ContentItem, item_id)
        user_state_after = [
            (state.object_type, state.object_id, state.read_status, state.read_later, state.starred)
            for state in session.scalars(select(UserState).order_by(UserState.id)).all()
        ]

        assert [stored.revision_no for stored in raws] == [1, 2]
        assert raws[0].id == old_raw_id
        assert raws[0].title == "Existing title"
        assert raws[0].raw_content == "<p>Existing body</p>"
        assert raws[1].title == "Existing title updated"
        assert raws[1].raw_content == "<p>Existing body updated</p>"
        assert identity.current_revision_no == 2
        assert document.raw_entry_id == raws[1].id
        assert item is not None
        assert item.title == "Existing title updated"
        assert item.content_text == "Existing body updated"
        assert item.embedding_vector is None
        assert item.embedding_model == ""
        assert session.scalar(select(func.count()).select_from(ContentEmbedding)) == 0
        assert session.scalar(
            select(func.count()).select_from(ClusterItem).where(
                ClusterItem.cluster_id == cluster_id,
                ClusterItem.content_item_id == item_id,
            )
        ) == 1
        assert user_state_after == user_state_before
        assert session.scalar(select(func.count()).select_from(SourceEntryIdentity)) == 1
        assert session.scalar(select(func.count()).select_from(SourceEntryKey)) == 2


def test_same_final_text_revision_preserves_embeddings_and_skips_requeue() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name="Metadata-only RSS",
            url="https://example.com/metadata-only.xml",
            status="trial",
        )
        session.add(source)
        session.flush()
        first = IngestEntry(
            source_guid="metadata-only-guid",
            external_id="metadata-only-guid",
            title="Original title",
            url="https://example.com/metadata-only",
            raw_content="<p>Stable final body.</p>",
            content_text="Stable final body.",
        )
        assert ingest_source_entries(session, source, [first]) == 1
        session.commit()

        item = session.scalars(select(ContentItem)).one()
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "stable-model"
        session.add(
            ContentEmbedding(
                content_item_id=item.id,
                representation="zh_canonical",
                model="stable-model",
                vector="[0.0,1.0]",
            )
        )
        session.commit()

        changed_metadata = IngestEntry(
            source_guid="metadata-only-guid",
            external_id="metadata-only-guid",
            title="Updated title",
            url="https://example.com/metadata-only",
            raw_content="<p>Stable final body with changed markup.</p>",
            content_text="Stable final body.",
        )
        requeued_item_ids: list[int] = []
        assert (
            ingest_source_entries(
                session,
                source,
                [changed_metadata],
                imported_item_ids=requeued_item_ids,
            )
            == 0
        )
        session.commit()

        raws = session.scalars(
            select(RawEntry).order_by(RawEntry.revision_no)
        ).all()
        document = session.scalars(select(Document)).one()
        session.refresh(item)

        assert [raw.revision_no for raw in raws] == [1, 2]
        assert document.raw_entry_id == raws[1].id
        assert document.title == "Updated title"
        assert item.content_text == "Stable final body."
        assert item.embedding_vector == "[1.0,0.0]"
        assert item.embedding_model == "stable-model"
        assert (
            session.scalar(
                select(func.count())
                .select_from(ContentEmbedding)
                .where(ContentEmbedding.content_item_id == item.id)
            )
            == 1
        )
        assert requeued_item_ids == []


def test_current_payload_media_enrichment_does_not_replace_final_body() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name="Media RSS",
            url="https://example.com/media.xml",
            status="trial",
        )
        session.add(source)
        session.flush()
        first = IngestEntry(
            source_guid="media-guid",
            external_id="media-guid",
            title="Media entry",
            url="https://example.com/media",
            raw_content="<p>RSS body.</p>",
            content_text="Fetched webpage body.",
            reading_html="<article><p>Fetched webpage body.</p></article>",
            body_source="webpage",
            web_fetch_status="succeeded",
        )
        assert ingest_source_entries(session, source, [first]) == 1
        session.commit()

        item = session.scalars(select(ContentItem)).one()
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "stable-model"
        session.commit()

        media_only = IngestEntry(
            source_guid="media-guid",
            external_id="media-guid",
            title="Media entry",
            url="https://example.com/media",
            raw_content="<p>RSS body.</p>",
            content_text="RSS body.",
            reading_html="<p>RSS body.</p>",
            media_url="https://cdn.example.com/audio.mp3",
            media_kind="audio",
            media_duration=90,
        )
        queued_item_ids: list[int] = []
        assert (
            ingest_source_entries(
                session,
                source,
                [media_only],
                imported_item_ids=queued_item_ids,
            )
            == 0
        )
        session.commit()

        document = session.scalars(select(Document)).one()
        session.refresh(item)

        assert document.content_text == "Fetched webpage body."
        assert document.reading_html == (
            "<article><p>Fetched webpage body.</p></article>"
        )
        assert document.body_source == "webpage"
        assert item.content_text == "Fetched webpage body."
        assert item.media_url == "https://cdn.example.com/audio.mp3"
        assert item.media_kind == "audio"
        assert item.media_duration == 90
        assert item.embedding_vector == "[1.0,0.0]"
        assert item.embedding_model == "stable-model"
        assert queued_item_ids == []


def test_failed_entry_projection_does_not_block_later_entry() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name="Isolated RSS",
            url="https://example.com/isolated.xml",
            status="trial",
        )
        session.add(source)
        session.flush()
        broken = IngestEntry(
            source_guid="broken-guid",
            external_id="broken-guid",
            title="Broken entry",
            url="https://example.com/broken",
            content_text="Broken body",
            body_source="webpage",
            web_fetch_status="failed",
        )
        valid = IngestEntry(
            source_guid="valid-guid",
            external_id="valid-guid",
            title="Valid entry",
            url="https://example.com/valid",
            content_text="Valid body",
        )
        imported_item_ids: list[int] = []
        failed_entry_ids: list[str] = []

        assert (
            ingest_source_entries(
                session,
                source,
                [broken, valid],
                imported_item_ids=imported_item_ids,
                failed_entry_ids=failed_entry_ids,
            )
            == 1
        )
        session.commit()

        raw = session.scalars(select(RawEntry)).one()
        document = session.scalars(select(Document)).one()
        item = session.scalars(select(ContentItem)).one()

        assert raw.external_id == "valid-guid"
        assert document.title == "Valid entry"
        assert item.title == "Valid entry"
        assert imported_item_ids == [item.id]
        assert failed_entry_ids == ["broken-guid"]


def test_normal_article_update_rolls_back_revision_and_projection_together(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name="Rollback RSS",
            url="https://example.com/rollback.xml",
            status="trial",
        )
        session.add(source)
        session.flush()
        first = IngestEntry(
            source_guid="rollback-guid",
            external_id="rollback-guid",
            title="Rollback title",
            url="https://example.com/rollback",
            raw_content="<p>Rollback body</p>",
            content_text="Rollback body",
        )
        assert ingest_source_entries(session, source, [first]) == 1
        session.commit()
        identity_id = session.scalars(select(SourceEntryIdentity.id)).one()
        original_raw_id = session.scalars(select(RawEntry.id)).one()
        document_id = session.scalars(select(Document.id)).one()

        def fail_projection(*_args, **_kwargs):
            raise RuntimeError("projection failed")

        monkeypatch.setattr(
            "reader_api.source_ingest.update_normal_article_projection",
            fail_projection,
        )
        changed = IngestEntry(
            source_guid="rollback-guid",
            external_id="rollback-guid",
            title="Rollback title updated",
            url="https://example.com/rollback",
            raw_content="<p>Rollback body updated</p>",
            content_text="Rollback body updated",
        )
        failed_entry_ids: list[str] = []
        assert (
            ingest_source_entries(
                session,
                source,
                [changed],
                failed_entry_ids=failed_entry_ids,
            )
            == 0
        )
        session.commit()

        assert session.get(SourceEntryIdentity, identity_id).current_revision_no == 1
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 1
        assert session.get(Document, document_id).raw_entry_id == original_raw_id
        assert failed_entry_ids == ["rollback-guid"]


def test_normal_article_projection_uses_shorter_current_revision_content() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name="Shortened RSS",
            url="https://example.com/shortened.xml",
            status="trial",
        )
        session.add(source)
        session.flush()
        original = IngestEntry(
            source_guid="shortened-guid",
            external_id="shortened-guid",
            title="Shortened title",
            url="https://example.com/shortened",
            raw_content="<p>Original body with text later removed.</p>",
            content_text="Original body with text later removed.",
        )
        assert ingest_source_entries(session, source, [original]) == 1
        session.commit()

        shortened = IngestEntry(
            source_guid="shortened-guid",
            external_id="shortened-guid",
            title="Shortened title",
            url="https://example.com/shortened",
            raw_content="<p>Short body.</p>",
            content_text="Short body.",
        )
        assert ingest_source_entries(session, source, [shortened]) == 0
        session.commit()

        raws = session.scalars(
            select(RawEntry).order_by(RawEntry.revision_no)
        ).all()
        document = session.scalars(select(Document)).one()
        item = session.scalars(select(ContentItem)).one()

        assert len(raws) == 2
        assert document.raw_entry_id == raws[1].id
        assert document.content_text == "Short body."
        assert item.content_text == "Short body."


def projection_snapshot(session) -> dict[str, list[dict[str, object]]]:
    models = (
        Document,
        ContentItem,
        ContentEmbedding,
        Cluster,
        ClusterItem,
        UserState,
    )
    return {
        model.__tablename__: [
            dict(row)
            for row in session.execute(
                select(*model.__table__.c).order_by(model.__table__.c.id)
            ).mappings()
        ]
        for model in models
    }


@pytest.mark.parametrize(
    ("expected_type", "title", "original_body"),
    (
        (
            "digest",
            "AI 早报：OpenAI / Nvidia / Anthropic / Microsoft / Google / Apple",
            "\n".join(f"{index}. 原始新闻 {index}" for index in range(1, 7)),
        ),
        (
            "mixed",
            "《全境封锁2》Steam 不再锁国区",
            """
            1. 战术战斗 - 在紧张刺激的掩体枪战中迎战敌人。
            2. 打造终极特工 - 搜刮并打造顶尖装备，学习强力技能。
            3. 团队协作方能挽救生命 - 与最多三位其他特工组队执行任务。
            4. 极致终局体验 - 达到最高等级仅是征程的开始。
            https://example.com/a https://example.com/b https://example.com/c
            """,
        ),
    ),
)
def test_digest_and_mixed_updates_append_revision_and_preserve_projection(
    expected_type: str,
    title: str,
    original_body: str,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    updated_body = original_body + "\n7. 新增新闻"
    latest_body = updated_body + "\n8. 后续新闻"

    with Session() as session:
        source = Source(
            name=f"{expected_type} RSS",
            url=f"https://example.com/{expected_type}.xml",
            status="active",
        )
        session.add(source)
        session.flush()
        first = IngestEntry(
            source_guid=f"{expected_type}-guid",
            external_id=f"{expected_type}-guid",
            title=title,
            url=f"https://example.com/{expected_type}",
            raw_content=original_body,
            content_text=original_body,
        )
        assert ingest_source_entries(session, source, [first]) == 1
        session.commit()
        document = session.scalars(select(Document)).one()
        document.document_type = expected_type
        session.commit()
        assert document.document_type == expected_type
        original_raw_id = document.raw_entry_id
        first_item_id = session.scalars(select(ContentItem.id).order_by(ContentItem.id)).first()
        session.add_all(
            [
                ContentEmbedding(
                    content_item_id=first_item_id,
                    representation="zh_canonical",
                    model="projection-snapshot",
                    vector="[1.0,0.0]",
                ),
                UserState(
                    object_type="item",
                    object_id=first_item_id,
                    read_status="summary_seen",
                    starred=True,
                ),
            ]
        )
        session.commit()
        projection_before = projection_snapshot(session)

        changed = IngestEntry(
            source_guid=f"{expected_type}-guid",
            external_id=f"{expected_type}-guid",
            title=title,
            url=f"https://example.com/{expected_type}",
            raw_content=updated_body,
            content_text=updated_body,
        )
        imported_item_ids: list[int] = []
        assert ingest_source_entries(
            session,
            source,
            [changed],
            imported_item_ids=imported_item_ids,
        ) == 0
        session.commit()

        identity = session.scalars(select(SourceEntryIdentity)).one()
        document = session.scalars(select(Document)).one()
        raws = session.scalars(
            select(RawEntry).order_by(RawEntry.revision_no)
        ).all()
        assert imported_item_ids == []
        assert [raw.revision_no for raw in raws] == [1, 2]
        assert raws[0].raw_content == original_body
        assert raws[1].raw_content == updated_body
        assert identity.current_revision_no == 2
        assert identity.projection_pending is True
        assert document.raw_entry_id == original_raw_id
        assert document.content_text == original_body
        assert projection_snapshot(session) == projection_before

        assert ingest_source_entries(session, source, [changed]) == 0
        session.commit()
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 2

        latest = IngestEntry(
            source_guid=f"{expected_type}-guid",
            external_id=f"{expected_type}-guid",
            title=title,
            url=f"https://example.com/{expected_type}",
            raw_content=latest_body,
            content_text=latest_body,
        )
        assert ingest_source_entries(session, source, [latest]) == 0
        session.commit()
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 3
        assert session.get(SourceEntryIdentity, identity.id).current_revision_no == 3
        assert session.get(SourceEntryIdentity, identity.id).projection_pending is True
        assert projection_snapshot(session) == projection_before

        assert ingest_source_entries(session, source, [first]) == 0
        session.commit()
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 3
        assert session.get(SourceEntryIdentity, identity.id).current_revision_no == 3
        assert session.get(SourceEntryIdentity, identity.id).projection_pending is True
        assert projection_snapshot(session) == projection_before


@pytest.mark.parametrize("document_type", ("digest", "mixed"))
def test_digest_and_mixed_missing_date_enrichment_preserves_split_projection(
    document_type: str,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    title = "AI 早报：OpenAI / Nvidia / Anthropic / Microsoft / Google / Apple"
    body = "\n".join(f"{index}. 原始新闻 {index}" for index in range(1, 7))
    original_body = body.replace("新闻", "新闻\u200b")
    published_at = datetime(2026, 8, 8, 8, 44, tzinfo=timezone.utc)

    with Session() as session:
        source = Source(
            name=f"{document_type} date RSS",
            url=f"https://example.com/{document_type}-date.xml",
            status="active",
        )
        session.add(source)
        session.flush()
        original = IngestEntry(
            source_guid=f"{document_type}-date-guid",
            external_id=f"{document_type}-date-guid",
            title=title,
            url=f"https://example.com/{document_type}-date",
            raw_content=original_body,
            content_text=original_body,
        )
        assert ingest_source_entries(session, source, [original]) == 1
        document = session.scalars(select(Document)).one()
        document.document_type = document_type
        cluster = Cluster(
            cluster_key=f"{document_type}-date-cluster",
            title=title,
        )
        session.add(cluster)
        session.flush()
        items = session.scalars(select(ContentItem).order_by(ContentItem.id)).all()
        session.add_all(
            ClusterItem(cluster_id=cluster.id, content_item_id=item.id)
            for item in items
        )
        session.commit()
        original_raw_id = document.raw_entry_id
        projection_before = [(item.id, item.content_text) for item in items]

        dated = IngestEntry(
            source_guid=original.source_guid,
            external_id=original.external_id,
            title=original.title,
            url=original.url,
            published_at=published_at,
            raw_content=body,
            content_text=body,
        )
        assert ingest_source_entries(session, source, [dated]) == 0
        session.commit()

        identity = session.scalars(select(SourceEntryIdentity)).one()
        raws = session.scalars(select(RawEntry).order_by(RawEntry.revision_no)).all()
        stored_published_at = published_at.replace(tzinfo=None)
        assert identity.current_revision_no == 2
        assert identity.projection_pending is False
        assert document.raw_entry_id == raws[1].id
        assert [(item.id, item.content_text) for item in items] == projection_before
        assert {item.published_at for item in items} == {stored_published_at}
        assert (cluster.first_seen_at, cluster.last_seen_at) == (
            stored_published_at,
            stored_published_at,
        )

        document.raw_entry_id = original_raw_id
        identity.projection_pending = True
        cluster.first_seen_at = cluster.last_seen_at = None
        for item in items:
            item.published_at = None
        session.commit()

        assert ingest_source_entries(session, source, [dated]) == 0
        session.commit()
        assert identity.projection_pending is False
        assert document.raw_entry_id == raws[1].id
        assert {item.published_at for item in items} == {stored_published_at}


def test_digest_pending_revision_and_marker_roll_back_together() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    title = "AI 晚报：OpenAI / Nvidia / Anthropic / Microsoft / Google / Apple"
    original_body = "\n".join(f"{index}. 原始新闻 {index}" for index in range(1, 7))

    with Session() as session:
        source = Source(
            name="Digest rollback RSS",
            url="https://example.com/digest-rollback.xml",
            status="trial",
        )
        session.add(source)
        session.flush()
        first = IngestEntry(
            source_guid="digest-rollback-guid",
            external_id="digest-rollback-guid",
            title=title,
            url="https://example.com/digest-rollback",
            raw_content=original_body,
            content_text=original_body,
        )
        assert ingest_source_entries(session, source, [first]) == 1
        session.commit()
        identity_id = session.scalars(select(SourceEntryIdentity.id)).one()
        document = session.scalars(select(Document)).one()
        document.document_type = "digest"
        session.commit()
        original_raw_id = document.raw_entry_id

        changed_body = original_body + "\n7. 新增新闻"
        changed = IngestEntry(
            source_guid="digest-rollback-guid",
            external_id="digest-rollback-guid",
            title=title,
            url="https://example.com/digest-rollback",
            raw_content=changed_body,
            content_text=changed_body,
        )
        assert ingest_source_entries(session, source, [changed]) == 0
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 2
        assert session.get(SourceEntryIdentity, identity_id).current_revision_no == 2
        assert session.get(SourceEntryIdentity, identity_id).projection_pending is True

        session.rollback()

        assert session.scalar(select(func.count()).select_from(RawEntry)) == 1
        assert session.get(SourceEntryIdentity, identity_id).current_revision_no == 1
        assert session.get(SourceEntryIdentity, identity_id).projection_pending is False
        assert session.scalars(select(Document)).one().raw_entry_id == original_raw_id


def test_source_entry_key_uniqueness_and_relation_audit_fields() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(name="Relations RSS", url="https://example.com/relations.xml")
        session.add(source)
        session.flush()
        duplicate = SourceEntryIdentity(source_id=source.id, current_revision_no=1)
        canonical = SourceEntryIdentity(source_id=source.id, current_revision_no=1)
        session.add_all([duplicate, canonical])
        session.flush()
        key_value = "legacy:" + "a" * 64
        session.add(
            SourceEntryKey(
                source_entry_id=duplicate.id,
                source_id=source.id,
                identity_kind="legacy",
                identity_key=key_value,
            )
        )
        relation = SourceEntryRelation(
            source_entry_id=duplicate.id,
            canonical_source_entry_id=canonical.id,
            relation_type="duplicate",
            reason="same canonical URL",
            rule_version="expand-schema-v1",
            active=True,
        )
        session.add(relation)
        session.commit()

        stored = session.scalars(select(SourceEntryRelation)).one()
        assert stored.relation_type == "duplicate"
        assert stored.reason == "same canonical URL"
        assert stored.rule_version == "expand-schema-v1"
        assert stored.active is True
        assert stored.revoked_at is None

        session.add(
            SourceEntryKey(
                source_entry_id=canonical.id,
                source_id=source.id,
                identity_kind="legacy",
                identity_key=key_value,
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()


def test_document_can_move_to_new_raw_revision_without_deleting_old_raw() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(name="Revision RSS", url="https://example.com/revisions.xml")
        session.add(source)
        session.flush()
        old_raw = make_raw_entry(
            source_id=source.id,
            external_id="revision-1",
            title="Revision 1",
        )
        new_raw = make_raw_entry(
            source_id=source.id,
            external_id="revision-2",
            title="Revision 2",
        )
        session.add_all([old_raw, new_raw])
        session.flush()
        document = Document(raw_entry=old_raw, title="Current projection")
        session.add(document)
        session.commit()
        old_raw_id = old_raw.id
        new_raw_id = new_raw.id

        document.raw_entry = new_raw
        session.commit()

        assert session.get(RawEntry, old_raw_id) is not None
        assert session.get(RawEntry, new_raw_id) is not None
        assert session.get(Document, document.id).raw_entry_id == new_raw_id
