from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker

from reader_api.cluster import assign_cluster
from reader_api.clustering_run import clustering_run
from reader_api.db import Base, engine
from reader_api.event_projection import (
    ProjectionEvidence,
    WHOLE_ENTRY_FRAGMENT_FINGERPRINT,
    canonical_evidence_fingerprint,
    cluster_current_event_state_projection,
    latest_live_cluster_projection,
    project_completed_clustering_run,
)
from reader_api.main import app
from reader_api.models import (
    ClusterEventProjection,
    ClusterItem,
    ClusteringRun,
    ContentItem,
    Document,
    Event,
    EventEvidence,
    EventEvidenceVersion,
    EventRevision,
    EventRevisionEvidence,
    Source,
    SourceEntryKey,
)
from tests.factories import make_raw_entry


def test_postgres_latest_projection_query_uses_current_mapping() -> None:
    class PostgreSQLBind:
        dialect = postgresql.dialect()

    class PostgreSQLSession:
        @staticmethod
        def get_bind() -> PostgreSQLBind:
            return PostgreSQLBind()

    query = latest_live_cluster_projection(PostgreSQLSession())
    compiled = str(query.select().compile(dialect=postgresql.dialect()))

    assert "cluster_current_event_projections" in compiled
    assert "LATERAL" not in compiled
    assert "GROUP BY" not in compiled
    assert "ORDER BY" not in compiled


def test_postgres_current_event_state_bounds_history_lookup_by_current_pointer() -> None:
    class PostgreSQLBind:
        dialect = postgresql.dialect()

    class PostgreSQLSession:
        @staticmethod
        def get_bind() -> PostgreSQLBind:
            return PostgreSQLBind()

    query = cluster_current_event_state_projection(PostgreSQLSession())
    compiled = str(query.select().compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    assert "JOIN LATERAL" in compiled
    assert "cluster_event_projections.id = anon_2.projection_id" in compiled
    assert "OFFSET 0" in compiled


def test_completed_projection_creates_one_idempotent_event_graph_and_exposes_it() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name="Projection source",
            url="https://example.com/projection.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="projection-entry",
            title="Projection title",
            url="https://example.com/projection",
            author="Projection author",
            published_at=datetime(2026, 7, 13, 8, 0, tzinfo=timezone.utc),
            raw_content="Projection body",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            document_type="normal_article",
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_text=raw.raw_content,
            url=raw.url,
            canonical_url=raw.url,
            published_at=raw.published_at,
            content_hash=raw.content_hash,
            embedding_vector="[1.0,0.0]",
            embedding_model="embedding-model",
        )
        session.add(item)
        session.flush()

        with clustering_run(
            session,
            scope_type="event-projection-test",
            item_ids=[item.id],
            rule_version="event-projection-test-v1",
        ) as run_id:
            cluster = assign_cluster(session, item)

        counts = tuple(
            session.scalar(select(func.count(model.id)))
            for model in (
                Event,
                EventRevision,
                EventEvidence,
                EventEvidenceVersion,
                EventRevisionEvidence,
                ClusterEventProjection,
            )
        )
        assert counts == (1, 1, 1, 1, 1, 1)

        mapping = session.scalar(select(ClusterEventProjection))
        event = session.scalar(select(Event))
        revision = session.scalar(select(EventRevision))
        assert mapping is not None
        assert event is not None
        assert revision is not None
        assert mapping.cluster_id == cluster.id
        assert mapping.cluster_id_snapshot == cluster.id
        assert mapping.event_id == event.id
        assert mapping.event_revision_id == revision.id
        assert event.current_revision_id == revision.id
        assert revision.event_id == event.id
        assert revision.revision_no == 1

        project_completed_clustering_run(session, run_id, [cluster.id])
        session.commit()
        assert tuple(
            session.scalar(select(func.count(model.id)))
            for model in (
                Event,
                EventRevision,
                EventEvidence,
                EventEvidenceVersion,
                EventRevisionEvidence,
                ClusterEventProjection,
            )
        ) == counts
        cluster_id = cluster.id
        event_uid = event.uid
        revision_uid = revision.uid

    client = TestClient(app)
    listed = client.get("/clusters").json()
    detail = client.get(f"/clusters/{cluster_id}").json()

    assert listed[0]["event_uid"] == event_uid
    assert listed[0]["current_revision_uid"] == revision_uid
    assert listed[0]["seen_revision_uid"] is None
    assert detail["event_uid"] == event_uid
    assert detail["current_revision_uid"] == revision_uid
    assert detail["seen_revision_uid"] is None


def test_projection_fingerprint_ignores_member_order_but_changes_with_role_or_version() -> None:
    def projected(
        version_fingerprint: str,
        *,
        uid: str,
        role: str = "material",
    ) -> ProjectionEvidence:
        return ProjectionEvidence(
            version=EventEvidenceVersion(
                uid=uid,
                version_fingerprint=version_fingerprint,
            ),
            evidence_type="article",
            role=role,
        )

    first = canonical_evidence_fingerprint(
        [
            projected("a" * 64, uid="11111111-1111-4111-8111-111111111111"),
            projected("b" * 64, uid="22222222-2222-4222-8222-222222222222"),
        ]
    )
    reordered = canonical_evidence_fingerprint(
        [
            projected("b" * 64, uid="33333333-3333-4333-8333-333333333333"),
            projected("a" * 64, uid="44444444-4444-4444-8444-444444444444"),
        ]
    )
    changed_role = canonical_evidence_fingerprint(
        [
            projected(
                "a" * 64,
                uid="55555555-5555-4555-8555-555555555555",
                role="primary_source",
            ),
            projected("b" * 64, uid="66666666-6666-4666-8666-666666666666"),
        ]
    )
    changed_version = canonical_evidence_fingerprint(
        [
            projected("c" * 64, uid="77777777-7777-4777-8777-777777777777"),
            projected("b" * 64, uid="88888888-8888-4888-8888-888888888888"),
        ]
    )

    assert first == reordered
    assert first != changed_role
    assert first != changed_version


def test_projection_fingerprint_ignores_database_allocation_and_revision_number() -> None:
    def project_with(
        *,
        pad_database_ids: bool,
        raw_revision_no: int,
        legacy_identity_fingerprint: str | None = None,
        add_identity_alias: bool = False,
        preexisting_legacy_v3: bool = False,
        orphan_alias_v3_after_initial_projection: bool = False,
    ) -> tuple[str, str, str, int, int]:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        with Session() as session:
            if pad_database_ids:
                padding_source = Source(
                    name="Padding source",
                    url="https://example.com/padding.xml",
                    status="active",
                    media_type="article",
                )
                session.add(padding_source)
                session.flush()
                session.add(
                    make_raw_entry(
                        source_id=padding_source.id,
                        external_id="padding-entry",
                        title="Padding entry",
                    )
                )
                session.flush()

            source = Source(
                name="Stable projection source",
                url="https://example.com/stable-projection.xml",
                status="active",
                media_type="article",
            )
            session.add(source)
            session.flush()
            raw = make_raw_entry(
                source_id=source.id,
                external_id="stable-projection-entry",
                title="Stable projection title",
                url="https://example.com/stable-projection",
                author="Stable author",
                published_at=datetime(2026, 7, 13, 8, 0, tzinfo=timezone.utc),
                raw_content="Stable projection body",
                revision_no=raw_revision_no,
            )
            raw.source_entry.current_revision_no = raw_revision_no
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                document_type="normal_article",
                title=raw.title,
                content_text=raw.raw_content,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=raw.title,
                content_text=raw.raw_content,
                url=raw.url,
                canonical_url=raw.url,
                published_at=raw.published_at,
                content_hash=raw.content_hash,
                embedding_vector="[1.0,0.0]",
                embedding_model="embedding-model",
            )
            session.add(item)
            session.flush()

            def add_preexisting_version(*, uid: str, fingerprint: str) -> None:
                evidence = session.scalar(select(EventEvidence))
                assert evidence is not None
                session.add(
                    EventEvidenceVersion(
                        uid=uid,
                        evidence_id=evidence.id,
                        version_fingerprint=fingerprint,
                        raw_entry_id=raw.id,
                        source_entry_id=raw.source_entry_id,
                        source_id=source.id,
                        raw_revision_no=raw.revision_no,
                        legacy_content_item_id=item.id,
                        legacy_content_item_id_snapshot=item.id,
                        fragment_fingerprint=WHOLE_ENTRY_FRAGMENT_FINGERPRINT,
                        title_snapshot=item.title,
                        url_snapshot=item.url,
                        author_snapshot=raw.author,
                        published_at_snapshot=item.published_at,
                        content_snapshot=item.content_text,
                    )
                )
                session.flush()

            if legacy_identity_fingerprint is not None:
                session.add(
                    EventEvidence(
                        uid="11111111-1111-4111-8111-111111111111",
                        identity_fingerprint=legacy_identity_fingerprint,
                        source_entry_id=raw.source_entry_id,
                        fragment_fingerprint=WHOLE_ENTRY_FRAGMENT_FINGERPRINT,
                    )
                )
                session.flush()
            if preexisting_legacy_v3:
                add_preexisting_version(
                    uid="22222222-2222-4222-8222-222222222222",
                    fingerprint=(
                        "7fa927e7153cde197333e0de5e480ab3"
                        "e8b0e5a9550ed8c73daa288405785556"
                    ),
                )
            if add_identity_alias and not orphan_alias_v3_after_initial_projection:
                session.add(
                    SourceEntryKey(
                        source_entry_id=raw.source_entry_id,
                        source_id=source.id,
                        identity_kind="guid",
                        identity_key=f"guid:{'a' * 64}",
                    )
                )
                session.flush()

            with clustering_run(
                session,
                scope_type="stable-event-projection-test",
                item_ids=[item.id],
                rule_version="stable-event-projection-test-v1",
            ):
                assign_cluster(session, item)

            if orphan_alias_v3_after_initial_projection:
                session.add(
                    SourceEntryKey(
                        source_entry_id=raw.source_entry_id,
                        source_id=source.id,
                        identity_kind="guid",
                        identity_key=f"guid:{'a' * 64}",
                    )
                )
                add_preexisting_version(
                    uid="33333333-3333-4333-8333-333333333333",
                    fingerprint=(
                        "f27ecb02854ed9e97048810944fd267f9"
                        "5706833dd8c5a7c8beac1a008e823a6"
                    ),
                )
                with clustering_run(
                    session,
                    scope_type="stable-event-projection-test",
                    item_ids=[item.id],
                    rule_version="stable-event-projection-test-v1",
                ):
                    assign_cluster(session, item)

            evidence = session.scalar(select(EventEvidence))
            version = session.scalar(
                select(EventEvidenceVersion).join(
                    EventRevisionEvidence,
                    EventRevisionEvidence.evidence_version_id
                    == EventEvidenceVersion.id,
                )
            )
            revision = session.scalar(select(EventRevision))
            assert evidence is not None
            assert version is not None
            assert revision is not None
            return (
                evidence.identity_fingerprint,
                version.version_fingerprint,
                revision.evidence_fingerprint,
                session.scalar(select(func.count(EventEvidenceVersion.id))),
                session.scalar(select(func.count(ClusterEventProjection.id))),
            )

    first = project_with(pad_database_ids=False, raw_revision_no=1)
    reordered = project_with(pad_database_ids=True, raw_revision_no=7)

    assert first == reordered

    legacy_first = project_with(
        pad_database_ids=False,
        raw_revision_no=1,
        legacy_identity_fingerprint="1" * 64,
    )
    legacy_reordered = project_with(
        pad_database_ids=True,
        raw_revision_no=7,
        legacy_identity_fingerprint="1" * 64,
        add_identity_alias=True,
    )

    assert legacy_first[1:3] == legacy_reordered[1:3]

    reused_legacy_v3 = project_with(
        pad_database_ids=False,
        raw_revision_no=1,
        legacy_identity_fingerprint="1" * 64,
        add_identity_alias=True,
        preexisting_legacy_v3=True,
    )

    assert reused_legacy_v3[3] == 1

    reused_current_legacy_v3 = project_with(
        pad_database_ids=False,
        raw_revision_no=1,
        legacy_identity_fingerprint="1" * 64,
        preexisting_legacy_v3=True,
        orphan_alias_v3_after_initial_projection=True,
    )

    assert reused_current_legacy_v3[1] == (
        "7fa927e7153cde197333e0de5e480ab3"
        "e8b0e5a9550ed8c73daa288405785556"
    )
    assert reused_current_legacy_v3[3:] == (2, 2)


def test_mixed_fragment_is_snapshotted_and_projection_failure_is_atomic() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name="Mixed projection source",
            url="https://example.com/mixed-projection.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="mixed-projection",
            title="Mixed digest",
            url="https://example.com/mixed-projection",
            raw_content="1. First fragment\n2. Second fragment",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            document_type="mixed",
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title="First fragment",
            content_text="First fragment body",
            url="https://example.com/mixed-projection#first",
            canonical_url="https://example.com/mixed-projection#first",
            content_hash="f" * 64,
            embedding_vector="[1.0,0.0]",
            embedding_model="embedding-model",
        )
        session.add(item)
        session.flush()
        with clustering_run(
            session,
            scope_type="mixed-event-projection-test",
            item_ids=[item.id],
            rule_version="mixed-event-projection-test-v1",
        ):
            assign_cluster(session, item)

        evidence = session.scalar(select(EventEvidence))
        version = session.scalar(select(EventEvidenceVersion))
        assert evidence is not None
        assert version is not None
        assert evidence.fragment_fingerprint is not None
        assert version.fragment_fingerprint == evidence.fragment_fingerprint
        assert version.legacy_content_item_id == item.id
        assert version.legacy_content_item_id_snapshot == item.id
        assert version.content_snapshot == "First fragment body"
        stable_fragment_fingerprint = evidence.fragment_fingerprint
        item.title = "First fragment (full text enriched)"
        item.content_text = "First fragment body with opportunistic full text"
        item.url = "https://example.com/mixed-projection#first-enriched"
        session.flush()
        with clustering_run(
            session,
            scope_type="mixed-event-projection-repeat-test",
            item_ids=[item.id],
            rule_version="mixed-event-projection-repeat-test-v1",
        ):
            pass

        assert session.scalar(select(func.count(EventEvidence.id))) == 1
        assert session.scalar(select(func.count(EventEvidenceVersion.id))) == 2
        assert set(
            session.scalars(select(EventEvidenceVersion.fragment_fingerprint))
        ) == {stable_fragment_fingerprint}
        event_count_before = session.scalar(select(func.count(Event.id)))

        unsupported = Source(
            name="Unsupported projection source",
            url="https://example.com/video-projection.xml",
            status="active",
            media_type="video",
        )
        session.add(unsupported)
        session.flush()
        unsupported_raw = make_raw_entry(
            source_id=unsupported.id,
            external_id="unsupported-projection",
            title="Unsupported projection",
            raw_content="Unsupported body",
        )
        session.add(unsupported_raw)
        session.flush()
        unsupported_document = Document(
            raw_entry_id=unsupported_raw.id,
            document_type="normal_article",
            title=unsupported_raw.title,
            content_text=unsupported_raw.raw_content,
        )
        session.add(unsupported_document)
        session.flush()
        unsupported_item = ContentItem(
            document_id=unsupported_document.id,
            source_id=unsupported.id,
            title=unsupported_raw.title,
            content_text=unsupported_raw.raw_content,
            content_hash=unsupported_raw.content_hash,
            embedding_vector="[1.0,0.0]",
            embedding_model="embedding-model",
        )
        session.add(unsupported_item)
        session.commit()
        unsupported_item_id = unsupported_item.id

        with pytest.raises(RuntimeError, match="Event Evidence 类型不受支持"):
            with clustering_run(
                session,
                scope_type="unsupported-event-projection-test",
                item_ids=[unsupported_item_id],
                rule_version="unsupported-event-projection-test-v1",
            ):
                assign_cluster(session, unsupported_item)

        assert session.scalar(select(func.count(Event.id))) == event_count_before
        assert session.scalar(
            select(func.count(ClusterItem.id)).where(
                ClusterItem.content_item_id == unsupported_item_id
            )
        ) == 0
        failed = session.scalar(
            select(ClusteringRun).where(
                ClusteringRun.scope_type == "unsupported-event-projection-test"
            )
        )
        assert failed is not None
        assert failed.status == "failed"


@pytest.mark.parametrize("media_type", ["social", "notification"])
def test_initial_projection_rejects_non_article_auto_connection(
    media_type: str,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(
            name=f"{media_type} projection source",
            url=f"https://example.com/{media_type}-projection.xml",
            status="active",
            media_type=media_type,
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id=f"{media_type}-projection",
            title=f"{media_type} projection",
            raw_content=f"{media_type} body",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            document_type="normal_article",
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_text=raw.raw_content,
            content_hash=raw.content_hash,
            embedding_vector="[1.0,0.0]",
            embedding_model="embedding-model",
        )
        session.add(item)
        session.flush()

        with pytest.raises(RuntimeError, match="首期只自动接入文章"):
            with clustering_run(
                session,
                scope_type=f"{media_type}-event-projection-test",
                item_ids=[item.id],
                rule_version=f"{media_type}-event-projection-test-v1",
            ):
                assign_cluster(session, item)

        assert session.scalar(select(func.count(Event.id))) == 0
        assert session.scalar(select(func.count(EventEvidence.id))) == 0
