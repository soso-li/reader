from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from reader_api.clustering_run import clustering_run
from reader_api.db import Base, engine
import reader_api.event_projection as event_projection
from reader_api.event_projection import project_completed_clustering_run
from reader_api.main import app
from reader_api.models import (
    Cluster,
    ClusterEventProjection,
    ClusterItem,
    ClusteringRun,
    ClusteringRunProjectionPredecessor,
    ContentItem,
    Document,
    Event,
    EventLineage,
    EventRevision,
    EventRevisionEvidence,
    EventUserState,
    InteractionEvent,
    MigrationBaseline,
    Source,
)
from reader_api.source_ingest import IngestEntry, ingest_source_entries
from tests.factories import assign_publishable_cluster as assign_cluster, make_raw_entry


SessionLocal = sessionmaker(bind=engine)


def _reset_database() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def _make_item(
    session: Session,
    source: Source,
    *,
    external_id: str,
    title: str,
    content: str,
    url: str,
) -> ContentItem:
    raw = make_raw_entry(
        source_id=source.id,
        external_id=external_id,
        title=title,
        url=url,
        published_at=datetime(2026, 7, 14, 8, 0, tzinfo=timezone.utc),
        raw_content=content,
    )
    session.add(raw)
    session.flush()
    document = Document(
        raw_entry_id=raw.id,
        document_type="normal_article",
        title=title,
        content_text=content,
    )
    session.add(document)
    session.flush()
    item = ContentItem(
        document_id=document.id,
        source_id=source.id,
        title=title,
        content_text=content,
        url=url,
        canonical_url=url,
        published_at=raw.published_at,
        content_hash=raw.content_hash,
    )
    session.add(item)
    session.flush()
    return item


def _seed_two_items(session: Session) -> tuple[ContentItem, ContentItem]:
    source = Source(
        name="Reconciliation source",
        url="https://example.com/reconciliation.xml",
        status="active",
        media_type="article",
    )
    session.add(source)
    session.flush()
    return (
        _make_item(
            session,
            source,
            external_id="reconcile-one",
            title="Reconcile one",
            content="First evidence",
            url="https://example.com/reconcile-one",
        ),
        _make_item(
            session,
            source,
            external_id="reconcile-two",
            title="Reconcile two",
            content="Second evidence",
            url="https://example.com/reconcile-two",
        ),
    )


def test_completed_run_bootstraps_legacy_cluster_without_event_projection() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, _second = _seed_two_items(session)
        cluster = assign_cluster(session, first)
        session.commit()

        assert session.scalar(select(func.count(ClusterEventProjection.id))) == 0

        with clustering_run(
            session,
            scope_type="legacy-event-bootstrap",
            item_ids=[first.id],
            rule_version="legacy-event-bootstrap-v1",
        ) as run_id:
            pass

        mapping = session.scalar(select(ClusterEventProjection))
        event = session.scalar(select(Event))
        assert mapping is not None
        assert event is not None
        assert mapping.cluster_id == cluster.id
        assert mapping.clustering_run_id == run_id
        assert mapping.reconciliation_kind == "initial"
        assert mapping.predecessor_projection_id is None
        assert event.status == "active"
        assert session.scalar(select(func.count(EventUserState.event_id))) == 0
        replayed = project_completed_clustering_run(
            session, run_id, [cluster.id]
        )
        session.commit()
        assert [row.id for row in replayed] == [mapping.id]
        assert session.scalar(select(func.count(Event.id))) == 1


def test_mutually_unique_member_add_continues_event_and_appends_one_revision() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        with clustering_run(
            session,
            scope_type="reconciliation-initial",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ):
            cluster = assign_cluster(session, first)

        initial_mapping = session.scalar(select(ClusterEventProjection))
        event = session.scalar(select(Event))
        initial_revision = session.scalar(select(EventRevision))
        assert initial_mapping is not None
        assert event is not None
        assert initial_revision is not None
        state = EventUserState(
            event_id=event.id,
            seen_revision_id=initial_revision.id,
            read_status="summary_seen",
            read_later=True,
            starred=True,
        )
        session.add(state)
        session.commit()

        with clustering_run(
            session,
            scope_type="reconciliation-member-add",
            item_ids=[first.id, second.id],
            rule_version="reconciliation-test-v1",
        ) as run_id:
            session.add(
                ClusterItem(cluster_id=cluster.id, content_item_id=second.id)
            )

        mappings = list(
            session.scalars(
                select(ClusterEventProjection).order_by(ClusterEventProjection.id)
            )
        )
        current = session.get(EventRevision, event.current_revision_id)
        session.refresh(state)

        assert len(mappings) == 2
        assert current is not None
        assert mappings[-1].event_id == event.id
        assert mappings[-1].predecessor_projection_id == initial_mapping.id
        predecessor = session.scalar(
            select(ClusteringRunProjectionPredecessor).where(
                ClusteringRunProjectionPredecessor.run_id == run_id
            )
        )
        assert predecessor is not None
        assert predecessor.predecessor_projection_id == initial_mapping.id
        assert mappings[-1].reconciliation_kind == "continued"
        assert mappings[-1].reconciliation_rule_version == "reconciliation-test-v1"
        assert mappings[-1].before_evidence_fingerprint == (
            initial_revision.evidence_fingerprint
        )
        assert mappings[-1].after_evidence_fingerprint == (
            current.evidence_fingerprint
        )
        assert current.revision_no == 2
        assert session.scalar(select(func.count(Event.id))) == 1
        assert session.scalar(select(func.count(EventRevision.id))) == 2
        assert state.seen_revision_id == initial_revision.id
        assert (state.read_status, state.read_later, state.starred) == (
            "summary_seen",
            True,
            True,
        )
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0

        counts = (
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
        )
        project_completed_clustering_run(session, run_id, [cluster.id])
        session.commit()
        assert (
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
        ) == counts


def test_noop_and_cluster_id_rebuild_keep_current_revision() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, _second = _seed_two_items(session)
        with clustering_run(
            session,
            scope_type="reconciliation-initial",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ):
            original_cluster = assign_cluster(session, first)

        event = session.scalar(select(Event))
        initial_revision = session.scalar(select(EventRevision))
        assert event is not None
        assert initial_revision is not None

        with clustering_run(
            session,
            scope_type="reconciliation-noop",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ):
            original_cluster.generated_title = "Generated title is not evidence"

        noop_mapping = session.scalar(
            select(ClusterEventProjection)
            .order_by(ClusterEventProjection.id.desc())
            .limit(1)
        )
        assert noop_mapping is not None
        assert noop_mapping.reconciliation_kind == "continued"
        assert noop_mapping.before_evidence_fingerprint == (
            noop_mapping.after_evidence_fingerprint
        )
        assert event.current_revision_id == initial_revision.id
        assert session.scalar(select(func.count(EventRevision.id))) == 1

        with clustering_run(
            session,
            scope_type="reconciliation-cluster-id-rebuild",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ):
            old_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == original_cluster.id,
                    ClusterItem.content_item_id == first.id,
                )
            )
            assert old_link is not None
            session.delete(old_link)
            rebuilt_cluster = Cluster(
                cluster_key="rebuilt-cluster-key",
                title="Rebuilt cluster",
                first_seen_at=first.published_at,
                last_seen_at=first.published_at,
            )
            session.add(rebuilt_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=rebuilt_cluster.id,
                    content_item_id=first.id,
                )
            )

        rebuilt_mapping = session.scalar(
            select(ClusterEventProjection)
            .where(ClusterEventProjection.cluster_id == rebuilt_cluster.id)
            .order_by(ClusterEventProjection.id.desc())
            .limit(1)
        )
        assert rebuilt_mapping is not None
        assert rebuilt_mapping.event_id == event.id
        assert rebuilt_mapping.event_revision_id == initial_revision.id
        assert rebuilt_mapping.predecessor_projection_id == noop_mapping.id
        assert rebuilt_mapping.cluster_id_snapshot == rebuilt_cluster.id
        assert session.scalar(select(func.count(Event.id))) == 1
        assert session.scalar(select(func.count(EventRevision.id))) == 1


def test_version_and_role_changes_each_append_exactly_one_continuous_revision(
    monkeypatch,
) -> None:
    _reset_database()
    with SessionLocal() as session:
        first, _second = _seed_two_items(session)
        with clustering_run(
            session,
            scope_type="reconciliation-initial",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ):
            cluster = assign_cluster(session, first)

        event = session.scalar(select(Event))
        assert event is not None

        with clustering_run(
            session,
            scope_type="reconciliation-version-change",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ):
            first.content_text = "First evidence enriched without identity change"

        assert session.get(EventRevision, event.current_revision_id).revision_no == 2

        monkeypatch.setattr(
            "reader_api.event_projection.DEFAULT_LEGACY_ARTICLE_ROLE",
            "primary_source",
        )
        with clustering_run(
            session,
            scope_type="reconciliation-role-change",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ):
            pass

        assert session.get(EventRevision, event.current_revision_id).revision_no == 3
        assert list(
            session.scalars(
                select(EventRevision.revision_no)
                .where(EventRevision.event_id == event.id)
                .order_by(EventRevision.revision_no)
            )
        ) == [1, 2, 3]
        assert session.scalar(select(func.count(Event.id))) == 1


def test_raw_entry_revision_change_continues_event_in_the_same_run() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, _second = _seed_two_items(session)
        source = session.get(Source, first.source_id)
        assert source is not None
        with clustering_run(
            session,
            scope_type="reconciliation-initial",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ):
            assign_cluster(session, first)

        event = session.scalar(select(Event))
        assert event is not None

        with clustering_run(
            session,
            scope_type="reconciliation-raw-revision-change",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ):
            assert ingest_source_entries(
                session,
                source,
                [
                    IngestEntry(
                        source_guid="reconcile-one",
                        external_id="reconcile-one",
                        title="Reconcile one revised",
                        url="https://example.com/reconcile-one",
                        raw_content="First evidence revised",
                        content_text="First evidence revised",
                        published_at=first.published_at,
                    )
                ],
            ) == 0

        revisions = list(
            session.scalars(
                select(EventRevision)
                .where(EventRevision.event_id == event.id)
                .order_by(EventRevision.revision_no)
            )
        )
        assert [revision.revision_no for revision in revisions] == [1, 2]
        assert event.current_revision_id == revisions[-1].id
        assert session.scalar(select(func.count(Event.id))) == 1


def test_return_to_historical_fingerprint_appends_a_new_revision() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        with clustering_run(
            session,
            scope_type="reconciliation-initial",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ):
            cluster = assign_cluster(session, first)

        event = session.scalar(select(Event))
        initial_revision = session.scalar(select(EventRevision))
        assert event is not None
        assert initial_revision is not None

        with clustering_run(
            session,
            scope_type="reconciliation-member-add",
            item_ids=[first.id, second.id],
            rule_version="reconciliation-test-v1",
        ):
            session.add(
                ClusterItem(cluster_id=cluster.id, content_item_id=second.id)
            )

        with clustering_run(
            session,
            scope_type="reconciliation-member-remove",
            item_ids=[first.id, second.id],
            rule_version="reconciliation-test-v1",
        ):
            added_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            assert added_link is not None
            session.delete(added_link)

        revisions = list(
            session.scalars(
                select(EventRevision)
                .where(EventRevision.event_id == event.id)
                .order_by(EventRevision.revision_no)
            )
        )
        assert [revision.revision_no for revision in revisions] == [1, 2, 3]
        assert revisions[0].evidence_fingerprint == revisions[2].evidence_fingerprint
        assert revisions[1].evidence_fingerprint != revisions[2].evidence_fingerprint
        assert event.current_revision_id == revisions[2].id


def test_one_parent_to_two_children_creates_new_split_events() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, _second = _seed_two_items(session)
        with clustering_run(
            session,
            scope_type="reconciliation-initial",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ):
            original_cluster = assign_cluster(session, first)

        original_mapping = session.scalar(select(ClusterEventProjection))
        assert original_mapping is not None
        original_event = session.get(Event, original_mapping.event_id)
        original_revision = session.get(
            EventRevision, original_mapping.event_revision_id
        )
        assert original_event is not None
        assert original_revision is not None
        state = EventUserState(
            event_id=original_event.id,
            seen_revision_id=original_revision.id,
            read_status="summary_seen",
            read_later=True,
            starred=True,
            uninterested=True,
            uninterested_reason="topic",
            uninterested_at=datetime(2026, 7, 14, 8, 30, tzinfo=timezone.utc),
        )
        session.add(state)
        interaction = InteractionEvent(
            operation_id="split-parent-starred",
            target_kind="event",
            event_id=original_event.id,
            observed_revision_id=original_revision.id,
            action="starred_set",
            set_value=True,
            payload={},
            occurred_at=datetime(2026, 7, 14, 9, 0, tzinfo=timezone.utc),
        )
        session.add(interaction)
        session.commit()
        original_evidence_links = list(
            session.scalars(
                select(EventRevisionEvidence).where(
                    EventRevisionEvidence.revision_id == original_revision.id
                )
            )
        )
        assert original_evidence_links
        with clustering_run(
            session,
            scope_type="reconciliation-ambiguous",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ) as ambiguous_run_id:
            duplicate_cluster = Cluster(
                cluster_key="ambiguous-duplicate-cluster",
                title="Ambiguous duplicate",
                first_seen_at=first.published_at,
                last_seen_at=first.published_at,
            )
            session.add(duplicate_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=duplicate_cluster.id,
                    content_item_id=first.id,
                )
            )

        split_mappings = list(
            session.scalars(
                select(ClusterEventProjection)
                .where(
                    ClusterEventProjection.clustering_run_id == ambiguous_run_id
                )
                .order_by(ClusterEventProjection.id)
            )
        )
        assert len(split_mappings) == 2
        assert {mapping.reconciliation_kind for mapping in split_mappings} == {
            "split"
        }
        assert {
            mapping.predecessor_projection_id for mapping in split_mappings
        } == {original_mapping.id}
        assert len({mapping.event_id for mapping in split_mappings}) == 2
        assert original_mapping.event_id not in {
            mapping.event_id for mapping in split_mappings
        }
        assert session.scalar(select(func.count(Event.id))) == 3
        assert session.scalar(select(func.count(EventRevision.id))) == 3
        session.refresh(original_event)
        assert original_event.status == "superseded"
        assert original_event.superseded_at is not None
        child_revisions = list(
            session.scalars(
                select(EventRevision).where(
                    EventRevision.event_id.in_(
                        mapping.event_id for mapping in split_mappings
                    )
                )
            )
        )
        assert [revision.revision_no for revision in child_revisions] == [1, 1]
        lineages = list(
            session.scalars(
                select(EventLineage)
                .where(EventLineage.clustering_run_id == ambiguous_run_id)
                .order_by(EventLineage.id)
            )
        )
        assert len(lineages) == 2
        assert {lineage.relation_type for lineage in lineages} == {"split_from"}
        assert {lineage.source_event_id for lineage in lineages} == {
            original_event.id
        }
        assert {lineage.target_event_id for lineage in lineages} == {
            mapping.event_id for mapping in split_mappings
        }
        assert {lineage.rule_version for lineage in lineages} == {
            "reconciliation-test-v1"
        }
        assert {lineage.before_evidence_fingerprint for lineage in lineages} == {
            original_revision.evidence_fingerprint
        }
        assert {
            lineage.after_evidence_fingerprint for lineage in lineages
        } == {
            mapping.after_evidence_fingerprint for mapping in split_mappings
        }
        assert list(session.scalars(select(EventUserState.event_id))) == [
            original_event.id
        ]
        assert original_mapping.cluster_id == original_cluster.id

        counts = (
            session.scalar(select(func.count(Event.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
            session.scalar(select(func.count(EventLineage.id))),
        )
        replayed = project_completed_clustering_run(
            session,
            ambiguous_run_id,
            [original_cluster.id, duplicate_cluster.id],
        )
        session.commit()
        assert [mapping.id for mapping in replayed] == [
            mapping.id for mapping in split_mappings
        ]
        assert (
            session.scalar(select(func.count(Event.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
            session.scalar(select(func.count(EventLineage.id))),
        ) == counts

        client = TestClient(app)
        listed = client.get("/clusters")
        assert listed.status_code == 200
        listed_by_id = {row["id"]: row for row in listed.json()}
        reused_child = listed_by_id[original_cluster.id]
        assert reused_child["event_uid"] != original_event.uid
        assert reused_child["read_status"] == "unread"
        assert reused_child["read_later"] is False
        assert reused_child["starred"] is False
        assert reused_child["uninterested"] is False

        detail = client.get(f"/clusters/{original_cluster.id}")
        assert detail.status_code == 200
        assert detail.json()["read_status"] == "unread"
        assert detail.json()["read_later"] is False
        assert detail.json()["starred"] is False

        assert client.get("/clusters", params={"starred": True}).json() == []
        assert client.get("/clusters/count", params={"starred": True}).json() == {
            "count": 0
        }
        assert {
            row["id"]
            for row in client.get(
                "/clusters", params={"read_status": "unread"}
            ).json()
        } == {original_cluster.id, duplicate_cluster.id}
        assert client.get(
            "/clusters/count", params={"read_status": "unread"}
        ).json() == {"count": 2}

        bulk = client.post(
            "/user-state/bulk-read/prepare",
            json={"object_type": "event"},
        )
        assert bulk.status_code == 200
        assert bulk.json()["target_count"] == 2
        sources = client.get("/sources")
        assert sources.status_code == 200
        assert sources.json()[0]["unread_count"] == 2

        rejected_parent_write = client.post(
            "/event-user-state",
            json={
                "event_uid": original_event.uid,
                "observed_revision_uid": original_revision.uid,
                "operation_id": "95959595-9595-4595-8595-959595959595",
                "action": "starred_set",
                "value": False,
            },
        )
        assert rejected_parent_write.status_code == 409
        session.refresh(state)
        assert state.read_status == "summary_seen"
        assert state.read_later is True
        assert state.starred is True

        split_cluster_ids = [original_cluster.id, duplicate_cluster.id]
        for link in session.scalars(
            select(ClusterItem).where(
                ClusterItem.cluster_id.in_(split_cluster_ids)
            )
        ):
            session.delete(link)
        session.flush()
        for cluster_id in split_cluster_ids:
            cluster = session.get(Cluster, cluster_id)
            assert cluster is not None
            session.delete(cluster)
        session.commit()

        session.refresh(original_mapping)
        for mapping in split_mappings:
            session.refresh(mapping)
        assert session.get(Cluster, original_cluster.id) is None
        assert session.get(Cluster, duplicate_cluster.id) is None
        assert session.get(ClusterEventProjection, original_mapping.id) is not None
        assert all(
            session.get(ClusterEventProjection, mapping.id) is not None
            for mapping in split_mappings
        )
        assert session.get(Event, original_event.id) is not None
        assert session.get(EventRevision, original_revision.id) is not None
        assert session.get(InteractionEvent, interaction.id) is not None
        assert session.scalar(
            select(func.count(EventRevisionEvidence.id)).where(
                EventRevisionEvidence.revision_id == original_revision.id
            )
        ) == len(original_evidence_links)
        assert session.scalar(
            select(func.count(EventLineage.id)).where(
                EventLineage.source_event_id == original_event.id
            )
        ) == 2
        assert session.get(EventUserState, state.id) is not None


def test_two_parents_to_one_child_creates_new_stateless_merge_event() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        with clustering_run(
            session,
            scope_type="merge-initial",
            item_ids=[first.id, second.id],
            rule_version="merge-test-v1",
        ):
            first_cluster = assign_cluster(session, first)
            second_cluster = Cluster(
                cluster_key="merge-second-parent",
                title="Merge second parent",
                first_seen_at=second.published_at,
                last_seen_at=second.published_at,
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=second.id,
                )
            )

        parent_mappings = list(
            session.scalars(
                select(ClusterEventProjection).order_by(
                    ClusterEventProjection.cluster_id_snapshot
                )
            )
        )
        assert len(parent_mappings) == 2
        parent_events = [
            session.get(Event, mapping.event_id) for mapping in parent_mappings
        ]
        parent_revisions = [
            session.get(EventRevision, mapping.event_revision_id)
            for mapping in parent_mappings
        ]
        assert all(parent is not None for parent in parent_events)
        assert all(revision is not None for revision in parent_revisions)
        parent_event_ids = {mapping.event_id for mapping in parent_mappings}
        parent_revision_ids = {
            mapping.event_revision_id for mapping in parent_mappings
        }
        baselines = [
            MigrationBaseline(
                idempotency_key="a" * 64,
                migration_version="legacy-user-state-baseline-v1",
                legacy_user_state_id=910001,
                legacy_object_type="cluster",
                legacy_object_id=parent_mappings[0].cluster_id_snapshot,
                resolved_event_id=parent_mappings[0].event_id,
                resolved_revision_id=parent_mappings[0].event_revision_id,
                read_status="summary_seen",
                read_later=True,
                starred=False,
                source_updated_at=datetime(
                    2026, 7, 14, 8, 0, tzinfo=timezone.utc
                ),
            ),
            MigrationBaseline(
                idempotency_key="b" * 64,
                migration_version="legacy-user-state-baseline-v1",
                legacy_user_state_id=910002,
                legacy_object_type="cluster",
                legacy_object_id=parent_mappings[1].cluster_id_snapshot,
                resolved_event_id=parent_mappings[1].event_id,
                resolved_revision_id=parent_mappings[1].event_revision_id,
                read_status="original_opened",
                read_later=False,
                starred=True,
                source_updated_at=datetime(
                    2026, 7, 14, 8, 1, tzinfo=timezone.utc
                ),
            ),
        ]
        session.add_all(baselines)
        session.flush()
        session.add_all(
            [
                EventUserState(
                    baseline_id=baselines[0].id,
                    event_id=parent_mappings[0].event_id,
                    seen_revision_id=parent_mappings[0].event_revision_id,
                    read_status="summary_seen",
                    read_later=True,
                    starred=False,
                    uninterested=True,
                    uninterested_reason="repetitive",
                    uninterested_at=datetime(
                        2026, 7, 14, 8, 30, tzinfo=timezone.utc
                    ),
                ),
                EventUserState(
                    baseline_id=baselines[1].id,
                    event_id=parent_mappings[1].event_id,
                    seen_revision_id=parent_mappings[1].event_revision_id,
                    read_status="original_opened",
                    read_later=False,
                    starred=True,
                ),
                InteractionEvent(
                    operation_id="29290000-0000-4000-8000-000000000001",
                    target_kind="event",
                    event_id=parent_mappings[0].event_id,
                    observed_revision_id=parent_mappings[0].event_revision_id,
                    action="read_later_set",
                    set_value=True,
                    payload={},
                    occurred_at=datetime(2026, 7, 14, 9, 0, tzinfo=timezone.utc),
                ),
                InteractionEvent(
                    operation_id="29290000-0000-4000-8000-000000000002",
                    target_kind="event",
                    event_id=parent_mappings[1].event_id,
                    observed_revision_id=parent_mappings[1].event_revision_id,
                    action="starred_set",
                    set_value=True,
                    payload={},
                    occurred_at=datetime(2026, 7, 14, 9, 1, tzinfo=timezone.utc),
                ),
            ]
        )
        session.commit()

        with clustering_run(
            session,
            scope_type="merge-change",
            item_ids=[first.id, second.id],
            rule_version="merge-test-v1",
        ) as merge_run_id:
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == second_cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            assert second_link is not None
            session.delete(second_link)
            session.add(
                ClusterItem(
                    cluster_id=first_cluster.id,
                    content_item_id=second.id,
                )
            )

        merge_mappings = list(
            session.scalars(
                select(ClusterEventProjection).where(
                    ClusterEventProjection.clustering_run_id == merge_run_id
                )
            )
        )
        assert len(merge_mappings) == 1
        merged_mapping = merge_mappings[0]
        assert merged_mapping.reconciliation_kind == "merged"
        assert merged_mapping.predecessor_projection_id is None
        assert merged_mapping.event_id not in parent_event_ids
        merged_event = session.get(Event, merged_mapping.event_id)
        merged_revision = session.get(
            EventRevision,
            merged_mapping.event_revision_id,
        )
        assert merged_event is not None
        assert merged_revision is not None
        assert merged_event.status == "active"
        assert merged_revision.revision_no == 1
        assert merged_revision.evidence_fingerprint == (
            merged_mapping.after_evidence_fingerprint
        )
        assert session.scalar(
            select(func.count(EventRevisionEvidence.id)).where(
                EventRevisionEvidence.revision_id == merged_revision.id
            )
        ) == 2

        for parent in parent_events:
            assert parent is not None
            session.refresh(parent)
            assert parent.status == "superseded"
            assert parent.superseded_at is not None
        lineages = list(
            session.scalars(
                select(EventLineage)
                .where(EventLineage.clustering_run_id == merge_run_id)
                .order_by(EventLineage.source_event_id)
            )
        )
        assert len(lineages) == 2
        assert {lineage.relation_type for lineage in lineages} == {
            "merged_from"
        }
        assert {lineage.source_event_id for lineage in lineages} == (
            parent_event_ids
        )
        assert {lineage.target_event_id for lineage in lineages} == {
            merged_event.id
        }
        assert {lineage.rule_version for lineage in lineages} == {
            "merge-test-v1"
        }
        assert {lineage.before_evidence_fingerprint for lineage in lineages} == {
            revision.evidence_fingerprint
            for revision in parent_revisions
            if revision is not None
        }
        assert {lineage.after_evidence_fingerprint for lineage in lineages} == {
            merged_revision.evidence_fingerprint
        }
        assert set(session.scalars(select(EventUserState.event_id))) == (
            parent_event_ids
        )
        assert set(
            session.scalars(
                select(MigrationBaseline.resolved_event_id)
            )
        ) == parent_event_ids
        assert set(
            session.scalars(select(InteractionEvent.event_id))
        ) == parent_event_ids
        assert session.scalar(
            select(func.count(EventRevision.id)).where(
                EventRevision.id.in_(parent_revision_ids)
            )
        ) == 2

        client = TestClient(app)
        listed = client.get("/clusters")
        assert listed.status_code == 200
        merged_row = {
            row["id"]: row for row in listed.json()
        }[first_cluster.id]
        assert merged_row["event_uid"] == merged_event.uid
        assert merged_row["read_status"] == "unread"
        assert merged_row["read_later"] is False
        assert merged_row["starred"] is False
        assert merged_row["uninterested"] is False
        detail = client.get(f"/clusters/{first_cluster.id}")
        assert detail.status_code == 200
        assert detail.json()["read_status"] == "unread"
        assert detail.json()["read_later"] is False
        assert detail.json()["starred"] is False
        assert first_cluster.id not in {
            row["id"]
            for row in client.get(
                "/clusters", params={"starred": True}
            ).json()
        }
        assert client.get(
            "/clusters/count", params={"starred": True}
        ).json() == {"count": 0}
        assert client.get(
            "/clusters/count", params={"read_status": "unread"}
        ).json() == {"count": 1}
        bulk = client.post(
            "/user-state/bulk-read/prepare",
            json={"object_type": "event"},
        )
        assert bulk.status_code == 200
        assert bulk.json()["target_count"] == 1
        sources = client.get("/sources")
        assert sources.status_code == 200
        assert sources.json()[0]["unread_count"] == 1

        first_parent = parent_events[0]
        first_parent_revision = parent_revisions[0]
        assert first_parent is not None
        assert first_parent_revision is not None
        rejected_parent_write = client.post(
            "/event-user-state",
            json={
                "event_uid": first_parent.uid,
                "observed_revision_uid": first_parent_revision.uid,
                "operation_id": "29290000-0000-4000-8000-000000000003",
                "action": "starred_set",
                "value": False,
            },
        )
        assert rejected_parent_write.status_code == 409

        counts = (
            session.scalar(select(func.count(Event.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
            session.scalar(select(func.count(EventLineage.id))),
        )
        replayed = project_completed_clustering_run(
            session,
            merge_run_id,
            [first_cluster.id],
        )
        session.commit()
        assert [projection.id for projection in replayed] == [
            merged_mapping.id
        ]
        assert (
            session.scalar(select(func.count(Event.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
            session.scalar(select(func.count(EventLineage.id))),
        ) == counts

        merge_cluster_ids = [first_cluster.id, second_cluster.id]
        for link in session.scalars(
            select(ClusterItem).where(
                ClusterItem.cluster_id.in_(merge_cluster_ids)
            )
        ):
            session.delete(link)
        session.flush()
        for cluster_id in merge_cluster_ids:
            cluster = session.get(Cluster, cluster_id)
            assert cluster is not None
            session.delete(cluster)
        session.commit()

        preserved_mappings = list(
            session.scalars(
                select(ClusterEventProjection).where(
                    ClusterEventProjection.id.in_(
                        [
                            *(mapping.id for mapping in parent_mappings),
                            merged_mapping.id,
                        ]
                    )
                )
            )
        )
        for mapping in preserved_mappings:
            session.refresh(mapping)
        assert len(preserved_mappings) == 3
        assert session.get(Cluster, first_cluster.id) is None
        assert session.get(Cluster, second_cluster.id) is None
        assert session.scalar(select(func.count(Event.id))) == 3
        assert session.scalar(select(func.count(EventRevision.id))) == 3
        assert session.scalar(select(func.count(EventLineage.id))) == 2
        assert session.scalar(select(func.count(EventUserState.id))) == 2
        assert session.scalar(select(func.count(MigrationBaseline.id))) == 2
        assert session.scalar(select(func.count(InteractionEvent.id))) == 2
        assert session.scalar(
            select(func.count(EventRevisionEvidence.id)).where(
                EventRevisionEvidence.revision_id == merged_revision.id
            )
        ) == 2


def test_three_parents_merge_without_selecting_a_winner() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        source = session.get(Source, first.source_id)
        assert source is not None
        third = _make_item(
            session,
            source,
            external_id="merge-three",
            title="Merge three",
            content="Third merge evidence",
            url="https://example.com/merge-three",
        )
        items = [first, second, third]
        with clustering_run(
            session,
            scope_type="merge-three-initial",
            item_ids=[item.id for item in items],
            rule_version="merge-three-v1",
        ):
            target_cluster = assign_cluster(session, first)
            other_clusters = []
            for index, item in enumerate(items[1:], start=2):
                cluster = Cluster(
                    cluster_key=f"merge-parent-{index}",
                    title=f"Merge parent {index}",
                )
                session.add(cluster)
                session.flush()
                session.add(
                    ClusterItem(
                        cluster_id=cluster.id,
                        content_item_id=item.id,
                    )
                )
                other_clusters.append(cluster)

        parents = list(
            session.scalars(
                select(ClusterEventProjection).order_by(
                    ClusterEventProjection.id
                )
            )
        )
        assert len(parents) == 3
        parent_event_ids = {parent.event_id for parent in parents}
        session.commit()

        with clustering_run(
            session,
            scope_type="merge-three-change",
            item_ids=[item.id for item in items],
            rule_version="merge-three-v1",
        ) as run_id:
            for cluster, item in zip(other_clusters, items[1:], strict=True):
                link = session.scalar(
                    select(ClusterItem).where(
                        ClusterItem.cluster_id == cluster.id,
                        ClusterItem.content_item_id == item.id,
                    )
                )
                assert link is not None
                session.delete(link)
                session.add(
                    ClusterItem(
                        cluster_id=target_cluster.id,
                        content_item_id=item.id,
                    )
                )

        merged = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == run_id
            )
        )
        assert merged is not None
        assert merged.reconciliation_kind == "merged"
        assert merged.event_id not in parent_event_ids
        assert set(
            session.scalars(
                select(EventLineage.source_event_id).where(
                    EventLineage.clustering_run_id == run_id,
                    EventLineage.relation_type == "merged_from",
                )
            )
        ) == parent_event_ids
        assert session.scalar(
            select(func.count(EventLineage.id)).where(
                EventLineage.clustering_run_id == run_id
            )
        ) == 3
        assert set(
            session.scalars(
                select(Event.status).where(Event.id.in_(parent_event_ids))
            )
        ) == {"superseded"}
        assert session.scalar(
            select(func.count(EventUserState.id)).where(
                EventUserState.event_id == merged.event_id
            )
        ) == 0


def test_conflicting_parent_candidates_create_new_ambiguous_event() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        source = session.get(Source, first.source_id)
        assert source is not None
        third = _make_item(
            session,
            source,
            external_id="merge-shrink-third",
            title="Merge shrink third",
            content="Third shrinking parent evidence",
            url="https://example.com/merge-shrink-third",
        )
        with clustering_run(
            session,
            scope_type="merge-shrink-initial",
            item_ids=[first.id, second.id, third.id],
            rule_version="merge-shrink-v1",
        ):
            target_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=target_cluster.id,
                    content_item_id=second.id,
                )
            )
            other_cluster = Cluster(
                cluster_key="merge-shrink-other-parent",
                title="Merge shrink other parent",
            )
            session.add(other_cluster)
            session.flush()
            session.add_all(
                [
                    ClusterItem(
                        cluster_id=other_cluster.id,
                        content_item_id=first.id,
                    ),
                    ClusterItem(
                        cluster_id=other_cluster.id,
                        content_item_id=third.id,
                    ),
                ]
            )

        parent_event_ids = set(session.scalars(select(Event.id)))
        assert len(parent_event_ids) == 2
        with clustering_run(
            session,
            scope_type="merge-shrink-change",
            item_ids=[first.id, second.id, third.id],
            rule_version="merge-shrink-v1",
        ) as run_id:
            for link in session.scalars(
                select(ClusterItem).where(
                    (ClusterItem.cluster_id == other_cluster.id)
                    | (
                        (ClusterItem.cluster_id == target_cluster.id)
                        & (ClusterItem.content_item_id == second.id)
                    )
                )
            ):
                session.delete(link)

        projection = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == run_id
            )
        )
        assert projection is not None
        assert projection.reconciliation_kind == "ambiguous"
        assert projection.event_id not in parent_event_ids
        lineages = list(
            session.scalars(
                select(EventLineage).where(
                    EventLineage.clustering_run_id == run_id
                )
            )
        )
        assert len(lineages) == 2
        assert {lineage.relation_type for lineage in lineages} == {
            "ambiguous_from"
        }
        assert {lineage.source_event_id for lineage in lineages} == (
            parent_event_ids
        )
        assert {lineage.target_event_id for lineage in lineages} == {
            projection.event_id
        }
        assert set(
            session.scalars(
                select(Event.id).where(Event.status == "superseded")
            )
        ) == parent_event_ids


def test_merge_projection_failure_rolls_back_successor_and_parent_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        with clustering_run(
            session,
            scope_type="merge-rollback-initial",
            item_ids=[first.id, second.id],
            rule_version="merge-rollback-v1",
        ):
            target_cluster = assign_cluster(session, first)
            other_cluster = Cluster(
                cluster_key="merge-rollback-parent",
                title="Merge rollback parent",
            )
            session.add(other_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=other_cluster.id,
                    content_item_id=second.id,
                )
            )

        parent_event_ids = set(session.scalars(select(Event.id)))
        assert len(parent_event_ids) == 2
        session.commit()

        import reader_api.event_projection as event_projection

        create_projection = event_projection._create_merge_projection

        def fail_after_successor_creation(*args: object, **kwargs: object):
            create_projection(*args, **kwargs)
            raise RuntimeError("forced merge projection failure")

        monkeypatch.setattr(
            event_projection,
            "_create_merge_projection",
            fail_after_successor_creation,
        )
        with pytest.raises(
            RuntimeError,
            match="forced merge projection failure",
        ):
            with clustering_run(
                session,
                scope_type="merge-rollback-change",
                item_ids=[first.id, second.id],
                rule_version="merge-rollback-v1",
            ):
                link = session.scalar(
                    select(ClusterItem).where(
                        ClusterItem.cluster_id == other_cluster.id,
                        ClusterItem.content_item_id == second.id,
                    )
                )
                assert link is not None
                session.delete(link)
                session.add(
                    ClusterItem(
                        cluster_id=target_cluster.id,
                        content_item_id=second.id,
                    )
                )

        assert set(session.scalars(select(Event.id))) == parent_event_ids
        assert set(session.scalars(select(Event.status))) == {"active"}
        assert session.scalar(select(func.count(EventRevision.id))) == 2
        assert session.scalar(select(func.count(ClusterEventProjection.id))) == 2
        assert session.scalar(select(func.count(EventLineage.id))) == 0
        failed_run = session.scalar(
            select(ClusteringRun).where(
                ClusteringRun.scope_type == "merge-rollback-change"
            )
        )
        assert failed_run is not None
        assert failed_run.status == "failed"


def test_no_anchor_replacement_creates_new_ambiguous_event_without_false_lineage() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        with clustering_run(
            session,
            scope_type="reconciliation-initial",
            item_ids=[first.id],
            rule_version="reconciliation-test-v1",
        ):
            cluster = assign_cluster(session, first)

        original_mapping = session.scalar(select(ClusterEventProjection))
        original_event = session.scalar(select(Event))
        assert original_mapping is not None
        assert original_event is not None
        session.add(
            EventUserState(
                event_id=original_event.id,
                seen_revision_id=original_mapping.event_revision_id,
                read_status="summary_seen",
                read_later=True,
                starred=True,
                uninterested=True,
                uninterested_reason="promotion",
                uninterested_at=datetime(
                    2026, 7, 14, 8, 30, tzinfo=timezone.utc
                ),
            )
        )
        session.commit()

        with clustering_run(
            session,
            scope_type="reconciliation-no-anchor",
            item_ids=[first.id, second.id],
            rule_version="reconciliation-test-v1",
        ) as run_id:
            first_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == cluster.id,
                    ClusterItem.content_item_id == first.id,
                )
            )
            assert first_link is not None
            session.delete(first_link)
            session.add(
                ClusterItem(cluster_id=cluster.id, content_item_id=second.id)
            )

        mapping = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == run_id
            )
        )
        assert mapping is not None
        assert mapping.reconciliation_kind == "ambiguous"
        assert mapping.predecessor_projection_id is None
        assert mapping.before_evidence_fingerprint is None
        assert mapping.reconciliation_rule_version == "reconciliation-test-v1"
        assert mapping.event_id != original_event.id
        session.refresh(original_event)
        assert original_event.status == "superseded"
        assert original_event.superseded_at is not None
        assert session.scalar(select(func.count(Event.id))) == 2
        assert session.scalar(select(func.count(EventRevision.id))) == 2
        assert session.scalar(select(func.count(EventLineage.id))) == 0
        assert set(session.scalars(select(EventUserState.event_id))) == {
            original_event.id
        }

        client = TestClient(app)
        row = client.get(f"/clusters/{cluster.id}").json()
        assert row["event_uid"] != original_event.uid
        assert row["read_status"] == "unread"
        assert row["read_later"] is False
        assert row["starred"] is False
        assert row["uninterested"] is False

        ambiguous_event = session.get(Event, mapping.event_id)
        ambiguous_revision = session.get(
            EventRevision,
            mapping.event_revision_id,
        )
        assert ambiguous_event is not None
        assert ambiguous_revision is not None
        mutation = client.post(
            "/event-user-state",
            json={
                "event_uid": ambiguous_event.uid,
                "observed_revision_uid": ambiguous_revision.uid,
                "operation_id": "30300000-0000-4000-8000-000000000001",
                "action": "starred_set",
                "value": True,
            },
        )
        assert mutation.status_code == 200
        ambiguous_state = session.scalar(
            select(EventUserState).where(
                EventUserState.event_id == ambiguous_event.id
            )
        )
        assert ambiguous_state is not None
        assert ambiguous_state.read_status == "unread"
        assert ambiguous_state.read_later is False
        assert ambiguous_state.starred is True

        with clustering_run(
            session,
            scope_type="reconciliation-no-anchor-continuation",
            item_ids=[second.id],
            rule_version="reconciliation-test-v1",
        ) as continuation_run_id:
            pass
        continuation = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id
                == continuation_run_id
            )
        )
        assert continuation is not None
        assert continuation.reconciliation_kind == "continued"
        assert continuation.event_id == ambiguous_event.id
        continued_row = client.get(f"/clusters/{cluster.id}").json()
        assert continued_row["read_status"] == "unread"
        assert continued_row["read_later"] is False
        assert continued_row["starred"] is True


def test_partial_member_split_never_selects_a_main_child() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        source = session.get(Source, first.source_id)
        assert source is not None
        third = _make_item(
            session,
            source,
            external_id="split-three",
            title="Split three",
            content="Third split evidence",
            url="https://example.com/split-three",
        )
        fourth = _make_item(
            session,
            source,
            external_id="split-four",
            title="Split four",
            content="Fourth split evidence",
            url="https://example.com/split-four",
        )
        with clustering_run(
            session,
            scope_type="split-partial-initial",
            item_ids=[first.id, second.id, third.id],
            rule_version="split-partial-v1",
        ):
            original_cluster = assign_cluster(session, first)
            session.add_all(
                [
                    ClusterItem(
                        cluster_id=original_cluster.id,
                        content_item_id=second.id,
                    ),
                    ClusterItem(
                        cluster_id=original_cluster.id,
                        content_item_id=third.id,
                    ),
                ]
            )

        original_mapping = session.scalar(select(ClusterEventProjection))
        assert original_mapping is not None
        with clustering_run(
            session,
            scope_type="split-partial-change",
            item_ids=[first.id, second.id, third.id, fourth.id],
            rule_version="split-partial-v1",
        ) as run_id:
            third_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == original_cluster.id,
                    ClusterItem.content_item_id == third.id,
                )
            )
            assert third_link is not None
            session.delete(third_link)
            second_child = Cluster(
                cluster_key="partial-split-second-child",
                title="Partial split second child",
                first_seen_at=third.published_at,
                last_seen_at=fourth.published_at,
            )
            session.add(second_child)
            session.flush()
            session.add_all(
                [
                    ClusterItem(
                        cluster_id=second_child.id,
                        content_item_id=third.id,
                    ),
                    ClusterItem(
                        cluster_id=second_child.id,
                        content_item_id=fourth.id,
                    ),
                ]
            )

        mappings = list(
            session.scalars(
                select(ClusterEventProjection).where(
                    ClusterEventProjection.clustering_run_id == run_id
                )
            )
        )
        assert len(mappings) == 2
        assert {mapping.reconciliation_kind for mapping in mappings} == {"split"}
        assert {mapping.predecessor_projection_id for mapping in mappings} == {
            original_mapping.id
        }
        assert original_mapping.event_id not in {
            mapping.event_id for mapping in mappings
        }
        assert len({mapping.event_id for mapping in mappings}) == 2
        assert session.scalar(
            select(func.count(EventLineage.id)).where(
                EventLineage.clustering_run_id == run_id
            )
        ) == 2


def test_empty_cluster_does_not_create_a_split_successor() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        with clustering_run(
            session,
            scope_type="split-empty-initial",
            item_ids=[first.id, second.id],
            rule_version="split-empty-v1",
        ):
            cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(cluster_id=cluster.id, content_item_id=second.id)
            )

        original_event = session.scalar(select(Event))
        assert original_event is not None
        with clustering_run(
            session,
            scope_type="split-empty-change",
            item_ids=[first.id, second.id],
            rule_version="split-empty-v1",
        ) as run_id:
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            assert second_link is not None
            session.delete(second_link)
            empty_cluster = Cluster(
                cluster_key="empty-split-child",
                title="Empty split child",
            )
            session.add(empty_cluster)

        mapping = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == run_id
            )
        )
        assert mapping is not None
        assert mapping.reconciliation_kind == "continued"
        assert mapping.event_id == original_event.id
        assert session.scalar(
            select(func.count(ClusterEventProjection.id)).where(
                ClusterEventProjection.cluster_id == empty_cluster.id
            )
        ) == 0
        assert session.scalar(select(func.count(EventLineage.id))) == 0
        assert session.scalar(select(func.count(Event.id))) == 1


def test_split_projection_failure_rolls_back_all_children_and_parent_status() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        source = session.get(Source, first.source_id)
        assert source is not None
        with clustering_run(
            session,
            scope_type="split-rollback-initial",
            item_ids=[first.id, second.id],
            rule_version="split-rollback-v1",
        ):
            cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(cluster_id=cluster.id, content_item_id=second.id)
            )

        original_event = session.scalar(select(Event))
        original_revision = session.scalar(select(EventRevision))
        assert original_event is not None
        assert original_revision is not None
        failed_run_id = ""
        with pytest.raises(RuntimeError, match="Event Evidence 类型不受支持"):
            with clustering_run(
                session,
                scope_type="split-rollback-change",
                item_ids=[first.id, second.id],
                rule_version="split-rollback-v1",
            ) as failed_run_id:
                second_link = session.scalar(
                    select(ClusterItem).where(
                        ClusterItem.cluster_id == cluster.id,
                        ClusterItem.content_item_id == second.id,
                    )
                )
                assert second_link is not None
                session.delete(second_link)
                second_child = Cluster(
                    cluster_key="split-rollback-second-child",
                    title="Split rollback second child",
                )
                session.add(second_child)
                session.flush()
                session.add(
                    ClusterItem(
                        cluster_id=second_child.id,
                        content_item_id=second.id,
                    )
                )
                source.media_type = "video"

        session.refresh(original_event)
        session.refresh(source)
        failed_run = session.get(ClusteringRun, failed_run_id)
        assert failed_run is not None
        assert failed_run.status == "failed"
        assert original_event.status == "active"
        assert original_event.superseded_at is None
        assert original_event.current_revision_id == original_revision.id
        assert source.media_type == "article"
        assert session.scalar(select(func.count(Event.id))) == 1
        assert session.scalar(select(func.count(EventRevision.id))) == 1
        assert session.scalar(select(func.count(EventLineage.id))) == 0
        assert session.scalar(
            select(func.count(ClusterEventProjection.id)).where(
                ClusterEventProjection.clustering_run_id == failed_run_id
            )
        ) == 0


def test_independent_cluster_added_beside_split_gets_initial_event() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        source = session.get(Source, first.source_id)
        assert source is not None
        third = _make_item(
            session,
            source,
            external_id="reconcile-three",
            title="Reconcile three",
            content="Third evidence",
            url="https://example.com/reconcile-three",
        )
        with clustering_run(
            session,
            scope_type="reconciliation-initial",
            item_ids=[first.id, second.id],
            rule_version="reconciliation-test-v1",
        ):
            original_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=original_cluster.id,
                    content_item_id=second.id,
                )
            )

        with clustering_run(
            session,
            scope_type="reconciliation-independent-add",
            item_ids=[first.id, second.id, third.id],
            rule_version="reconciliation-test-v1",
        ) as run_id:
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == original_cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            assert second_link is not None
            session.delete(second_link)
            split_child = Cluster(
                cluster_key="split-child-cluster",
                title="Split child cluster",
                first_seen_at=second.published_at,
                last_seen_at=second.published_at,
            )
            added_cluster = Cluster(
                cluster_key="independent-added-cluster",
                title="Independent added cluster",
                first_seen_at=third.published_at,
                last_seen_at=third.published_at,
            )
            session.add_all([split_child, added_cluster])
            session.flush()
            session.add_all(
                [
                    ClusterItem(
                        cluster_id=split_child.id,
                        content_item_id=second.id,
                    ),
                    ClusterItem(
                        cluster_id=added_cluster.id,
                        content_item_id=third.id,
                    ),
                ]
            )

        run_mappings = list(
            session.scalars(
                select(ClusterEventProjection)
                .where(ClusterEventProjection.clustering_run_id == run_id)
                .order_by(ClusterEventProjection.id)
            )
        )
        assert sorted(
            mapping.reconciliation_kind for mapping in run_mappings
        ) == ["initial", "split", "split"]
        initial_mapping = next(
            mapping
            for mapping in run_mappings
            if mapping.reconciliation_kind == "initial"
        )
        assert initial_mapping.cluster_id == added_cluster.id
        assert session.scalar(select(func.count(Event.id))) == 4
        assert session.scalar(select(func.count(EventRevision.id))) == 4


def test_overlapping_without_inclusion_creates_ambiguous_lineage() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        source = session.get(Source, first.source_id)
        assert source is not None
        third = _make_item(
            session,
            source,
            external_id="reconcile-three",
            title="Reconcile three",
            content="Third evidence",
            url="https://example.com/reconcile-three",
        )
        with clustering_run(
            session,
            scope_type="reconciliation-initial",
            item_ids=[first.id, second.id],
            rule_version="reconciliation-test-v1",
        ):
            cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(cluster_id=cluster.id, content_item_id=second.id)
            )

        original_mapping = session.scalar(select(ClusterEventProjection))
        original_event = session.scalar(select(Event))
        original_revision = session.scalar(select(EventRevision))
        assert original_mapping is not None
        assert original_event is not None
        assert original_revision is not None

        with clustering_run(
            session,
            scope_type="reconciliation-partial-replacement",
            item_ids=[first.id, second.id, third.id],
            rule_version="reconciliation-test-v1",
        ) as run_id:
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            assert second_link is not None
            session.delete(second_link)
            session.add(
                ClusterItem(cluster_id=cluster.id, content_item_id=third.id)
            )

        mapping = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == run_id
            )
        )
        assert mapping is not None
        assert mapping.reconciliation_kind == "ambiguous"
        assert mapping.predecessor_projection_id is None
        assert mapping.event_id != original_event.id
        session.refresh(original_event)
        assert original_event.status == "superseded"

        lineage = session.scalar(
            select(EventLineage).where(
                EventLineage.clustering_run_id == run_id
            )
        )
        assert lineage is not None
        assert lineage.relation_type == "ambiguous_from"
        assert lineage.source_event_id == original_event.id
        assert lineage.target_event_id == mapping.event_id
        assert lineage.rule_version == "reconciliation-test-v1"
        assert lineage.before_evidence_fingerprint == (
            original_revision.evidence_fingerprint
        )
        assert lineage.after_evidence_fingerprint == (
            mapping.after_evidence_fingerprint
        )
        assert lineage.decision_reason == (
            "overlap_without_unique_continuation_split_or_merge"
        )
        assert session.scalar(select(func.count(Event.id))) == 2
        assert session.scalar(select(func.count(EventRevision.id))) == 2


def test_many_to_many_overlap_creates_stateless_ambiguous_graph_and_replays() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        source = session.get(Source, first.source_id)
        assert source is not None
        third = _make_item(
            session,
            source,
            external_id="ambiguous-three",
            title="Ambiguous three",
            content="Third ambiguous evidence",
            url="https://example.com/ambiguous-three",
        )
        fourth = _make_item(
            session,
            source,
            external_id="ambiguous-four",
            title="Ambiguous four",
            content="Fourth ambiguous evidence",
            url="https://example.com/ambiguous-four",
        )
        with clustering_run(
            session,
            scope_type="ambiguous-many-initial",
            item_ids=[first.id, second.id, third.id, fourth.id],
            rule_version="ambiguous-test-v1",
        ):
            first_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=first_cluster.id,
                    content_item_id=second.id,
                )
            )
            second_cluster = Cluster(
                cluster_key="ambiguous-second-parent",
                title="Ambiguous second parent",
                first_seen_at=third.published_at,
                last_seen_at=fourth.published_at,
            )
            session.add(second_cluster)
            session.flush()
            session.add_all(
                [
                    ClusterItem(
                        cluster_id=second_cluster.id,
                        content_item_id=third.id,
                    ),
                    ClusterItem(
                        cluster_id=second_cluster.id,
                        content_item_id=fourth.id,
                    ),
                ]
            )

        parent_mappings = list(
            session.scalars(
                select(ClusterEventProjection).order_by(
                    ClusterEventProjection.id
                )
            )
        )
        assert len(parent_mappings) == 2
        parent_event_ids = {mapping.event_id for mapping in parent_mappings}
        session.add_all(
            EventUserState(
                event_id=mapping.event_id,
                seen_revision_id=mapping.event_revision_id,
                read_status="summary_seen",
                read_later=True,
                starred=True,
            )
            for mapping in parent_mappings
        )
        session.commit()

        with clustering_run(
            session,
            scope_type="ambiguous-many-change",
            item_ids=[fourth.id, second.id, third.id, first.id],
            rule_version="ambiguous-test-v1",
        ) as run_id:
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == first_cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            third_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == second_cluster.id,
                    ClusterItem.content_item_id == third.id,
                )
            )
            assert second_link is not None
            assert third_link is not None
            session.delete(second_link)
            session.delete(third_link)
            session.add_all(
                [
                    ClusterItem(
                        cluster_id=second_cluster.id,
                        content_item_id=second.id,
                    ),
                    ClusterItem(
                        cluster_id=first_cluster.id,
                        content_item_id=third.id,
                    ),
                ]
            )

        mappings = list(
            session.scalars(
                select(ClusterEventProjection)
                .where(ClusterEventProjection.clustering_run_id == run_id)
                .order_by(
                    ClusterEventProjection.cluster_anchor,
                    ClusterEventProjection.cluster_occurrence,
                )
            )
        )
        assert len(mappings) == 2
        assert {mapping.reconciliation_kind for mapping in mappings} == {
            "ambiguous"
        }
        assert {mapping.predecessor_projection_id for mapping in mappings} == {
            None
        }
        assert not ({mapping.event_id for mapping in mappings} & parent_event_ids)
        assert set(
            session.scalars(
                select(Event.id).where(Event.status == "superseded")
            )
        ) == parent_event_ids

        lineages = list(
            session.scalars(
                select(EventLineage).where(
                    EventLineage.clustering_run_id == run_id
                )
            )
        )
        assert len(lineages) == 4
        assert {lineage.relation_type for lineage in lineages} == {
            "ambiguous_from"
        }
        assert {
            (lineage.source_event_id, lineage.target_event_id)
            for lineage in lineages
        } == {
            (source_event_id, target_event_id)
            for source_event_id in parent_event_ids
            for target_event_id in {mapping.event_id for mapping in mappings}
        }
        assert set(session.scalars(select(EventUserState.event_id))) == (
            parent_event_ids
        )

        counts = (
            session.scalar(select(func.count(Event.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
            session.scalar(select(func.count(EventLineage.id))),
        )
        replayed = project_completed_clustering_run(
            session,
            run_id,
            [second_cluster.id, first_cluster.id],
        )
        session.commit()
        assert [mapping.id for mapping in replayed] == [
            mapping.id for mapping in mappings
        ]
        assert (
            session.scalar(select(func.count(Event.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
            session.scalar(select(func.count(EventLineage.id))),
        ) == counts


def test_partial_overlap_blocks_otherwise_unique_continuations() -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        source = session.get(Source, first.source_id)
        assert source is not None
        third = _make_item(
            session,
            source,
            external_id="ambiguous-partial-three",
            title="Ambiguous partial three",
            content="Third partial evidence",
            url="https://example.com/ambiguous-partial-three",
        )
        with clustering_run(
            session,
            scope_type="ambiguous-partial-initial",
            item_ids=[first.id, second.id, third.id],
            rule_version="ambiguous-partial-v1",
        ):
            first_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=first_cluster.id,
                    content_item_id=second.id,
                )
            )
            second_cluster = Cluster(
                cluster_key="ambiguous-partial-second",
                title="Ambiguous partial second",
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=third.id,
                )
            )

        parent_event_ids = set(session.scalars(select(Event.id)))
        assert len(parent_event_ids) == 2
        with clustering_run(
            session,
            scope_type="ambiguous-partial-change",
            item_ids=[first.id, second.id, third.id],
            rule_version="ambiguous-partial-v1",
        ) as run_id:
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=second.id,
                )
            )

        mappings = list(
            session.scalars(
                select(ClusterEventProjection).where(
                    ClusterEventProjection.clustering_run_id == run_id
                )
            )
        )
        assert len(mappings) == 2
        assert {mapping.reconciliation_kind for mapping in mappings} == {
            "ambiguous"
        }
        assert not ({mapping.event_id for mapping in mappings} & parent_event_ids)
        lineages = list(
            session.scalars(
                select(EventLineage).where(
                    EventLineage.clustering_run_id == run_id
                )
            )
        )
        assert len(lineages) == 3
        assert {lineage.source_event_id for lineage in lineages} == (
            parent_event_ids
        )
        assert {lineage.target_event_id for lineage in lineages} == {
            mapping.event_id for mapping in mappings
        }


def test_ambiguous_projection_failure_rolls_back_whole_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_database()
    with SessionLocal() as session:
        first, second = _seed_two_items(session)
        source = session.get(Source, first.source_id)
        assert source is not None
        third = _make_item(
            session,
            source,
            external_id="ambiguous-rollback-three",
            title="Ambiguous rollback three",
            content="Third rollback evidence",
            url="https://example.com/ambiguous-rollback-three",
        )
        fourth = _make_item(
            session,
            source,
            external_id="ambiguous-rollback-four",
            title="Ambiguous rollback four",
            content="Fourth rollback evidence",
            url="https://example.com/ambiguous-rollback-four",
        )
        with clustering_run(
            session,
            scope_type="ambiguous-rollback-initial",
            item_ids=[first.id, second.id, third.id, fourth.id],
            rule_version="ambiguous-rollback-v1",
        ):
            first_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=first_cluster.id,
                    content_item_id=second.id,
                )
            )
            second_cluster = Cluster(
                cluster_key="ambiguous-rollback-second",
                title="Ambiguous rollback second",
            )
            session.add(second_cluster)
            session.flush()
            session.add_all(
                [
                    ClusterItem(
                        cluster_id=second_cluster.id,
                        content_item_id=third.id,
                    ),
                    ClusterItem(
                        cluster_id=second_cluster.id,
                        content_item_id=fourth.id,
                    ),
                ]
            )

        parent_event_ids = set(session.scalars(select(Event.id)))
        original_create = event_projection._create_ambiguous_projection
        call_count = 0

        def fail_second_projection(*args: object, **kwargs: object):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("forced ambiguous projection failure")
            return original_create(*args, **kwargs)

        monkeypatch.setattr(
            event_projection,
            "_create_ambiguous_projection",
            fail_second_projection,
        )
        with pytest.raises(
            RuntimeError,
            match="forced ambiguous projection failure",
        ):
            with clustering_run(
                session,
                scope_type="ambiguous-rollback-change",
                item_ids=[first.id, second.id, third.id, fourth.id],
                rule_version="ambiguous-rollback-v1",
            ):
                second_link = session.scalar(
                    select(ClusterItem).where(
                        ClusterItem.cluster_id == first_cluster.id,
                        ClusterItem.content_item_id == second.id,
                    )
                )
                third_link = session.scalar(
                    select(ClusterItem).where(
                        ClusterItem.cluster_id == second_cluster.id,
                        ClusterItem.content_item_id == third.id,
                    )
                )
                assert second_link is not None
                assert third_link is not None
                session.delete(second_link)
                session.delete(third_link)
                session.add_all(
                    [
                        ClusterItem(
                            cluster_id=second_cluster.id,
                            content_item_id=second.id,
                        ),
                        ClusterItem(
                            cluster_id=first_cluster.id,
                            content_item_id=third.id,
                        ),
                    ]
                )

        assert set(session.scalars(select(Event.id))) == parent_event_ids
        assert set(session.scalars(select(Event.status))) == {"active"}
        assert session.scalar(select(func.count(EventRevision.id))) == 2
        assert session.scalar(select(func.count(ClusterEventProjection.id))) == 2
        assert session.scalar(select(func.count(EventLineage.id))) == 0
        failed_run = session.scalar(
            select(ClusteringRun).where(
                ClusteringRun.scope_type == "ambiguous-rollback-change"
            )
        )
        assert failed_run is not None
        assert failed_run.status == "failed"
