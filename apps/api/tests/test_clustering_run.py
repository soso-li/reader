from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
from threading import Event

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

import reader_api.cluster as cluster_module
import reader_api.clustering_run as clustering_run_module
from reader_api.cluster import (
    EmbeddingRepairCandidate,
    assign_cluster,
    cluster_source_items,
    decluster_source_items,
    move_item_to_cluster,
    repair_embedding_clusters,
    repair_embedding_clusters_indexed,
    repair_embedding_clusters_python,
)
from reader_api.clustering_run import (
    ClusteringRule,
    clustering_run,
    evidence_anchor_for_item,
)
from reader_api.db import Base, engine
from reader_api.digest import content_hash
from reader_api.models import (
    Cluster,
    ClusteringRun,
    ClusteringRunMembership,
    ClusteringRunSnapshotSeal,
    ClusteringRunScopeEvidence,
    ClusterItem,
    ContentItem,
    Document,
    Source,
    UserState,
)
from tests.factories import make_raw_entry


SessionFactory = sessionmaker(bind=engine, expire_on_commit=False)


def test_rule_version_distinguishes_strategy_and_runtime_models() -> None:
    base = ClusteringRule(
        "embedding-assign-v1",
        embedding_model="embedding-a",
        translation_model="translation-a",
    ).version

    assert base != ClusteringRule(
        "embedding-assign-v1",
        embedding_model="embedding-b",
        translation_model="translation-a",
    ).version
    assert base != ClusteringRule("repair-embedding-v1").version
    long_version = ClusteringRule(
        "embedding-assign-v1",
        embedding_model="model/" + "x" * 200,
        translation_model="translation/" + "y" * 200,
    ).version
    assert len(long_version) <= 120
    assert long_version == ClusteringRule(
        "embedding-assign-v1",
        embedding_model="model/" + "x" * 200,
        translation_model="translation/" + "y" * 200,
    ).version
    assert len(ClusteringRule("r" * 500).version) <= 120


def seed_items(session: Session) -> tuple[list[ContentItem], list[Cluster]]:
    sources = [
        Source(name=f"Run source {index}", url=f"https://example.com/run-{index}.xml")
        for index in range(3)
    ]
    session.add_all(sources)
    session.flush()

    items: list[ContentItem] = []
    for index, source in enumerate(sources):
        raw = make_raw_entry(
            source=source,
            external_id=f"run-entry-{index}",
            title=f"Run item {index}",
            url=f"https://example.com/run-item-{index}",
            published_at=datetime(2026, 7, 13, index, tzinfo=timezone.utc),
            raw_content=f"Body {index}",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
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
            content_hash=content_hash(raw.title, raw.raw_content, raw.url),
            published_at=raw.published_at,
        )
        session.add(item)
        session.flush()
        items.append(item)

    clusters = [
        Cluster(cluster_key="run-cluster-a", title="Run cluster A"),
        Cluster(cluster_key="run-cluster-b", title="Run cluster B"),
    ]
    session.add_all(clusters)
    session.flush()
    session.add_all(
        [
            ClusterItem(cluster_id=clusters[0].id, content_item_id=items[0].id),
            ClusterItem(cluster_id=clusters[1].id, content_item_id=items[1].id),
            ClusterItem(cluster_id=clusters[1].id, content_item_id=items[2].id),
        ]
    )
    session.commit()
    return items, clusters


def duplicate_item(session: Session, item: ContentItem) -> ContentItem:
    duplicate = ContentItem(
        document_id=item.document_id,
        source_id=item.source_id,
        title=item.title,
        content_text=item.content_text,
        url=item.url,
        canonical_url=item.canonical_url,
        content_hash=item.content_hash,
        published_at=item.published_at,
    )
    session.add(duplicate)
    session.flush()
    return duplicate


def snapshot_evidence_rows(
    session: Session, run_id: str, phase: str
) -> list[str]:
    return list(
        session.scalars(
            select(ClusteringRunMembership.evidence_anchor)
            .where(
                ClusteringRunMembership.run_id == run_id,
                ClusteringRunMembership.snapshot_phase == phase,
            )
            .order_by(
                ClusteringRunMembership.cluster_anchor,
                ClusteringRunMembership.cluster_occurrence,
                ClusteringRunMembership.evidence_anchor,
                ClusteringRunMembership.evidence_occurrence,
            )
        )
    )


def test_before_snapshot_captures_existing_target_for_unclustered_item() -> None:
    with SessionFactory() as session:
        items, _clusters = seed_items(session)
        duplicate = duplicate_item(session, items[0])
        duplicate.embedding_vector = "[1.0,0.0]"
        duplicate.embedding_model = "embedding-model"
        session.commit()

        with clustering_run(
            session,
            scope_type="unclustered-existing-target",
            item_ids=[duplicate.id],
            rule_version="test-rule-v1",
        ) as run_id:
            assign_cluster(session, duplicate)

        anchor = evidence_anchor_for_item(session, items[0])
        assert snapshot_evidence_rows(session, run_id, "before") == [anchor]
        assert snapshot_evidence_rows(session, run_id, "after") == [anchor, anchor]


def test_before_snapshot_captures_out_of_scope_repair_target() -> None:
    with SessionFactory() as session:
        items, clusters = seed_items(session)
        with clustering_run(
            session,
            scope_type="repair-out-of-scope-target",
            item_ids=[items[0].id],
            rule_version="test-rule-v1",
        ) as run_id:
            move_item_to_cluster(session, items[0], clusters[1], 0.99)

        expected_before = sorted(
            evidence_anchor_for_item(session, item) for item in items
        )
        assert sorted(snapshot_evidence_rows(session, run_id, "before")) == (
            expected_before
        )
        assert sorted(snapshot_evidence_rows(session, run_id, "after")) == (
            expected_before
        )


def move_item(session: Session, item: ContentItem, target: Cluster) -> None:
    session.execute(delete(ClusterItem).where(ClusterItem.content_item_id == item.id))
    session.add(ClusterItem(cluster_id=target.id, content_item_id=item.id))
    session.flush()


def test_source_cluster_refreshes_status_after_waiting_for_execution_lock() -> None:
    with SessionFactory() as session:
        items, _clusters = seed_items(session)
        item = items[0]
        source = session.get(Source, item.source_id)
        assert source is not None and source.status == "active"
        session.execute(
            delete(ClusterItem).where(ClusterItem.content_item_id == item.id)
        )
        session.execute(
            update(Source)
            .where(Source.id == source.id)
            .values(status="muted")
            .execution_options(synchronize_session=False)
        )
        session.commit()
        assert source.status == "active"

        cluster_source_items(session, source.id)
        session.commit()

        assert session.scalar(
            select(ClusterItem.id).where(ClusterItem.content_item_id == item.id)
        ) is None


def test_source_cluster_rule_version_records_existing_embedding_model() -> None:
    with SessionFactory() as session:
        items, _clusters = seed_items(session)
        item = items[0]
        session.execute(
            delete(ClusterItem).where(ClusterItem.content_item_id == item.id)
        )
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "existing-model-a"
        session.commit()

        cluster_source_items(session, item.source_id)
        session.commit()

        session.execute(
            delete(ClusterItem).where(ClusterItem.content_item_id == item.id)
        )
        item.embedding_model = "existing-model-b"
        session.commit()

        cluster_source_items(session, item.source_id)
        session.commit()

        versions = [
            json.loads(version)
            for version in session.scalars(
                select(ClusteringRun.rule_version)
                .where(ClusteringRun.scope_type == "source-cluster")
                .order_by(ClusteringRun.started_at, ClusteringRun.id)
            )
        ]
        assert [version["embedding_model"] for version in versions] == [
            "existing-model-a",
            "existing-model-b",
        ]
        assert versions[0] != versions[1]

        session.execute(
            delete(ClusterItem).where(ClusterItem.content_item_id == item.id)
        )
        item.embedding_model = "   "
        session.commit()

        cluster_source_items(session, item.source_id)
        session.commit()

        assert session.scalar(
            select(ClusterItem.id).where(ClusterItem.content_item_id == item.id)
        ) is None
        assert len(
            session.scalars(
                select(ClusteringRun).where(
                    ClusteringRun.scope_type == "source-cluster"
                )
            ).all()
        ) == 2


def test_source_decluster_refreshes_status_after_waiting_for_execution_lock() -> None:
    with SessionFactory() as session:
        items, _clusters = seed_items(session)
        item = items[0]
        source = session.get(Source, item.source_id)
        assert source is not None
        source.status = "muted"
        session.commit()
        session.execute(
            update(Source)
            .where(Source.id == source.id)
            .values(status="active")
            .execution_options(synchronize_session=False)
        )
        session.commit()
        assert source.status == "muted"

        decluster_source_items(session, source.id)
        session.commit()

        assert session.scalar(
            select(ClusterItem.id).where(ClusterItem.content_item_id == item.id)
        ) is not None


@pytest.mark.parametrize("operation", ["cluster", "decluster"])
def test_source_noop_holds_execution_lock_until_caller_transaction_ends(
    operation: str,
) -> None:
    with SessionFactory() as session:
        items, _clusters = seed_items(session)
        item_id = items[0].id
        source = session.get(Source, items[0].source_id)
        assert source is not None
        if operation == "decluster":
            source.status = "muted"
            session.execute(
                delete(ClusterItem).where(ClusterItem.content_item_id == item_id)
            )
            session.commit()
        source_id = source.id

    first_returned = Event()
    release_first = Event()
    second_entered = Event()

    def first_request() -> None:
        with SessionFactory() as session:
            if operation == "cluster":
                cluster_source_items(session, source_id)
            else:
                decluster_source_items(session, source_id)
            first_returned.set()
            assert release_first.wait(timeout=5)
            session.commit()

    def second_run() -> None:
        assert first_returned.wait(timeout=5)
        with SessionFactory() as session:
            with clustering_run(
                session,
                scope_type="source-noop-lock-test",
                item_ids=[item_id],
                rule_version="test-rule-v1",
            ):
                second_entered.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(first_request)
        second_future = executor.submit(second_run)
        try:
            assert first_returned.wait(timeout=5)
            assert second_entered.wait(timeout=0.2) is False
        finally:
            release_first.set()
        first_future.result(timeout=5)
        second_future.result(timeout=5)


def memberships(
    session: Session, run_id: str, phase: str
) -> set[tuple[str, str]]:
    return set(
        session.execute(
            select(
                ClusteringRunMembership.cluster_anchor,
                ClusteringRunMembership.evidence_anchor,
            ).where(
                ClusteringRunMembership.run_id == run_id,
                ClusteringRunMembership.snapshot_phase == phase,
            )
        ).all()
    )


def membership_rows(
    session: Session, run_id: str, phase: str
) -> list[tuple[str, int, str, int]]:
    return list(
        session.execute(
            select(
                ClusteringRunMembership.cluster_anchor,
                ClusteringRunMembership.cluster_occurrence,
                ClusteringRunMembership.evidence_anchor,
                ClusteringRunMembership.evidence_occurrence,
            )
            .where(
                ClusteringRunMembership.run_id == run_id,
                ClusteringRunMembership.snapshot_phase == phase,
            )
            .order_by(
                ClusteringRunMembership.cluster_anchor,
                ClusteringRunMembership.cluster_occurrence,
                ClusteringRunMembership.evidence_anchor,
                ClusteringRunMembership.evidence_occurrence,
            )
        ).all()
    )


def setup_function() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def test_completed_run_records_stable_complete_before_and_after_snapshots() -> None:
    with SessionFactory() as session:
        items, clusters = seed_items(session)
        expected_scope = {
            evidence_anchor_for_item(session, items[0]),
            evidence_anchor_for_item(session, items[1]),
        }

        with clustering_run(
            session,
            scope_type="test-items",
            item_ids=[items[1].id, items[0].id],
            rule_version="test-rule-v1",
        ) as run_id:
            move_item(session, items[1], clusters[0])
            move_item(session, items[2], clusters[0])

        run = session.get(ClusteringRun, run_id)
        assert run is not None
        assert run.status == "completed"
        assert run.after_snapshot_finalized is True
        assert run.started_at is not None
        assert run.completed_at is not None
        assert run.failed_at is None
        assert run.failure_info == ""
        seal = session.scalar(
            select(ClusteringRunSnapshotSeal).where(
                ClusteringRunSnapshotSeal.run_id == run_id,
                ClusteringRunSnapshotSeal.snapshot_phase == "after",
            )
        )
        assert seal is not None
        assert seal.snapshot_row_count == len(
            membership_rows(session, run_id, "after")
        )
        assert len(seal.snapshot_fingerprint) == 64
        assert list(
            session.scalars(
                select(ClusteringRunSnapshotSeal.snapshot_phase)
                .where(ClusteringRunSnapshotSeal.run_id == run_id)
                .order_by(ClusteringRunSnapshotSeal.snapshot_phase)
            )
        ) == ["after", "before"]
        assert set(
            session.scalars(
                select(ClusteringRunScopeEvidence.evidence_anchor).where(
                    ClusteringRunScopeEvidence.run_id == run_id
                )
            )
        ) == expected_scope
        assert {anchor for _cluster, anchor in memberships(session, run_id, "before")} == {
            evidence_anchor_for_item(session, item) for item in items
        }
        assert {anchor for _cluster, anchor in memberships(session, run_id, "after")} == {
            evidence_anchor_for_item(session, item) for item in items
        }
        before_groups = {
            cluster for cluster, _anchor in memberships(session, run_id, "before")
        }
        after_groups = {
            cluster for cluster, _anchor in memberships(session, run_id, "after")
        }
        assert len(before_groups) == 2
        assert len(after_groups) == 1


def test_snapshot_identity_is_independent_of_scope_order_and_run_time() -> None:
    with SessionFactory() as session:
        items, _clusters = seed_items(session)
        run_ids: list[str] = []
        for item_ids in ([items[0].id, items[1].id], [items[1].id, items[0].id]):
            with clustering_run(
                session,
                scope_type="test-items",
                item_ids=item_ids,
                rule_version="test-rule-v1",
            ) as run_id:
                pass
            run_ids.append(run_id)

        first = session.get(ClusteringRun, run_ids[0])
        second = session.get(ClusteringRun, run_ids[1])
        assert first is not None and second is not None
        assert first.id != second.id
        assert first.scope_key == second.scope_key
        assert memberships(session, first.id, "before") == memberships(
            session, second.id, "before"
        )
        assert memberships(session, first.id, "after") == memberships(
            session, second.id, "after"
        )


def test_failed_run_rolls_back_uncommitted_changes_and_has_no_after_snapshot() -> None:
    with SessionFactory() as session:
        items, clusters = seed_items(session)
        session.add(
            UserState(
                object_type="item",
                object_id=items[1].id,
                read_status="summary_seen",
                starred=True,
            )
        )
        session.commit()
        run_id = ""

        with pytest.raises(RuntimeError, match="boom"):
            with clustering_run(
                session,
                scope_type="test-items",
                item_ids=[items[1].id],
                rule_version="test-rule-v1",
            ) as active_run_id:
                run_id = active_run_id
                move_item(session, items[1], clusters[0])
                raise RuntimeError("boom")

        run = session.get(ClusteringRun, run_id)
        assert run is not None
        assert run.status == "failed"
        assert run.completed_at is None
        assert run.failed_at is not None
        assert run.failure_info == "boom"
        assert memberships(session, run_id, "before")
        assert memberships(session, run_id, "after") == set()
        assert list(
            session.scalars(
                select(ClusteringRunSnapshotSeal.snapshot_phase).where(
                    ClusteringRunSnapshotSeal.run_id == run_id
                )
            )
        ) == ["before"]
        assert session.scalar(
            select(ClusterItem.cluster_id).where(
                ClusterItem.content_item_id == items[1].id
            )
        ) == clusters[1].id
        state = session.scalar(
            select(UserState).where(
                UserState.object_type == "item",
                UserState.object_id == items[1].id,
            )
        )
        assert state is not None and state.starred is True


def test_default_clustering_run_failure_still_persists_a_failed_audit_run() -> None:
    with SessionFactory() as session:
        items, _clusters = seed_items(session)
        run_id = ""

        with pytest.raises(RuntimeError, match="default failure"):
            with clustering_run(
                session,
                scope_type="default-failure-contract",
                item_ids=[items[0].id],
                rule_version="test-rule-v1",
            ) as active_run_id:
                run_id = active_run_id
                raise RuntimeError("default failure")

        run = session.get(ClusteringRun, run_id)
        assert run is not None
        assert run.status == "failed"
        assert run.failure_info == "default failure"


def test_atomic_failure_after_deferred_success_releases_execution_lock() -> None:
    with SessionFactory() as session:
        items, _clusters = seed_items(session)
        session.commit()

        with clustering_run(
            session,
            scope_type="atomic-success-contract",
            item_ids=[items[0].id],
            rule_version="test-rule-v1",
            commit_on_success=False,
            rollback_on_failure=True,
        ) as completed_run_id:
            assert completed_run_id
        assert session.info.get(clustering_run_module.DEFER_LOCK_RELEASE_SESSION_KEY)
        assert clustering_run_module.EXECUTION_LOCK_SESSION_KEY in session.info

        failed_run_id = ""
        with pytest.raises(RuntimeError, match="atomic failure"):
            with clustering_run(
                session,
                scope_type="atomic-failure-contract",
                item_ids=[items[0].id],
                rule_version="test-rule-v1",
                commit_on_success=False,
                rollback_on_failure=True,
            ) as active_run_id:
                failed_run_id = active_run_id
                raise RuntimeError("atomic failure")

        assert session.get(ClusteringRun, completed_run_id) is None
        assert session.get(ClusteringRun, failed_run_id) is None
        assert clustering_run_module.EXECUTION_LOCK_SESSION_KEY not in session.info
        assert clustering_run_module.DEFER_LOCK_RELEASE_SESSION_KEY not in session.info

        # SQLite uses an RLock, so assert the stored lock is gone before retrying.
        with clustering_run(
            session,
            scope_type="atomic-failure-retry",
            item_ids=[items[0].id],
            rule_version="test-rule-v1",
        ) as retry_run_id:
            assert retry_run_id
        retry = session.get(ClusteringRun, retry_run_id)
        assert retry is not None and retry.status == "completed"


def test_completion_failure_marks_partially_committed_run_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with SessionFactory() as session:
        items, _clusters = seed_items(session)
        run_id = ""
        original_snapshot = clustering_run_module.snapshot_memberships
        snapshot_calls = 0

        def fail_after_snapshot(*args, **kwargs):
            nonlocal snapshot_calls
            snapshot_calls += 1
            if snapshot_calls == 1:
                raise RuntimeError("after snapshot failed")
            return original_snapshot(*args, **kwargs)

        monkeypatch.setattr(
            clustering_run_module,
            "snapshot_memberships",
            fail_after_snapshot,
        )
        with pytest.raises(RuntimeError, match="after snapshot failed"):
            with clustering_run(
                session,
                scope_type="completion-failure",
                item_ids=[items[0].id],
                rule_version="test-rule-v1",
            ) as active_run_id:
                run_id = active_run_id
                session.commit()

    with SessionFactory() as session:
        run = session.get(ClusteringRun, run_id)
        assert run is not None and run.status == "failed"
        assert run.completed_at is None
        assert run.failed_at is not None
        assert run.failure_info == "after snapshot failed"
        assert memberships(session, run_id, "before")
        assert memberships(session, run_id, "after") == set()


def test_embedding_repair_deadlock_fails_partial_run_without_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with SessionFactory() as session:
        items, clusters = seed_items(session)
        for item in items[:2]:
            item.embedding_vector = "[1.0,0.0]"
            item.embedding_model = "test-model"
            item.published_at = datetime.now(timezone.utc)
        session.commit()

        targets = {
            items[1].id: clusters[0],
            items[0].id: clusters[1],
        }
        original_move = cluster_module.move_item_to_cluster
        move_calls = 0

        def target_cluster(_session, item, *_args, **_kwargs):
            return targets[item.id], 0.99

        class DeadlockDetected(Exception):
            pgcode = "40P01"

        def fail_second_move(session, item, cluster, score):
            nonlocal move_calls
            move_calls += 1
            if move_calls == 2:
                raise OperationalError(
                    "UPDATE cluster_items",
                    {},
                    DeadlockDetected("deadlock detected"),
                )
            return original_move(session, item, cluster, score)

        monkeypatch.setattr(cluster_module, "is_postgres", lambda _session: True)
        monkeypatch.setattr(cluster_module, "embedding_cluster", target_cluster)
        monkeypatch.setattr(cluster_module, "move_item_to_cluster", fail_second_move)

        assert repair_embedding_clusters(session, "test-model") == 1
        run = session.scalars(select(ClusteringRun)).one()
        assert run.status == "failed"
        assert memberships(session, run.id, "before")
        assert memberships(session, run.id, "after") == set()


def test_embedding_repair_freezes_only_existing_cluster_members(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with SessionFactory() as session:
        items, _clusters = seed_items(session)
        clustered = items[0]
        clustered.embedding_vector = "[1.0,0.0]"
        clustered.embedding_model = "test-model"
        now = datetime.now(timezone.utc)
        clustered.published_at = now - timedelta(days=1)

        raw = make_raw_entry(
            source=clustered.source,
            external_id="newer-unclustered",
            title="Newer unclustered",
            url="https://example.com/newer-unclustered",
            published_at=now,
            raw_content="Newer unclustered body",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        unclustered = ContentItem(
            document_id=document.id,
            source_id=clustered.source_id,
            title=raw.title,
            content_text=raw.raw_content,
            url=raw.url,
            canonical_url=raw.url,
            content_hash=content_hash(raw.title, raw.raw_content, raw.url),
            published_at=raw.published_at,
            embedding_vector="[1.0,0.0]",
            embedding_model="test-model",
        )
        session.add(unclustered)
        session.commit()

        captured_candidates = []

        def capture_scope(_session, _model, candidates, *_args):
            captured_candidates.extend(candidates)
            return 0

        monkeypatch.setattr(cluster_module, "_repair_embedding_clusters", capture_scope)

        assert repair_embedding_clusters(session, "test-model", limit=1) == 0
        assert [row.item_id for row in captured_candidates] == [clustered.id]
        assert unclustered.id not in {row.item_id for row in captured_candidates}


def test_embedding_repair_limit_freezes_exact_legacy_membership_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with SessionFactory() as session:
        items, clusters = seed_items(session)
        item = items[0]
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "test-model"
        item.published_at = datetime.now(timezone.utc)
        session.add(
            ClusterItem(cluster_id=clusters[1].id, content_item_id=item.id)
        )
        session.commit()

        captured_candidates = []

        def capture_scope(_session, _model, candidates, *_args):
            captured_candidates.extend(candidates)
            return 0

        monkeypatch.setattr(cluster_module, "_repair_embedding_clusters", capture_scope)

        assert repair_embedding_clusters(session, "test-model", limit=1) == 0
        assert [
            (row.item_id, row.cluster_id) for row in captured_candidates
        ] == [(item.id, clusters[0].id)]


def test_embedding_repair_indexed_executes_frozen_row_after_window_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with SessionFactory() as session:
        items, clusters = seed_items(session)
        item = items[0]
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "test-model"
        item.published_at = datetime(2026, 5, 1, tzinfo=timezone.utc)
        session.add(
            ClusterItem(cluster_id=clusters[1].id, content_item_id=item.id)
        )
        session.commit()
        visited: list[int] = []

        def keep_current(_session, candidate, *_args, **_kwargs):
            visited.append(candidate.id)
            return clusters[0], 1.0

        monkeypatch.setattr(cluster_module, "embedding_cluster", keep_current)

        assert (
            repair_embedding_clusters_indexed(
                session,
                "test-model",
                [EmbeddingRepairCandidate(item.id, clusters[0].id)],
                None,
                None,
                "",
            )
            == 0
        )
        assert visited == [item.id]


def test_embedding_repair_python_executes_frozen_row_after_window_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with SessionFactory() as session:
        items, clusters = seed_items(session)
        item = items[0]
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "test-model"
        item.published_at = datetime(2026, 5, 1, tzinfo=timezone.utc)
        session.commit()
        parsed_vectors: list[str] = []
        original_parse_vector = cluster_module.parse_vector

        def capture_vector(value: str) -> list[float]:
            parsed_vectors.append(value)
            return original_parse_vector(value)

        monkeypatch.setattr(cluster_module, "parse_vector", capture_vector)

        assert (
            repair_embedding_clusters_python(
                session,
                "test-model",
                [EmbeddingRepairCandidate(item.id, clusters[0].id)],
                None,
                None,
                "",
            )
            == 0
        )
        assert parsed_vectors == ["[1.0,0.0]"]


def test_partial_commit_is_failed_without_after_and_retry_gets_new_run() -> None:
    with SessionFactory() as session:
        items, clusters = seed_items(session)
        failed_run_id = ""

        with pytest.raises(RuntimeError, match="batch failed"):
            with clustering_run(
                session,
                scope_type="test-items",
                item_ids=[items[1].id, items[2].id],
                rule_version="test-rule-v1",
            ) as active_run_id:
                failed_run_id = active_run_id
                move_item(session, items[1], clusters[0])
                session.commit()
                raise RuntimeError("batch failed")

        failed = session.get(ClusteringRun, failed_run_id)
        assert failed is not None and failed.status == "failed"
        assert memberships(session, failed_run_id, "after") == set()
        assert session.scalar(
            select(ClusterItem.cluster_id).where(
                ClusterItem.content_item_id == items[1].id
            )
        ) == clusters[0].id

        with clustering_run(
            session,
            scope_type="test-items",
            item_ids=[items[1].id, items[2].id],
            rule_version="test-rule-v1",
        ) as retry_run_id:
            move_item(session, items[2], clusters[0])

        retry = session.get(ClusteringRun, retry_run_id)
        assert retry is not None and retry.status == "completed"
        assert retry.id != failed_run_id
        assert memberships(session, retry_run_id, "after")


def test_source_scope_captures_evidence_created_during_the_run() -> None:
    with SessionFactory() as session:
        source = Source(
            name="Dynamic source", url="https://example.com/dynamic.xml"
        )
        session.add(source)
        session.flush()

        with clustering_run(
            session,
            scope_type="rss-source-ingest",
            source_id=source.id,
            rule_version="test-rule-v1",
        ) as run_id:
            raw = make_raw_entry(
                source=source,
                external_id="dynamic-entry",
                title="Dynamic item",
                raw_content="Dynamic body",
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
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
                content_hash=content_hash(raw.title, raw.raw_content),
            )
            cluster = Cluster(cluster_key="dynamic-cluster", title=raw.title)
            session.add_all([item, cluster])
            session.flush()
            session.add(
                ClusterItem(cluster_id=cluster.id, content_item_id=item.id)
            )

        scope_anchors = set(
            session.scalars(
                select(ClusteringRunScopeEvidence.evidence_anchor).where(
                    ClusteringRunScopeEvidence.run_id == run_id
                )
            )
        )
        assert scope_anchors == {evidence_anchor_for_item(session, item)}
        assert memberships(session, run_id, "before") == set()
        assert memberships(session, run_id, "after")


def test_nested_clustering_boundaries_share_one_run() -> None:
    with SessionFactory() as session:
        items, _clusters = seed_items(session)
        with clustering_run(
            session,
            scope_type="outer",
            item_ids=[items[0].id],
            rule_version="test-rule-v1",
        ) as outer_run_id:
            with clustering_run(
                session,
                scope_type="inner",
                item_ids=[items[0].id],
                rule_version="test-rule-v2",
            ) as inner_run_id:
                assert inner_run_id == outer_run_id

        runs = session.scalars(select(ClusteringRun)).all()
        assert [run.id for run in runs] == [outer_run_id]
        assert runs[0].scope_type == "outer"
        assert runs[0].rule_version == "test-rule-v1"


def test_caller_owned_run_does_not_commit_the_surrounding_transaction() -> None:
    with SessionFactory() as session:
        items, clusters = seed_items(session)
        items[0].title = "尚未提交的标题"

        with clustering_run(
            session,
            scope_type="caller-owned",
            item_ids=[items[0].id],
            rule_version="test-rule-v1",
            commit_on_success=False,
        ):
            move_item(session, items[0], clusters[1])

        assert session.in_transaction() is True
        session.rollback()
        assert session.get(ContentItem, items[0].id).title == "Run item 0"


def test_caller_owned_run_holds_execution_lock_until_transaction_end() -> None:
    with SessionFactory() as session:
        items, clusters = seed_items(session)
        item_id = items[0].id
        target_cluster_id = clusters[1].id

    first_context_exited = Event()
    release_first = Event()
    second_entered = Event()

    def first_run() -> None:
        with SessionFactory() as session:
            item = session.get(ContentItem, item_id)
            target = session.get(Cluster, target_cluster_id)
            assert item is not None and target is not None
            with clustering_run(
                session,
                scope_type="caller-owned-lock",
                item_ids=[item_id],
                rule_version="test-rule-v1",
                commit_on_success=False,
            ):
                move_item(session, item, target)
            first_context_exited.set()
            assert release_first.wait(timeout=5)
            session.commit()

    def second_run() -> None:
        assert first_context_exited.wait(timeout=5)
        with SessionFactory() as session:
            with clustering_run(
                session,
                scope_type="caller-owned-lock",
                item_ids=[item_id],
                rule_version="test-rule-v1",
            ):
                second_entered.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(first_run)
        second_future = executor.submit(second_run)
        try:
            assert first_context_exited.wait(timeout=5)
            assert second_entered.wait(timeout=0.2) is False
        finally:
            release_first.set()
        first_future.result(timeout=5)
        second_future.result(timeout=5)


def test_snapshot_can_represent_one_evidence_member_in_multiple_clusters() -> None:
    with SessionFactory() as session:
        items, clusters = seed_items(session)
        session.add(
            ClusterItem(
                cluster_id=clusters[1].id,
                content_item_id=items[0].id,
            )
        )
        session.commit()

        with clustering_run(
            session,
            scope_type="legacy-multi-membership",
            item_ids=[items[0].id],
            rule_version="test-rule-v1",
        ) as run_id:
            pass

        anchor = evidence_anchor_for_item(session, items[0])
        before_rows = {
            (cluster_anchor, evidence_anchor)
            for cluster_anchor, evidence_anchor in memberships(
                session, run_id, "before"
            )
            if evidence_anchor == anchor
        }
        assert len(before_rows) == 2


def test_snapshot_preserves_duplicate_evidence_and_identical_clusters() -> None:
    with SessionFactory() as session:
        items, clusters = seed_items(session)
        duplicate = duplicate_item(session, items[0])
        identical_cluster = Cluster(
            cluster_key="run-cluster-identical",
            title="Run cluster identical",
        )
        session.add(identical_cluster)
        session.flush()
        session.add_all(
            [
                ClusterItem(
                    cluster_id=clusters[0].id,
                    content_item_id=duplicate.id,
                ),
                ClusterItem(
                    cluster_id=identical_cluster.id,
                    content_item_id=items[0].id,
                ),
                ClusterItem(
                    cluster_id=identical_cluster.id,
                    content_item_id=duplicate.id,
                ),
            ]
        )
        session.commit()

        with clustering_run(
            session,
            scope_type="duplicate-membership",
            item_ids=[items[0].id, duplicate.id],
            rule_version="test-rule-v1",
        ) as run_id:
            pass

        anchor = evidence_anchor_for_item(session, items[0])
        assert evidence_anchor_for_item(session, duplicate) == anchor
        assert list(
            session.scalars(
                select(ClusteringRunScopeEvidence.evidence_anchor)
                .where(ClusteringRunScopeEvidence.run_id == run_id)
                .order_by(ClusteringRunScopeEvidence.evidence_occurrence)
            )
        ) == [anchor, anchor]
        assert list(
            session.scalars(
                select(ClusteringRunScopeEvidence.evidence_occurrence)
                .where(ClusteringRunScopeEvidence.run_id == run_id)
                .order_by(ClusteringRunScopeEvidence.evidence_occurrence)
            )
        ) == [1, 2]
        duplicate_rows = [
            row for row in membership_rows(session, run_id, "before")
            if row[2] == anchor
        ]
        assert len(duplicate_rows) == 4
        assert {(row[1], row[3]) for row in duplicate_rows} == {
            (1, 1),
            (1, 2),
            (2, 1),
            (2, 2),
        }

        with clustering_run(
            session,
            scope_type="duplicate-membership",
            item_ids=[duplicate.id, items[0].id],
            rule_version="test-rule-v1",
        ) as retry_run_id:
            pass
        assert membership_rows(session, run_id, "before") == membership_rows(
            session, retry_run_id, "before"
        )


def test_snapshot_distinguishes_fragments_from_the_same_source_entry() -> None:
    with SessionFactory() as session:
        source = Source(
            name="Digest fragment source",
            url="https://example.com/digest-fragments.xml",
        )
        raw = make_raw_entry(
            source=source,
            external_id="digest-fragments",
            title="Digest fragments",
            raw_content="Two frozen fragments",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            document_type="digest",
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        items = [
            ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=f"Digest fragment {index}",
                content_text=f"Frozen fragment {index}",
                content_hash=str(index) * 64,
            )
            for index in (1, 2)
        ]
        session.add_all(items)
        session.flush()
        cluster = Cluster(
            cluster_key="digest-fragment-cluster",
            title="Digest fragment cluster",
        )
        session.add(cluster)
        session.flush()
        session.add_all(
            ClusterItem(cluster_id=cluster.id, content_item_id=item.id)
            for item in items
        )
        session.commit()

        with clustering_run(
            session,
            scope_type="digest-fragment-membership",
            item_ids=[item.id for item in items],
            rule_version="test-rule-v1",
        ) as run_id:
            pass

        anchors = list(
            session.scalars(
                select(ClusteringRunScopeEvidence.evidence_anchor)
                .where(ClusteringRunScopeEvidence.run_id == run_id)
                .order_by(ClusteringRunScopeEvidence.evidence_anchor)
            )
        )
        assert len(anchors) == 2
        assert anchors[0] != anchors[1]


def test_failed_unclustered_run_preserves_duplicate_scope_without_memberships() -> None:
    with SessionFactory() as session:
        items, _clusters = seed_items(session)
        duplicate = duplicate_item(session, items[0])
        session.execute(
            delete(ClusterItem).where(
                ClusterItem.content_item_id.in_([items[0].id, duplicate.id])
            )
        )
        session.commit()

        run_id = ""
        with pytest.raises(RuntimeError, match="scope failed"):
            with clustering_run(
                session,
                scope_type="duplicate-unclustered",
                item_ids=[items[0].id, duplicate.id],
                rule_version="test-rule-v1",
            ) as active_run_id:
                run_id = active_run_id
                raise RuntimeError("scope failed")

        anchor = evidence_anchor_for_item(session, items[0])
        assert list(
            session.scalars(
                select(ClusteringRunScopeEvidence.evidence_anchor)
                .where(ClusteringRunScopeEvidence.run_id == run_id)
                .order_by(ClusteringRunScopeEvidence.evidence_occurrence)
            )
        ) == [anchor, anchor]
        assert list(
            session.scalars(
                select(ClusteringRunScopeEvidence.evidence_occurrence)
                .where(ClusteringRunScopeEvidence.run_id == run_id)
                .order_by(ClusteringRunScopeEvidence.evidence_occurrence)
            )
        ) == [1, 2]
        assert memberships(session, run_id, "before") == set()
        assert memberships(session, run_id, "after") == set()
