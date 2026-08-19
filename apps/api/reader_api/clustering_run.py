from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from threading import RLock
from typing import Literal

from sqlalchemy import event, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

from .event_projection import latest_live_cluster_projection
from .models import (
    ClusterEventProjection,
    ClusterItem,
    ClusteringRun,
    ClusteringRunMembership,
    ClusteringRunProjectionPredecessor,
    ClusteringRunSnapshotSeal,
    ClusteringRunScopeEvidence,
    ContentItem,
)


ACTIVE_RUN_SESSION_KEY = "reader_clustering_run_id"
ACTIVE_RUN_START_SESSION_KEY = "reader_clustering_run_start"
INCOMPLETE_RUN_SESSION_KEY = "reader_clustering_run_incomplete"
EXECUTION_LOCK_SESSION_KEY = "reader_clustering_run_execution_lock"
DEFER_LOCK_RELEASE_SESSION_KEY = "reader_clustering_run_defer_lock_release"
CLUSTERING_RUN_LOCK_KEY = int.from_bytes(
    hashlib.sha256(b"reader-clustering-run-v1").digest()[:8],
    signed=True,
)
_LOCAL_CLUSTERING_RUN_LOCK = RLock()
SnapshotPhase = Literal["before", "after"]


@dataclass(frozen=True)
class ClusteringRule:
    name: str
    embedding_model: str = ""
    translation_model: str = ""

    @property
    def version(self) -> str:
        payload = json.dumps(
            {
                key: value
                for key, value in (
                    ("rule", self.name.strip()),
                    ("embedding_model", self.embedding_model.strip()),
                    ("translation_model", self.translation_model.strip()),
                )
                if value
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if not self.name.strip():
            raise ValueError("Clustering Rule 名称不能为空")
        if len(payload) <= 120:
            return payload
        digest = sha256_text(payload)
        suffix = f"config-sha256={digest}"
        prefix_limit = 120 - len(suffix) - 1
        prefix = self.name.strip()[:prefix_limit]
        return f"{prefix}|{suffix}" if prefix else suffix


class ClusteringRunExecutionLock:
    def __init__(self, connection: Connection | None) -> None:
        self.connection = connection
        self.released = False

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        if self.connection is None:
            _LOCAL_CLUSTERING_RUN_LOCK.release()
            return
        try:
            self.connection.execute(
                text("SELECT pg_advisory_unlock(:lock_key)"),
                {"lock_key": CLUSTERING_RUN_LOCK_KEY},
            )
        finally:
            self.connection.close()


@dataclass(frozen=True)
class MembershipSnapshot:
    cluster_anchor: str
    cluster_occurrence: int
    evidence_anchor: str
    evidence_occurrence: int


@dataclass(frozen=True)
class FrozenClusterSnapshot:
    cluster_id: int
    cluster_anchor: str
    cluster_occurrence: int
    evidence_anchors: tuple[str, ...]


@dataclass(frozen=True)
class ItemClusteringScope:
    item_ids: tuple[int, ...]

    def current_item_ids(self, _session: Session) -> tuple[int, ...]:
        return self.item_ids

    def validate_items(self, items: list[ContentItem]) -> None:
        if len(items) != len(self.item_ids):
            missing = sorted(set(self.item_ids) - {item.id for item in items})
            raise RuntimeError(f"Clustering Run scope 条目不存在：{missing}")

    def key_material(
        self, scope_type: str, evidence_anchors: tuple[str, ...]
    ) -> dict[str, object]:
        return {
            "scope_type": scope_type,
            "source_id": None,
            "evidence": sorted(evidence_anchors),
            "item_count": len(self.item_ids),
        }


@dataclass(frozen=True)
class SourceClusteringScope:
    source_id: int

    def current_item_ids(self, session: Session) -> tuple[int, ...]:
        return tuple(
            session.scalars(
                select(ContentItem.id)
                .where(ContentItem.source_id == self.source_id)
                .order_by(ContentItem.id)
            ).all()
        )

    def validate_items(self, _items: list[ContentItem]) -> None:
        return None

    def key_material(
        self, scope_type: str, _evidence_anchors: tuple[str, ...]
    ) -> dict[str, object]:
        return {
            "scope_type": scope_type,
            "source_id": self.source_id,
            "evidence": [],
            "item_count": 0,
        }


type ClusteringScope = ItemClusteringScope | SourceClusteringScope


@dataclass
class RunStart:
    run_id: str
    scope_type: str
    scope_key: str
    rule_version: str
    started_at: datetime
    scope: ClusteringScope
    scope_anchors: tuple[str, ...]
    before_cluster_evidence: dict[int, tuple[str, ...]]
    before_projection_ids: dict[int, int]

    @property
    def before_memberships(self) -> tuple[MembershipSnapshot, ...]:
        return memberships_from_cluster_evidence(self.before_cluster_evidence)

    @property
    def before_cluster_ids(self) -> tuple[int, ...]:
        return tuple(sorted(self.before_cluster_evidence))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def evidence_anchor_for_item(_session: Session, item: ContentItem) -> str:
    document = item.document
    raw = document.raw_entry
    if raw is None or raw.source_entry_id is None:
        raise RuntimeError(f"ContentItem 缺少稳定 RawEntry 证据：item_id={item.id}")
    if document.document_type == "normal_article":
        fragment_anchor = "whole-entry"
    else:
        if not item.content_hash:
            raise RuntimeError(
                f"拆分 ContentItem 缺少稳定 fragment hash：item_id={item.id}"
            )
        fragment_anchor = f"content-hash:{item.content_hash}"
    return f"source-entry:{raw.source_entry_id}:fragment:{fragment_anchor}"


def cluster_anchor_for(evidence_anchors: list[str]) -> str:
    canonical = json.dumps(
        sorted(evidence_anchors), ensure_ascii=False, separators=(",", ":")
    )
    return sha256_text(f"cluster-membership-v1:{canonical}")


def clustering_scope(
    item_ids: list[int] | tuple[int, ...] | None,
    source_id: int | None,
) -> ClusteringScope:
    fixed_item_ids = tuple(sorted(set(item_ids or ())))
    if fixed_item_ids and source_id is None:
        return ItemClusteringScope(fixed_item_ids)
    if not fixed_item_ids and source_id is not None:
        return SourceClusteringScope(source_id)
    raise ValueError("Clustering Run 必须且只能提供 item_ids 或 source_id")


def scope_key_for(
    scope_type: str,
    scope: ClusteringScope,
    scope_anchors: tuple[str, ...],
) -> str:
    return sha256_text(
        json.dumps(
            scope.key_material(scope_type, scope_anchors),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def scope_items(
    session: Session,
    scope: ClusteringScope,
) -> list[ContentItem]:
    item_ids = scope.current_item_ids(session)
    if not item_ids:
        return []
    rows = session.scalars(
        select(ContentItem)
        .where(ContentItem.id.in_(item_ids))
        .order_by(ContentItem.id)
    ).all()
    items = list(rows)
    scope.validate_items(items)
    return items


def cluster_evidence_for_ids(
    session: Session,
    cluster_ids: set[int],
) -> dict[int, tuple[str, ...]]:
    if not cluster_ids:
        return {}
    rows = session.execute(
        select(ClusterItem.cluster_id, ContentItem)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .where(ClusterItem.cluster_id.in_(cluster_ids))
        .order_by(ClusterItem.cluster_id, ContentItem.id)
    ).all()
    grouped: dict[int, list[str]] = {}
    for cluster_id, item in rows:
        grouped.setdefault(cluster_id, []).append(evidence_anchor_for_item(session, item))
    return {
        cluster_id: tuple(sorted(anchors))
        for cluster_id, anchors in grouped.items()
    }


def memberships_from_cluster_evidence(
    grouped: dict[int, tuple[str, ...]],
) -> tuple[MembershipSnapshot, ...]:
    memberships: list[MembershipSnapshot] = []
    for cluster in snapshot_clusters_from_cluster_evidence(grouped):
        evidence_counts: dict[str, int] = {}
        for evidence_anchor in cluster.evidence_anchors:
            evidence_occurrence = evidence_counts.get(evidence_anchor, 0) + 1
            evidence_counts[evidence_anchor] = evidence_occurrence
            memberships.append(
                MembershipSnapshot(
                    cluster_anchor=cluster.cluster_anchor,
                    cluster_occurrence=cluster.cluster_occurrence,
                    evidence_anchor=evidence_anchor,
                    evidence_occurrence=evidence_occurrence,
                )
            )
    return tuple(memberships)


def snapshot_clusters_from_cluster_evidence(
    grouped: dict[int, tuple[str, ...]],
) -> tuple[FrozenClusterSnapshot, ...]:
    identical_groups: dict[tuple[str, ...], list[int]] = {}
    for cluster_id, anchors in grouped.items():
        identical_groups.setdefault(anchors, []).append(cluster_id)

    snapshot_clusters: list[FrozenClusterSnapshot] = []
    for anchors, cluster_ids in sorted(identical_groups.items()):
        cluster_anchor = cluster_anchor_for(list(anchors))
        for occurrence, cluster_id in enumerate(sorted(cluster_ids), start=1):
            snapshot_clusters.append(
                FrozenClusterSnapshot(
                    cluster_id=cluster_id,
                    cluster_anchor=cluster_anchor,
                    cluster_occurrence=occurrence,
                    evidence_anchors=anchors,
                )
            )
    return tuple(snapshot_clusters)


def latest_projection_ids_for_clusters(
    session: Session, cluster_ids: set[int]
) -> dict[int, int]:
    if not cluster_ids:
        return {}
    latest = latest_live_cluster_projection(session, cluster_ids)
    return dict(
        session.execute(
            select(latest.c.cluster_id, latest.c.projection_id)
        ).all()
    )


def cluster_ids_for_scope(
    session: Session,
    scope: ClusteringScope,
    include_cluster_ids: tuple[int, ...] = (),
) -> set[int]:
    scoped_ids = scope.current_item_ids(session)
    cluster_ids = set(include_cluster_ids)
    if scoped_ids:
        cluster_ids.update(
            session.scalars(
                select(ClusterItem.cluster_id).where(
                    ClusterItem.content_item_id.in_(scoped_ids)
                )
            ).all()
        )
    return cluster_ids


def snapshot_memberships(
    session: Session,
    scope: ClusteringScope,
    include_cluster_ids: tuple[int, ...] = (),
) -> tuple[tuple[MembershipSnapshot, ...], tuple[int, ...]]:
    cluster_ids = cluster_ids_for_scope(session, scope, include_cluster_ids)
    if not cluster_ids:
        return (), ()

    grouped = cluster_evidence_for_ids(session, cluster_ids)
    return memberships_from_cluster_evidence(grouped), tuple(sorted(grouped))


def register_clustering_target(session: Session, cluster_id: int) -> None:
    started = session.info.get(ACTIVE_RUN_START_SESSION_KEY)
    if not isinstance(started, RunStart):
        return
    if cluster_id in started.before_cluster_evidence:
        return
    evidence = cluster_evidence_for_ids(session, {cluster_id})
    started.before_cluster_evidence[cluster_id] = evidence.get(cluster_id, ())
    projection_id = latest_projection_ids_for_clusters(session, {cluster_id}).get(
        cluster_id
    )
    if projection_id is not None:
        started.before_projection_ids[cluster_id] = projection_id


def add_scope_rows(
    session: Session, run_id: str, evidence_anchors: tuple[str, ...]
) -> None:
    existing = set(
        session.execute(
            select(
                ClusteringRunScopeEvidence.evidence_anchor,
                ClusteringRunScopeEvidence.evidence_occurrence,
            ).where(ClusteringRunScopeEvidence.run_id == run_id)
        ).all()
    )
    counts: dict[str, int] = {}
    rows: list[ClusteringRunScopeEvidence] = []
    for anchor in sorted(evidence_anchors):
        occurrence = counts.get(anchor, 0) + 1
        counts[anchor] = occurrence
        if (anchor, occurrence) not in existing:
            rows.append(
                ClusteringRunScopeEvidence(
                    run_id=run_id,
                    evidence_anchor=anchor,
                    evidence_occurrence=occurrence,
                )
            )
    session.add_all(rows)


def add_membership_rows(
    session: Session,
    run_id: str,
    phase: SnapshotPhase,
    memberships: tuple[MembershipSnapshot, ...],
) -> None:
    session.add_all(
        ClusteringRunMembership(
            run_id=run_id,
            snapshot_phase=phase,
            cluster_anchor=row.cluster_anchor,
            cluster_occurrence=row.cluster_occurrence,
            evidence_anchor=row.evidence_anchor,
            evidence_occurrence=row.evidence_occurrence,
        )
        for row in memberships
    )


def add_projection_predecessor_rows(
    session: Session,
    run_id: str,
    grouped: dict[int, tuple[str, ...]],
    projection_ids: dict[int, int],
) -> None:
    session.add_all(
        ClusteringRunProjectionPredecessor(
            run_id=run_id,
            cluster_anchor=cluster.cluster_anchor,
            cluster_occurrence=cluster.cluster_occurrence,
            predecessor_projection_id=projection_ids[cluster.cluster_id],
        )
        for cluster in snapshot_clusters_from_cluster_evidence(grouped)
        if cluster.cluster_id in projection_ids
    )


def snapshot_fingerprint(memberships: tuple[MembershipSnapshot, ...]) -> str:
    rows = sorted(
        f"{row.cluster_anchor}|{row.cluster_occurrence}|"
        f"{row.evidence_anchor}|{row.evidence_occurrence}"
        for row in memberships
    )
    return sha256_text("\n".join(rows))


def persist_membership_snapshot(
    session: Session,
    run_id: str,
    phase: SnapshotPhase,
    memberships: tuple[MembershipSnapshot, ...],
) -> None:
    add_membership_rows(session, run_id, phase, memberships)
    session.flush()
    session.add(
        ClusteringRunSnapshotSeal(
            run_id=run_id,
            snapshot_phase=phase,
            snapshot_row_count=len(memberships),
            snapshot_fingerprint=snapshot_fingerprint(memberships),
        )
    )
    session.flush()


def persist_before_snapshot(
    session: Session,
    run_id: str,
    scope_anchors: tuple[str, ...],
    memberships: tuple[MembershipSnapshot, ...],
    grouped: dict[int, tuple[str, ...]],
    projection_ids: dict[int, int],
) -> None:
    add_scope_rows(session, run_id, scope_anchors)
    add_membership_rows(session, run_id, "before", memberships)
    session.flush()
    add_projection_predecessor_rows(
        session,
        run_id,
        grouped,
        projection_ids,
    )
    session.flush()
    session.add(
        ClusteringRunSnapshotSeal(
            run_id=run_id,
            snapshot_phase="before",
            snapshot_row_count=len(memberships),
            snapshot_fingerprint=snapshot_fingerprint(memberships),
        )
    )
    session.flush()


def start_run(
    session: Session,
    *,
    scope_type: str,
    item_ids: list[int] | tuple[int, ...] | None,
    source_id: int | None,
    rule_version: str,
) -> RunStart:
    scope = clustering_scope(item_ids, source_id)
    if not scope_type.strip() or not rule_version.strip():
        raise ValueError("Clustering Run scope_type 与 rule_version 不能为空")

    session.flush()
    items = scope_items(session, scope)
    scope_anchors = tuple(
        sorted(evidence_anchor_for_item(session, item) for item in items)
    )
    before_cluster_evidence = cluster_evidence_for_ids(
        session, cluster_ids_for_scope(session, scope)
    )
    before_projection_ids = latest_projection_ids_for_clusters(
        session, set(before_cluster_evidence)
    )
    started_at = datetime.now(timezone.utc)
    run = ClusteringRun(
        scope_type=scope_type,
        scope_key=scope_key_for(scope_type, scope, scope_anchors),
        rule_version=rule_version,
        status="started",
        started_at=started_at,
    )
    session.add(run)
    session.flush()
    add_scope_rows(session, run.id, scope_anchors)
    session.flush()
    return RunStart(
        run_id=run.id,
        scope_type=scope_type,
        scope_key=run.scope_key,
        rule_version=rule_version,
        started_at=started_at,
        scope=scope,
        scope_anchors=scope_anchors,
        before_cluster_evidence=before_cluster_evidence,
        before_projection_ids=before_projection_ids,
    )


def complete_run(
    session: Session,
    started: RunStart,
    *,
    commit_on_success: bool,
) -> None:
    session.flush()
    persist_before_snapshot(
        session,
        started.run_id,
        started.scope_anchors,
        started.before_memberships,
        started.before_cluster_evidence,
        started.before_projection_ids,
    )
    after, after_cluster_ids = snapshot_memberships(
        session,
        started.scope,
        include_cluster_ids=started.before_cluster_ids,
    )
    run = session.get(ClusteringRun, started.run_id)
    if run is None or run.status != "started":
        raise RuntimeError(f"Clustering Run 无法完成：run_id={started.run_id}")
    current_scope_anchors = tuple(
        sorted(
            evidence_anchor_for_item(session, item)
            for item in scope_items(session, started.scope)
        )
    )
    add_scope_rows(session, started.run_id, current_scope_anchors)
    persist_membership_snapshot(session, started.run_id, "after", after)
    run.after_snapshot_finalized = True
    run.status = "completed"
    run.completed_at = datetime.now(timezone.utc)
    run.failed_at = None
    run.failure_info = ""
    session.flush()
    from .event_projection import project_completed_clustering_run

    project_completed_clustering_run(
        session,
        started.run_id,
        list(after_cluster_ids),
    )
    if session.get_bind().dialect.name == "postgresql":
        # ponytail: fixed safety ceiling; make configurable only if a valid run needs longer.
        session.execute(text("SET LOCAL statement_timeout = '5min'"))
    if commit_on_success:
        session.commit()
    else:
        session.flush()


def fail_run(session: Session, started: RunStart, exc: BaseException) -> None:
    session.rollback()
    run = session.get(ClusteringRun, started.run_id)
    if run is None:
        run = ClusteringRun(
            id=started.run_id,
            scope_type=started.scope_type,
            scope_key=started.scope_key,
            rule_version=started.rule_version,
            status="started",
            started_at=started.started_at,
        )
        session.add(run)
        session.flush()
    else:
        if run.status != "started":
            raise RuntimeError(
                f"Clustering Run 已进入终态：run_id={started.run_id}, status={run.status}"
            ) from exc
    persist_before_snapshot(
        session,
        run.id,
        started.scope_anchors,
        started.before_memberships,
        started.before_cluster_evidence,
        started.before_projection_ids,
    )
    run.status = "failed"
    run.completed_at = None
    run.failed_at = datetime.now(timezone.utc)
    run.failure_info = str(exc) or exc.__class__.__name__
    session.commit()


def mark_clustering_run_incomplete(session: Session, reason: str) -> None:
    if session.info.get(ACTIVE_RUN_SESSION_KEY):
        session.info.setdefault(INCOMPLETE_RUN_SESSION_KEY, reason)


def defer_clustering_run_lock_until_transaction_end(session: Session) -> None:
    if session.info.get(EXECUTION_LOCK_SESSION_KEY) is None:
        raise RuntimeError("延迟释放 Clustering Run 锁前必须先取得执行锁")
    session.info[DEFER_LOCK_RELEASE_SESSION_KEY] = True


@contextmanager
def clustering_run_execution_lock(session: Session) -> Iterator[None]:
    """Serialize outer clustering runs across commits, sessions, and workers."""
    if session.info.get(EXECUTION_LOCK_SESSION_KEY) is not None:
        yield
        return

    bind = session.get_bind()
    if bind.dialect.name == "postgresql":
        engine: Engine = bind.engine if isinstance(bind, Connection) else bind
        lock_connection = engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        )
        try:
            lock_connection.execute(
                text("SELECT pg_advisory_lock(:lock_key)"),
                {"lock_key": CLUSTERING_RUN_LOCK_KEY},
            )
        except BaseException:
            lock_connection.close()
            raise
        execution_lock = ClusteringRunExecutionLock(lock_connection)
    else:
        _LOCAL_CLUSTERING_RUN_LOCK.acquire()
        execution_lock = ClusteringRunExecutionLock(None)

    session.info[EXECUTION_LOCK_SESSION_KEY] = execution_lock
    try:
        yield
    finally:
        if not session.info.get(DEFER_LOCK_RELEASE_SESSION_KEY):
            session.info.pop(EXECUTION_LOCK_SESSION_KEY, None)
            execution_lock.release()


@event.listens_for(Session, "after_transaction_end")
def release_deferred_clustering_run_lock(session: Session, transaction: object) -> None:
    if getattr(transaction, "parent", None) is not None:
        return
    if session.info.get(ACTIVE_RUN_SESSION_KEY):
        return
    session.info.pop(DEFER_LOCK_RELEASE_SESSION_KEY, None)
    execution_lock = session.info.pop(EXECUTION_LOCK_SESSION_KEY, None)
    if isinstance(execution_lock, ClusteringRunExecutionLock):
        execution_lock.release()


def _rollback_clustering_run_transaction(session: Session) -> None:
    """Abort a migration-owned run and release its deferred execution lock."""
    session.info.pop(DEFER_LOCK_RELEASE_SESSION_KEY, None)
    try:
        session.rollback()
    finally:
        execution_lock = session.info.pop(EXECUTION_LOCK_SESSION_KEY, None)
        if isinstance(execution_lock, ClusteringRunExecutionLock):
            execution_lock.release()


@contextmanager
def clustering_run(
    session: Session,
    *,
    scope_type: str,
    item_ids: list[int] | tuple[int, ...] | None = None,
    source_id: int | None = None,
    rule_version: str,
    commit_on_success: bool = True,
    rollback_on_failure: bool = False,
) -> Iterator[str]:
    active_run_id = session.info.get(ACTIVE_RUN_SESSION_KEY)
    if active_run_id:
        yield str(active_run_id)
        return
    with clustering_run_execution_lock(session):
        session.info.pop(DEFER_LOCK_RELEASE_SESSION_KEY, None)
        started = start_run(
            session,
            scope_type=scope_type,
            item_ids=item_ids,
            source_id=source_id,
            rule_version=rule_version,
        )
        session.info[ACTIVE_RUN_SESSION_KEY] = started.run_id
        session.info[ACTIVE_RUN_START_SESSION_KEY] = started
        try:
            yield started.run_id
        except BaseException as exc:
            if rollback_on_failure:
                # Migration preparation must be all-or-nothing: unlike normal
                # worker runs, it must not write a failed audit row and commit
                # a partial source-type conversion.
                _rollback_clustering_run_transaction(session)
            else:
                fail_run(session, started, exc)
            raise
        else:
            incomplete_reason = session.info.get(INCOMPLETE_RUN_SESSION_KEY)
            if incomplete_reason:
                incomplete_error = RuntimeError(str(incomplete_reason))
                if rollback_on_failure:
                    _rollback_clustering_run_transaction(session)
                    raise incomplete_error
                fail_run(session, started, incomplete_error)
            else:
                try:
                    complete_run(
                        session,
                        started,
                        commit_on_success=commit_on_success,
                    )
                except BaseException as exc:
                    if rollback_on_failure:
                        _rollback_clustering_run_transaction(session)
                    else:
                        fail_run(session, started, exc)
                    raise
                if not commit_on_success:
                    session.info[DEFER_LOCK_RELEASE_SESSION_KEY] = True
        finally:
            session.info.pop(ACTIVE_RUN_SESSION_KEY, None)
            session.info.pop(ACTIVE_RUN_START_SESSION_KEY, None)
            session.info.pop(INCOMPLETE_RUN_SESSION_KEY, None)
