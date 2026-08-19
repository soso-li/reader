from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from uuid import uuid4

from sqlalchemy import and_, case, func, select, true
from sqlalchemy.orm import Session, aliased

from .digest import canonical_url
from .models import (
    Cluster,
    ClusterCurrentEventProjection,
    ClusterEventProjection,
    ClusterItem,
    ClusteringRun,
    ClusteringRunMembership,
    ClusteringRunProjectionPredecessor,
    ClusteringRunSnapshotSeal,
    ContentItem,
    Document,
    Event,
    EVENT_EVIDENCE_ROLES,
    EVENT_EVIDENCE_TYPES,
    EventEvidence,
    EventEvidenceVersion,
    EventLineage,
    EvidenceReview,
    EventRevision,
    EventRevisionEvidence,
    EventUserState,
    RawEntry,
    Source,
    SourceEntryKey,
)


DEFAULT_LEGACY_ARTICLE_ROLE = "material"
WHOLE_ENTRY_FRAGMENT_FINGERPRINT = sha256(
    b"event-evidence-whole-entry-v1"
).hexdigest()


@dataclass(frozen=True)
class ClusterEventIdentity:
    event_uid: str
    current_revision_uid: str
    seen_revision_uid: str | None = None
    current_revision_differs_from_seen: bool = True
    has_material_update: bool = False
    material_update_revision_uid: str | None = None
    read_status: str = "unread"
    read_later: bool = False
    starred: bool = False
    uninterested: bool = False
    uninterested_reason: str | None = None
    uninterested_note: str | None = None
    uninterested_at: datetime | None = None


@dataclass(frozen=True)
class ProjectionEvidence:
    version: EventEvidenceVersion
    evidence_type: str
    role: str

    def __post_init__(self) -> None:
        if self.evidence_type not in EVENT_EVIDENCE_TYPES:
            raise ValueError(f"不受支持的 Event Evidence 类型：{self.evidence_type}")
        if self.role not in EVENT_EVIDENCE_ROLES:
            raise ValueError(f"不受支持的 Event Evidence 角色：{self.role}")

    @property
    def fingerprint_row(self) -> str:
        return (
            f"{self.evidence_type}|{self.role}|"
            f"{self.version.version_fingerprint}"
        )


def _sha256_json(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def canonical_evidence_fingerprint(
    evidence: list[ProjectionEvidence],
) -> str:
    rows = sorted(item.fingerprint_row for item in evidence)
    return sha256("\n".join(rows).encode("utf-8")).hexdigest()


def _fragment_fingerprint(document: Document, item: ContentItem) -> str:
    if document.document_type == "normal_article":
        return WHOLE_ENTRY_FRAGMENT_FINGERPRINT
    return _sha256_json(
        [
            "legacy-content-fragment-v2",
            item.content_hash,
        ]
    )


def _stable_source_entry_keys(
    session: Session, source_entry_id: int
) -> tuple[str, ...]:
    keys = session.execute(
        select(SourceEntryKey.identity_kind, SourceEntryKey.identity_key).where(
            SourceEntryKey.source_entry_id == source_entry_id
        )
    ).all()
    if not keys:
        raise RuntimeError(
            f"Event Evidence 缺少 Source Entry identity key：{source_entry_id}"
        )
    kind_priority = {"guid": 0, "url": 1, "fallback": 2, "legacy": 3}
    ordered_keys = sorted(
        keys,
        key=lambda row: (kind_priority.get(row.identity_kind, 99), row.identity_key),
    )
    return tuple(
        f"{identity_kind}|{identity_key}"
        for identity_kind, identity_key in ordered_keys
    )


def _stable_evidence_identity_fingerprint(
    *,
    source_url: str,
    source_entry_key: str,
    fragment_fingerprint: str,
) -> str:
    return _sha256_json(
        [
            "event-evidence-identity-v2",
            canonical_url(source_url),
            source_entry_key,
            fragment_fingerprint,
        ]
    )


def _event_evidence_version(
    session: Session,
    *,
    item: ContentItem,
    document: Document,
    raw: RawEntry,
    source_id: int,
    source_url: str,
    preferred_version_ids: frozenset[int],
) -> EventEvidenceVersion:
    fragment_fingerprint = _fragment_fingerprint(document, item)
    stable_source_entry_keys = _stable_source_entry_keys(
        session, raw.source_entry_id
    )
    stable_identity_fingerprint = _stable_evidence_identity_fingerprint(
        source_url=source_url,
        source_entry_key=stable_source_entry_keys[0],
        fragment_fingerprint=fragment_fingerprint,
    )
    evidence = session.scalar(
        select(EventEvidence).where(
            EventEvidence.source_entry_id == raw.source_entry_id,
            EventEvidence.fragment_fingerprint == fragment_fingerprint,
        )
    )
    if evidence is None:
        evidence = EventEvidence(
            uid=str(uuid4()),
            identity_fingerprint=stable_identity_fingerprint,
            source_entry_id=raw.source_entry_id,
            fragment_fingerprint=fragment_fingerprint,
        )
        session.add(evidence)
        session.flush()

    content_snapshot = item.content_text or item.summary or ""
    legacy_version_fingerprint = _sha256_json(
        [
            "event-evidence-version-v2",
            evidence.identity_fingerprint,
            raw.payload_fingerprint,
            raw.revision_no,
            item.content_hash or "",
            item.title or "",
            item.canonical_url or item.url or "",
            content_snapshot,
            fragment_fingerprint or "",
        ]
    )
    legacy_stable_version_fingerprints = tuple(
        _sha256_json(
            [
                "event-evidence-version-v3",
                _stable_evidence_identity_fingerprint(
                    source_url=source_url,
                    source_entry_key=stable_source_entry_key,
                    fragment_fingerprint=fragment_fingerprint,
                ),
                raw.payload_fingerprint,
                item.content_hash or "",
                item.title or "",
                item.canonical_url or item.url or "",
                content_snapshot,
                fragment_fingerprint,
            ]
        )
        for stable_source_entry_key in stable_source_entry_keys
    )
    version_fingerprint = _sha256_json(
        [
            "event-evidence-version-v4",
            canonical_url(source_url),
            raw.external_id,
            raw.payload_fingerprint,
            item.content_hash or "",
            item.title or "",
            item.canonical_url or item.url or "",
            content_snapshot,
            fragment_fingerprint,
        ]
    )
    existing_candidates: list[EventEvidenceVersion] = []
    for candidate_fingerprint in (
        legacy_version_fingerprint,
        *legacy_stable_version_fingerprints,
        version_fingerprint,
    ):
        version = session.scalar(
            select(EventEvidenceVersion).where(
                EventEvidenceVersion.evidence_id == evidence.id,
                EventEvidenceVersion.version_fingerprint == candidate_fingerprint,
            )
        )
        if version is not None:
            existing_candidates.append(version)

    for version in existing_candidates:
        if version.id in preferred_version_ids:
            return version
    if existing_candidates:
        return existing_candidates[0]

    version = EventEvidenceVersion(
        uid=str(uuid4()),
        evidence_id=evidence.id,
        version_fingerprint=version_fingerprint,
        raw_entry_id=raw.id,
        source_entry_id=raw.source_entry_id,
        source_id=source_id,
        raw_revision_no=raw.revision_no,
        legacy_content_item_id=item.id,
        legacy_content_item_id_snapshot=item.id,
        fragment_fingerprint=fragment_fingerprint,
        title_snapshot=item.title or raw.title or "",
        url_snapshot=item.url or raw.url or "",
        author_snapshot=raw.author or "",
        published_at_snapshot=item.published_at or raw.published_at,
        content_snapshot=content_snapshot,
    )
    session.add(version)
    session.flush()
    return version


def _projection_evidence(
    session: Session,
    cluster_id: int,
    *,
    preferred_version_ids: frozenset[int],
) -> list[ProjectionEvidence]:
    rows = session.execute(
        select(
            ContentItem,
            Document,
            RawEntry,
            Source.id,
            Source.url,
            Source.media_type,
        )
        .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
        .join(Document, Document.id == ContentItem.document_id)
        .join(RawEntry, RawEntry.id == Document.raw_entry_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(ClusterItem.cluster_id == cluster_id)
        .order_by(ContentItem.id)
    ).all()
    if not rows:
        raise RuntimeError(f"Cluster 投影没有可定位证据：cluster_id={cluster_id}")

    evidence_by_key: dict[tuple[str, str, str], ProjectionEvidence] = {}
    for item, document, raw, source_id, source_url, media_type in rows:
        if media_type in EVENT_EVIDENCE_TYPES and media_type != "article":
            raise RuntimeError(
                f"首期只自动接入文章：cluster_id={cluster_id}, "
                f"media_type={media_type}"
            )
        if media_type != "article":
            raise RuntimeError(
                f"Event Evidence 类型不受支持：cluster_id={cluster_id}, "
                f"media_type={media_type}"
            )
        evidence_type = "article"
        role = DEFAULT_LEGACY_ARTICLE_ROLE
        version = _event_evidence_version(
            session,
            item=item,
            document=document,
            raw=raw,
            source_id=source_id,
            source_url=source_url,
            preferred_version_ids=preferred_version_ids,
        )
        projected = ProjectionEvidence(
            version=version,
            evidence_type=evidence_type,
            role=role,
        )
        evidence_by_key[
            (version.version_fingerprint, evidence_type, role)
        ] = projected
    return [evidence_by_key[key] for key in sorted(evidence_by_key)]


type SnapshotClusterKey = tuple[str, int]
type SnapshotEvidence = frozenset[tuple[str, int]]


def _sealed_snapshot_clusters(
    session: Session,
    run_id: str,
) -> tuple[
    dict[SnapshotClusterKey, SnapshotEvidence],
    dict[SnapshotClusterKey, SnapshotEvidence],
]:
    seals = {
        seal.snapshot_phase: seal
        for seal in session.scalars(
            select(ClusteringRunSnapshotSeal).where(
                ClusteringRunSnapshotSeal.run_id == run_id
            )
        )
    }
    if set(seals) != {"before", "after"}:
        raise RuntimeError(
            f"Clustering Run 缺少完整 before/after snapshot seal：run_id={run_id}"
        )

    grouped: dict[str, dict[SnapshotClusterKey, set[tuple[str, int]]]] = {
        "before": {},
        "after": {},
    }
    row_counts = {"before": 0, "after": 0}
    rows = session.scalars(
        select(ClusteringRunMembership)
        .where(ClusteringRunMembership.run_id == run_id)
        .order_by(
            ClusteringRunMembership.snapshot_phase,
            ClusteringRunMembership.cluster_anchor,
            ClusteringRunMembership.cluster_occurrence,
            ClusteringRunMembership.evidence_anchor,
            ClusteringRunMembership.evidence_occurrence,
        )
    )
    for row in rows:
        phase = row.snapshot_phase
        if phase not in grouped:
            raise RuntimeError(
                f"Clustering Run snapshot phase 非法：run_id={run_id}, phase={phase}"
            )
        key = (row.cluster_anchor, row.cluster_occurrence)
        grouped[phase].setdefault(key, set()).add(
            (row.evidence_anchor, row.evidence_occurrence)
        )
        row_counts[phase] += 1

    for phase in ("before", "after"):
        if row_counts[phase] != seals[phase].snapshot_row_count:
            raise RuntimeError(
                f"Clustering Run snapshot seal 行数不匹配：run_id={run_id}, "
                f"phase={phase}"
            )
    return (
        {key: frozenset(value) for key, value in grouped["before"].items()},
        {key: frozenset(value) for key, value in grouped["after"].items()},
    )


def _live_after_clusters(
    session: Session,
    cluster_ids: list[int] | tuple[int, ...],
) -> dict[SnapshotClusterKey, Cluster]:
    from .clustering_run import (
        cluster_evidence_for_ids,
        snapshot_clusters_from_cluster_evidence,
    )

    grouped = cluster_evidence_for_ids(session, set(cluster_ids))
    live: dict[SnapshotClusterKey, Cluster] = {}
    for snapshot in snapshot_clusters_from_cluster_evidence(grouped):
        cluster = session.get(Cluster, snapshot.cluster_id)
        if cluster is not None:
            live[
                (snapshot.cluster_anchor, snapshot.cluster_occurrence)
            ] = cluster
    return live


def _snapshot_edges(
    before: dict[SnapshotClusterKey, SnapshotEvidence],
    after: dict[SnapshotClusterKey, SnapshotEvidence],
    *,
    require_inclusion: bool,
) -> tuple[
    dict[SnapshotClusterKey, set[SnapshotClusterKey]],
    dict[SnapshotClusterKey, set[SnapshotClusterKey]],
]:
    before_to_after = {key: set() for key in before}
    after_to_before = {key: set() for key in after}
    for before_key, before_evidence in before.items():
        for after_key, after_evidence in after.items():
            shared_anchor = before_evidence & after_evidence
            has_inclusion = (
                before_evidence <= after_evidence
                or after_evidence <= before_evidence
            )
            if shared_anchor and (has_inclusion or not require_inclusion):
                before_to_after[before_key].add(after_key)
                after_to_before[after_key].add(before_key)
    return before_to_after, after_to_before


def _predecessor_projection(
    session: Session,
    *,
    run: ClusteringRun,
    before_key: SnapshotClusterKey,
) -> ClusterEventProjection | None:
    return session.scalar(
        select(ClusterEventProjection)
        .join(
            ClusteringRunProjectionPredecessor,
            ClusteringRunProjectionPredecessor.predecessor_projection_id
            == ClusterEventProjection.id,
        )
        .where(
            ClusteringRunProjectionPredecessor.run_id == run.id,
            ClusteringRunProjectionPredecessor.cluster_anchor == before_key[0],
            ClusteringRunProjectionPredecessor.cluster_occurrence
            == before_key[1],
        )
        .limit(1)
    )


def _revision_evidence_version_ids(
    session: Session, revision_id: int
) -> frozenset[int]:
    return frozenset(
        session.scalars(
            select(EventRevisionEvidence.evidence_version_id).where(
                EventRevisionEvidence.revision_id == revision_id
            )
        ).all()
    )


def _create_event_with_initial_revision(
    session: Session,
    *,
    cluster: Cluster,
) -> tuple[Event, EventRevision, str]:
    evidence = _projection_evidence(
        session,
        cluster.id,
        preferred_version_ids=frozenset(),
    )
    fingerprint = canonical_evidence_fingerprint(evidence)
    event = Event(uid=str(uuid4()), status="active")
    session.add(event)
    session.flush()
    revision = EventRevision(
        uid=str(uuid4()),
        event_id=event.id,
        revision_no=1,
        evidence_fingerprint=fingerprint,
        title_snapshot=(cluster.generated_title or "").strip() or cluster.title,
        event_time_snapshot=cluster.first_seen_at,
    )
    session.add(revision)
    session.flush()
    session.add_all(
        EventRevisionEvidence(
            revision_id=revision.id,
            evidence_version_id=item.version.id,
            evidence_type=item.evidence_type,
            role=item.role,
        )
        for item in evidence
    )
    event.current_revision_id = revision.id
    session.flush()
    return event, revision, fingerprint


def _create_initial_projection(
    session: Session,
    *,
    run: ClusteringRun,
    cluster: Cluster,
    cluster_anchor: str,
    cluster_occurrence: int,
) -> ClusterEventProjection:
    event, revision, fingerprint = _create_event_with_initial_revision(
        session,
        cluster=cluster,
    )
    projection = ClusterEventProjection(
        cluster_id=cluster.id,
        cluster_id_snapshot=cluster.id,
        clustering_run_id=run.id,
        cluster_anchor=cluster_anchor,
        cluster_occurrence=cluster_occurrence,
        event_id=event.id,
        event_revision_id=revision.id,
        predecessor_projection_id=None,
        reconciliation_kind="initial",
        reconciliation_rule_version=None,
        before_evidence_fingerprint=None,
        after_evidence_fingerprint=fingerprint,
    )
    session.add(projection)
    session.flush()
    return projection


def _create_split_projection(
    session: Session,
    *,
    run: ClusteringRun,
    cluster: Cluster,
    cluster_anchor: str,
    cluster_occurrence: int,
    predecessor: ClusterEventProjection,
) -> ClusterEventProjection:
    event, revision, fingerprint = _create_event_with_initial_revision(
        session,
        cluster=cluster,
    )
    projection = ClusterEventProjection(
        cluster_id=cluster.id,
        cluster_id_snapshot=cluster.id,
        clustering_run_id=run.id,
        cluster_anchor=cluster_anchor,
        cluster_occurrence=cluster_occurrence,
        event_id=event.id,
        event_revision_id=revision.id,
        predecessor_projection_id=predecessor.id,
        reconciliation_kind="split",
        reconciliation_rule_version=run.rule_version,
        before_evidence_fingerprint=predecessor.after_evidence_fingerprint,
        after_evidence_fingerprint=fingerprint,
    )
    session.add(projection)
    session.flush()
    return projection


def _create_merge_projection(
    session: Session,
    *,
    run: ClusteringRun,
    cluster: Cluster,
    cluster_anchor: str,
    cluster_occurrence: int,
) -> ClusterEventProjection:
    event, revision, fingerprint = _create_event_with_initial_revision(
        session,
        cluster=cluster,
    )
    projection = ClusterEventProjection(
        cluster_id=cluster.id,
        cluster_id_snapshot=cluster.id,
        clustering_run_id=run.id,
        cluster_anchor=cluster_anchor,
        cluster_occurrence=cluster_occurrence,
        event_id=event.id,
        event_revision_id=revision.id,
        predecessor_projection_id=None,
        reconciliation_kind="merged",
        reconciliation_rule_version=run.rule_version,
        before_evidence_fingerprint=None,
        after_evidence_fingerprint=fingerprint,
    )
    session.add(projection)
    session.flush()
    return projection


def _create_ambiguous_projection(
    session: Session,
    *,
    run: ClusteringRun,
    cluster: Cluster,
    cluster_anchor: str,
    cluster_occurrence: int,
) -> ClusterEventProjection:
    event, revision, fingerprint = _create_event_with_initial_revision(
        session,
        cluster=cluster,
    )
    projection = ClusterEventProjection(
        cluster_id=cluster.id,
        cluster_id_snapshot=cluster.id,
        clustering_run_id=run.id,
        cluster_anchor=cluster_anchor,
        cluster_occurrence=cluster_occurrence,
        event_id=event.id,
        event_revision_id=revision.id,
        predecessor_projection_id=None,
        reconciliation_kind="ambiguous",
        reconciliation_rule_version=run.rule_version,
        before_evidence_fingerprint=None,
        after_evidence_fingerprint=fingerprint,
    )
    session.add(projection)
    session.flush()
    return projection


def _continue_projection(
    session: Session,
    *,
    run: ClusteringRun,
    cluster: Cluster,
    cluster_anchor: str,
    cluster_occurrence: int,
    predecessor: ClusterEventProjection,
) -> ClusterEventProjection | None:
    event = session.scalar(
        select(Event)
        .where(Event.id == predecessor.event_id)
        .with_for_update()
    )
    if event is None or event.status != "active" or event.current_revision_id is None:
        return None
    current_revision = session.get(EventRevision, event.current_revision_id)
    if current_revision is None:
        raise RuntimeError(f"Event 缺少 current Revision：event_id={event.id}")

    evidence = _projection_evidence(
        session,
        cluster.id,
        preferred_version_ids=_revision_evidence_version_ids(
            session, current_revision.id
        ),
    )
    before_fingerprint = current_revision.evidence_fingerprint
    after_fingerprint = canonical_evidence_fingerprint(evidence)
    revision = current_revision
    if after_fingerprint != before_fingerprint:
        revision = EventRevision(
            uid=str(uuid4()),
            event_id=event.id,
            revision_no=current_revision.revision_no + 1,
            evidence_fingerprint=after_fingerprint,
            title_snapshot=(cluster.generated_title or "").strip() or cluster.title,
            event_time_snapshot=cluster.first_seen_at,
        )
        session.add(revision)
        session.flush()
        session.add_all(
            EventRevisionEvidence(
                revision_id=revision.id,
                evidence_version_id=item.version.id,
                evidence_type=item.evidence_type,
                role=item.role,
            )
            for item in evidence
        )
        event.current_revision_id = revision.id

    projection = ClusterEventProjection(
        cluster_id=cluster.id,
        cluster_id_snapshot=cluster.id,
        clustering_run_id=run.id,
        cluster_anchor=cluster_anchor,
        cluster_occurrence=cluster_occurrence,
        event_id=event.id,
        event_revision_id=revision.id,
        predecessor_projection_id=predecessor.id,
        reconciliation_kind="continued",
        reconciliation_rule_version=run.rule_version,
        before_evidence_fingerprint=before_fingerprint,
        after_evidence_fingerprint=after_fingerprint,
    )
    session.add(projection)
    session.flush()
    return projection


def project_completed_clustering_run(
    session: Session,
    run_id: str,
    cluster_ids: list[int] | tuple[int, ...],
) -> list[ClusterEventProjection]:
    run = session.scalar(
        select(ClusteringRun)
        .where(ClusteringRun.id == run_id)
        .with_for_update()
    )
    if (
        run is None
        or run.status != "completed"
        or not run.after_snapshot_finalized
    ):
        raise RuntimeError(
            f"只有完成且封印的 Clustering Run 可建立 Event 投影：run_id={run_id}"
        )

    before, after = _sealed_snapshot_clusters(session, run_id)
    live_after = _live_after_clusters(session, cluster_ids)
    if set(live_after) != set(after):
        raise RuntimeError(
            f"Clustering Run sealed after snapshot 与当前投影目标不一致：run_id={run_id}"
        )
    before_to_after, after_to_before = _snapshot_edges(
        before,
        after,
        require_inclusion=True,
    )
    overlap_before_to_after, overlap_after_to_before = _snapshot_edges(
        before,
        after,
        require_inclusion=False,
    )
    bootstrap_after_keys = {
        after_key
        for after_key, before_keys in overlap_after_to_before.items()
        if before_keys
        and not any(
            _predecessor_projection(
                session,
                run=run,
                before_key=before_key,
            )
            is not None
            for before_key in before_keys
        )
    }
    split_children_by_parent = {
        before_key: children
        for before_key, children in overlap_before_to_after.items()
        if len(children) >= 2
        and all(overlap_after_to_before[child] == {before_key} for child in children)
    }
    split_after_keys = {
        child
        for children in split_children_by_parent.values()
        for child in children
    }
    merge_parents_by_child = {
        after_key: parents
        for after_key, parents in overlap_after_to_before.items()
        if len(parents) >= 2
        and after_to_before[after_key] == parents
        and all(before[parent] <= after[after_key] for parent in parents)
        and all(
            overlap_before_to_after[parent] == {after_key}
            for parent in parents
        )
    }
    merge_after_keys = set(merge_parents_by_child)
    continuation_predecessors: dict[
        SnapshotClusterKey, tuple[SnapshotClusterKey, ClusterEventProjection]
    ] = {}
    for after_key, predecessor_keys in after_to_before.items():
        if after_key in split_after_keys:
            continue
        if len(predecessor_keys) != 1:
            continue
        before_key = next(iter(predecessor_keys))
        if before_to_after[before_key] != {after_key}:
            continue
        if overlap_after_to_before[after_key] != {before_key}:
            continue
        if overlap_before_to_after[before_key] != {after_key}:
            continue
        predecessor = _predecessor_projection(
            session,
            run=run,
            before_key=before_key,
        )
        if predecessor is not None:
            continuation_predecessors[after_key] = (before_key, predecessor)
    continued_before_keys = {
        before_key
        for before_key, _predecessor in continuation_predecessors.values()
    }
    created: list[ClusterEventProjection] = []
    for after_key in sorted(bootstrap_after_keys):
        existing = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == run.id,
                ClusterEventProjection.cluster_anchor == after_key[0],
                ClusterEventProjection.cluster_occurrence == after_key[1],
            )
        )
        if existing is not None:
            if (
                existing.reconciliation_kind != "initial"
                or existing.predecessor_projection_id is not None
            ):
                raise RuntimeError(
                    f"Clustering Run legacy bootstrap 投影不一致：run_id={run.id}"
                )
            created.append(existing)
            continue
        created.append(
            _create_initial_projection(
                session,
                run=run,
                cluster=live_after[after_key],
                cluster_anchor=after_key[0],
                cluster_occurrence=after_key[1],
            )
        )
    for before_key, children in sorted(split_children_by_parent.items()):
        predecessor = _predecessor_projection(
            session,
            run=run,
            before_key=before_key,
        )
        if predecessor is None:
            continue
        existing_children = [
            session.scalar(
                select(ClusterEventProjection).where(
                    ClusterEventProjection.clustering_run_id == run.id,
                    ClusterEventProjection.cluster_anchor == after_key[0],
                    ClusterEventProjection.cluster_occurrence == after_key[1],
                )
            )
            for after_key in sorted(children)
        ]
        if all(existing_children):
            for projection in existing_children:
                assert projection is not None
                lineage = session.scalar(
                    select(EventLineage).where(
                        EventLineage.clustering_run_id == run.id,
                        EventLineage.relation_type == "split_from",
                        EventLineage.source_event_id == predecessor.event_id,
                        EventLineage.target_event_id == projection.event_id,
                    )
                )
                if (
                    projection.reconciliation_kind != "split"
                    or projection.predecessor_projection_id != predecessor.id
                    or lineage is None
                ):
                    raise RuntimeError(
                        f"Clustering Run split 投影审计链不完整：run_id={run.id}"
                    )
                created.append(projection)
            continue
        if any(existing_children):
            raise RuntimeError(
                f"Clustering Run split 投影只完成了一部分：run_id={run.id}"
            )
        event = session.scalar(
            select(Event)
            .where(Event.id == predecessor.event_id)
            .with_for_update()
        )
        if event is None or event.status != "active":
            continue
        split_projections: list[ClusterEventProjection] = []
        for after_key in sorted(children):
            cluster = live_after[after_key]
            projection = _create_split_projection(
                session,
                run=run,
                cluster=cluster,
                cluster_anchor=after_key[0],
                cluster_occurrence=after_key[1],
                predecessor=predecessor,
            )
            split_projections.append(projection)
            created.append(projection)
        event.status = "superseded"
        event.superseded_at = datetime.now(timezone.utc)
        session.flush()
        session.add_all(
            EventLineage(
                uid=str(uuid4()),
                clustering_run_id=run.id,
                relation_type="split_from",
                source_event_id=event.id,
                target_event_id=projection.event_id,
                rule_version=run.rule_version,
                before_evidence_fingerprint=(
                    predecessor.after_evidence_fingerprint
                ),
                after_evidence_fingerprint=(
                    projection.after_evidence_fingerprint
                ),
                decision_reason=(
                    "one_before_cluster_overlaps_multiple_unique_after_clusters"
                ),
            )
            for projection in split_projections
        )
        session.flush()
    for after_key, parent_keys in sorted(merge_parents_by_child.items()):
        predecessors = [
            _predecessor_projection(
                session,
                run=run,
                before_key=before_key,
            )
            for before_key in sorted(parent_keys)
        ]
        if any(predecessor is None for predecessor in predecessors):
            continue
        frozen_predecessors = [
            predecessor
            for predecessor in predecessors
            if predecessor is not None
        ]
        parent_event_ids = {
            predecessor.event_id for predecessor in frozen_predecessors
        }
        if len(parent_event_ids) != len(frozen_predecessors):
            continue

        existing = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == run.id,
                ClusterEventProjection.cluster_anchor == after_key[0],
                ClusterEventProjection.cluster_occurrence == after_key[1],
            )
        )
        if existing is not None:
            lineages = list(
                session.scalars(
                    select(EventLineage).where(
                        EventLineage.clustering_run_id == run.id,
                        EventLineage.relation_type == "merged_from",
                        EventLineage.target_event_id == existing.event_id,
                    )
                )
            )
            if (
                existing.reconciliation_kind != "merged"
                or existing.predecessor_projection_id is not None
                or {lineage.source_event_id for lineage in lineages}
                != parent_event_ids
                or any(
                    lineage.rule_version != run.rule_version
                    or lineage.after_evidence_fingerprint
                    != existing.after_evidence_fingerprint
                    for lineage in lineages
                )
            ):
                raise RuntimeError(
                    f"Clustering Run merge 投影审计链不完整：run_id={run.id}"
                )
            created.append(existing)
            continue

        parent_events = list(
            session.scalars(
                select(Event)
                .where(Event.id.in_(parent_event_ids))
                .order_by(Event.id)
                .with_for_update()
            )
        )
        parents_by_id = {parent.id: parent for parent in parent_events}
        if len(parents_by_id) != len(parent_event_ids) or any(
            parents_by_id[predecessor.event_id].status != "active"
            or parents_by_id[predecessor.event_id].current_revision_id
            != predecessor.event_revision_id
            for predecessor in frozen_predecessors
        ):
            continue

        cluster = live_after[after_key]
        projection = _create_merge_projection(
            session,
            run=run,
            cluster=cluster,
            cluster_anchor=after_key[0],
            cluster_occurrence=after_key[1],
        )
        for parent in parent_events:
            parent.status = "superseded"
            parent.superseded_at = datetime.now(timezone.utc)
        session.flush()
        predecessor_by_event_id = {
            predecessor.event_id: predecessor
            for predecessor in frozen_predecessors
        }
        session.add_all(
            EventLineage(
                uid=str(uuid4()),
                clustering_run_id=run.id,
                relation_type="merged_from",
                source_event_id=parent.id,
                target_event_id=projection.event_id,
                rule_version=run.rule_version,
                before_evidence_fingerprint=(
                    predecessor_by_event_id[
                        parent.id
                    ].after_evidence_fingerprint
                ),
                after_evidence_fingerprint=(
                    projection.after_evidence_fingerprint
                ),
                decision_reason=(
                    "multiple_unique_before_clusters_merge_into_one_after_cluster"
                ),
            )
            for parent in parent_events
        )
        session.flush()
        created.append(projection)

    handled_before_keys = (
        set(split_children_by_parent)
        | {
            parent_key
            for parent_keys in merge_parents_by_child.values()
            for parent_key in parent_keys
        }
        | continued_before_keys
    )
    candidate_ambiguous_before_keys = set(before) - handled_before_keys
    frozen_by_before_key = {
        before_key: _predecessor_projection(
            session,
            run=run,
            before_key=before_key,
        )
        for before_key in sorted(candidate_ambiguous_before_keys)
    }
    ambiguous_before_keys = {
        before_key
        for before_key, predecessor in frozen_by_before_key.items()
        if predecessor is not None
    }
    handled_after_keys = (
        split_after_keys | merge_after_keys | set(continuation_predecessors)
    )
    ambiguous_after_keys = {
        after_key
        for after_key in set(after) - handled_after_keys
        if ambiguous_before_keys and after_key not in bootstrap_after_keys
    }
    if ambiguous_after_keys:
        frozen_predecessors = {
            before_key: predecessor
            for before_key, predecessor in frozen_by_before_key.items()
            if before_key in ambiguous_before_keys and predecessor is not None
        }
        parent_event_ids = {
            predecessor.event_id
            for predecessor in frozen_predecessors.values()
        }
        if len(parent_event_ids) != len(frozen_predecessors):
            raise RuntimeError(
                f"Clustering Run ambiguous 冻结前驱 Event 不唯一：run_id={run.id}"
            )

        existing_projections = {
            after_key: session.scalar(
                select(ClusterEventProjection).where(
                    ClusterEventProjection.clustering_run_id == run.id,
                    ClusterEventProjection.cluster_anchor == after_key[0],
                    ClusterEventProjection.cluster_occurrence == after_key[1],
                )
            )
            for after_key in sorted(ambiguous_after_keys)
        }
        if any(existing_projections.values()):
            if not all(existing_projections.values()):
                raise RuntimeError(
                    f"Clustering Run ambiguous 投影只完成了一部分：run_id={run.id}"
                )
            expected_relations = {
                (
                    frozen_predecessors[before_key].event_id,
                    existing_projections[after_key].event_id,
                    frozen_predecessors[
                        before_key
                    ].after_evidence_fingerprint,
                    existing_projections[
                        after_key
                    ].after_evidence_fingerprint,
                )
                for before_key in ambiguous_before_keys
                for after_key in overlap_before_to_after[before_key]
                if after_key in ambiguous_after_keys
            }
            stored_relations = {
                (
                    lineage.source_event_id,
                    lineage.target_event_id,
                    lineage.before_evidence_fingerprint,
                    lineage.after_evidence_fingerprint,
                )
                for lineage in session.scalars(
                    select(EventLineage).where(
                        EventLineage.clustering_run_id == run.id,
                        EventLineage.relation_type == "ambiguous_from",
                    )
                )
            }
            if expected_relations != stored_relations or any(
                projection is None
                or projection.reconciliation_kind != "ambiguous"
                or projection.predecessor_projection_id is not None
                or projection.reconciliation_rule_version != run.rule_version
                for projection in existing_projections.values()
            ):
                raise RuntimeError(
                    f"Clustering Run ambiguous 投影审计链不完整：run_id={run.id}"
                )
            created.extend(
                projection
                for projection in existing_projections.values()
                if projection is not None
            )
        else:
            parent_events = list(
                session.scalars(
                    select(Event)
                    .where(Event.id.in_(parent_event_ids))
                    .order_by(Event.id)
                    .with_for_update()
                )
            )
            parents_by_id = {parent.id: parent for parent in parent_events}
            if len(parents_by_id) != len(parent_event_ids) or any(
                parents_by_id[predecessor.event_id].status != "active"
                or parents_by_id[predecessor.event_id].current_revision_id
                != predecessor.event_revision_id
                for predecessor in frozen_predecessors.values()
            ):
                raise RuntimeError(
                    f"Clustering Run ambiguous 冻结前驱已变化：run_id={run.id}"
                )

            ambiguous_projections: dict[
                SnapshotClusterKey, ClusterEventProjection
            ] = {}
            for after_key in sorted(ambiguous_after_keys):
                projection = _create_ambiguous_projection(
                    session,
                    run=run,
                    cluster=live_after[after_key],
                    cluster_anchor=after_key[0],
                    cluster_occurrence=after_key[1],
                )
                ambiguous_projections[after_key] = projection
                created.append(projection)
            for parent in parent_events:
                parent.status = "superseded"
                parent.superseded_at = datetime.now(timezone.utc)
            session.flush()
            session.add_all(
                EventLineage(
                    uid=str(uuid4()),
                    clustering_run_id=run.id,
                    relation_type="ambiguous_from",
                    source_event_id=(
                        frozen_predecessors[before_key].event_id
                    ),
                    target_event_id=ambiguous_projections[after_key].event_id,
                    rule_version=run.rule_version,
                    before_evidence_fingerprint=(
                        frozen_predecessors[
                            before_key
                        ].after_evidence_fingerprint
                    ),
                    after_evidence_fingerprint=(
                        ambiguous_projections[
                            after_key
                        ].after_evidence_fingerprint
                    ),
                    decision_reason=(
                        "overlap_without_unique_continuation_split_or_merge"
                    ),
                )
                for before_key in sorted(ambiguous_before_keys)
                for after_key in sorted(
                    overlap_before_to_after[before_key]
                )
                if after_key in ambiguous_after_keys
            )
            session.flush()
    for after_key, cluster in sorted(live_after.items()):
        if (
            after_key in split_after_keys
            or after_key in merge_after_keys
            or after_key in ambiguous_after_keys
            or after_key in bootstrap_after_keys
        ):
            continue
        existing = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == run_id,
                ClusterEventProjection.cluster_anchor == after_key[0],
                ClusterEventProjection.cluster_occurrence == after_key[1],
            )
        )
        if existing is not None:
            created.append(existing)
            continue

        predecessor_keys = after_to_before[after_key]
        if not predecessor_keys:
            created.append(
                _create_initial_projection(
                    session,
                    run=run,
                    cluster=cluster,
                    cluster_anchor=after_key[0],
                    cluster_occurrence=after_key[1],
                )
            )
            continue

        continuation = continuation_predecessors.get(after_key)
        if continuation is None:
            continue
        _before_key, predecessor = continuation
        projection = _continue_projection(
            session,
            run=run,
            cluster=cluster,
            cluster_anchor=after_key[0],
            cluster_occurrence=after_key[1],
            predecessor=predecessor,
        )
        if projection is not None:
            created.append(projection)
    return created


def latest_live_cluster_projection(
    session: Session,
    cluster_ids: set[int] | None = None,
):
    projection = aliased(ClusterEventProjection)
    dialect_name = session.get_bind().dialect.name
    if dialect_name == "postgresql":
        statement = (
            select(
                Cluster.id.label("cluster_id"),
                ClusterCurrentEventProjection.projection_id,
            )
            .select_from(Cluster)
            .join(
                ClusterCurrentEventProjection,
                ClusterCurrentEventProjection.cluster_id == Cluster.id,
            )
        )
    else:
        latest_id = (
            select(projection.id)
            .where(projection.cluster_id == Cluster.id)
            .order_by(projection.id.desc())
            .limit(1)
            .correlate(Cluster)
            .scalar_subquery()
        )
        statement = select(
            Cluster.id.label("cluster_id"),
            latest_id.label("projection_id"),
        ).where(latest_id.is_not(None))
    if cluster_ids is not None:
        statement = statement.where(Cluster.id.in_(cluster_ids))
    return statement.subquery()


def cluster_current_event_state_projection(session: Session):
    material_updates = event_material_update_projection()
    latest_projection = latest_live_cluster_projection(session)
    if session.get_bind().dialect.name == "postgresql":
        # OFFSET 0 keeps PostgreSQL from flattening this pointer-bounded lookup
        # into a scan of the multi-million-row projection history.
        current_projection = (
            select(ClusterEventProjection.event_id.label("event_id"))
            .where(ClusterEventProjection.id == latest_projection.c.projection_id)
            .offset(0)
            .lateral("current_event_projection")
        )
        projection_from = latest_projection.join(current_projection, true())
        projection_event_id = current_projection.c.event_id
    else:
        projection_from = latest_projection.join(
            ClusterEventProjection,
            ClusterEventProjection.id == latest_projection.c.projection_id,
        )
        projection_event_id = ClusterEventProjection.event_id
    return (
        select(
            latest_projection.c.cluster_id,
            latest_projection.c.projection_id,
            Event.id.label("event_id"),
            Event.uid.label("event_uid"),
            Event.current_revision_id,
            EventUserState.seen_revision_id,
            EventUserState.read_status,
            EventUserState.read_later,
            EventUserState.starred,
            EventUserState.uninterested,
            EventUserState.uninterested_reason,
            EventUserState.uninterested_note,
            EventUserState.uninterested_at,
            material_updates.c.material_update_revision_uid,
        )
        .select_from(projection_from)
        .join(Event, Event.id == projection_event_id)
        .outerjoin(EventUserState, EventUserState.event_id == Event.id)
        .outerjoin(material_updates, material_updates.c.event_id == Event.id)
        .where(Event.status == "active")
        .subquery()
    )


def latest_material_review_projection():
    ranked = (
        select(
            EvidenceReview.event_id,
            EvidenceReview.target_revision_id,
            EventRevision.uid.label("target_revision_uid"),
            EventRevision.revision_no.label("target_revision_no"),
            func.row_number()
            .over(
                partition_by=EvidenceReview.event_id,
                order_by=(
                    EventRevision.revision_no.desc(),
                    EvidenceReview.id.desc(),
                ),
            )
            .label("material_rank"),
        )
        .join(EventRevision, EventRevision.id == EvidenceReview.target_revision_id)
        .where(EvidenceReview.result == "material")
        .subquery()
    )
    return (
        select(
            ranked.c.event_id,
            ranked.c.target_revision_id,
            ranked.c.target_revision_uid,
            ranked.c.target_revision_no,
        )
        .where(ranked.c.material_rank == 1)
        .subquery()
    )


def event_material_update_projection():
    latest_material = latest_material_review_projection()
    seen_revision = aliased(EventRevision)
    return (
        select(
            Event.id.label("event_id"),
            case(
                (
                    and_(
                        seen_revision.id.is_not(None),
                        latest_material.c.target_revision_no
                        > seen_revision.revision_no,
                    ),
                    latest_material.c.target_revision_uid,
                ),
                else_=None,
            ).label("material_update_revision_uid"),
        )
        .outerjoin(EventUserState, EventUserState.event_id == Event.id)
        .outerjoin(
            seen_revision,
            seen_revision.id == EventUserState.seen_revision_id,
        )
        .outerjoin(latest_material, latest_material.c.event_id == Event.id)
        .subquery()
    )


def event_material_updates_for(
    session: Session, event_ids: list[int]
) -> dict[int, str | None]:
    if not event_ids:
        return {}
    material_updates = event_material_update_projection()
    return dict(
        session.execute(
            select(
                Event.id,
                material_updates.c.material_update_revision_uid,
            )
            .outerjoin(material_updates, material_updates.c.event_id == Event.id)
            .where(Event.id.in_(event_ids))
        ).all()
    )


def cluster_event_identities_for(
    session: Session, cluster_ids: list[int]
) -> dict[int, ClusterEventIdentity]:
    if not cluster_ids:
        return {}
    event_state = cluster_current_event_state_projection(session)
    seen_revision = aliased(EventRevision)
    rows = session.execute(
        select(
            event_state.c.cluster_id,
            event_state.c.projection_id,
            event_state.c.event_uid,
            EventRevision.uid,
            seen_revision.uid,
            event_state.c.read_status,
            event_state.c.read_later,
            event_state.c.starred,
            event_state.c.uninterested,
            event_state.c.uninterested_reason,
            event_state.c.uninterested_note,
            event_state.c.uninterested_at,
            event_state.c.material_update_revision_uid,
        )
        .select_from(event_state)
        .join(
            EventRevision,
            EventRevision.id == event_state.c.current_revision_id,
        )
        .outerjoin(
            seen_revision,
            seen_revision.id == event_state.c.seen_revision_id,
        )
        .where(event_state.c.cluster_id.in_(cluster_ids))
        .order_by(
            event_state.c.cluster_id.asc(),
            event_state.c.projection_id.desc(),
        )
    ).all()
    identities: dict[int, ClusterEventIdentity] = {}
    for (
        cluster_id,
        _mapping_id,
        event_uid,
        revision_uid,
        seen_revision_uid,
        read_status,
        read_later,
        starred,
        uninterested,
        uninterested_reason,
        uninterested_note,
        uninterested_at,
        material_update_revision_uid,
    ) in rows:
        if cluster_id is None or cluster_id in identities:
            continue
        identities[cluster_id] = ClusterEventIdentity(
            event_uid=event_uid,
            current_revision_uid=revision_uid,
            seen_revision_uid=seen_revision_uid,
            current_revision_differs_from_seen=(
                revision_uid != seen_revision_uid
            ),
            has_material_update=material_update_revision_uid is not None,
            material_update_revision_uid=material_update_revision_uid,
            read_status=read_status or "unread",
            read_later=bool(read_later),
            starred=bool(starred),
            uninterested=bool(uninterested),
            uninterested_reason=uninterested_reason,
            uninterested_note=uninterested_note,
            uninterested_at=uninterested_at,
        )
    return identities
