from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .models import ClusterItem, ContentItem, FeedMetric, Source, UserState, now_utc


def locked_feed_metric(session: Session, source_id: int) -> tuple[Source, FeedMetric]:
    """Serialize all metric writers through the Source row.

    The durable Source lock keeps metric deltas ordered and makes creation deterministic;
    the database also enforces at most one FeedMetric row per source.
    """

    source = session.scalar(
        select(Source).where(Source.id == source_id).with_for_update()
    )
    if source is None:
        raise RuntimeError(f"来源不存在：{source_id}")
    metric = session.scalar(
        select(FeedMetric)
        .where(FeedMetric.source_id == source_id)
        .with_for_update()
    )
    if metric is None:
        metric = FeedMetric(
            source_id=source_id,
            fetched_count=0,
            read_count=0,
            opened_count=0,
            starred_count=0,
            read_later_count=0,
            cluster_count=0,
            duplicate_count=0,
        )
        session.add(metric)
        session.flush()
    return source, metric


def source_cluster_counts(session: Session, source_id: int) -> tuple[int, int]:
    cluster_count = (
        session.scalar(
            select(func.count(func.distinct(ClusterItem.cluster_id)))
            .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
            .where(ContentItem.source_id == source_id)
        )
        or 0
    )
    cluster_sizes = (
        select(
            ClusterItem.cluster_id.label("cluster_id"),
            func.count(ClusterItem.id).label("item_count"),
        )
        .group_by(ClusterItem.cluster_id)
        .subquery()
    )
    duplicate_count = (
        session.scalar(
            select(func.count(ClusterItem.id))
            .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
            .join(
                cluster_sizes,
                cluster_sizes.c.cluster_id == ClusterItem.cluster_id,
            )
            .where(
                ContentItem.source_id == source_id,
                cluster_sizes.c.item_count > 1,
            )
        )
        or 0
    )
    return int(cluster_count), int(duplicate_count)


def feed_trust_score(
    metric: FeedMetric,
    cluster_count: int = 0,
    duplicate_count: int = 0,
) -> float:
    fetched = max(metric.fetched_count or 0, 1)
    value = (
        (metric.read_count or 0)
        + (metric.opened_count or 0) * 2
        + (metric.starred_count or 0) * 3
        + (metric.read_later_count or 0)
        + cluster_count
        - duplicate_count
    ) * 100 / fetched
    value = max(value, 0.0)
    return round(min(value, 100.0), 1)


def refresh_source_trust_score(
    session: Session,
    source: Source,
    metric: FeedMetric,
) -> None:
    cluster_count, duplicate_count = source_cluster_counts(session, source.id)
    source.feed_trust_score = feed_trust_score(
        metric,
        cluster_count,
        duplicate_count,
    )
    metric.updated_at = now_utc()


def project_item_user_state_metrics(
    session: Session,
    source_id: int,
    state: UserState,
    previous: tuple[str, bool, bool],
    *,
    locked: tuple[Source, FeedMetric] | None = None,
) -> dict[str, int]:
    """Project one item state transition and return its auditable deltas."""

    source, metric = locked or locked_feed_metric(session, source_id)
    previous_status, previous_read_later, previous_starred = previous
    deltas: dict[str, int] = {}

    def apply_delta(field: str, delta: int) -> None:
        current = int(getattr(metric, field) or 0)
        setattr(metric, field, max(current + delta, 0))
        deltas[field] = int(getattr(metric, field) or 0) - current

    previous_read = previous_status in {"summary_seen", "original_opened"}
    current_read = state.read_status in {"summary_seen", "original_opened"}
    if current_read != previous_read:
        apply_delta("read_count", 1 if current_read else -1)

    previous_opened = previous_status == "original_opened"
    current_opened = state.read_status == "original_opened"
    if current_opened != previous_opened:
        apply_delta("opened_count", 1 if current_opened else -1)

    if state.starred != previous_starred:
        apply_delta("starred_count", 1 if state.starred else -1)
    if state.read_later != previous_read_later:
        apply_delta("read_later_count", 1 if state.read_later else -1)

    refresh_source_trust_score(session, source, metric)
    return deltas
