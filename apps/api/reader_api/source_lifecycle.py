from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from .cluster import decluster_source_items
from .clustering_run import (
    clustering_run_execution_lock,
    defer_clustering_run_lock_until_transaction_end,
)
from .models import (
    ContentItem,
    DELETED_SOURCE_STATUS,
    FilterMatch,
    FilterRule,
    Source,
    now_utc,
)


def tombstone_source(session: Session, source_id: int) -> bool:
    with clustering_run_execution_lock(session):
        defer_clustering_run_lock_until_transaction_end(session)
        source = session.get(
            Source,
            source_id,
            populate_existing=True,
            with_for_update=True,
        )
        if source is None or source.status == DELETED_SOURCE_STATUS:
            return False
        source.status = DELETED_SOURCE_STATUS
        source.enabled = False
        source.folder_id = None
        source.status_changed_at = now_utc()
        if source.external_generation_allowed:
            source.external_generation_allowed = False
            source.generation_policy_version += 1
        rule_ids = list(
            session.scalars(
                select(FilterRule.id).where(FilterRule.source_id == source.id)
            )
        )
        session.execute(
            delete(FilterMatch).where(
                FilterMatch.content_item_id.in_(
                    select(ContentItem.id).where(ContentItem.source_id == source.id)
                )
            )
        )
        if rule_ids:
            session.execute(delete(FilterRule).where(FilterRule.id.in_(rule_ids)))
        decluster_source_items(session, source.id)
        session.commit()
        return True
