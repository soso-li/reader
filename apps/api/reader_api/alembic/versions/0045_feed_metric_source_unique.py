"""Make one FeedMetric row authoritative for each Source."""

from __future__ import annotations

from alembic import op


revision: str = "0045_feed_metric_source_unique"
down_revision: str | None = "0044_ambiguous_upgrade_audit"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
        WITH merged AS (
            SELECT source_id,
                   min(id) AS keeper_id,
                   sum(fetched_count) AS fetched_count,
                   sum(read_count) AS read_count,
                   sum(opened_count) AS opened_count,
                   sum(starred_count) AS starred_count,
                   sum(read_later_count) AS read_later_count,
                   sum(cluster_count) AS cluster_count,
                   sum(duplicate_count) AS duplicate_count,
                   max(updated_at) AS updated_at
            FROM feed_metrics
            GROUP BY source_id
        )
        UPDATE feed_metrics metric
        SET fetched_count = merged.fetched_count,
            read_count = merged.read_count,
            opened_count = merged.opened_count,
            starred_count = merged.starred_count,
            read_later_count = merged.read_later_count,
            cluster_count = merged.cluster_count,
            duplicate_count = merged.duplicate_count,
            updated_at = merged.updated_at
        FROM merged
        WHERE metric.id = merged.keeper_id;

        DELETE FROM feed_metrics metric
        USING (
            SELECT source_id, min(id) AS keeper_id
            FROM feed_metrics
            GROUP BY source_id
        ) keeper
        WHERE metric.source_id = keeper.source_id
          AND metric.id <> keeper.keeper_id;
        """
    )
    op.create_unique_constraint(
        "uq_feed_metric_source",
        "feed_metrics",
        ["source_id"],
    )


def downgrade() -> None:
    raise RuntimeError(
        "FeedMetric 来源唯一性不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
