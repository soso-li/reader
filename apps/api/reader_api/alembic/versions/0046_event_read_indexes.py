"""Index stable Event read projection lookups."""

from __future__ import annotations

from alembic import op


revision: str = "0046_event_read_indexes"
down_revision: str | None = "0045_feed_metric_source_unique"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_index(
        "ix_cluster_event_projections_cluster_snapshot_id",
        "cluster_event_projections",
        ["cluster_id_snapshot", "id"],
    )
    op.create_index(
        "ix_cluster_event_projections_event_id",
        "cluster_event_projections",
        ["event_id", "id"],
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event Read API 索引不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
