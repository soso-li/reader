"""Index cluster stream ordering and content item source lookups."""

import sqlalchemy as sa
from alembic import op


revision: str = "0075_stream_hot_path_indexes"
down_revision: str | None = "0074_event_evidence_reading_html"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_index(
        "ix_clusters_stream_order",
        "clusters",
        [sa.text("first_seen_at DESC NULLS LAST"), sa.text("id DESC")],
    )
    op.create_index(
        "ix_content_items_source_id",
        "content_items",
        ["source_id"],
    )
    op.create_index(
        "ix_cluster_items_content_item_id",
        "cluster_items",
        ["content_item_id"],
    )


def downgrade() -> None:
    raise RuntimeError("0075 索引迁移不支持原地 downgrade；回滚必须恢复迁移前备份")
