"""Index merge projections used by transaction terminal guards."""

from __future__ import annotations

from alembic import op


revision: str = "0040_merge_transaction_index"
down_revision: str | None = "0039_event_merge_lineage"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX ix_cluster_event_projection_merge_created_transaction "
        "ON cluster_event_projections (created_transaction_id, event_id) "
        "WHERE reconciliation_kind = 'merged'"
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event merge 事务终态索引不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
