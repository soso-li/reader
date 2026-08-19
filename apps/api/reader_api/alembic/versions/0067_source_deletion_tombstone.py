"""Allow irreversible source tombstones while preserving immutable history."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0067_source_deletion_tombstone"
down_revision: str | None = "0066_content_filter_projection"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.drop_constraint("sources_url_key", "sources", type_="unique")
    op.create_index(
        "uq_sources_live_url",
        "sources",
        ["url"],
        unique=True,
        postgresql_where=sa.text("status <> 'deleted'"),
    )


def downgrade() -> None:
    raise RuntimeError(
        "订阅源删除墓碑不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
