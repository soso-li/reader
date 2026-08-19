"""Persist successful RSS validators for unchanged-feed short-circuiting."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0071_source_fetch_validators"
down_revision: str | None = "0070_uninterested_projection"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    with op.batch_alter_table("sources") as batch:
        batch.add_column(sa.Column("fetch_etag", sa.Text(), nullable=True))
        batch.add_column(sa.Column("fetch_last_modified", sa.Text(), nullable=True))
        batch.add_column(
            sa.Column(
                "last_successful_payload_hash",
                sa.String(length=64),
                nullable=True,
            )
        )


def downgrade() -> None:
    raise RuntimeError(
        "RSS 抓取校验状态不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
