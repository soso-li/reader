"""Represent the exact legacy schema found in the stable production database."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0001_production_legacy"
down_revision: str | None = "0000_strict_legacy"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "content_items",
        "media_url",
        existing_type=sa.Text(),
        nullable=True,
    )
    op.alter_column(
        "content_items",
        "media_kind",
        existing_type=sa.String(length=32),
        nullable=True,
    )


def downgrade() -> None:
    raise RuntimeError(
        "production legacy revision 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
