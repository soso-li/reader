"""Align the strict legacy snapshot with the canonical legacy baseline."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0001_legacy_baseline"
down_revision: str | None = "0000_strict_legacy"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "clusters",
        "generated_content",
        existing_type=sa.Text(),
        nullable=True,
    )
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
    op.alter_column(
        "content_items",
        "embedding_model",
        existing_type=sa.String(length=120),
        nullable=True,
    )


def downgrade() -> None:
    raise RuntimeError(
        "legacy baseline 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
