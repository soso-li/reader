"""Require published synthesis versions to cover multiple sources."""

from __future__ import annotations

from alembic import op


revision: str = "0050_synthesis_source_count"
down_revision: str | None = "0049_event_synthesis"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.drop_constraint(
        "ck_synthesis_version_source_count",
        "synthesis_versions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_synthesis_version_source_count",
        "synthesis_versions",
        "source_count >= 2",
    )


def downgrade() -> None:
    raise RuntimeError(
        "P0.3 多来源合成稿契约不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
