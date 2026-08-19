"""Add durable audit records for explicit maintenance operations."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0003_maintenance_runs"
down_revision: str = "0002_legacy_schema_head"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "maintenance_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("operation_type", sa.String(length=80), nullable=False),
        sa.Column("start_status", sa.String(length=40), nullable=False),
        sa.Column("end_status", sa.String(length=40), nullable=False),
        sa.Column("processed_count", sa.Integer(), nullable=False),
        sa.Column("failure_info", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    raise RuntimeError(
        "maintenance audit migration 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
