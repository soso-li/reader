"""Reject split graphs that used the pre-0036 projection trigger hole."""

from __future__ import annotations

from alembic import op


revision: str = "0037_split_projection_audit"
down_revision: str | None = "0036_split_projection_guard"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM cluster_event_projections split_projection
                JOIN cluster_event_projections extra_projection
                  ON extra_projection.event_id = split_projection.event_id
                 AND extra_projection.created_transaction_id
                       = split_projection.created_transaction_id
                 AND extra_projection.id <> split_projection.id
                WHERE split_projection.reconciliation_kind = 'split'
            ) THEN
                RAISE EXCEPTION 'event_split_projection_upgrade_requires_terminal_graph: 0036 以前的 split child 在创建事务中已有额外 projection，必须恢复或人工审计';
            END IF;
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event split projection 历史图审计不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
