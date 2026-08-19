"""Allow an Event evidence fingerprint to recur in a later Revision."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0030_event_revision_recurrence"
down_revision: str = "0029_run_projection_predecessors"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET CONSTRAINTS ALL IMMEDIATE")
    op.drop_constraint(
        "uq_event_revision_fingerprint",
        "event_revisions",
        type_="unique",
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event Revision fingerprint recurrence 不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
