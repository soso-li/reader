"""Make the frozen ambiguous upgrade audit linear for continued clusters."""

from __future__ import annotations

from alembic import op

from reader_api.migrations.ambiguous_audit_fast_path import (
    AMBIGUOUS_AFTER_FAST_PATH_SQL,
    AMBIGUOUS_MEMBERSHIP_INDEX_SQL,
)


revision: str = "0048_ambiguous_audit_fast_path"
down_revision: str | None = "0047_event_authority_contract"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(AMBIGUOUS_MEMBERSHIP_INDEX_SQL)
    op.execute(AMBIGUOUS_AFTER_FAST_PATH_SQL)


def downgrade() -> None:
    raise RuntimeError(
        "ambiguous audit fast path 不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
