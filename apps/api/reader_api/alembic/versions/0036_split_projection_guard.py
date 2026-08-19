"""Revalidate a current split when any later projection row is inserted."""

from __future__ import annotations

from alembic import op


revision: str = "0036_split_projection_guard"
down_revision: str | None = "0035_split_transaction_integrity"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION reader_split_terminal_projection_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            PERFORM reader_require_complete_split_projection(NEW);
            PERFORM reader_validate_current_split_for_event(NEW.event_id);
            RETURN NULL;
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event split projection 的提交终态补充校验不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
