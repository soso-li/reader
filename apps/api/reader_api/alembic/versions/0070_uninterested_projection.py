"""Add recoverable uninterested projections for Events and standalone items."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0070_uninterested_projection"
down_revision: str | None = "0069_folder_media_types"
branch_labels: str | None = None
depends_on: str | None = None


REASON_CHECK = (
    "uninterested_reason IS NULL OR uninterested_reason IN "
    "('promotion', 'repetitive', 'topic', 'low_quality', 'other')"
)
SHAPE_CHECK = (
    "(uninterested = false AND uninterested_reason IS NULL "
    "AND uninterested_note IS NULL AND uninterested_at IS NULL) OR "
    "(uninterested = true AND uninterested_at IS NOT NULL "
    "AND ((uninterested_reason = 'other' "
    "AND length(trim(uninterested_note)) > 0) OR "
    "(uninterested_reason <> 'other' AND uninterested_note IS NULL) OR "
    "(uninterested_reason IS NULL AND uninterested_note IS NULL)))"
)


def upgrade() -> None:
    _add_projection_columns(
        "event_user_states",
        reason_constraint="ck_event_user_state_uninterested_reason",
        shape_constraint="ck_event_user_state_uninterested_shape",
    )
    _add_projection_columns(
        "user_states",
        reason_constraint="ck_user_state_uninterested_reason",
        shape_constraint="ck_user_state_uninterested_shape",
    )
    op.create_index(
        "ix_event_user_states_uninterested_at",
        "event_user_states",
        ["uninterested_at"],
    )
    op.create_index(
        "ix_user_states_uninterested_at",
        "user_states",
        ["uninterested_at"],
    )


def _add_projection_columns(
    table_name: str,
    *,
    reason_constraint: str,
    shape_constraint: str,
) -> None:
    with op.batch_alter_table(table_name) as batch:
        batch.add_column(
            sa.Column(
                "uninterested",
                sa.Boolean(),
                server_default=sa.text("false"),
                nullable=False,
            )
        )
        batch.add_column(
            sa.Column("uninterested_reason", sa.String(length=40), nullable=True)
        )
        batch.add_column(sa.Column("uninterested_note", sa.Text(), nullable=True))
        batch.add_column(
            sa.Column(
                "uninterested_at",
                sa.DateTime(timezone=True),
                nullable=True,
            )
        )
        batch.create_check_constraint(reason_constraint, REASON_CHECK)
        batch.create_check_constraint(shape_constraint, SHAPE_CHECK)


def downgrade() -> None:
    raise RuntimeError(
        "不感兴趣投影不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
