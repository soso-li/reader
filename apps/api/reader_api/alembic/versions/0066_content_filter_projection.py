"""Add personal keyword filter rules and materialized matches."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0066_content_filter_projection"
down_revision: str | None = "0065_ambiguous_topology_plan"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "filter_rules",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=True),
        sa.Column("match_type", sa.String(length=20), nullable=False),
        sa.Column("pattern", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "match_type IN ('literal', 'regex')",
            name="ck_filter_rule_match_type",
        ),
        sa.CheckConstraint(
            "length(trim(pattern)) > 0",
            name="ck_filter_rule_pattern_present",
        ),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "filter_matches",
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column("content_item_id", sa.Integer(), nullable=False),
        sa.Column("matched_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["rule_id"], ["filter_rules.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["content_item_id"], ["content_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("rule_id", "content_item_id"),
    )
    op.create_index(
        "ix_filter_matches_content_item_id",
        "filter_matches",
        ["content_item_id"],
    )


def downgrade() -> None:
    raise RuntimeError(
        "关键词过滤投影不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
