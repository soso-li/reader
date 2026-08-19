"""Add the nullable Source Entry identity expansion schema."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0004_source_entry_expand"
down_revision: str = "0003_maintenance_runs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "source_entry_identities",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column(
            "current_revision_no",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column(
            "projection_pending",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"]),
        sa.UniqueConstraint(
            "id",
            "source_id",
            name="uq_source_entry_identity_id_source",
        ),
    )
    op.create_table(
        "source_entry_keys",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_entry_id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("identity_kind", sa.String(length=20), nullable=False),
        sa.Column("identity_key", sa.String(length=80), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["source_entry_id", "source_id"],
            ["source_entry_identities.id", "source_entry_identities.source_id"],
            name="fk_source_entry_key_identity_source",
        ),
        sa.UniqueConstraint(
            "source_id",
            "identity_kind",
            "identity_key",
            name="uq_source_entry_key_source_kind_value",
        ),
    )
    op.create_table(
        "source_entry_relations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_entry_id", sa.Integer(), nullable=False),
        sa.Column("canonical_source_entry_id", sa.Integer(), nullable=False),
        sa.Column("relation_type", sa.String(length=40), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rule_version", sa.String(length=80), nullable=False),
        sa.Column(
            "active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["source_entry_id"],
            ["source_entry_identities.id"],
        ),
        sa.ForeignKeyConstraint(
            ["canonical_source_entry_id"],
            ["source_entry_identities.id"],
        ),
        sa.UniqueConstraint(
            "source_entry_id",
            "canonical_source_entry_id",
            "relation_type",
            "rule_version",
            name="uq_source_entry_relation_rule",
        ),
    )
    op.add_column(
        "raw_entries",
        sa.Column("source_entry_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "raw_entries",
        sa.Column("revision_no", sa.Integer(), nullable=True),
    )
    op.add_column(
        "raw_entries",
        sa.Column("payload_fingerprint", sa.String(length=64), nullable=True),
    )
    op.create_foreign_key(
        "fk_raw_entry_identity_source",
        "raw_entries",
        "source_entry_identities",
        ["source_entry_id", "source_id"],
        ["id", "source_id"],
    )


def downgrade() -> None:
    raise RuntimeError(
        "Source Entry expand migration 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
