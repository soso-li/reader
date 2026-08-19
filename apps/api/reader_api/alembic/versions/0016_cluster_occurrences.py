"""Preserve duplicate evidence and structurally identical clusters."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0016_cluster_occurrences"
down_revision: str = "0015_clustering_runs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "clustering_run_memberships",
        sa.Column(
            "cluster_occurrence",
            sa.Integer(),
            server_default="1",
            nullable=False,
        ),
    )
    op.add_column(
        "clustering_run_memberships",
        sa.Column(
            "evidence_occurrence",
            sa.Integer(),
            server_default="1",
            nullable=False,
        ),
    )
    op.drop_constraint(
        "uq_clustering_run_membership_evidence",
        "clustering_run_memberships",
        type_="unique",
    )
    op.create_check_constraint(
        "ck_clustering_run_membership_occurrence",
        "clustering_run_memberships",
        "cluster_occurrence >= 1 AND evidence_occurrence >= 1",
    )
    op.create_unique_constraint(
        "uq_clustering_run_membership_evidence",
        "clustering_run_memberships",
        [
            "run_id",
            "snapshot_phase",
            "cluster_anchor",
            "cluster_occurrence",
            "evidence_anchor",
            "evidence_occurrence",
        ],
    )


def downgrade() -> None:
    raise RuntimeError(
        "Clustering Run occurrence 快照不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
