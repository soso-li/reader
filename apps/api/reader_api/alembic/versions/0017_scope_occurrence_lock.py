"""Preserve scope multiplicity and serialize snapshot finalization."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0017_scope_occurrence_lock"
down_revision: str = "0016_cluster_occurrences"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "clustering_run_scope_evidence",
        sa.Column(
            "evidence_occurrence",
            sa.Integer(),
            server_default="1",
            nullable=False,
        ),
    )
    op.drop_constraint(
        "uq_clustering_run_scope_evidence",
        "clustering_run_scope_evidence",
        type_="unique",
    )
    op.create_check_constraint(
        "ck_clustering_run_scope_occurrence",
        "clustering_run_scope_evidence",
        "evidence_occurrence >= 1",
    )
    op.create_unique_constraint(
        "uq_clustering_run_scope_evidence",
        "clustering_run_scope_evidence",
        ["run_id", "evidence_anchor", "evidence_occurrence"],
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION reader_clustering_snapshot_immutable()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            run_status text;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                SELECT status INTO run_status
                FROM clustering_runs
                WHERE id = NEW.run_id
                FOR UPDATE;
                IF run_status IS DISTINCT FROM 'started' THEN
                    RAISE EXCEPTION 'clustering_snapshot_terminal: 终态 Clustering Run 不可追加 scope 或 membership';
                END IF;
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'clustering_snapshot_immutable: Clustering Run scope 与 membership 快照不可修改或删除';
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Clustering Run scope occurrence 与终态互斥不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
