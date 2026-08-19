"""Seal the exact Event projection mapped by each before Cluster."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0029_run_projection_predecessors"
down_revision: str = "0028_event_continuation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


SHA256_CHECK = "{column} ~ '^[0-9a-f]{{64}}$'"


def upgrade() -> None:
    op.create_table(
        "clustering_run_projection_predecessors",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("cluster_anchor", sa.String(length=64), nullable=False),
        sa.Column("cluster_occurrence", sa.Integer(), nullable=False),
        sa.Column("predecessor_projection_id", sa.Integer(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            SHA256_CHECK.format(column="cluster_anchor"),
            name="ck_clustering_run_projection_predecessor_anchor_sha256",
        ),
        sa.CheckConstraint(
            "cluster_occurrence >= 1",
            name="ck_clustering_run_projection_predecessor_occurrence",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["clustering_runs.id"],
            name="fk_clustering_run_projection_predecessor_run",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["predecessor_projection_id"],
            ["cluster_event_projections.id"],
            name="fk_clustering_run_projection_predecessor_mapping",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id",
            "cluster_anchor",
            "cluster_occurrence",
            name="uq_clustering_run_projection_predecessor_cluster",
        ),
        sa.UniqueConstraint(
            "run_id",
            "predecessor_projection_id",
            name="uq_clustering_run_projection_predecessor_mapping",
        ),
    )
    op.create_index(
        "ix_clustering_run_projection_predecessors_mapping",
        "clustering_run_projection_predecessors",
        ["predecessor_projection_id"],
    )

    op.execute(
        """
        CREATE FUNCTION reader_clustering_run_projection_predecessor_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            target_status text;
            target_started_at timestamptz;
            membership_exists boolean;
            predecessor_is_prior boolean;
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION 'clustering_run_projection_predecessor_immutable: before projection 旁证不可修改或删除';
            END IF;

            SELECT status, started_at
            INTO target_status, target_started_at
            FROM clustering_runs
            WHERE id = NEW.run_id
            FOR UPDATE;
            IF target_status IS DISTINCT FROM 'started' THEN
                RAISE EXCEPTION 'clustering_run_projection_predecessor_closed: 只能为 started run 记录 before projection';
            END IF;
            IF EXISTS (
                SELECT 1 FROM clustering_run_snapshot_seals
                WHERE run_id = NEW.run_id AND snapshot_phase = 'before'
            ) THEN
                RAISE EXCEPTION 'clustering_run_projection_predecessor_sealed: before seal 后不可追加 projection 旁证';
            END IF;

            SELECT EXISTS (
                SELECT 1
                FROM clustering_run_memberships membership
                WHERE membership.run_id = NEW.run_id
                  AND membership.snapshot_phase = 'before'
                  AND membership.cluster_anchor = NEW.cluster_anchor
                  AND membership.cluster_occurrence = NEW.cluster_occurrence
            ) INTO membership_exists;
            IF membership_exists IS DISTINCT FROM true THEN
                RAISE EXCEPTION 'clustering_run_projection_predecessor_membership_missing: projection 旁证必须对应 before snapshot Cluster';
            END IF;

            SELECT EXISTS (
                SELECT 1
                FROM cluster_event_projections projection
                JOIN clustering_runs predecessor_run
                  ON predecessor_run.id = projection.clustering_run_id
                WHERE projection.id = NEW.predecessor_projection_id
                  AND predecessor_run.status = 'completed'
                  AND predecessor_run.completed_at <= target_started_at
            ) INTO predecessor_is_prior;
            IF predecessor_is_prior IS DISTINCT FROM true THEN
                RAISE EXCEPTION 'clustering_run_projection_predecessor_not_prior: projection 旁证必须来自 run 开始前已完成的映射';
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE TRIGGER trg_clustering_run_projection_predecessor_guard
        BEFORE INSERT OR UPDATE OR DELETE
        ON clustering_run_projection_predecessors
        FOR EACH ROW
        EXECUTE FUNCTION reader_clustering_run_projection_predecessor_guard();
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Clustering Run predecessor 旁证不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
