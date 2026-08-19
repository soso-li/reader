"""Add immutable Clustering Run scope and membership snapshots."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0015_clustering_runs"
down_revision: str = "0014_identity_key_kind_unique"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "clustering_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("scope_type", sa.String(length=80), nullable=False),
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("rule_version", sa.String(length=120), nullable=False),
        sa.Column(
            "status",
            sa.String(length=20),
            server_default="started",
            nullable=False,
        ),
        sa.Column(
            "failure_info", sa.Text(), server_default="", nullable=False
        ),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('started', 'completed', 'failed')",
            name="ck_clustering_run_status",
        ),
        sa.CheckConstraint(
            "scope_key ~ '^[0-9a-f]{64}$'",
            name="ck_clustering_run_scope_key",
        ),
        sa.CheckConstraint(
            "length(btrim(scope_type)) > 0 AND length(btrim(rule_version)) > 0",
            name="ck_clustering_run_identity_fields",
        ),
        sa.CheckConstraint(
            "(status = 'started' AND completed_at IS NULL AND failed_at IS NULL) OR "
            "(status = 'completed' AND completed_at IS NOT NULL AND failed_at IS NULL) OR "
            "(status = 'failed' AND completed_at IS NULL AND failed_at IS NOT NULL)",
            name="ck_clustering_run_terminal_time",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_clustering_runs_status_started_at",
        "clustering_runs",
        ["status", "started_at"],
    )
    op.create_table(
        "clustering_run_scope_evidence",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("evidence_anchor", sa.String(length=160), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["clustering_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id",
            "evidence_anchor",
            name="uq_clustering_run_scope_evidence",
        ),
    )
    op.create_table(
        "clustering_run_memberships",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("snapshot_phase", sa.String(length=16), nullable=False),
        sa.Column("cluster_anchor", sa.String(length=64), nullable=False),
        sa.Column("evidence_anchor", sa.String(length=160), nullable=False),
        sa.CheckConstraint(
            "snapshot_phase IN ('before', 'after')",
            name="ck_clustering_run_membership_phase",
        ),
        sa.CheckConstraint(
            "cluster_anchor ~ '^[0-9a-f]{64}$'",
            name="ck_clustering_run_cluster_anchor",
        ),
        sa.ForeignKeyConstraint(["run_id"], ["clustering_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id",
            "snapshot_phase",
            "evidence_anchor",
            name="uq_clustering_run_membership_evidence",
        ),
    )
    op.create_index(
        "ix_clustering_run_membership_cluster",
        "clustering_run_memberships",
        ["run_id", "snapshot_phase", "cluster_anchor"],
    )
    op.execute(
        """
        CREATE FUNCTION reader_clustering_run_lifecycle_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'clustering_run_immutable: Clustering Run 不可删除';
            END IF;
            IF OLD.status <> 'started' THEN
                RAISE EXCEPTION 'clustering_run_terminal: 终态 Clustering Run 不可修改';
            END IF;
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.scope_type IS DISTINCT FROM OLD.scope_type
               OR NEW.scope_key IS DISTINCT FROM OLD.scope_key
               OR NEW.rule_version IS DISTINCT FROM OLD.rule_version
               OR NEW.started_at IS DISTINCT FROM OLD.started_at THEN
                RAISE EXCEPTION 'clustering_run_identity_immutable: 运行身份、scope、规则与开始时间不可修改';
            END IF;
            IF NEW.status NOT IN ('completed', 'failed') THEN
                RAISE EXCEPTION 'clustering_run_transition: started 只能进入 completed 或 failed';
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE TRIGGER trg_clustering_run_lifecycle
        BEFORE UPDATE OR DELETE ON clustering_runs
        FOR EACH ROW EXECUTE FUNCTION reader_clustering_run_lifecycle_guard();

        CREATE FUNCTION reader_clustering_snapshot_immutable()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            run_status text;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                SELECT status INTO run_status
                FROM clustering_runs
                WHERE id = NEW.run_id;
                IF run_status IS DISTINCT FROM 'started' THEN
                    RAISE EXCEPTION 'clustering_snapshot_terminal: 终态 Clustering Run 不可追加 scope 或 membership';
                END IF;
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'clustering_snapshot_immutable: Clustering Run scope 与 membership 快照不可修改或删除';
        END
        $reader$;

        CREATE TRIGGER trg_clustering_scope_immutable
        BEFORE INSERT OR UPDATE OR DELETE ON clustering_run_scope_evidence
        FOR EACH ROW EXECUTE FUNCTION reader_clustering_snapshot_immutable();

        CREATE TRIGGER trg_clustering_membership_immutable
        BEFORE INSERT OR UPDATE OR DELETE ON clustering_run_memberships
        FOR EACH ROW EXECUTE FUNCTION reader_clustering_snapshot_immutable();
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Clustering Run 不可变快照不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
