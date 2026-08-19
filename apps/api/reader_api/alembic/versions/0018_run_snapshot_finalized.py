"""Require finalized after snapshots before a clustering run completes."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0018_run_snapshot_finalized"
down_revision: str = "0017_scope_occurrence_lock"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "clustering_runs",
        sa.Column(
            "after_snapshot_finalized",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.execute("DROP TRIGGER trg_clustering_run_lifecycle ON clustering_runs")
    op.execute(
        "UPDATE clustering_runs SET after_snapshot_finalized = true "
        "WHERE status = 'completed'"
    )
    op.create_check_constraint(
        "ck_clustering_run_snapshot_finalized",
        "clustering_runs",
        "after_snapshot_finalized = (status = 'completed')",
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION reader_clustering_run_lifecycle_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.status <> 'started' OR NEW.after_snapshot_finalized THEN
                    RAISE EXCEPTION 'clustering_run_insert_started: Clustering Run 必须从未完成快照的 started 状态创建';
                END IF;
                RETURN NEW;
            END IF;
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
            IF NEW.status = 'completed' AND NOT NEW.after_snapshot_finalized THEN
                RAISE EXCEPTION 'clustering_run_snapshot_incomplete: completed 必须声明 after 快照已完成';
            END IF;
            RETURN NEW;
        END
        $reader$;
        CREATE TRIGGER trg_clustering_run_lifecycle
        BEFORE INSERT OR UPDATE OR DELETE ON clustering_runs
        FOR EACH ROW EXECUTE FUNCTION reader_clustering_run_lifecycle_guard();
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Clustering Run 完成快照证明不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
