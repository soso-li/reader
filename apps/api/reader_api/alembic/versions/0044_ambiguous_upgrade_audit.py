"""Reject incomplete ambiguous runs created before the run guard existed."""

from __future__ import annotations

from alembic import op


revision: str = "0044_ambiguous_upgrade_audit"
down_revision: str | None = "0043_ambiguous_run_guard"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        r"""
        DO $reader$
        DECLARE
            invalid_run_id varchar;
        BEGIN
            SELECT run.id INTO invalid_run_id
            FROM clustering_runs run
            WHERE run.status = 'completed'
              AND run.after_snapshot_finalized
              AND EXISTS (
                    SELECT 1
                    FROM (
                        SELECT DISTINCT after_member.cluster_anchor,
                                        after_member.cluster_occurrence
                        FROM clustering_run_memberships after_member
                        WHERE after_member.run_id = run.id
                          AND after_member.snapshot_phase = 'after'
                    ) after_cluster
                    WHERE reader_expected_ambiguous_after(
                              run.id,
                              after_cluster.cluster_anchor,
                              after_cluster.cluster_occurrence
                          )
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM cluster_event_projections projection
                    WHERE projection.clustering_run_id = run.id
                      AND projection.reconciliation_kind = 'ambiguous'
              )
            ORDER BY run.id
            LIMIT 1;

            IF FOUND THEN
                RAISE EXCEPTION 'event_ambiguous_run_upgrade_requires_complete_graph: completed run % 在 run-level guard 安装前缺少 sealed-topology ambiguous graph；请恢复迁移前备份或人工审计', invalid_run_id;
            END IF;
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "ambiguous completed-run 升级审计不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
