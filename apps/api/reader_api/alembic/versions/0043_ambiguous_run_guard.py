"""Require the complete ambiguous graph when a sealed run commits."""

from __future__ import annotations

from alembic import op


revision: str = "0043_ambiguous_run_guard"
down_revision: str | None = "0042_ambiguous_topology_guard"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        r"""
        CREATE FUNCTION reader_require_complete_ambiguous_run(
            run_id_value varchar
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            candidate cluster_event_projections%ROWTYPE;
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM clustering_runs run
                WHERE run.id = run_id_value
                  AND run.status = 'completed'
                  AND run.after_snapshot_finalized
            ) THEN
                RETURN;
            END IF;
            IF NOT EXISTS (
                SELECT 1
                FROM (
                    SELECT DISTINCT after_member.cluster_anchor,
                                    after_member.cluster_occurrence
                    FROM clustering_run_memberships after_member
                    WHERE after_member.run_id = run_id_value
                      AND after_member.snapshot_phase = 'after'
                ) after_cluster
                WHERE reader_expected_ambiguous_after(
                          run_id_value,
                          after_cluster.cluster_anchor,
                          after_cluster.cluster_occurrence
                      )
            ) THEN
                RETURN;
            END IF;

            SELECT stored.* INTO candidate
            FROM cluster_event_projections stored
            WHERE stored.clustering_run_id = run_id_value
              AND stored.reconciliation_kind = 'ambiguous'
            ORDER BY stored.id
            LIMIT 1;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'event_ambiguous_terminal_graph_incomplete: completed run 必须在同一事务提交全部 sealed-topology ambiguous Event、Projection、父级 supersede 与可证明 overlap Lineage';
            END IF;
            PERFORM reader_require_complete_ambiguous_projection(candidate);
        END
        $reader$;

        CREATE FUNCTION reader_ambiguous_terminal_run_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            PERFORM reader_require_complete_ambiguous_run(NEW.id);
            RETURN NULL;
        END
        $reader$;

        CREATE CONSTRAINT TRIGGER trg_ambiguous_terminal_run
        AFTER UPDATE OF status, after_snapshot_finalized ON clustering_runs
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        EXECUTE FUNCTION reader_ambiguous_terminal_run_guard();
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "ambiguous run-level 完整性约束不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
