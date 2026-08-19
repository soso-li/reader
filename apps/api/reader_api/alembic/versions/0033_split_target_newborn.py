"""Require every split target Event to be born stateless in the split transaction."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0033_split_target_newborn"
down_revision: str = "0032_split_lineage_integrity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM cluster_event_projections
                WHERE reconciliation_kind = 'split'
            ) OR EXISTS (
                SELECT 1
                FROM event_lineages
                WHERE relation_type = 'split_from'
            ) THEN
                RAISE EXCEPTION 'event_split_newborn_upgrade_requires_empty_graph: 0033 以前的 split 图无法证明目标 Event 与 projection 同事务新生，必须先恢复或人工审计';
            END IF;
        END
        $reader$;

        CREATE FUNCTION reader_split_projection_has_newborn_target(
            candidate cluster_event_projections
        )
        RETURNS boolean
        LANGUAGE sql
        VOLATILE
        AS $reader$
            SELECT EXISTS (
                SELECT 1
                FROM events target
                JOIN event_revisions target_revision
                  ON target_revision.id = candidate.event_revision_id
                 AND target_revision.event_id = target.id
                WHERE target.id = candidate.event_id
                  AND target.status = 'active'
                  AND target.superseded_at IS NULL
                  AND target.current_revision_id = candidate.event_revision_id
                  AND target_revision.revision_no = 1
                  AND target.xmin::text::bigint
                        = pg_current_xact_id()::text::bigint
                  AND target_revision.xmin::text::bigint
                        = pg_current_xact_id()::text::bigint
                  AND NOT EXISTS (
                      SELECT 1
                      FROM event_revisions other_revision
                      WHERE other_revision.event_id = candidate.event_id
                        AND other_revision.id <> candidate.event_revision_id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM cluster_event_projections other_projection
                      WHERE other_projection.event_id = candidate.event_id
                        AND other_projection.id IS DISTINCT FROM candidate.id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM event_user_states state
                      WHERE state.event_id = candidate.event_id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM migration_baselines baseline
                      WHERE baseline.resolved_event_id = candidate.event_id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM interaction_events interaction
                      WHERE interaction.event_id = candidate.event_id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM event_lineages lineage
                      WHERE lineage.source_event_id = candidate.event_id
                         OR lineage.target_event_id = candidate.event_id
                  )
            )
        $reader$;

        CREATE OR REPLACE FUNCTION reader_split_lineage_matches_projection(
            candidate event_lineages
        )
        RETURNS boolean
        LANGUAGE sql
        VOLATILE
        AS $reader$
            SELECT EXISTS (
                SELECT 1
                FROM cluster_event_projections projection
                JOIN clustering_run_projection_predecessors frozen
                  ON frozen.run_id = projection.clustering_run_id
                 AND frozen.predecessor_projection_id
                       = projection.predecessor_projection_id
                JOIN cluster_event_projections predecessor
                  ON predecessor.id = frozen.predecessor_projection_id
                JOIN event_revisions predecessor_revision
                  ON predecessor_revision.id = predecessor.event_revision_id
                 AND predecessor_revision.event_id = predecessor.event_id
                JOIN event_revisions target_revision
                  ON target_revision.id = projection.event_revision_id
                 AND target_revision.event_id = projection.event_id
                JOIN clustering_runs run
                  ON run.id = projection.clustering_run_id
                WHERE projection.clustering_run_id
                        = candidate.clustering_run_id
                  AND projection.event_id = candidate.target_event_id
                  AND projection.reconciliation_kind = 'split'
                  AND predecessor.event_id = candidate.source_event_id
                  AND projection.before_evidence_fingerprint
                        = candidate.before_evidence_fingerprint
                  AND predecessor.after_evidence_fingerprint
                        = candidate.before_evidence_fingerprint
                  AND predecessor_revision.evidence_fingerprint
                        = candidate.before_evidence_fingerprint
                  AND projection.after_evidence_fingerprint
                        = candidate.after_evidence_fingerprint
                  AND target_revision.evidence_fingerprint
                        = candidate.after_evidence_fingerprint
                  AND candidate.rule_version = run.rule_version
                  AND reader_split_projection_matches_frozen_predecessor(
                          projection
                      )
                  AND reader_split_projection_has_newborn_target(projection)
            )
        $reader$;

        CREATE OR REPLACE FUNCTION reader_cluster_event_projection_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            run_completed boolean;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                SELECT status = 'completed' AND after_snapshot_finalized
                INTO run_completed
                FROM clustering_runs WHERE id = NEW.clustering_run_id;
                IF run_completed IS DISTINCT FROM true THEN
                    RAISE EXCEPTION 'event_projection_requires_completed_run: 投影只能引用完成且封印的 Clustering Run';
                END IF;
                IF NEW.reconciliation_kind = 'split'
                   AND NOT reader_split_projection_matches_frozen_predecessor(NEW) THEN
                    RAISE EXCEPTION 'event_projection_split_frozen_predecessor_mismatch: split projection 必须匹配 run 冻结前驱、唯一 1→N 拓扑及 Revision 指纹';
                END IF;
                IF NEW.reconciliation_kind = 'split'
                   AND NOT reader_split_projection_has_newborn_target(NEW) THEN
                    RAISE EXCEPTION 'event_projection_split_target_not_newborn: split 子 Event 与初始 Revision 必须在当前事务新建，且不得已有 projection、状态、baseline、交互或 Lineage';
                END IF;
                RETURN NEW;
            END IF;
            IF TG_OP = 'UPDATE'
               AND OLD.cluster_id IS NOT NULL
               AND NEW.cluster_id IS NULL
               AND ROW(NEW.id, NEW.cluster_id_snapshot, NEW.clustering_run_id,
                       NEW.cluster_anchor, NEW.cluster_occurrence, NEW.event_id,
                       NEW.event_revision_id, NEW.predecessor_projection_id,
                       NEW.reconciliation_kind, NEW.reconciliation_rule_version,
                       NEW.before_evidence_fingerprint,
                       NEW.after_evidence_fingerprint, NEW.projected_at)
                   IS NOT DISTINCT FROM
                   ROW(OLD.id, OLD.cluster_id_snapshot, OLD.clustering_run_id,
                       OLD.cluster_anchor, OLD.cluster_occurrence, OLD.event_id,
                       OLD.event_revision_id, OLD.predecessor_projection_id,
                       OLD.reconciliation_kind, OLD.reconciliation_rule_version,
                       OLD.before_evidence_fingerprint,
                       OLD.after_evidence_fingerprint, OLD.projected_at) THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'event_projection_immutable: Cluster projection mapping 不可修改或删除';
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event split 子目标新生约束不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
