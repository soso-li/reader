"""Enforce unique split topology and complete newborn graphs at commit."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0034_split_terminal_integrity"
down_revision: str = "0033_split_target_newborn"
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
                RAISE EXCEPTION 'event_split_terminal_upgrade_requires_empty_graph: 0034 以前的 split 图无法证明完整 Lineage 与事务终态，必须先恢复或人工审计';
            END IF;
        END
        $reader$;

        CREATE OR REPLACE FUNCTION reader_split_projection_matches_frozen_predecessor(
            candidate cluster_event_projections
        )
        RETURNS boolean
        LANGUAGE plpgsql
        STABLE
        AS $reader$
        DECLARE
            before_anchor text;
            before_occurrence integer;
            child_count integer;
        BEGIN
            SELECT frozen.cluster_anchor, frozen.cluster_occurrence
            INTO before_anchor, before_occurrence
            FROM clustering_run_projection_predecessors frozen
            JOIN cluster_event_projections predecessor
              ON predecessor.id = frozen.predecessor_projection_id
            JOIN event_revisions predecessor_revision
              ON predecessor_revision.id = predecessor.event_revision_id
             AND predecessor_revision.event_id = predecessor.event_id
            JOIN event_revisions target_revision
              ON target_revision.id = candidate.event_revision_id
             AND target_revision.event_id = candidate.event_id
            JOIN clustering_runs run
              ON run.id = candidate.clustering_run_id
            WHERE frozen.run_id = candidate.clustering_run_id
              AND frozen.predecessor_projection_id
                    = candidate.predecessor_projection_id
              AND predecessor.event_id <> candidate.event_id
              AND predecessor.after_evidence_fingerprint
                    = candidate.before_evidence_fingerprint
              AND predecessor_revision.evidence_fingerprint
                    = candidate.before_evidence_fingerprint
              AND target_revision.revision_no = 1
              AND target_revision.evidence_fingerprint
                    = candidate.after_evidence_fingerprint
              AND candidate.reconciliation_rule_version = run.rule_version;
            IF before_anchor IS NULL THEN
                RETURN false;
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM clustering_run_memberships before_member
                JOIN clustering_run_memberships after_member
                  ON after_member.run_id = before_member.run_id
                 AND after_member.snapshot_phase = 'after'
                 AND after_member.evidence_anchor
                       = before_member.evidence_anchor
                 AND after_member.evidence_occurrence
                       = before_member.evidence_occurrence
                WHERE before_member.run_id = candidate.clustering_run_id
                  AND before_member.snapshot_phase = 'before'
                  AND before_member.cluster_anchor = before_anchor
                  AND before_member.cluster_occurrence = before_occurrence
                  AND after_member.cluster_anchor = candidate.cluster_anchor
                  AND after_member.cluster_occurrence
                        = candidate.cluster_occurrence
            ) THEN
                RETURN false;
            END IF;

            IF EXISTS (
                SELECT 1
                FROM clustering_run_memberships parent_member
                JOIN clustering_run_memberships after_member
                  ON after_member.run_id = parent_member.run_id
                 AND after_member.snapshot_phase = 'after'
                 AND after_member.evidence_anchor
                       = parent_member.evidence_anchor
                 AND after_member.evidence_occurrence
                       = parent_member.evidence_occurrence
                JOIN clustering_run_memberships child_member
                  ON child_member.run_id = after_member.run_id
                 AND child_member.snapshot_phase = 'after'
                 AND child_member.cluster_anchor = after_member.cluster_anchor
                 AND child_member.cluster_occurrence
                       = after_member.cluster_occurrence
                JOIN clustering_run_memberships other_before
                  ON other_before.run_id = child_member.run_id
                 AND other_before.snapshot_phase = 'before'
                 AND other_before.evidence_anchor
                       = child_member.evidence_anchor
                 AND other_before.evidence_occurrence
                       = child_member.evidence_occurrence
                WHERE parent_member.run_id = candidate.clustering_run_id
                  AND parent_member.snapshot_phase = 'before'
                  AND parent_member.cluster_anchor = before_anchor
                  AND parent_member.cluster_occurrence = before_occurrence
                  AND ROW(other_before.cluster_anchor,
                          other_before.cluster_occurrence)
                      IS DISTINCT FROM ROW(before_anchor, before_occurrence)
            ) THEN
                RETURN false;
            END IF;

            SELECT count(*)
            INTO child_count
            FROM (
                SELECT after_member.cluster_anchor,
                       after_member.cluster_occurrence
                FROM clustering_run_memberships parent_member
                JOIN clustering_run_memberships after_member
                  ON after_member.run_id = parent_member.run_id
                 AND after_member.snapshot_phase = 'after'
                 AND after_member.evidence_anchor
                       = parent_member.evidence_anchor
                 AND after_member.evidence_occurrence
                       = parent_member.evidence_occurrence
                WHERE parent_member.run_id = candidate.clustering_run_id
                  AND parent_member.snapshot_phase = 'before'
                  AND parent_member.cluster_anchor = before_anchor
                  AND parent_member.cluster_occurrence = before_occurrence
                GROUP BY after_member.cluster_anchor,
                         after_member.cluster_occurrence
            ) children;
            RETURN child_count >= 2;
        END
        $reader$;

        CREATE FUNCTION reader_split_projection_has_complete_terminal_graph(
            candidate cluster_event_projections
        )
        RETURNS boolean
        LANGUAGE plpgsql
        VOLATILE
        AS $reader$
        DECLARE
            before_anchor text;
            before_occurrence integer;
            parent_event_id integer;
            current_transaction_id bigint;
        BEGIN
            IF candidate.reconciliation_kind <> 'split'
               OR NOT reader_split_projection_matches_frozen_predecessor(
                          candidate
                      ) THEN
                RETURN false;
            END IF;

            SELECT frozen.cluster_anchor,
                   frozen.cluster_occurrence,
                   predecessor.event_id
            INTO before_anchor, before_occurrence, parent_event_id
            FROM clustering_run_projection_predecessors frozen
            JOIN cluster_event_projections predecessor
              ON predecessor.id = frozen.predecessor_projection_id
            WHERE frozen.run_id = candidate.clustering_run_id
              AND frozen.predecessor_projection_id
                    = candidate.predecessor_projection_id;
            IF parent_event_id IS NULL THEN
                RETURN false;
            END IF;
            current_transaction_id := pg_current_xact_id()::text::bigint;

            IF NOT EXISTS (
                SELECT 1
                FROM events source
                WHERE source.id = parent_event_id
                  AND source.status = 'superseded'
                  AND source.superseded_at IS NOT NULL
                  AND source.xmin::text::bigint = current_transaction_id
            ) THEN
                RETURN false;
            END IF;

            IF EXISTS (
                SELECT expected.cluster_anchor,
                       expected.cluster_occurrence
                FROM (
                    SELECT after_member.cluster_anchor,
                           after_member.cluster_occurrence
                    FROM clustering_run_memberships parent_member
                    JOIN clustering_run_memberships after_member
                      ON after_member.run_id = parent_member.run_id
                     AND after_member.snapshot_phase = 'after'
                     AND after_member.evidence_anchor
                           = parent_member.evidence_anchor
                     AND after_member.evidence_occurrence
                           = parent_member.evidence_occurrence
                    WHERE parent_member.run_id = candidate.clustering_run_id
                      AND parent_member.snapshot_phase = 'before'
                      AND parent_member.cluster_anchor = before_anchor
                      AND parent_member.cluster_occurrence = before_occurrence
                    GROUP BY after_member.cluster_anchor,
                             after_member.cluster_occurrence
                ) expected
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM cluster_event_projections projection
                    WHERE projection.clustering_run_id
                            = candidate.clustering_run_id
                      AND projection.predecessor_projection_id
                            = candidate.predecessor_projection_id
                      AND projection.reconciliation_kind = 'split'
                      AND projection.cluster_anchor = expected.cluster_anchor
                      AND projection.cluster_occurrence
                            = expected.cluster_occurrence
                )
            ) THEN
                RETURN false;
            END IF;

            IF EXISTS (
                SELECT 1
                FROM cluster_event_projections projection
                WHERE projection.clustering_run_id
                        = candidate.clustering_run_id
                  AND projection.predecessor_projection_id
                        = candidate.predecessor_projection_id
                  AND projection.reconciliation_kind = 'split'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM clustering_run_memberships parent_member
                      JOIN clustering_run_memberships after_member
                        ON after_member.run_id = parent_member.run_id
                       AND after_member.snapshot_phase = 'after'
                       AND after_member.evidence_anchor
                             = parent_member.evidence_anchor
                       AND after_member.evidence_occurrence
                             = parent_member.evidence_occurrence
                      WHERE parent_member.run_id = candidate.clustering_run_id
                        AND parent_member.snapshot_phase = 'before'
                        AND parent_member.cluster_anchor = before_anchor
                        AND parent_member.cluster_occurrence = before_occurrence
                        AND after_member.cluster_anchor
                              = projection.cluster_anchor
                        AND after_member.cluster_occurrence
                              = projection.cluster_occurrence
                  )
            ) THEN
                RETURN false;
            END IF;

            IF EXISTS (
                SELECT 1
                FROM cluster_event_projections projection
                JOIN events target ON target.id = projection.event_id
                JOIN event_revisions target_revision
                  ON target_revision.id = projection.event_revision_id
                 AND target_revision.event_id = target.id
                WHERE projection.clustering_run_id
                        = candidate.clustering_run_id
                  AND projection.predecessor_projection_id
                        = candidate.predecessor_projection_id
                  AND projection.reconciliation_kind = 'split'
                  AND (
                      projection.xmin::text::bigint
                            <> current_transaction_id
                      OR target.xmin::text::bigint
                            <> current_transaction_id
                      OR target_revision.xmin::text::bigint
                            <> current_transaction_id
                      OR target.status <> 'active'
                      OR target.superseded_at IS NOT NULL
                      OR target.current_revision_id
                            <> projection.event_revision_id
                      OR target_revision.revision_no <> 1
                      OR target_revision.evidence_fingerprint
                            <> projection.after_evidence_fingerprint
                      OR EXISTS (
                          SELECT 1
                          FROM event_revisions other_revision
                          WHERE other_revision.event_id = projection.event_id
                            AND other_revision.id
                                  <> projection.event_revision_id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM cluster_event_projections other_projection
                          WHERE other_projection.event_id = projection.event_id
                            AND other_projection.id <> projection.id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM event_user_states state
                          WHERE state.event_id = projection.event_id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM migration_baselines baseline
                          WHERE baseline.resolved_event_id
                                = projection.event_id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM interaction_events interaction
                          WHERE interaction.event_id = projection.event_id
                      )
                      OR NOT EXISTS (
                          SELECT 1
                          FROM event_lineages lineage
                          WHERE lineage.clustering_run_id
                                  = projection.clustering_run_id
                            AND lineage.relation_type = 'split_from'
                            AND lineage.source_event_id = parent_event_id
                            AND lineage.target_event_id = projection.event_id
                            AND lineage.rule_version
                                  = projection.reconciliation_rule_version
                            AND lineage.before_evidence_fingerprint
                                  = projection.before_evidence_fingerprint
                            AND lineage.after_evidence_fingerprint
                                  = projection.after_evidence_fingerprint
                            AND lineage.xmin::text::bigint
                                  = current_transaction_id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM event_lineages other_lineage
                          WHERE (
                              other_lineage.source_event_id
                                    = projection.event_id
                              OR other_lineage.target_event_id
                                    = projection.event_id
                          )
                            AND NOT (
                                other_lineage.clustering_run_id
                                      = projection.clustering_run_id
                                AND other_lineage.relation_type = 'split_from'
                                AND other_lineage.source_event_id
                                      = parent_event_id
                                AND other_lineage.target_event_id
                                      = projection.event_id
                            )
                      )
                  )
            ) THEN
                RETURN false;
            END IF;

            IF EXISTS (
                SELECT 1
                FROM event_lineages lineage
                WHERE lineage.clustering_run_id
                        = candidate.clustering_run_id
                  AND lineage.relation_type = 'split_from'
                  AND lineage.source_event_id = parent_event_id
                  AND NOT EXISTS (
                      SELECT 1
                      FROM cluster_event_projections projection
                      WHERE projection.clustering_run_id
                              = lineage.clustering_run_id
                        AND projection.predecessor_projection_id
                              = candidate.predecessor_projection_id
                        AND projection.reconciliation_kind = 'split'
                        AND projection.event_id = lineage.target_event_id
                  )
            ) THEN
                RETURN false;
            END IF;

            RETURN true;
        END
        $reader$;

        CREATE FUNCTION reader_split_terminal_graph_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            projection cluster_event_projections%ROWTYPE;
            current_transaction_id bigint;
        BEGIN
            current_transaction_id := pg_current_xact_id()::text::bigint;
            FOR projection IN
                SELECT candidate.*
                FROM cluster_event_projections candidate
                WHERE candidate.reconciliation_kind = 'split'
                  AND candidate.xmin::text::bigint = current_transaction_id
            LOOP
                IF NOT reader_split_projection_has_complete_terminal_graph(
                           projection
                       ) THEN
                    RAISE EXCEPTION 'event_split_terminal_graph_incomplete: split 必须在同一事务提交完整唯一拓扑、superseded 父 Event、逐子 Lineage 与无状态新生 Event';
                END IF;
            END LOOP;
            RETURN NULL;
        END
        $reader$;

        CREATE CONSTRAINT TRIGGER trg_split_terminal_projection
        AFTER INSERT ON cluster_event_projections
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_graph_guard();

        CREATE CONSTRAINT TRIGGER trg_split_terminal_event
        AFTER INSERT OR UPDATE ON events
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_graph_guard();

        CREATE CONSTRAINT TRIGGER trg_split_terminal_revision
        AFTER INSERT ON event_revisions
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_graph_guard();

        CREATE CONSTRAINT TRIGGER trg_split_terminal_lineage
        AFTER INSERT ON event_lineages
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_graph_guard();

        CREATE CONSTRAINT TRIGGER trg_split_terminal_user_state
        AFTER INSERT OR UPDATE ON event_user_states
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_graph_guard();

        CREATE CONSTRAINT TRIGGER trg_split_terminal_baseline
        AFTER INSERT OR UPDATE ON migration_baselines
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_graph_guard();

        CREATE CONSTRAINT TRIGGER trg_split_terminal_interaction
        AFTER INSERT ON interaction_events
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_graph_guard();
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event split 提交终态完整性约束不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
