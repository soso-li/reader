"""Use explicit transaction evidence for one-time split transitions."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0035_split_transaction_integrity"
down_revision: str = "0034_split_terminal_integrity"
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
                RAISE EXCEPTION 'event_split_transaction_upgrade_requires_empty_graph: 0035 以前的 split 图没有显式 xid8 事务证据，必须先恢复或人工审计';
            END IF;
        END
        $reader$;

        ALTER TABLE events
            ADD COLUMN created_transaction_id xid8 NOT NULL
                DEFAULT pg_current_xact_id(),
            ADD COLUMN superseded_transaction_id xid8 NULL;
        ALTER TABLE event_revisions
            ADD COLUMN created_transaction_id xid8 NOT NULL
                DEFAULT pg_current_xact_id();
        ALTER TABLE cluster_event_projections
            ADD COLUMN created_transaction_id xid8 NOT NULL
                DEFAULT pg_current_xact_id();
        ALTER TABLE event_lineages
            ADD COLUMN created_transaction_id xid8 NOT NULL
                DEFAULT pg_current_xact_id();

        CREATE INDEX ix_cluster_event_projection_split_created_transaction
        ON cluster_event_projections (created_transaction_id, event_id)
        WHERE reconciliation_kind = 'split';

        CREATE OR REPLACE FUNCTION reader_event_lifecycle_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            old_revision_no integer;
            new_revision_no integer;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'event_immutable: Event 不可删除';
            END IF;
            IF TG_OP = 'INSERT' THEN
                IF NEW.status IS DISTINCT FROM 'active'
                   OR NEW.superseded_at IS NOT NULL
                   OR NEW.superseded_transaction_id IS NOT NULL THEN
                    RAISE EXCEPTION 'event_new_must_be_active: 新 Event 必须从 active 且未 superseded 开始';
                END IF;
                NEW.created_transaction_id := pg_current_xact_id();
                RETURN NEW;
            END IF;
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.uid IS DISTINCT FROM OLD.uid
               OR NEW.created_at IS DISTINCT FROM OLD.created_at
               OR NEW.created_transaction_id
                    IS DISTINCT FROM OLD.created_transaction_id THEN
                RAISE EXCEPTION 'event_identity_immutable: Event 身份、创建时间与创建事务不可修改';
            END IF;
            IF OLD.status = 'superseded' THEN
                RAISE EXCEPTION 'event_lifecycle_terminal: superseded Event 不可再次修改';
            END IF;
            IF NEW.status = 'superseded' THEN
                NEW.superseded_transaction_id := pg_current_xact_id();
            ELSIF NEW.status = 'active' THEN
                IF NEW.superseded_at IS NOT NULL
                   OR NEW.superseded_transaction_id IS NOT NULL THEN
                    RAISE EXCEPTION 'event_active_not_superseded: active Event 不得携带 superseded 证据';
                END IF;
            END IF;
            IF OLD.current_revision_id IS NOT NULL
               AND NEW.current_revision_id IS DISTINCT FROM OLD.current_revision_id THEN
                SELECT revision_no INTO old_revision_no
                FROM event_revisions WHERE id = OLD.current_revision_id;
                SELECT revision_no INTO new_revision_no
                FROM event_revisions
                WHERE id = NEW.current_revision_id AND event_id = NEW.id;
                IF new_revision_no IS NULL OR new_revision_no <= old_revision_no THEN
                    RAISE EXCEPTION 'event_revision_order: current revision 只能单调推进';
                END IF;
            END IF;
            RETURN NEW;
        END
        $reader$;

        DROP TRIGGER trg_event_lifecycle ON events;
        CREATE TRIGGER trg_event_lifecycle
        BEFORE INSERT OR UPDATE OR DELETE ON events
        FOR EACH ROW EXECUTE FUNCTION reader_event_lifecycle_guard();

        CREATE OR REPLACE FUNCTION reader_event_revision_sequence_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            expected_revision_no integer;
            event_status text;
        BEGIN
            SELECT status INTO event_status
            FROM events WHERE id = NEW.event_id FOR UPDATE;
            IF event_status IS NULL THEN
                RAISE EXCEPTION 'event_revision_event_missing: Event 不存在';
            END IF;
            IF event_status <> 'active' THEN
                RAISE EXCEPTION 'event_revision_event_terminal: superseded Event 不可追加 Revision';
            END IF;
            SELECT COALESCE(max(revision_no), 0) + 1
            INTO expected_revision_no
            FROM event_revisions
            WHERE event_id = NEW.event_id;
            IF NEW.revision_no <> expected_revision_no THEN
                RAISE EXCEPTION 'event_revision_sequence: 期望 revision_no %，实际 %',
                    expected_revision_no, NEW.revision_no;
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE OR REPLACE FUNCTION reader_split_projection_has_newborn_target(
            candidate cluster_event_projections
        )
        RETURNS boolean
        LANGUAGE sql
        VOLATILE
        AS $reader$
            SELECT candidate.created_transaction_id = pg_current_xact_id()
               AND EXISTS (
                SELECT 1
                FROM events target
                JOIN event_revisions target_revision
                  ON target_revision.id = candidate.event_revision_id
                 AND target_revision.event_id = target.id
                WHERE target.id = candidate.event_id
                  AND target.status = 'active'
                  AND target.superseded_at IS NULL
                  AND target.superseded_transaction_id IS NULL
                  AND target.current_revision_id = candidate.event_revision_id
                  AND target_revision.revision_no = 1
                  AND target.created_transaction_id = pg_current_xact_id()
                  AND target_revision.created_transaction_id
                        = pg_current_xact_id()
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
                       NEW.after_evidence_fingerprint, NEW.projected_at,
                       NEW.created_transaction_id)
                   IS NOT DISTINCT FROM
                   ROW(OLD.id, OLD.cluster_id_snapshot, OLD.clustering_run_id,
                       OLD.cluster_anchor, OLD.cluster_occurrence, OLD.event_id,
                       OLD.event_revision_id, OLD.predecessor_projection_id,
                       OLD.reconciliation_kind, OLD.reconciliation_rule_version,
                       OLD.before_evidence_fingerprint,
                       OLD.after_evidence_fingerprint, OLD.projected_at,
                       OLD.created_transaction_id) THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'event_projection_immutable: Cluster projection mapping 不可修改或删除';
        END
        $reader$;

        CREATE OR REPLACE FUNCTION reader_split_projection_has_complete_terminal_graph(
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
            parent_revision_id integer;
            current_transaction_id xid8;
        BEGIN
            IF candidate.reconciliation_kind <> 'split'
               OR NOT reader_split_projection_matches_frozen_predecessor(
                          candidate
                      ) THEN
                RETURN false;
            END IF;

            SELECT frozen.cluster_anchor,
                   frozen.cluster_occurrence,
                   predecessor.event_id,
                   predecessor.event_revision_id
            INTO before_anchor,
                 before_occurrence,
                 parent_event_id,
                 parent_revision_id
            FROM clustering_run_projection_predecessors frozen
            JOIN cluster_event_projections predecessor
              ON predecessor.id = frozen.predecessor_projection_id
            WHERE frozen.run_id = candidate.clustering_run_id
              AND frozen.predecessor_projection_id
                    = candidate.predecessor_projection_id;
            IF parent_event_id IS NULL THEN
                RETURN false;
            END IF;
            current_transaction_id := pg_current_xact_id();

            IF NOT EXISTS (
                SELECT 1
                FROM events source
                WHERE source.id = parent_event_id
                  AND source.status = 'superseded'
                  AND source.superseded_at IS NOT NULL
                  AND source.superseded_transaction_id
                        = current_transaction_id
                  AND source.current_revision_id = parent_revision_id
                  AND NOT EXISTS (
                      SELECT 1
                      FROM event_revisions source_revision
                      WHERE source_revision.event_id = source.id
                        AND source_revision.created_transaction_id
                              = current_transaction_id
                  )
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
                      projection.created_transaction_id
                            <> current_transaction_id
                      OR target.created_transaction_id
                            <> current_transaction_id
                      OR target_revision.created_transaction_id
                            <> current_transaction_id
                      OR target.status <> 'active'
                      OR target.superseded_at IS NOT NULL
                      OR target.superseded_transaction_id IS NOT NULL
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
                            AND lineage.created_transaction_id
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

        DROP TRIGGER trg_split_terminal_projection
            ON cluster_event_projections;
        DROP TRIGGER trg_split_terminal_event ON events;
        DROP TRIGGER trg_split_terminal_revision ON event_revisions;
        DROP TRIGGER trg_split_terminal_lineage ON event_lineages;
        DROP TRIGGER trg_split_terminal_user_state ON event_user_states;
        DROP TRIGGER trg_split_terminal_baseline ON migration_baselines;
        DROP TRIGGER trg_split_terminal_interaction ON interaction_events;
        DROP FUNCTION reader_split_terminal_graph_guard();

        CREATE FUNCTION reader_require_complete_split_projection(
            candidate cluster_event_projections
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            IF candidate.reconciliation_kind = 'split'
               AND NOT reader_split_projection_has_complete_terminal_graph(
                           candidate
                       ) THEN
                RAISE EXCEPTION 'event_split_terminal_graph_incomplete: split 必须在同一事务提交完整唯一拓扑、active→superseded 父 Event、逐子 Lineage 与无状态新生 Event';
            END IF;
        END
        $reader$;

        CREATE FUNCTION reader_validate_current_split_for_event(
            affected_event_id integer
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            projection cluster_event_projections%ROWTYPE;
        BEGIN
            IF affected_event_id IS NULL THEN
                RETURN;
            END IF;
            FOR projection IN
                SELECT candidate.*
                FROM cluster_event_projections candidate
                JOIN cluster_event_projections predecessor
                  ON predecessor.id = candidate.predecessor_projection_id
                WHERE candidate.reconciliation_kind = 'split'
                  AND candidate.created_transaction_id
                        = pg_current_xact_id()
                  AND (
                      candidate.event_id = affected_event_id
                      OR predecessor.event_id = affected_event_id
                  )
            LOOP
                PERFORM reader_require_complete_split_projection(projection);
            END LOOP;
        END
        $reader$;

        CREATE FUNCTION reader_split_terminal_projection_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            PERFORM reader_require_complete_split_projection(NEW);
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_split_terminal_event_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            PERFORM reader_validate_current_split_for_event(NEW.id);
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_split_terminal_revision_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            PERFORM reader_validate_current_split_for_event(NEW.event_id);
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_split_terminal_lineage_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            PERFORM reader_validate_current_split_for_event(
                NEW.source_event_id
            );
            PERFORM reader_validate_current_split_for_event(
                NEW.target_event_id
            );
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_split_terminal_user_state_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            PERFORM reader_validate_current_split_for_event(NEW.event_id);
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_split_terminal_baseline_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            PERFORM reader_validate_current_split_for_event(
                NEW.resolved_event_id
            );
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_split_terminal_interaction_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            PERFORM reader_validate_current_split_for_event(NEW.event_id);
            RETURN NULL;
        END
        $reader$;

        CREATE CONSTRAINT TRIGGER trg_split_terminal_projection
        AFTER INSERT ON cluster_event_projections
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        EXECUTE FUNCTION reader_split_terminal_projection_guard();

        CREATE CONSTRAINT TRIGGER trg_split_terminal_event
        AFTER INSERT OR UPDATE ON events
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_event_guard();

        CREATE CONSTRAINT TRIGGER trg_split_terminal_revision
        AFTER INSERT ON event_revisions
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_revision_guard();

        CREATE CONSTRAINT TRIGGER trg_split_terminal_lineage
        AFTER INSERT ON event_lineages
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_lineage_guard();

        CREATE CONSTRAINT TRIGGER trg_split_terminal_user_state
        AFTER INSERT OR UPDATE ON event_user_states
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_user_state_guard();

        CREATE CONSTRAINT TRIGGER trg_split_terminal_baseline
        AFTER INSERT OR UPDATE ON migration_baselines
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_baseline_guard();

        CREATE CONSTRAINT TRIGGER trg_split_terminal_interaction
        AFTER INSERT ON interaction_events
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_split_terminal_interaction_guard();
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event split 显式事务证据与一次性父状态跃迁不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
