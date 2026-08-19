"""Create stateless Event successors for conservative many-to-one merges."""

from __future__ import annotations

from alembic import op


revision: str = "0039_event_merge_lineage"
down_revision: str | None = "0038_split_xid_provenance"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.drop_constraint(
        "ck_cluster_event_projection_reconciliation_kind",
        "cluster_event_projections",
        type_="check",
    )
    op.drop_constraint(
        "ck_cluster_event_projection_reconciliation_shape",
        "cluster_event_projections",
        type_="check",
    )
    op.create_check_constraint(
        "ck_cluster_event_projection_reconciliation_kind",
        "cluster_event_projections",
        "reconciliation_kind IN ('initial', 'continued', 'split', 'merged')",
    )
    op.create_check_constraint(
        "ck_cluster_event_projection_reconciliation_shape",
        "cluster_event_projections",
        "(reconciliation_kind = 'initial' "
        " AND predecessor_projection_id IS NULL "
        " AND reconciliation_rule_version IS NULL "
        " AND before_evidence_fingerprint IS NULL) OR "
        "(reconciliation_kind IN ('continued', 'split') "
        " AND predecessor_projection_id IS NOT NULL "
        " AND reconciliation_rule_version IS NOT NULL "
        " AND btrim(reconciliation_rule_version) <> '' "
        " AND before_evidence_fingerprint IS NOT NULL) OR "
        "(reconciliation_kind = 'merged' "
        " AND predecessor_projection_id IS NULL "
        " AND reconciliation_rule_version IS NOT NULL "
        " AND btrim(reconciliation_rule_version) <> '' "
        " AND before_evidence_fingerprint IS NULL)",
    )
    op.drop_constraint(
        "ck_event_lineage_relation_type",
        "event_lineages",
        type_="check",
    )
    op.create_check_constraint(
        "ck_event_lineage_relation_type",
        "event_lineages",
        "relation_type IN ('split_from', 'merged_from')",
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_cluster_event_projection_merged_event "
        "ON cluster_event_projections (clustering_run_id, event_id) "
        "WHERE reconciliation_kind = 'merged'"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_event_lineage_merge_source "
        "ON event_lineages (clustering_run_id, source_event_id) "
        "WHERE relation_type = 'merged_from'"
    )

    op.execute(
        r"""
        CREATE FUNCTION reader_merge_projection_matches_frozen_parents(
            candidate cluster_event_projections
        )
        RETURNS boolean
        LANGUAGE plpgsql
        STABLE
        AS $reader$
        DECLARE
            parent record;
            overlap_parent_count integer := 0;
        BEGIN
            IF candidate.reconciliation_kind <> 'merged'
               OR candidate.predecessor_projection_id IS NOT NULL
               OR candidate.before_evidence_fingerprint IS NOT NULL THEN
                RETURN false;
            END IF;
            IF NOT EXISTS (
                SELECT 1
                FROM clustering_runs run
                JOIN events target
                  ON target.id = candidate.event_id
                JOIN event_revisions target_revision
                  ON target_revision.id = candidate.event_revision_id
                 AND target_revision.event_id = target.id
                WHERE run.id = candidate.clustering_run_id
                  AND candidate.reconciliation_rule_version = run.rule_version
                  AND target_revision.revision_no = 1
                  AND target_revision.evidence_fingerprint
                        = candidate.after_evidence_fingerprint
            ) THEN
                RETURN false;
            END IF;

            FOR parent IN
                SELECT DISTINCT before_member.cluster_anchor,
                                before_member.cluster_occurrence
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
                  AND after_member.cluster_anchor = candidate.cluster_anchor
                  AND after_member.cluster_occurrence
                        = candidate.cluster_occurrence
            LOOP
                overlap_parent_count := overlap_parent_count + 1;
                IF EXISTS (
                    SELECT 1
                    FROM clustering_run_memberships before_evidence
                    WHERE before_evidence.run_id
                            = candidate.clustering_run_id
                      AND before_evidence.snapshot_phase = 'before'
                      AND before_evidence.cluster_anchor
                            = parent.cluster_anchor
                      AND before_evidence.cluster_occurrence
                            = parent.cluster_occurrence
                      AND NOT EXISTS (
                          SELECT 1
                          FROM clustering_run_memberships after_evidence
                          WHERE after_evidence.run_id
                                  = before_evidence.run_id
                            AND after_evidence.snapshot_phase = 'after'
                            AND after_evidence.cluster_anchor
                                  = candidate.cluster_anchor
                            AND after_evidence.cluster_occurrence
                                  = candidate.cluster_occurrence
                            AND after_evidence.evidence_anchor
                                  = before_evidence.evidence_anchor
                            AND after_evidence.evidence_occurrence
                                  = before_evidence.evidence_occurrence
                      )
                ) THEN
                    RETURN false;
                END IF;
                IF EXISTS (
                    SELECT 1
                    FROM clustering_run_memberships before_evidence
                    JOIN clustering_run_memberships other_after
                      ON other_after.run_id = before_evidence.run_id
                     AND other_after.snapshot_phase = 'after'
                     AND other_after.evidence_anchor
                           = before_evidence.evidence_anchor
                     AND other_after.evidence_occurrence
                           = before_evidence.evidence_occurrence
                    WHERE before_evidence.run_id
                            = candidate.clustering_run_id
                      AND before_evidence.snapshot_phase = 'before'
                      AND before_evidence.cluster_anchor
                            = parent.cluster_anchor
                      AND before_evidence.cluster_occurrence
                            = parent.cluster_occurrence
                      AND ROW(other_after.cluster_anchor,
                              other_after.cluster_occurrence)
                          IS DISTINCT FROM
                              ROW(candidate.cluster_anchor,
                                  candidate.cluster_occurrence)
                ) THEN
                    RETURN false;
                END IF;
                IF NOT EXISTS (
                    SELECT 1
                    FROM clustering_run_projection_predecessors frozen
                    JOIN cluster_event_projections predecessor
                      ON predecessor.id = frozen.predecessor_projection_id
                    JOIN event_revisions predecessor_revision
                      ON predecessor_revision.id
                            = predecessor.event_revision_id
                     AND predecessor_revision.event_id
                            = predecessor.event_id
                    WHERE frozen.run_id = candidate.clustering_run_id
                      AND frozen.cluster_anchor = parent.cluster_anchor
                      AND frozen.cluster_occurrence
                            = parent.cluster_occurrence
                      AND predecessor_revision.evidence_fingerprint
                            = predecessor.after_evidence_fingerprint
                      AND predecessor.event_id <> candidate.event_id
                ) THEN
                    RETURN false;
                END IF;
            END LOOP;
            RETURN overlap_parent_count >= 2;
        END
        $reader$;

        CREATE FUNCTION reader_merge_lineage_matches_projection(
            candidate event_lineages
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT EXISTS (
                SELECT 1
                FROM cluster_event_projections projection
                JOIN clustering_run_memberships after_member
                  ON after_member.run_id = projection.clustering_run_id
                 AND after_member.snapshot_phase = 'after'
                 AND after_member.cluster_anchor = projection.cluster_anchor
                 AND after_member.cluster_occurrence
                       = projection.cluster_occurrence
                JOIN clustering_run_memberships before_member
                  ON before_member.run_id = after_member.run_id
                 AND before_member.snapshot_phase = 'before'
                 AND before_member.evidence_anchor
                       = after_member.evidence_anchor
                 AND before_member.evidence_occurrence
                       = after_member.evidence_occurrence
                JOIN clustering_run_projection_predecessors frozen
                  ON frozen.run_id = before_member.run_id
                 AND frozen.cluster_anchor = before_member.cluster_anchor
                 AND frozen.cluster_occurrence
                       = before_member.cluster_occurrence
                JOIN cluster_event_projections predecessor
                  ON predecessor.id = frozen.predecessor_projection_id
                JOIN event_revisions predecessor_revision
                  ON predecessor_revision.id
                        = predecessor.event_revision_id
                 AND predecessor_revision.event_id = predecessor.event_id
                JOIN event_revisions target_revision
                  ON target_revision.id = projection.event_revision_id
                 AND target_revision.event_id = projection.event_id
                JOIN clustering_runs run
                  ON run.id = projection.clustering_run_id
                WHERE projection.clustering_run_id
                        = candidate.clustering_run_id
                  AND projection.event_id = candidate.target_event_id
                  AND projection.reconciliation_kind = 'merged'
                  AND predecessor.event_id = candidate.source_event_id
                  AND predecessor.after_evidence_fingerprint
                        = candidate.before_evidence_fingerprint
                  AND predecessor_revision.evidence_fingerprint
                        = candidate.before_evidence_fingerprint
                  AND projection.after_evidence_fingerprint
                        = candidate.after_evidence_fingerprint
                  AND target_revision.evidence_fingerprint
                        = candidate.after_evidence_fingerprint
                  AND candidate.rule_version = run.rule_version
                  AND reader_merge_projection_matches_frozen_parents(
                          projection
                      )
            )
        $reader$;
        """
    )

    op.execute(
        r"""
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
                IF NEW.reconciliation_kind = 'merged'
                   AND NOT reader_merge_projection_matches_frozen_parents(NEW) THEN
                    RAISE EXCEPTION 'event_projection_merge_frozen_parents_mismatch: merge projection 必须匹配 run 冻结的唯一 N→1 父集合及新 Revision 指纹';
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

        CREATE OR REPLACE FUNCTION reader_event_lineage_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            run_completed boolean;
            run_rule_version text;
            source_status text;
            target_status text;
            creates_cycle boolean;
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION 'event_lineage_immutable: Event Lineage 不可修改或删除';
            END IF;
            SELECT status = 'completed' AND after_snapshot_finalized, rule_version
            INTO run_completed, run_rule_version
            FROM clustering_runs
            WHERE id = NEW.clustering_run_id;
            IF run_completed IS DISTINCT FROM true THEN
                RAISE EXCEPTION 'event_lineage_requires_completed_run: Lineage 只能引用完成且封印的 Clustering Run';
            END IF;
            IF NEW.rule_version IS DISTINCT FROM run_rule_version THEN
                RAISE EXCEPTION 'event_lineage_rule_mismatch: Lineage rule version 必须匹配 Clustering Run';
            END IF;
            SELECT status INTO source_status
            FROM events WHERE id = NEW.source_event_id;
            SELECT status INTO target_status
            FROM events WHERE id = NEW.target_event_id;
            IF source_status IS DISTINCT FROM 'superseded'
               OR target_status IS DISTINCT FROM 'active' THEN
                RAISE EXCEPTION 'event_lineage_lifecycle_mismatch: Lineage 来源必须 superseded，后继必须 active';
            END IF;
            IF NEW.relation_type = 'split_from' THEN
                IF NOT reader_split_lineage_matches_projection(NEW) THEN
                    RAISE EXCEPTION 'event_lineage_projection_mismatch: split Lineage 必须匹配 run 冻结前驱、projection 与 Revision 指纹';
                END IF;
            ELSIF NEW.relation_type = 'merged_from' THEN
                IF NOT reader_merge_lineage_matches_projection(NEW) THEN
                    RAISE EXCEPTION 'event_lineage_projection_mismatch: merge Lineage 必须匹配 run 冻结父集合、projection 与 Revision 指纹';
                END IF;
            ELSE
                RAISE EXCEPTION 'event_lineage_relation_unsupported: 不支持的 Event Lineage 关系';
            END IF;
            WITH RECURSIVE descendants(event_id) AS (
                SELECT target_event_id
                FROM event_lineages
                WHERE source_event_id = NEW.target_event_id
                UNION
                SELECT lineage.target_event_id
                FROM event_lineages lineage
                JOIN descendants
                  ON lineage.source_event_id = descendants.event_id
            )
            SELECT EXISTS (
                SELECT 1 FROM descendants
                WHERE event_id = NEW.source_event_id
            ) INTO creates_cycle;
            IF creates_cycle THEN
                RAISE EXCEPTION 'event_lineage_cycle: Event Lineage 必须保持有向无环';
            END IF;
            RETURN NEW;
        END
        $reader$;
        """
    )

    op.execute(
        r"""
        CREATE FUNCTION reader_merge_projection_has_complete_terminal_graph(
            candidate cluster_event_projections
        )
        RETURNS boolean
        LANGUAGE plpgsql
        VOLATILE
        AS $reader$
        DECLARE
            parent record;
            parent_count integer := 0;
            current_transaction_id xid8 := pg_current_xact_id();
        BEGIN
            IF NOT reader_merge_projection_matches_frozen_parents(candidate)
               OR candidate.created_transaction_id
                    <> current_transaction_id THEN
                RETURN false;
            END IF;
            IF NOT EXISTS (
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
                  AND target.created_transaction_id = current_transaction_id
                  AND target_revision.created_transaction_id
                        = current_transaction_id
                  AND target_revision.revision_no = 1
                  AND target_revision.evidence_fingerprint
                        = candidate.after_evidence_fingerprint
                  AND NOT EXISTS (
                      SELECT 1 FROM event_revisions other_revision
                      WHERE other_revision.event_id = target.id
                        AND other_revision.id <> target_revision.id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM cluster_event_projections other_projection
                      WHERE other_projection.event_id = target.id
                        AND other_projection.id <> candidate.id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM event_user_states state
                      WHERE state.event_id = target.id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM migration_baselines baseline
                      WHERE baseline.resolved_event_id = target.id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM interaction_events interaction
                      WHERE interaction.event_id = target.id
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM event_lineages outgoing
                      WHERE outgoing.source_event_id = target.id
                  )
            ) THEN
                RETURN false;
            END IF;

            FOR parent IN
                SELECT DISTINCT predecessor.event_id,
                                predecessor.event_revision_id,
                                predecessor.after_evidence_fingerprint
                FROM clustering_run_memberships after_member
                JOIN clustering_run_memberships before_member
                  ON before_member.run_id = after_member.run_id
                 AND before_member.snapshot_phase = 'before'
                 AND before_member.evidence_anchor
                       = after_member.evidence_anchor
                 AND before_member.evidence_occurrence
                       = after_member.evidence_occurrence
                JOIN clustering_run_projection_predecessors frozen
                  ON frozen.run_id = before_member.run_id
                 AND frozen.cluster_anchor = before_member.cluster_anchor
                 AND frozen.cluster_occurrence
                       = before_member.cluster_occurrence
                JOIN cluster_event_projections predecessor
                  ON predecessor.id = frozen.predecessor_projection_id
                WHERE after_member.run_id = candidate.clustering_run_id
                  AND after_member.snapshot_phase = 'after'
                  AND after_member.cluster_anchor = candidate.cluster_anchor
                  AND after_member.cluster_occurrence
                        = candidate.cluster_occurrence
            LOOP
                parent_count := parent_count + 1;
                IF NOT EXISTS (
                    SELECT 1
                    FROM events source
                    WHERE source.id = parent.event_id
                      AND source.status = 'superseded'
                      AND source.superseded_at IS NOT NULL
                      AND source.superseded_transaction_id
                            = current_transaction_id
                      AND source.current_revision_id
                            = parent.event_revision_id
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
                IF NOT EXISTS (
                    SELECT 1
                    FROM event_lineages lineage
                    WHERE lineage.clustering_run_id
                            = candidate.clustering_run_id
                      AND lineage.relation_type = 'merged_from'
                      AND lineage.source_event_id = parent.event_id
                      AND lineage.target_event_id = candidate.event_id
                      AND lineage.rule_version
                            = candidate.reconciliation_rule_version
                      AND lineage.before_evidence_fingerprint
                            = parent.after_evidence_fingerprint
                      AND lineage.after_evidence_fingerprint
                            = candidate.after_evidence_fingerprint
                      AND lineage.created_transaction_id
                            = current_transaction_id
                ) THEN
                    RETURN false;
                END IF;
                IF EXISTS (
                    SELECT 1
                    FROM event_lineages other_outgoing
                    WHERE other_outgoing.source_event_id = parent.event_id
                      AND NOT (
                          other_outgoing.clustering_run_id
                                = candidate.clustering_run_id
                          AND other_outgoing.relation_type = 'merged_from'
                          AND other_outgoing.target_event_id
                                = candidate.event_id
                      )
                ) THEN
                    RETURN false;
                END IF;
            END LOOP;
            IF parent_count < 2 THEN
                RETURN false;
            END IF;
            IF (
                SELECT count(*)
                FROM event_lineages lineage
                WHERE lineage.clustering_run_id = candidate.clustering_run_id
                  AND lineage.relation_type = 'merged_from'
                  AND lineage.target_event_id = candidate.event_id
            ) <> parent_count THEN
                RETURN false;
            END IF;
            IF EXISTS (
                SELECT 1
                FROM event_lineages other_incoming
                WHERE other_incoming.target_event_id = candidate.event_id
                  AND NOT (
                      other_incoming.clustering_run_id
                            = candidate.clustering_run_id
                      AND other_incoming.relation_type = 'merged_from'
                  )
            ) THEN
                RETURN false;
            END IF;
            RETURN true;
        END
        $reader$;

        CREATE FUNCTION reader_require_complete_merge_projection(
            candidate cluster_event_projections
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            IF candidate.reconciliation_kind = 'merged'
               AND NOT reader_merge_projection_has_complete_terminal_graph(
                           candidate
                       ) THEN
                RAISE EXCEPTION 'event_merge_terminal_graph_incomplete: merge 必须在同一事务提交唯一 N→1 冻结父集合、逐父 Lineage、全部 superseded 父 Event 与无状态新生后继';
            END IF;
        END
        $reader$;

        CREATE FUNCTION reader_validate_current_merge_for_event(
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
                WHERE candidate.reconciliation_kind = 'merged'
                  AND candidate.created_transaction_id
                        = pg_current_xact_id()
                  AND (
                      candidate.event_id = affected_event_id
                      OR EXISTS (
                          SELECT 1
                          FROM clustering_run_memberships after_member
                          JOIN clustering_run_memberships before_member
                            ON before_member.run_id = after_member.run_id
                           AND before_member.snapshot_phase = 'before'
                           AND before_member.evidence_anchor
                                 = after_member.evidence_anchor
                           AND before_member.evidence_occurrence
                                 = after_member.evidence_occurrence
                          JOIN clustering_run_projection_predecessors frozen
                            ON frozen.run_id = before_member.run_id
                           AND frozen.cluster_anchor
                                 = before_member.cluster_anchor
                           AND frozen.cluster_occurrence
                                 = before_member.cluster_occurrence
                          JOIN cluster_event_projections predecessor
                            ON predecessor.id
                                  = frozen.predecessor_projection_id
                          WHERE after_member.run_id
                                  = candidate.clustering_run_id
                            AND after_member.snapshot_phase = 'after'
                            AND after_member.cluster_anchor
                                  = candidate.cluster_anchor
                            AND after_member.cluster_occurrence
                                  = candidate.cluster_occurrence
                            AND predecessor.event_id = affected_event_id
                      )
                  )
            LOOP
                PERFORM reader_require_complete_merge_projection(projection);
            END LOOP;
        END
        $reader$;

        CREATE FUNCTION reader_merge_terminal_projection_guard()
        RETURNS trigger LANGUAGE plpgsql AS $reader$
        BEGIN
            PERFORM reader_require_complete_merge_projection(NEW);
            PERFORM reader_validate_current_merge_for_event(NEW.event_id);
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_merge_terminal_event_guard()
        RETURNS trigger LANGUAGE plpgsql AS $reader$
        BEGIN
            PERFORM reader_validate_current_merge_for_event(NEW.id);
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_merge_terminal_revision_guard()
        RETURNS trigger LANGUAGE plpgsql AS $reader$
        BEGIN
            PERFORM reader_validate_current_merge_for_event(NEW.event_id);
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_merge_terminal_lineage_guard()
        RETURNS trigger LANGUAGE plpgsql AS $reader$
        BEGIN
            PERFORM reader_validate_current_merge_for_event(
                NEW.source_event_id
            );
            PERFORM reader_validate_current_merge_for_event(
                NEW.target_event_id
            );
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_merge_terminal_user_state_guard()
        RETURNS trigger LANGUAGE plpgsql AS $reader$
        BEGIN
            PERFORM reader_validate_current_merge_for_event(NEW.event_id);
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_merge_terminal_baseline_guard()
        RETURNS trigger LANGUAGE plpgsql AS $reader$
        BEGIN
            PERFORM reader_validate_current_merge_for_event(
                NEW.resolved_event_id
            );
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_merge_terminal_interaction_guard()
        RETURNS trigger LANGUAGE plpgsql AS $reader$
        BEGIN
            PERFORM reader_validate_current_merge_for_event(NEW.event_id);
            RETURN NULL;
        END
        $reader$;

        CREATE CONSTRAINT TRIGGER trg_merge_terminal_projection
        AFTER INSERT ON cluster_event_projections
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
        EXECUTE FUNCTION reader_merge_terminal_projection_guard();

        CREATE CONSTRAINT TRIGGER trg_merge_terminal_event
        AFTER INSERT OR UPDATE ON events
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
        EXECUTE FUNCTION reader_merge_terminal_event_guard();

        CREATE CONSTRAINT TRIGGER trg_merge_terminal_revision
        AFTER INSERT ON event_revisions
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
        EXECUTE FUNCTION reader_merge_terminal_revision_guard();

        CREATE CONSTRAINT TRIGGER trg_merge_terminal_lineage
        AFTER INSERT ON event_lineages
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
        EXECUTE FUNCTION reader_merge_terminal_lineage_guard();

        CREATE CONSTRAINT TRIGGER trg_merge_terminal_user_state
        AFTER INSERT OR UPDATE ON event_user_states
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
        EXECUTE FUNCTION reader_merge_terminal_user_state_guard();

        CREATE CONSTRAINT TRIGGER trg_merge_terminal_baseline
        AFTER INSERT OR UPDATE ON migration_baselines
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
        EXECUTE FUNCTION reader_merge_terminal_baseline_guard();

        CREATE CONSTRAINT TRIGGER trg_merge_terminal_interaction
        AFTER INSERT ON interaction_events
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
        EXECUTE FUNCTION reader_merge_terminal_interaction_guard();
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event merge Lineage 与多父终态约束不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
