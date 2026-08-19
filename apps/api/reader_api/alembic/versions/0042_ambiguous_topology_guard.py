"""Derive ambiguous Event graphs from sealed topology at commit time."""

from __future__ import annotations

from alembic import op


revision: str = "0042_ambiguous_topology_guard"
down_revision: str | None = "0041_event_ambiguous_lineage"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        r"""
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM cluster_event_projections
                WHERE reconciliation_kind = 'ambiguous'
            ) OR EXISTS (
                SELECT 1 FROM event_lineages
                WHERE relation_type = 'ambiguous_from'
            ) THEN
                RAISE EXCEPTION 'event_ambiguous_topology_upgrade_requires_empty_graph: 0042 前的 ambiguous 图没有 sealed-topology 完整性证明，必须恢复或人工审计';
            END IF;
        END
        $reader$;

        CREATE FUNCTION reader_snapshot_clusters_overlap(
            run_id_value varchar,
            before_anchor_value varchar,
            before_occurrence_value integer,
            after_anchor_value varchar,
            after_occurrence_value integer
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT EXISTS (
                SELECT 1
                FROM clustering_run_memberships before_member
                JOIN clustering_run_memberships after_member
                  ON after_member.run_id = before_member.run_id
                 AND after_member.snapshot_phase = 'after'
                 AND after_member.evidence_anchor
                       = before_member.evidence_anchor
                 AND after_member.evidence_occurrence
                       = before_member.evidence_occurrence
                WHERE before_member.run_id = run_id_value
                  AND before_member.snapshot_phase = 'before'
                  AND before_member.cluster_anchor = before_anchor_value
                  AND before_member.cluster_occurrence
                        = before_occurrence_value
                  AND after_member.cluster_anchor = after_anchor_value
                  AND after_member.cluster_occurrence
                        = after_occurrence_value
            )
        $reader$;

        CREATE FUNCTION reader_snapshot_before_subset_after(
            run_id_value varchar,
            before_anchor_value varchar,
            before_occurrence_value integer,
            after_anchor_value varchar,
            after_occurrence_value integer
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT NOT EXISTS (
                SELECT 1
                FROM clustering_run_memberships before_member
                WHERE before_member.run_id = run_id_value
                  AND before_member.snapshot_phase = 'before'
                  AND before_member.cluster_anchor = before_anchor_value
                  AND before_member.cluster_occurrence
                        = before_occurrence_value
                  AND NOT EXISTS (
                      SELECT 1
                      FROM clustering_run_memberships after_member
                      WHERE after_member.run_id = before_member.run_id
                        AND after_member.snapshot_phase = 'after'
                        AND after_member.cluster_anchor = after_anchor_value
                        AND after_member.cluster_occurrence
                              = after_occurrence_value
                        AND after_member.evidence_anchor
                              = before_member.evidence_anchor
                        AND after_member.evidence_occurrence
                              = before_member.evidence_occurrence
                  )
            )
        $reader$;

        CREATE FUNCTION reader_snapshot_after_subset_before(
            run_id_value varchar,
            before_anchor_value varchar,
            before_occurrence_value integer,
            after_anchor_value varchar,
            after_occurrence_value integer
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT NOT EXISTS (
                SELECT 1
                FROM clustering_run_memberships after_member
                WHERE after_member.run_id = run_id_value
                  AND after_member.snapshot_phase = 'after'
                  AND after_member.cluster_anchor = after_anchor_value
                  AND after_member.cluster_occurrence
                        = after_occurrence_value
                  AND NOT EXISTS (
                      SELECT 1
                      FROM clustering_run_memberships before_member
                      WHERE before_member.run_id = after_member.run_id
                        AND before_member.snapshot_phase = 'before'
                        AND before_member.cluster_anchor = before_anchor_value
                        AND before_member.cluster_occurrence
                              = before_occurrence_value
                        AND before_member.evidence_anchor
                              = after_member.evidence_anchor
                        AND before_member.evidence_occurrence
                              = after_member.evidence_occurrence
                  )
            )
        $reader$;

        CREATE FUNCTION reader_snapshot_inclusion_edge(
            run_id_value varchar,
            before_anchor_value varchar,
            before_occurrence_value integer,
            after_anchor_value varchar,
            after_occurrence_value integer
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT reader_snapshot_clusters_overlap(
                       run_id_value,
                       before_anchor_value,
                       before_occurrence_value,
                       after_anchor_value,
                       after_occurrence_value
                   )
               AND (
                   reader_snapshot_before_subset_after(
                       run_id_value,
                       before_anchor_value,
                       before_occurrence_value,
                       after_anchor_value,
                       after_occurrence_value
                   )
                   OR reader_snapshot_after_subset_before(
                       run_id_value,
                       before_anchor_value,
                       before_occurrence_value,
                       after_anchor_value,
                       after_occurrence_value
                   )
               )
        $reader$;

        CREATE FUNCTION reader_snapshot_before_overlap_degree(
            run_id_value varchar,
            before_anchor_value varchar,
            before_occurrence_value integer
        )
        RETURNS integer
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT count(*)::integer
            FROM (
                SELECT DISTINCT after_member.cluster_anchor,
                                after_member.cluster_occurrence
                FROM clustering_run_memberships after_member
                WHERE after_member.run_id = run_id_value
                  AND after_member.snapshot_phase = 'after'
                  AND reader_snapshot_clusters_overlap(
                      run_id_value,
                      before_anchor_value,
                      before_occurrence_value,
                      after_member.cluster_anchor,
                      after_member.cluster_occurrence
                  )
            ) children
        $reader$;

        CREATE FUNCTION reader_snapshot_after_overlap_degree(
            run_id_value varchar,
            after_anchor_value varchar,
            after_occurrence_value integer
        )
        RETURNS integer
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT count(*)::integer
            FROM (
                SELECT DISTINCT before_member.cluster_anchor,
                                before_member.cluster_occurrence
                FROM clustering_run_memberships before_member
                WHERE before_member.run_id = run_id_value
                  AND before_member.snapshot_phase = 'before'
                  AND reader_snapshot_clusters_overlap(
                      run_id_value,
                      before_member.cluster_anchor,
                      before_member.cluster_occurrence,
                      after_anchor_value,
                      after_occurrence_value
                  )
            ) parents
        $reader$;

        CREATE FUNCTION reader_expected_continuation_edge(
            run_id_value varchar,
            before_anchor_value varchar,
            before_occurrence_value integer,
            after_anchor_value varchar,
            after_occurrence_value integer
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT EXISTS (
                       SELECT 1
                       FROM clustering_run_projection_predecessors frozen
                       WHERE frozen.run_id = run_id_value
                         AND frozen.cluster_anchor = before_anchor_value
                         AND frozen.cluster_occurrence
                               = before_occurrence_value
                   )
               AND reader_snapshot_inclusion_edge(
                       run_id_value,
                       before_anchor_value,
                       before_occurrence_value,
                       after_anchor_value,
                       after_occurrence_value
                   )
               AND reader_snapshot_before_overlap_degree(
                       run_id_value,
                       before_anchor_value,
                       before_occurrence_value
                   ) = 1
               AND reader_snapshot_after_overlap_degree(
                       run_id_value,
                       after_anchor_value,
                       after_occurrence_value
                   ) = 1
        $reader$;

        CREATE FUNCTION reader_expected_split_parent(
            run_id_value varchar,
            before_anchor_value varchar,
            before_occurrence_value integer
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT reader_snapshot_before_overlap_degree(
                       run_id_value,
                       before_anchor_value,
                       before_occurrence_value
                   ) >= 2
               AND NOT EXISTS (
                   SELECT 1
                   FROM (
                       SELECT DISTINCT after_member.cluster_anchor,
                                       after_member.cluster_occurrence
                       FROM clustering_run_memberships after_member
                       WHERE after_member.run_id = run_id_value
                         AND after_member.snapshot_phase = 'after'
                         AND reader_snapshot_clusters_overlap(
                             run_id_value,
                             before_anchor_value,
                             before_occurrence_value,
                             after_member.cluster_anchor,
                             after_member.cluster_occurrence
                         )
                   ) child
                   WHERE reader_snapshot_after_overlap_degree(
                             run_id_value,
                             child.cluster_anchor,
                             child.cluster_occurrence
                         ) <> 1
               )
        $reader$;

        CREATE FUNCTION reader_expected_split_child(
            run_id_value varchar,
            after_anchor_value varchar,
            after_occurrence_value integer
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT EXISTS (
                SELECT 1
                FROM (
                    SELECT DISTINCT before_member.cluster_anchor,
                                    before_member.cluster_occurrence
                    FROM clustering_run_memberships before_member
                    WHERE before_member.run_id = run_id_value
                      AND before_member.snapshot_phase = 'before'
                ) parent
                WHERE reader_expected_split_parent(
                          run_id_value,
                          parent.cluster_anchor,
                          parent.cluster_occurrence
                      )
                  AND reader_snapshot_clusters_overlap(
                          run_id_value,
                          parent.cluster_anchor,
                          parent.cluster_occurrence,
                          after_anchor_value,
                          after_occurrence_value
                      )
            )
        $reader$;

        CREATE FUNCTION reader_expected_merge_child(
            run_id_value varchar,
            after_anchor_value varchar,
            after_occurrence_value integer
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT reader_snapshot_after_overlap_degree(
                       run_id_value,
                       after_anchor_value,
                       after_occurrence_value
                   ) >= 2
               AND NOT EXISTS (
                   SELECT 1
                   FROM (
                       SELECT DISTINCT before_member.cluster_anchor,
                                       before_member.cluster_occurrence
                       FROM clustering_run_memberships before_member
                       WHERE before_member.run_id = run_id_value
                         AND before_member.snapshot_phase = 'before'
                         AND reader_snapshot_clusters_overlap(
                             run_id_value,
                             before_member.cluster_anchor,
                             before_member.cluster_occurrence,
                             after_anchor_value,
                             after_occurrence_value
                         )
                   ) parent
                   WHERE NOT reader_snapshot_before_subset_after(
                                 run_id_value,
                                 parent.cluster_anchor,
                                 parent.cluster_occurrence,
                                 after_anchor_value,
                                 after_occurrence_value
                             )
                      OR reader_snapshot_before_overlap_degree(
                             run_id_value,
                             parent.cluster_anchor,
                             parent.cluster_occurrence
                         ) <> 1
               )
        $reader$;

        CREATE FUNCTION reader_expected_merge_parent(
            run_id_value varchar,
            before_anchor_value varchar,
            before_occurrence_value integer
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT EXISTS (
                SELECT 1
                FROM (
                    SELECT DISTINCT after_member.cluster_anchor,
                                    after_member.cluster_occurrence
                    FROM clustering_run_memberships after_member
                    WHERE after_member.run_id = run_id_value
                      AND after_member.snapshot_phase = 'after'
                ) child
                WHERE reader_expected_merge_child(
                          run_id_value,
                          child.cluster_anchor,
                          child.cluster_occurrence
                      )
                  AND reader_snapshot_clusters_overlap(
                          run_id_value,
                          before_anchor_value,
                          before_occurrence_value,
                          child.cluster_anchor,
                          child.cluster_occurrence
                      )
            )
        $reader$;

        CREATE FUNCTION reader_expected_ambiguous_before(
            run_id_value varchar,
            before_anchor_value varchar,
            before_occurrence_value integer
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT EXISTS (
                       SELECT 1
                       FROM clustering_run_projection_predecessors frozen
                       WHERE frozen.run_id = run_id_value
                         AND frozen.cluster_anchor = before_anchor_value
                         AND frozen.cluster_occurrence
                               = before_occurrence_value
                   )
               AND NOT reader_expected_split_parent(
                           run_id_value,
                           before_anchor_value,
                           before_occurrence_value
                       )
               AND NOT reader_expected_merge_parent(
                           run_id_value,
                           before_anchor_value,
                           before_occurrence_value
                       )
               AND NOT EXISTS (
                   SELECT 1
                   FROM (
                       SELECT DISTINCT after_member.cluster_anchor,
                                       after_member.cluster_occurrence
                       FROM clustering_run_memberships after_member
                       WHERE after_member.run_id = run_id_value
                         AND after_member.snapshot_phase = 'after'
                   ) child
                   WHERE reader_expected_continuation_edge(
                             run_id_value,
                             before_anchor_value,
                             before_occurrence_value,
                             child.cluster_anchor,
                             child.cluster_occurrence
                         )
               )
        $reader$;

        CREATE FUNCTION reader_expected_ambiguous_after(
            run_id_value varchar,
            after_anchor_value varchar,
            after_occurrence_value integer
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT EXISTS (
                       SELECT 1
                       FROM clustering_run_memberships after_member
                       WHERE after_member.run_id = run_id_value
                         AND after_member.snapshot_phase = 'after'
                         AND after_member.cluster_anchor = after_anchor_value
                         AND after_member.cluster_occurrence
                               = after_occurrence_value
                   )
               AND NOT reader_expected_split_child(
                           run_id_value,
                           after_anchor_value,
                           after_occurrence_value
                       )
               AND NOT reader_expected_merge_child(
                           run_id_value,
                           after_anchor_value,
                           after_occurrence_value
                       )
               AND NOT EXISTS (
                   SELECT 1
                   FROM (
                       SELECT DISTINCT before_member.cluster_anchor,
                                       before_member.cluster_occurrence
                       FROM clustering_run_memberships before_member
                       WHERE before_member.run_id = run_id_value
                         AND before_member.snapshot_phase = 'before'
                   ) parent
                   WHERE reader_expected_continuation_edge(
                             run_id_value,
                             parent.cluster_anchor,
                             parent.cluster_occurrence,
                             after_anchor_value,
                             after_occurrence_value
                         )
               )
               AND EXISTS (
                   SELECT 1
                   FROM (
                       SELECT DISTINCT before_member.cluster_anchor,
                                       before_member.cluster_occurrence
                       FROM clustering_run_memberships before_member
                       WHERE before_member.run_id = run_id_value
                         AND before_member.snapshot_phase = 'before'
                   ) unresolved
                   WHERE reader_expected_ambiguous_before(
                             run_id_value,
                             unresolved.cluster_anchor,
                             unresolved.cluster_occurrence
                         )
               )
        $reader$;
        """
    )

    op.execute(
        r"""
        CREATE OR REPLACE FUNCTION reader_ambiguous_projection_matches_snapshot(
            candidate cluster_event_projections
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT candidate.reconciliation_kind = 'ambiguous'
               AND candidate.predecessor_projection_id IS NULL
               AND candidate.before_evidence_fingerprint IS NULL
               AND reader_expected_ambiguous_after(
                       candidate.clustering_run_id,
                       candidate.cluster_anchor,
                       candidate.cluster_occurrence
                   )
               AND EXISTS (
                    SELECT 1
                    FROM clustering_runs run
                    JOIN event_revisions target_revision
                      ON target_revision.id = candidate.event_revision_id
                     AND target_revision.event_id = candidate.event_id
                    WHERE run.id = candidate.clustering_run_id
                      AND run.status = 'completed'
                      AND run.after_snapshot_finalized
                      AND candidate.reconciliation_rule_version
                            = run.rule_version
                      AND target_revision.revision_no = 1
                      AND target_revision.evidence_fingerprint
                            = candidate.after_evidence_fingerprint
               )
        $reader$;

        CREATE OR REPLACE FUNCTION reader_ambiguous_lineage_matches_projection(
            candidate event_lineages
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
        AS $reader$
            SELECT EXISTS (
                SELECT 1
                FROM cluster_event_projections projection
                JOIN event_revisions target_revision
                  ON target_revision.id = projection.event_revision_id
                 AND target_revision.event_id = projection.event_id
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
                JOIN clustering_runs run
                  ON run.id = projection.clustering_run_id
                WHERE projection.clustering_run_id
                        = candidate.clustering_run_id
                  AND projection.reconciliation_kind = 'ambiguous'
                  AND projection.event_id = candidate.target_event_id
                  AND predecessor.event_id = candidate.source_event_id
                  AND reader_expected_ambiguous_before(
                          run.id,
                          before_member.cluster_anchor,
                          before_member.cluster_occurrence
                      )
                  AND reader_expected_ambiguous_after(
                          run.id,
                          after_member.cluster_anchor,
                          after_member.cluster_occurrence
                      )
                  AND predecessor.after_evidence_fingerprint
                        = candidate.before_evidence_fingerprint
                  AND predecessor_revision.evidence_fingerprint
                        = candidate.before_evidence_fingerprint
                  AND projection.after_evidence_fingerprint
                        = candidate.after_evidence_fingerprint
                  AND target_revision.evidence_fingerprint
                        = candidate.after_evidence_fingerprint
                  AND candidate.rule_version = run.rule_version
            )
        $reader$;
        """
    )

    op.execute(
        r"""
        CREATE OR REPLACE FUNCTION reader_ambiguous_projection_has_complete_terminal_graph(
            candidate cluster_event_projections
        )
        RETURNS boolean
        LANGUAGE plpgsql
        VOLATILE
        AS $reader$
        DECLARE
            expected_after record;
            expected_parent record;
            expected_edge record;
            projection cluster_event_projections%ROWTYPE;
            expected_after_count integer := 0;
            expected_parent_count integer := 0;
            expected_edge_count integer := 0;
            current_transaction_id xid8 := pg_current_xact_id();
        BEGIN
            IF NOT reader_ambiguous_projection_has_newborn_target(candidate) THEN
                RETURN false;
            END IF;

            FOR expected_after IN
                SELECT DISTINCT after_member.cluster_anchor,
                                after_member.cluster_occurrence
                FROM clustering_run_memberships after_member
                WHERE after_member.run_id = candidate.clustering_run_id
                  AND after_member.snapshot_phase = 'after'
                  AND reader_expected_ambiguous_after(
                          after_member.run_id,
                          after_member.cluster_anchor,
                          after_member.cluster_occurrence
                      )
                ORDER BY after_member.cluster_anchor,
                         after_member.cluster_occurrence
            LOOP
                expected_after_count := expected_after_count + 1;
                SELECT stored.* INTO projection
                FROM cluster_event_projections stored
                WHERE stored.clustering_run_id
                        = candidate.clustering_run_id
                  AND stored.cluster_anchor = expected_after.cluster_anchor
                  AND stored.cluster_occurrence
                        = expected_after.cluster_occurrence;
                IF NOT FOUND
                   OR projection.reconciliation_kind <> 'ambiguous'
                   OR projection.created_transaction_id
                        <> current_transaction_id
                   OR NOT reader_ambiguous_projection_has_newborn_target(
                              projection
                          ) THEN
                    RETURN false;
                END IF;
            END LOOP;
            IF expected_after_count < 1 THEN
                RETURN false;
            END IF;
            IF (
                SELECT count(*)
                FROM cluster_event_projections stored
                WHERE stored.clustering_run_id = candidate.clustering_run_id
                  AND stored.reconciliation_kind = 'ambiguous'
            ) <> expected_after_count THEN
                RETURN false;
            END IF;

            FOR expected_parent IN
                SELECT DISTINCT predecessor.event_id,
                                predecessor.event_revision_id,
                                predecessor.after_evidence_fingerprint,
                                before_member.cluster_anchor,
                                before_member.cluster_occurrence
                FROM clustering_run_memberships before_member
                JOIN clustering_run_projection_predecessors frozen
                  ON frozen.run_id = before_member.run_id
                 AND frozen.cluster_anchor = before_member.cluster_anchor
                 AND frozen.cluster_occurrence
                       = before_member.cluster_occurrence
                JOIN cluster_event_projections predecessor
                  ON predecessor.id = frozen.predecessor_projection_id
                WHERE before_member.run_id = candidate.clustering_run_id
                  AND before_member.snapshot_phase = 'before'
                  AND reader_expected_ambiguous_before(
                          before_member.run_id,
                          before_member.cluster_anchor,
                          before_member.cluster_occurrence
                      )
                ORDER BY predecessor.event_id
            LOOP
                expected_parent_count := expected_parent_count + 1;
                IF NOT EXISTS (
                    SELECT 1 FROM events source
                    WHERE source.id = expected_parent.event_id
                      AND source.status = 'superseded'
                      AND source.superseded_at IS NOT NULL
                      AND source.superseded_transaction_id
                            = current_transaction_id
                      AND source.current_revision_id
                            = expected_parent.event_revision_id
                      AND NOT EXISTS (
                          SELECT 1 FROM event_revisions new_revision
                          WHERE new_revision.event_id = source.id
                            AND new_revision.created_transaction_id
                                  = current_transaction_id
                      )
                ) THEN
                    RETURN false;
                END IF;
            END LOOP;
            IF expected_parent_count < 1 THEN
                RETURN false;
            END IF;

            FOR expected_edge IN
                SELECT DISTINCT predecessor.event_id AS source_event_id,
                                target.event_id AS target_event_id,
                                predecessor.after_evidence_fingerprint
                                    AS before_fingerprint,
                                target.after_evidence_fingerprint
                                    AS after_fingerprint
                FROM clustering_run_memberships before_member
                JOIN clustering_run_projection_predecessors frozen
                  ON frozen.run_id = before_member.run_id
                 AND frozen.cluster_anchor = before_member.cluster_anchor
                 AND frozen.cluster_occurrence
                       = before_member.cluster_occurrence
                JOIN cluster_event_projections predecessor
                  ON predecessor.id = frozen.predecessor_projection_id
                JOIN clustering_run_memberships after_member
                  ON after_member.run_id = before_member.run_id
                 AND after_member.snapshot_phase = 'after'
                 AND after_member.evidence_anchor
                       = before_member.evidence_anchor
                 AND after_member.evidence_occurrence
                       = before_member.evidence_occurrence
                JOIN cluster_event_projections target
                  ON target.clustering_run_id = after_member.run_id
                 AND target.cluster_anchor = after_member.cluster_anchor
                 AND target.cluster_occurrence
                       = after_member.cluster_occurrence
                 AND target.reconciliation_kind = 'ambiguous'
                WHERE before_member.run_id = candidate.clustering_run_id
                  AND before_member.snapshot_phase = 'before'
                  AND reader_expected_ambiguous_before(
                          before_member.run_id,
                          before_member.cluster_anchor,
                          before_member.cluster_occurrence
                      )
                  AND reader_expected_ambiguous_after(
                          after_member.run_id,
                          after_member.cluster_anchor,
                          after_member.cluster_occurrence
                      )
            LOOP
                expected_edge_count := expected_edge_count + 1;
                IF NOT EXISTS (
                    SELECT 1 FROM event_lineages lineage
                    WHERE lineage.clustering_run_id
                            = candidate.clustering_run_id
                      AND lineage.relation_type = 'ambiguous_from'
                      AND lineage.source_event_id
                            = expected_edge.source_event_id
                      AND lineage.target_event_id
                            = expected_edge.target_event_id
                      AND lineage.rule_version
                            = candidate.reconciliation_rule_version
                      AND lineage.before_evidence_fingerprint
                            = expected_edge.before_fingerprint
                      AND lineage.after_evidence_fingerprint
                            = expected_edge.after_fingerprint
                      AND lineage.created_transaction_id
                            = current_transaction_id
                ) THEN
                    RETURN false;
                END IF;
            END LOOP;
            IF (
                SELECT count(*)
                FROM event_lineages lineage
                WHERE lineage.clustering_run_id = candidate.clustering_run_id
                  AND lineage.relation_type = 'ambiguous_from'
            ) <> expected_edge_count THEN
                RETURN false;
            END IF;
            RETURN true;
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "ambiguous sealed-topology 完整性约束不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
