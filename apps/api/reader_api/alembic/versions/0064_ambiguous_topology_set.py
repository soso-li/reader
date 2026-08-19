"""Evaluate ambiguous topology in one set query."""

from __future__ import annotations

from alembic import op


revision: str = "0064_ambiguous_topology_set"
down_revision: str | None = "0063_generation_retention"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        r"""
        ALTER TABLE clustering_runs
            ADD COLUMN completed_transaction_id xid8;

        ALTER TABLE clustering_runs
            ADD CONSTRAINT ck_clustering_run_completed_transaction
            CHECK (
                completed_transaction_id IS NULL
                OR (status = 'completed' AND after_snapshot_finalized)
            );

        CREATE FUNCTION reader_force_clustering_run_completed_transaction()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.status = 'completed'
                   AND NEW.after_snapshot_finalized THEN
                    NEW.completed_transaction_id := pg_current_xact_id();
                ELSE
                    NEW.completed_transaction_id := NULL;
                END IF;
                RETURN NEW;
            END IF;

            IF OLD.status <> 'completed'
               AND NEW.status = 'completed'
               AND NEW.after_snapshot_finalized THEN
                NEW.completed_transaction_id := pg_current_xact_id();
            ELSIF NEW.completed_transaction_id
                      IS DISTINCT FROM OLD.completed_transaction_id THEN
                RAISE EXCEPTION 'clustering_run_completed_transaction_immutable: Clustering Run 完成事务身份不可改写';
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE TRIGGER trg_00_clustering_run_completed_transaction
        BEFORE INSERT OR UPDATE ON clustering_runs
        FOR EACH ROW
        EXECUTE FUNCTION reader_force_clustering_run_completed_transaction();

        CREATE FUNCTION reader_expected_ambiguous_clusters(
            run_id_value varchar
        )
        RETURNS TABLE (
            snapshot_phase varchar,
            cluster_anchor varchar,
            cluster_occurrence integer
        )
        LANGUAGE sql
        STABLE
        AS $reader$
            WITH before_clusters AS MATERIALIZED (
                SELECT member.cluster_anchor,
                       member.cluster_occurrence,
                       count(*)::bigint AS evidence_count
                FROM clustering_run_memberships member
                WHERE member.run_id = run_id_value
                  AND member.snapshot_phase = 'before'
                GROUP BY member.cluster_anchor,
                         member.cluster_occurrence
            ),
            after_clusters AS MATERIALIZED (
                SELECT member.cluster_anchor,
                       member.cluster_occurrence,
                       count(*)::bigint AS evidence_count
                FROM clustering_run_memberships member
                WHERE member.run_id = run_id_value
                  AND member.snapshot_phase = 'after'
                GROUP BY member.cluster_anchor,
                         member.cluster_occurrence
            ),
            overlap_edges AS MATERIALIZED (
                SELECT before_member.cluster_anchor AS before_anchor,
                       before_member.cluster_occurrence
                           AS before_occurrence,
                       after_member.cluster_anchor AS after_anchor,
                       after_member.cluster_occurrence
                           AS after_occurrence,
                       count(*)::bigint AS shared_count
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
                GROUP BY before_member.cluster_anchor,
                         before_member.cluster_occurrence,
                         after_member.cluster_anchor,
                         after_member.cluster_occurrence
            ),
            before_degrees AS MATERIALIZED (
                SELECT edge.before_anchor,
                       edge.before_occurrence,
                       count(*)::bigint AS overlap_degree
                FROM overlap_edges edge
                GROUP BY edge.before_anchor,
                         edge.before_occurrence
            ),
            after_degrees AS MATERIALIZED (
                SELECT edge.after_anchor,
                       edge.after_occurrence,
                       count(*)::bigint AS overlap_degree
                FROM overlap_edges edge
                GROUP BY edge.after_anchor,
                         edge.after_occurrence
            ),
            continuation_edges AS MATERIALIZED (
                SELECT edge.before_anchor,
                       edge.before_occurrence,
                       edge.after_anchor,
                       edge.after_occurrence
                FROM overlap_edges edge
                JOIN before_clusters before_cluster
                  ON before_cluster.cluster_anchor = edge.before_anchor
                 AND before_cluster.cluster_occurrence
                       = edge.before_occurrence
                JOIN after_clusters after_cluster
                  ON after_cluster.cluster_anchor = edge.after_anchor
                 AND after_cluster.cluster_occurrence
                       = edge.after_occurrence
                JOIN before_degrees before_degree
                  ON before_degree.before_anchor = edge.before_anchor
                 AND before_degree.before_occurrence
                       = edge.before_occurrence
                JOIN after_degrees after_degree
                  ON after_degree.after_anchor = edge.after_anchor
                 AND after_degree.after_occurrence
                       = edge.after_occurrence
                JOIN clustering_run_projection_predecessors predecessor
                  ON predecessor.run_id = run_id_value
                 AND predecessor.cluster_anchor = edge.before_anchor
                 AND predecessor.cluster_occurrence
                       = edge.before_occurrence
                WHERE (
                    edge.shared_count = before_cluster.evidence_count
                    OR edge.shared_count = after_cluster.evidence_count
                )
                  AND before_degree.overlap_degree = 1
                  AND after_degree.overlap_degree = 1
            ),
            split_parents AS MATERIALIZED (
                SELECT edge.before_anchor,
                       edge.before_occurrence
                FROM overlap_edges edge
                JOIN after_degrees after_degree
                  ON after_degree.after_anchor = edge.after_anchor
                 AND after_degree.after_occurrence
                       = edge.after_occurrence
                GROUP BY edge.before_anchor,
                         edge.before_occurrence
                HAVING count(*) >= 2
                   AND bool_and(after_degree.overlap_degree = 1)
            ),
            split_children AS MATERIALIZED (
                SELECT DISTINCT edge.after_anchor,
                                edge.after_occurrence
                FROM overlap_edges edge
                JOIN split_parents parent
                  ON parent.before_anchor = edge.before_anchor
                 AND parent.before_occurrence = edge.before_occurrence
            ),
            merge_children AS MATERIALIZED (
                SELECT edge.after_anchor,
                       edge.after_occurrence
                FROM overlap_edges edge
                JOIN before_clusters before_cluster
                  ON before_cluster.cluster_anchor = edge.before_anchor
                 AND before_cluster.cluster_occurrence
                       = edge.before_occurrence
                JOIN before_degrees before_degree
                  ON before_degree.before_anchor = edge.before_anchor
                 AND before_degree.before_occurrence
                       = edge.before_occurrence
                GROUP BY edge.after_anchor,
                         edge.after_occurrence
                HAVING count(*) >= 2
                   AND bool_and(
                       edge.shared_count = before_cluster.evidence_count
                       AND before_degree.overlap_degree = 1
                   )
            ),
            merge_parents AS MATERIALIZED (
                SELECT DISTINCT edge.before_anchor,
                                edge.before_occurrence
                FROM overlap_edges edge
                JOIN merge_children child
                  ON child.after_anchor = edge.after_anchor
                 AND child.after_occurrence = edge.after_occurrence
            ),
            ambiguous_before AS MATERIALIZED (
                SELECT before_cluster.cluster_anchor,
                       before_cluster.cluster_occurrence
                FROM before_clusters before_cluster
                JOIN clustering_run_projection_predecessors predecessor
                  ON predecessor.run_id = run_id_value
                 AND predecessor.cluster_anchor
                       = before_cluster.cluster_anchor
                 AND predecessor.cluster_occurrence
                       = before_cluster.cluster_occurrence
                WHERE NOT EXISTS (
                          SELECT 1
                          FROM split_parents parent
                          WHERE parent.before_anchor
                                  = before_cluster.cluster_anchor
                            AND parent.before_occurrence
                                  = before_cluster.cluster_occurrence
                      )
                  AND NOT EXISTS (
                          SELECT 1
                          FROM merge_parents parent
                          WHERE parent.before_anchor
                                  = before_cluster.cluster_anchor
                            AND parent.before_occurrence
                                  = before_cluster.cluster_occurrence
                      )
                  AND NOT EXISTS (
                          SELECT 1
                          FROM continuation_edges continuation
                          WHERE continuation.before_anchor
                                  = before_cluster.cluster_anchor
                            AND continuation.before_occurrence
                                  = before_cluster.cluster_occurrence
                      )
            ),
            ambiguous_after AS MATERIALIZED (
                SELECT after_cluster.cluster_anchor,
                       after_cluster.cluster_occurrence
                FROM after_clusters after_cluster
                WHERE EXISTS (SELECT 1 FROM ambiguous_before)
                  AND NOT EXISTS (
                          SELECT 1
                          FROM split_children child
                          WHERE child.after_anchor
                                  = after_cluster.cluster_anchor
                            AND child.after_occurrence
                                  = after_cluster.cluster_occurrence
                      )
                  AND NOT EXISTS (
                          SELECT 1
                          FROM merge_children child
                          WHERE child.after_anchor
                                  = after_cluster.cluster_anchor
                            AND child.after_occurrence
                                  = after_cluster.cluster_occurrence
                      )
                  AND NOT EXISTS (
                          SELECT 1
                          FROM continuation_edges continuation
                          WHERE continuation.after_anchor
                                  = after_cluster.cluster_anchor
                            AND continuation.after_occurrence
                                  = after_cluster.cluster_occurrence
                      )
            )
            SELECT 'before'::varchar,
                   cluster_anchor,
                   cluster_occurrence
            FROM ambiguous_before
            UNION ALL
            SELECT 'after'::varchar,
                   cluster_anchor,
                   cluster_occurrence
            FROM ambiguous_after
        $reader$;

        CREATE OR REPLACE FUNCTION reader_expected_ambiguous_before(
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
                FROM reader_expected_ambiguous_clusters(run_id_value) expected
                WHERE expected.snapshot_phase = 'before'
                  AND expected.cluster_anchor = before_anchor_value
                  AND expected.cluster_occurrence = before_occurrence_value
            )
        $reader$;

        CREATE OR REPLACE FUNCTION reader_expected_ambiguous_after(
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
                FROM reader_expected_ambiguous_clusters(run_id_value) expected
                WHERE expected.snapshot_phase = 'after'
                  AND expected.cluster_anchor = after_anchor_value
                  AND expected.cluster_occurrence = after_occurrence_value
            )
        $reader$;

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
               AND EXISTS (
                    SELECT 1
                    FROM clustering_runs run
                    JOIN event_revisions target_revision
                      ON target_revision.id = candidate.event_revision_id
                     AND target_revision.event_id = candidate.event_id
                    WHERE run.id = candidate.clustering_run_id
                      AND run.status = 'completed'
                      AND run.after_snapshot_finalized
                      AND run.completed_transaction_id = pg_current_xact_id()
                      AND candidate.created_transaction_id
                            = run.completed_transaction_id
                      AND candidate.reconciliation_rule_version
                            = run.rule_version
                      AND target_revision.revision_no = 1
                      AND target_revision.evidence_fingerprint
                            = candidate.after_evidence_fingerprint
                      AND EXISTS (
                          SELECT 1
                          FROM clustering_run_memberships after_member
                          WHERE after_member.run_id = run.id
                            AND after_member.snapshot_phase = 'after'
                            AND after_member.cluster_anchor
                                  = candidate.cluster_anchor
                            AND after_member.cluster_occurrence
                                  = candidate.cluster_occurrence
                      )
                      AND EXISTS (
                          SELECT 1
                          FROM clustering_run_projection_predecessors frozen
                          WHERE frozen.run_id = run.id
                      )
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
                  AND predecessor.after_evidence_fingerprint
                        = candidate.before_evidence_fingerprint
                  AND predecessor_revision.evidence_fingerprint
                        = candidate.before_evidence_fingerprint
                  AND projection.after_evidence_fingerprint
                        = candidate.after_evidence_fingerprint
                  AND target_revision.evidence_fingerprint
                        = candidate.after_evidence_fingerprint
                  AND candidate.rule_version = run.rule_version
                  AND candidate.created_transaction_id
                        = projection.created_transaction_id
                  AND candidate.created_transaction_id
                        = run.completed_transaction_id
            )
        $reader$;

        CREATE FUNCTION reader_ambiguous_run_terminal_graph_status(
            run_id_value varchar
        )
        RETURNS varchar
        LANGUAGE sql
        VOLATILE
        AS $reader$
            WITH expected AS MATERIALIZED (
                SELECT *
                FROM reader_expected_ambiguous_clusters(run_id_value)
            ),
            run_row AS MATERIALIZED (
                SELECT run.id, run.rule_version
                FROM clustering_runs run
                WHERE run.id = run_id_value
                  AND run.status = 'completed'
                  AND run.after_snapshot_finalized
                  AND run.completed_transaction_id = pg_current_xact_id()
            ),
            transaction_row AS MATERIALIZED (
                SELECT pg_current_xact_id() AS transaction_id
            ),
            expected_after AS MATERIALIZED (
                SELECT cluster_anchor, cluster_occurrence
                FROM expected
                WHERE snapshot_phase = 'after'
            ),
            stored_ambiguous AS MATERIALIZED (
                SELECT stored.*
                FROM cluster_event_projections stored
                WHERE stored.clustering_run_id = run_id_value
                  AND stored.reconciliation_kind = 'ambiguous'
            ),
            expected_parent AS MATERIALIZED (
                SELECT DISTINCT predecessor.event_id,
                                predecessor.event_revision_id,
                                before_expected.cluster_anchor,
                                before_expected.cluster_occurrence
                FROM expected before_expected
                JOIN clustering_run_projection_predecessors frozen
                  ON frozen.run_id = run_id_value
                 AND frozen.cluster_anchor
                       = before_expected.cluster_anchor
                 AND frozen.cluster_occurrence
                       = before_expected.cluster_occurrence
                JOIN cluster_event_projections predecessor
                  ON predecessor.id = frozen.predecessor_projection_id
                WHERE before_expected.snapshot_phase = 'before'
            ),
            expected_edge AS MATERIALIZED (
                SELECT DISTINCT predecessor.event_id AS source_event_id,
                                target.event_id AS target_event_id,
                                predecessor.after_evidence_fingerprint
                                    AS before_fingerprint,
                                target.after_evidence_fingerprint
                                    AS after_fingerprint
                FROM expected before_expected
                JOIN clustering_run_memberships before_member
                  ON before_member.run_id = run_id_value
                 AND before_member.snapshot_phase = 'before'
                 AND before_member.cluster_anchor
                       = before_expected.cluster_anchor
                 AND before_member.cluster_occurrence
                       = before_expected.cluster_occurrence
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
                JOIN expected after_expected
                  ON after_expected.snapshot_phase = 'after'
                 AND after_expected.cluster_anchor
                       = after_member.cluster_anchor
                 AND after_expected.cluster_occurrence
                       = after_member.cluster_occurrence
                JOIN cluster_event_projections target
                  ON target.clustering_run_id = after_member.run_id
                 AND target.cluster_anchor = after_member.cluster_anchor
                 AND target.cluster_occurrence
                       = after_member.cluster_occurrence
                 AND target.reconciliation_kind = 'ambiguous'
                WHERE before_expected.snapshot_phase = 'before'
            )
            SELECT CASE
                WHEN NOT EXISTS (SELECT 1 FROM expected_after) THEN
                    CASE
                        WHEN EXISTS (SELECT 1 FROM stored_ambiguous)
                            THEN 'unexpected_ambiguous'::varchar
                        ELSE 'complete'::varchar
                    END
                WHEN (
                    EXISTS (SELECT 1 FROM run_row)
                    AND EXISTS (SELECT 1 FROM expected_parent)
                    AND NOT EXISTS (
                        SELECT 1
                        FROM expected_after expected_cluster
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM run_row run
                            CROSS JOIN transaction_row current_transaction
                            JOIN cluster_event_projections projection
                              ON projection.clustering_run_id = run.id
                             AND projection.cluster_anchor
                                   = expected_cluster.cluster_anchor
                             AND projection.cluster_occurrence
                                   = expected_cluster.cluster_occurrence
                            JOIN event_revisions target_revision
                              ON target_revision.id
                                   = projection.event_revision_id
                             AND target_revision.event_id
                                   = projection.event_id
                            JOIN events target
                              ON target.id = projection.event_id
                            WHERE projection.reconciliation_kind
                                    = 'ambiguous'
                              AND projection.predecessor_projection_id IS NULL
                              AND projection.before_evidence_fingerprint IS NULL
                              AND projection.reconciliation_rule_version
                                    = run.rule_version
                              AND projection.created_transaction_id
                                    = current_transaction.transaction_id
                              AND target_revision.revision_no = 1
                              AND target_revision.evidence_fingerprint
                                    = projection.after_evidence_fingerprint
                              AND target.status = 'active'
                              AND target.superseded_at IS NULL
                              AND target.superseded_transaction_id IS NULL
                              AND target.current_revision_id
                                    = target_revision.id
                              AND target.created_transaction_id
                                    = current_transaction.transaction_id
                              AND target_revision.created_transaction_id
                                    = current_transaction.transaction_id
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM event_revisions other_revision
                                  WHERE other_revision.event_id = target.id
                                    AND other_revision.id
                                          <> target_revision.id
                              )
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM cluster_event_projections other_projection
                                  WHERE other_projection.event_id = target.id
                                    AND other_projection.id <> projection.id
                              )
                              AND NOT EXISTS (
                                  SELECT 1 FROM event_user_states state
                                  WHERE state.event_id = target.id
                              )
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM migration_baselines baseline
                                  WHERE baseline.resolved_event_id = target.id
                              )
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM interaction_events interaction
                                  WHERE interaction.event_id = target.id
                              )
                              AND NOT EXISTS (
                                  SELECT 1 FROM event_lineages outgoing
                                  WHERE outgoing.source_event_id = target.id
                              )
                        )
                    )
                    AND (
                        SELECT count(*)
                        FROM stored_ambiguous
                    ) = (SELECT count(*) FROM expected_after)
                    AND NOT EXISTS (
                        SELECT 1
                        FROM expected_parent parent
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM events source
                            CROSS JOIN transaction_row current_transaction
                            WHERE source.id = parent.event_id
                              AND source.status = 'superseded'
                              AND source.superseded_at IS NOT NULL
                              AND source.superseded_transaction_id
                                    = current_transaction.transaction_id
                              AND source.current_revision_id
                                    = parent.event_revision_id
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM event_revisions new_revision
                                  WHERE new_revision.event_id = source.id
                                    AND new_revision.created_transaction_id
                                          = current_transaction.transaction_id
                              )
                        )
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM expected_edge edge
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM event_lineages lineage
                            CROSS JOIN run_row run
                            CROSS JOIN transaction_row current_transaction
                            WHERE lineage.clustering_run_id = run.id
                              AND lineage.relation_type = 'ambiguous_from'
                              AND lineage.source_event_id
                                    = edge.source_event_id
                              AND lineage.target_event_id
                                    = edge.target_event_id
                              AND lineage.rule_version = run.rule_version
                              AND lineage.before_evidence_fingerprint
                                    = edge.before_fingerprint
                              AND lineage.after_evidence_fingerprint
                                    = edge.after_fingerprint
                              AND lineage.created_transaction_id
                                    = current_transaction.transaction_id
                        )
                    )
                    AND (
                        SELECT count(*)
                        FROM event_lineages lineage
                        WHERE lineage.clustering_run_id = run_id_value
                          AND lineage.relation_type = 'ambiguous_from'
                    ) = (SELECT count(*) FROM expected_edge)
                ) THEN 'complete'::varchar
                ELSE 'incomplete'::varchar
            END
        $reader$;

        CREATE FUNCTION reader_ambiguous_run_has_complete_terminal_graph(
            run_id_value varchar
        )
        RETURNS boolean
        LANGUAGE sql
        VOLATILE
        AS $reader$
            SELECT reader_ambiguous_run_terminal_graph_status(run_id_value)
                       = 'complete'
        $reader$;

        CREATE OR REPLACE FUNCTION reader_ambiguous_projection_has_complete_terminal_graph(
            candidate cluster_event_projections
        )
        RETURNS boolean
        LANGUAGE sql
        VOLATILE
        AS $reader$
            SELECT candidate.reconciliation_kind = 'ambiguous'
               AND reader_expected_ambiguous_after(
                       candidate.clustering_run_id,
                       candidate.cluster_anchor,
                       candidate.cluster_occurrence
                   )
               AND reader_ambiguous_projection_has_newborn_target(candidate)
               AND reader_ambiguous_run_has_complete_terminal_graph(
                       candidate.clustering_run_id
                   )
        $reader$;

        CREATE OR REPLACE FUNCTION reader_require_complete_ambiguous_projection(
            candidate cluster_event_projections
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            IF candidate.reconciliation_kind = 'ambiguous'
               AND NOT reader_ambiguous_projection_has_newborn_target(
                          candidate
                      ) THEN
                RAISE EXCEPTION 'event_ambiguous_terminal_graph_incomplete: ambiguous 必须在同一事务提交全部未决父 Event、无状态新生后继及全部可证明 overlap Lineage';
            END IF;
        END
        $reader$;

        CREATE OR REPLACE FUNCTION reader_validate_current_ambiguous_for_event(
            affected_event_id integer
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            frozen_parent record;
        BEGIN
            IF affected_event_id IS NULL THEN
                RETURN;
            END IF;

            IF EXISTS (
                SELECT 1
                FROM cluster_event_projections projection
                WHERE projection.reconciliation_kind = 'ambiguous'
                  AND projection.created_transaction_id
                        = pg_current_xact_id()
                  AND projection.event_id = affected_event_id
                  AND NOT reader_ambiguous_projection_has_newborn_target(
                              projection
                          )
            ) THEN
                RAISE EXCEPTION 'event_ambiguous_terminal_graph_incomplete: ambiguous 必须在同一事务提交全部未决父 Event、无状态新生后继及全部可证明 overlap Lineage';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM event_lineages lineage
                JOIN cluster_event_projections target
                  ON target.clustering_run_id = lineage.clustering_run_id
                 AND target.event_id = lineage.target_event_id
                 AND target.reconciliation_kind = 'ambiguous'
                 AND target.created_transaction_id = pg_current_xact_id()
                WHERE lineage.relation_type = 'ambiguous_from'
                  AND lineage.created_transaction_id = pg_current_xact_id()
                  AND lineage.source_event_id = affected_event_id
                  AND NOT EXISTS (
                      SELECT 1
                      FROM events source
                      JOIN clustering_run_projection_predecessors frozen
                        ON frozen.run_id = lineage.clustering_run_id
                      JOIN cluster_event_projections predecessor
                        ON predecessor.id = frozen.predecessor_projection_id
                       AND predecessor.event_id = source.id
                      WHERE source.id = affected_event_id
                        AND source.status = 'superseded'
                        AND source.superseded_at IS NOT NULL
                        AND source.superseded_transaction_id
                              = pg_current_xact_id()
                        AND source.current_revision_id
                              = predecessor.event_revision_id
                        AND NOT EXISTS (
                            SELECT 1
                            FROM event_revisions new_revision
                            WHERE new_revision.event_id = source.id
                              AND new_revision.created_transaction_id
                                    = pg_current_xact_id()
                        )
                  )
            ) THEN
                RAISE EXCEPTION 'event_ambiguous_terminal_graph_incomplete: ambiguous 必须在同一事务提交全部未决父 Event、无状态新生后继及全部可证明 overlap Lineage';
            END IF;

            FOR frozen_parent IN
                SELECT DISTINCT projection.clustering_run_id AS run_id,
                                frozen.cluster_anchor,
                                frozen.cluster_occurrence,
                                predecessor.event_revision_id
                FROM cluster_event_projections projection
                JOIN clustering_run_projection_predecessors frozen
                  ON frozen.run_id = projection.clustering_run_id
                JOIN cluster_event_projections predecessor
                  ON predecessor.id = frozen.predecessor_projection_id
                WHERE projection.reconciliation_kind = 'ambiguous'
                  AND projection.created_transaction_id
                        = pg_current_xact_id()
                  AND predecessor.event_id = affected_event_id
                  AND NOT EXISTS (
                      SELECT 1
                      FROM event_lineages lineage
                      WHERE lineage.clustering_run_id
                              = projection.clustering_run_id
                        AND lineage.relation_type = 'ambiguous_from'
                        AND lineage.source_event_id = affected_event_id
                        AND lineage.created_transaction_id
                              = pg_current_xact_id()
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM cluster_event_projections resolved
                      WHERE resolved.clustering_run_id
                              = projection.clustering_run_id
                        AND resolved.reconciliation_kind
                              IN ('continued', 'split')
                        AND resolved.predecessor_projection_id
                              = predecessor.id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM event_lineages resolved_lineage
                      WHERE resolved_lineage.clustering_run_id
                              = projection.clustering_run_id
                        AND resolved_lineage.source_event_id
                              = predecessor.event_id
                        AND resolved_lineage.relation_type
                              IN ('split_from', 'merged_from')
                  )
            LOOP
                IF NOT EXISTS (
                       SELECT 1
                       FROM events source
                       WHERE source.id = affected_event_id
                         AND source.status = 'superseded'
                         AND source.superseded_at IS NOT NULL
                         AND source.superseded_transaction_id
                               = pg_current_xact_id()
                         AND source.current_revision_id
                               = frozen_parent.event_revision_id
                         AND NOT EXISTS (
                             SELECT 1
                             FROM event_revisions new_revision
                             WHERE new_revision.event_id = source.id
                               AND new_revision.created_transaction_id
                                     = pg_current_xact_id()
                         )
                   ) THEN
                    RAISE EXCEPTION 'event_ambiguous_terminal_graph_incomplete: ambiguous 必须在同一事务提交全部未决父 Event、无状态新生后继及全部可证明 overlap Lineage';
                END IF;
            END LOOP;
        END
        $reader$;

        CREATE OR REPLACE FUNCTION reader_require_complete_ambiguous_run(
            run_id_value varchar
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            audit_status varchar;
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
            SELECT reader_ambiguous_run_terminal_graph_status(run_id_value)
            INTO audit_status;
            IF audit_status = 'unexpected_ambiguous' THEN
                RAISE EXCEPTION 'event_projection_ambiguous_target_invalid: ambiguous projection 必须匹配完成快照及当前事务新建的无状态 Event/Revision';
            END IF;
            IF audit_status IS DISTINCT FROM 'complete' THEN
                RAISE EXCEPTION 'event_ambiguous_terminal_graph_incomplete: completed run 必须在同一事务提交全部 sealed-topology ambiguous Event、Projection、父级 supersede 与可证明 overlap Lineage';
            END IF;
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "ambiguous topology 集合化审计不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
