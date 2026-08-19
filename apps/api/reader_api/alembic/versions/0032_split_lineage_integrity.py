"""Bind split projections and Lineage to the run-frozen predecessor graph."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0032_split_lineage_integrity"
down_revision: str = "0031_event_split_lineage"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE FUNCTION reader_split_projection_matches_frozen_predecessor(
            candidate cluster_event_projections
        )
        RETURNS boolean
        LANGUAGE plpgsql
        STABLE
        AS $reader$
        DECLARE
            before_anchor text;
            before_occurrence integer;
            unique_child_count integer;
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
                FROM clustering_run_memberships after_member
                JOIN clustering_run_memberships other_before
                  ON other_before.run_id = after_member.run_id
                 AND other_before.snapshot_phase = 'before'
                 AND other_before.evidence_anchor
                       = after_member.evidence_anchor
                 AND other_before.evidence_occurrence
                       = after_member.evidence_occurrence
                WHERE after_member.run_id = candidate.clustering_run_id
                  AND after_member.snapshot_phase = 'after'
                  AND after_member.cluster_anchor = candidate.cluster_anchor
                  AND after_member.cluster_occurrence
                        = candidate.cluster_occurrence
                  AND ROW(other_before.cluster_anchor,
                          other_before.cluster_occurrence)
                      IS DISTINCT FROM ROW(before_anchor, before_occurrence)
            ) THEN
                RETURN false;
            END IF;

            SELECT count(*)
            INTO unique_child_count
            FROM (
                SELECT after_member.cluster_anchor,
                       after_member.cluster_occurrence
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
                  AND NOT EXISTS (
                      SELECT 1
                      FROM clustering_run_memberships child_member
                      JOIN clustering_run_memberships other_before
                        ON other_before.run_id = child_member.run_id
                       AND other_before.snapshot_phase = 'before'
                       AND other_before.evidence_anchor
                             = child_member.evidence_anchor
                       AND other_before.evidence_occurrence
                             = child_member.evidence_occurrence
                      WHERE child_member.run_id = after_member.run_id
                        AND child_member.snapshot_phase = 'after'
                        AND child_member.cluster_anchor
                              = after_member.cluster_anchor
                        AND child_member.cluster_occurrence
                              = after_member.cluster_occurrence
                        AND ROW(other_before.cluster_anchor,
                                other_before.cluster_occurrence)
                            IS DISTINCT FROM
                                ROW(before_anchor, before_occurrence)
                  )
                GROUP BY after_member.cluster_anchor,
                         after_member.cluster_occurrence
            ) unique_children;
            RETURN unique_child_count >= 2;
        END
        $reader$;

        CREATE FUNCTION reader_split_lineage_matches_projection(
            candidate event_lineages
        )
        RETURNS boolean
        LANGUAGE sql
        STABLE
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
            )
        $reader$;

        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM cluster_event_projections projection
                WHERE projection.reconciliation_kind = 'split'
                  AND NOT reader_split_projection_matches_frozen_predecessor(
                              projection
                          )
            ) THEN
                RAISE EXCEPTION 'event_split_projection_integrity_upgrade_invalid: 既有 split projection 未绑定 run 冻结前驱';
            END IF;
            IF EXISTS (
                SELECT 1
                FROM event_lineages lineage
                WHERE lineage.relation_type = 'split_from'
                  AND NOT reader_split_lineage_matches_projection(lineage)
            ) THEN
                RAISE EXCEPTION 'event_split_lineage_integrity_upgrade_invalid: 既有 split Lineage 与 projection 审计链不一致';
            END IF;
        END
        $reader$;
        """
    )

    op.execute(
        """
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
                RAISE EXCEPTION 'event_lineage_lifecycle_mismatch: split 来源必须 superseded，后继必须 active';
            END IF;
            IF NOT reader_split_lineage_matches_projection(NEW) THEN
                RAISE EXCEPTION 'event_lineage_projection_mismatch: split Lineage 必须匹配 run 冻结前驱、projection 与 Revision 指纹';
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


def downgrade() -> None:
    raise RuntimeError(
        "Event split Lineage 完整性约束不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
