from __future__ import annotations


AMBIGUOUS_AUDIT_STAGE_REVISION = "0043_ambiguous_run_guard"

AMBIGUOUS_MEMBERSHIP_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS ix_clustering_run_membership_evidence_lookup
ON clustering_run_memberships (
    run_id,
    snapshot_phase,
    evidence_anchor,
    evidence_occurrence,
    cluster_anchor,
    cluster_occurrence
)
"""

AMBIGUOUS_AFTER_FAST_PATH_SQL = r"""
CREATE OR REPLACE FUNCTION reader_expected_ambiguous_after(
    run_id_value varchar,
    after_anchor_value varchar,
    after_occurrence_value integer
)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $reader$
DECLARE
    parent_record record;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM clustering_run_memberships after_member
        WHERE after_member.run_id = run_id_value
          AND after_member.snapshot_phase = 'after'
          AND after_member.cluster_anchor = after_anchor_value
          AND after_member.cluster_occurrence = after_occurrence_value
    ) THEN
        RETURN false;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM clustering_run_projection_predecessors predecessor
        WHERE predecessor.run_id = run_id_value
    ) THEN
        RETURN false;
    END IF;

    FOR parent_record IN
        SELECT DISTINCT before_member.cluster_anchor,
                        before_member.cluster_occurrence
        FROM clustering_run_memberships after_member
        JOIN clustering_run_memberships before_member
          ON before_member.run_id = after_member.run_id
         AND before_member.snapshot_phase = 'before'
         AND before_member.evidence_anchor = after_member.evidence_anchor
         AND before_member.evidence_occurrence = after_member.evidence_occurrence
        WHERE after_member.run_id = run_id_value
          AND after_member.snapshot_phase = 'after'
          AND after_member.cluster_anchor = after_anchor_value
          AND after_member.cluster_occurrence = after_occurrence_value
    LOOP
        IF reader_expected_continuation_edge(
            run_id_value,
            parent_record.cluster_anchor,
            parent_record.cluster_occurrence,
            after_anchor_value,
            after_occurrence_value
        ) THEN
            RETURN false;
        END IF;
    END LOOP;

    IF reader_expected_split_child(
        run_id_value,
        after_anchor_value,
        after_occurrence_value
    ) OR reader_expected_merge_child(
        run_id_value,
        after_anchor_value,
        after_occurrence_value
    ) THEN
        RETURN false;
    END IF;

    RETURN EXISTS (
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
    );
END
$reader$
"""
