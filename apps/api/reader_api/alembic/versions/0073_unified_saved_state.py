"""Merge starred and read-later into the authoritative saved state."""

from alembic import op


revision: str = "0073_unified_saved_state"
down_revision: str | None = "0072_reading_body_contract"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TEMPORARY TABLE issue87_saved_counts (
            source_id bigint PRIMARY KEY,
            saved_count bigint NOT NULL
        ) ON COMMIT DROP
        """
    )
    op.execute(
        """
        INSERT INTO issue87_saved_counts (source_id, saved_count)
        WITH ranked_saved_interactions AS (
            SELECT
                interaction.event_id,
                interaction.action,
                interaction.set_value,
                interaction.payload,
                row_number() OVER (
                    PARTITION BY interaction.event_id, interaction.action
                    ORDER BY interaction.recorded_at DESC, interaction.id DESC
                ) AS position
            FROM interaction_events AS interaction
            WHERE interaction.target_kind = 'event'
              AND interaction.action IN ('starred_set', 'read_later_set')
        ),
        latest_saved_interactions AS (
            SELECT event_id, action, set_value, payload
            FROM ranked_saved_interactions
            WHERE position = 1
        ),
        interaction_saved_sources AS (
            SELECT
                interaction.event_id,
                source_id.value::bigint AS source_id
            FROM latest_saved_interactions AS interaction
            CROSS JOIN LATERAL json_array_elements_text(
                interaction.payload -> 'metric_source_ids'
            ) AS source_id(value)
            WHERE interaction.set_value::jsonb = 'true'::jsonb
        ),
        baseline_saved_sources AS (
            SELECT
                baseline.resolved_event_id AS event_id,
                evidence.source_id
            FROM migration_baselines AS baseline
            JOIN event_revision_evidence AS member
              ON member.revision_id = baseline.resolved_revision_id
            JOIN event_evidence_versions AS evidence
              ON evidence.id = member.evidence_version_id
            WHERE baseline.legacy_object_type = 'cluster'
              AND (
                (
                    baseline.starred
                    AND NOT EXISTS (
                        SELECT 1
                        FROM latest_saved_interactions AS interaction
                        WHERE interaction.event_id = baseline.resolved_event_id
                          AND interaction.action = 'starred_set'
                    )
                )
                OR
                (
                    baseline.read_later
                    AND NOT EXISTS (
                        SELECT 1
                        FROM latest_saved_interactions AS interaction
                        WHERE interaction.event_id = baseline.resolved_event_id
                          AND interaction.action = 'read_later_set'
                    )
                )
              )
        ),
        saved_targets AS (
            SELECT DISTINCT
                source_id,
                'event'::text AS target_kind,
                event_id AS target_id
            FROM (
                SELECT event_id, source_id FROM interaction_saved_sources
                UNION ALL
                SELECT event_id, source_id FROM baseline_saved_sources
            ) AS event_sources
            UNION
            SELECT
                item.source_id,
                'item'::text AS target_kind,
                state.object_id AS target_id
            FROM user_states AS state
            JOIN content_items AS item
              ON state.object_type = 'item'
             AND item.id = state.object_id
            WHERE state.starred OR state.read_later
        )
        SELECT source_id, count(*)
        FROM saved_targets
        GROUP BY source_id
        """
    )
    op.execute(
        """
        INSERT INTO feed_metrics (
            source_id, fetched_count, read_count, opened_count, starred_count,
            read_later_count, cluster_count, duplicate_count, updated_at
        )
        SELECT source_id, 0, 0, 0, 0, 0, 0, 0, CURRENT_TIMESTAMP
        FROM issue87_saved_counts
        ON CONFLICT (source_id) DO NOTHING
        """
    )
    op.execute(
        """
        UPDATE feed_metrics
        SET starred_count = 0,
            read_later_count = 0,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    op.execute(
        """
        UPDATE feed_metrics AS metric
        SET starred_count = saved.saved_count,
            updated_at = CURRENT_TIMESTAMP
        FROM issue87_saved_counts AS saved
        WHERE saved.source_id = metric.source_id
        """
    )
    op.execute(
        """
        UPDATE event_user_states
        SET starred = starred OR read_later,
            read_later = false
        WHERE read_later
        """
    )
    op.execute(
        """
        UPDATE user_states
        SET starred = starred OR read_later,
            read_later = false
        WHERE read_later
        """
    )
    op.execute(
        """
        WITH cluster_sizes AS (
            SELECT cluster_id, count(*) AS item_count
            FROM cluster_items
            GROUP BY cluster_id
        ),
        source_topology AS (
            SELECT item.source_id,
                   count(DISTINCT member.cluster_id) AS cluster_count,
                   count(member.id) FILTER (
                       WHERE cluster_sizes.item_count > 1
                   ) AS duplicate_count
            FROM cluster_items AS member
            JOIN content_items AS item ON item.id = member.content_item_id
            JOIN cluster_sizes ON cluster_sizes.cluster_id = member.cluster_id
            GROUP BY item.source_id
        )
        UPDATE sources AS source
        SET feed_trust_score = LEAST(
            100.0,
            GREATEST(
                0.0,
                round(
                    (
                        metric.read_count
                        + metric.opened_count * 2
                        + metric.starred_count * 3
                        + COALESCE(topology.cluster_count, 0)
                        - COALESCE(topology.duplicate_count, 0)
                    ) * 100.0 / GREATEST(metric.fetched_count, 1),
                    1
                )
            )
        )
        FROM feed_metrics AS metric
        LEFT JOIN source_topology AS topology
          ON topology.source_id = metric.source_id
        WHERE metric.source_id = source.id
        """
    )
    op.execute("SET CONSTRAINTS ALL IMMEDIATE")
    with op.batch_alter_table("event_user_states") as batch:
        batch.create_check_constraint(
            "ck_event_user_state_read_later_retired",
            "read_later = false",
        )
    with op.batch_alter_table("user_states") as batch:
        batch.create_check_constraint(
            "ck_user_state_read_later_retired",
            "read_later = false",
        )
    with op.batch_alter_table("feed_metrics") as batch:
        batch.create_check_constraint(
            "ck_feed_metric_read_later_retired",
            "read_later_count = 0",
        )


def downgrade() -> None:
    raise RuntimeError(
        "统一收藏语义不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
