"""Record stateless Event successors for conservative one-to-many splits."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0031_event_split_lineage"
down_revision: str = "0030_event_revision_recurrence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


UUID_CHECK = (
    "uid ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    "[89ab][0-9a-f]{3}-[0-9a-f]{12}$'"
)
SHA256_CHECK = "{column} ~ '^[0-9a-f]{{64}}$'"


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
        "reconciliation_kind IN ('initial', 'continued', 'split')",
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
        " AND before_evidence_fingerprint IS NOT NULL)",
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_cluster_event_projection_split_event "
        "ON cluster_event_projections (clustering_run_id, event_id) "
        "WHERE reconciliation_kind = 'split'"
    )

    op.create_table(
        "event_lineages",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uid", sa.String(length=36), nullable=False),
        sa.Column("clustering_run_id", sa.String(length=36), nullable=False),
        sa.Column("relation_type", sa.String(length=24), nullable=False),
        sa.Column("source_event_id", sa.Integer(), nullable=False),
        sa.Column("target_event_id", sa.Integer(), nullable=False),
        sa.Column("rule_version", sa.String(length=120), nullable=False),
        sa.Column(
            "before_evidence_fingerprint", sa.String(length=64), nullable=False
        ),
        sa.Column(
            "after_evidence_fingerprint", sa.String(length=64), nullable=False
        ),
        sa.Column("decision_reason", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(UUID_CHECK, name="ck_event_lineage_uid_uuid"),
        sa.CheckConstraint(
            "relation_type = 'split_from'",
            name="ck_event_lineage_relation_type",
        ),
        sa.CheckConstraint(
            "source_event_id <> target_event_id",
            name="ck_event_lineage_distinct_events",
        ),
        sa.CheckConstraint(
            SHA256_CHECK.format(column="before_evidence_fingerprint"),
            name="ck_event_lineage_before_fingerprint_sha256",
        ),
        sa.CheckConstraint(
            SHA256_CHECK.format(column="after_evidence_fingerprint"),
            name="ck_event_lineage_after_fingerprint_sha256",
        ),
        sa.CheckConstraint(
            "btrim(rule_version) <> ''",
            name="ck_event_lineage_rule_version_nonempty",
        ),
        sa.CheckConstraint(
            "btrim(decision_reason) <> ''",
            name="ck_event_lineage_decision_reason_nonempty",
        ),
        sa.ForeignKeyConstraint(
            ["clustering_run_id"],
            ["clustering_runs.id"],
            name="fk_event_lineage_run",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_event_id"],
            ["events.id"],
            name="fk_event_lineage_source_event",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["target_event_id"],
            ["events.id"],
            name="fk_event_lineage_target_event",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uid", name="uq_event_lineages_uid"),
        sa.UniqueConstraint(
            "clustering_run_id",
            "relation_type",
            "source_event_id",
            "target_event_id",
            name="uq_event_lineage_relation",
        ),
    )
    op.create_index(
        "ix_event_lineages_source",
        "event_lineages",
        ["source_event_id", "id"],
    )
    op.create_index(
        "ix_event_lineages_target",
        "event_lineages",
        ["target_event_id", "id"],
    )
    op.create_index(
        "ix_event_lineages_run",
        "event_lineages",
        ["clustering_run_id", "id"],
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_event_lineage_split_target "
        "ON event_lineages (clustering_run_id, target_event_id) "
        "WHERE relation_type = 'split_from'"
    )

    op.execute(
        """
        CREATE FUNCTION reader_event_lineage_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            run_completed boolean;
            run_rule_version text;
            source_status text;
            target_status text;
            projection_source_event_id integer;
            projection_before_fingerprint text;
            projection_after_fingerprint text;
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

            SELECT predecessor.event_id,
                   projection.before_evidence_fingerprint,
                   projection.after_evidence_fingerprint
            INTO projection_source_event_id,
                 projection_before_fingerprint,
                 projection_after_fingerprint
            FROM cluster_event_projections projection
            JOIN cluster_event_projections predecessor
              ON predecessor.id = projection.predecessor_projection_id
            WHERE projection.clustering_run_id = NEW.clustering_run_id
              AND projection.event_id = NEW.target_event_id
              AND projection.reconciliation_kind = 'split';
            IF projection_source_event_id IS DISTINCT FROM NEW.source_event_id THEN
                RAISE EXCEPTION 'event_lineage_projection_mismatch: split Lineage 必须匹配精确 predecessor projection';
            END IF;
            IF NEW.before_evidence_fingerprint
                   IS DISTINCT FROM projection_before_fingerprint
               OR NEW.after_evidence_fingerprint
                   IS DISTINCT FROM projection_after_fingerprint THEN
                RAISE EXCEPTION 'event_lineage_fingerprint_mismatch: Lineage 指纹必须匹配 split projection';
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

        CREATE TRIGGER trg_event_lineage_guard
        BEFORE INSERT OR UPDATE OR DELETE ON event_lineages
        FOR EACH ROW EXECUTE FUNCTION reader_event_lineage_guard();
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event split Lineage 历史不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
