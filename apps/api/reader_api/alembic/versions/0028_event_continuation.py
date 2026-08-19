"""Record conservative one-to-one Event continuation projections."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0028_event_continuation"
down_revision: str = "0027_user_state_baseline"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


SHA256_CHECK = "{column} ~ '^[0-9a-f]{{64}}$'"


def upgrade() -> None:
    op.add_column(
        "cluster_event_projections",
        sa.Column("predecessor_projection_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "cluster_event_projections",
        sa.Column("reconciliation_kind", sa.String(length=20), nullable=True),
    )
    op.add_column(
        "cluster_event_projections",
        sa.Column(
            "reconciliation_rule_version", sa.String(length=120), nullable=True
        ),
    )
    op.add_column(
        "cluster_event_projections",
        sa.Column(
            "before_evidence_fingerprint", sa.String(length=64), nullable=True
        ),
    )
    op.add_column(
        "cluster_event_projections",
        sa.Column(
            "after_evidence_fingerprint", sa.String(length=64), nullable=True
        ),
    )

    op.execute(
        "ALTER TABLE cluster_event_projections "
        "DISABLE TRIGGER trg_cluster_event_projection_guard"
    )
    op.execute(
        "UPDATE cluster_event_projections projection "
        "SET reconciliation_kind = 'initial', "
        "    after_evidence_fingerprint = revision.evidence_fingerprint "
        "FROM event_revisions revision "
        "WHERE revision.id = projection.event_revision_id "
        "  AND revision.event_id = projection.event_id"
    )
    op.execute(
        "ALTER TABLE cluster_event_projections "
        "ENABLE TRIGGER trg_cluster_event_projection_guard"
    )

    op.alter_column(
        "cluster_event_projections",
        "reconciliation_kind",
        existing_type=sa.String(length=20),
        nullable=False,
    )
    op.alter_column(
        "cluster_event_projections",
        "after_evidence_fingerprint",
        existing_type=sa.String(length=64),
        nullable=False,
    )
    op.create_foreign_key(
        "fk_cluster_event_projection_predecessor",
        "cluster_event_projections",
        "cluster_event_projections",
        ["predecessor_projection_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_cluster_event_projections_predecessor",
        "cluster_event_projections",
        ["predecessor_projection_id"],
    )
    op.create_check_constraint(
        "ck_cluster_event_projection_reconciliation_kind",
        "cluster_event_projections",
        "reconciliation_kind IN ('initial', 'continued')",
    )
    op.create_check_constraint(
        "ck_cluster_event_projection_before_fingerprint_sha256",
        "cluster_event_projections",
        "before_evidence_fingerprint IS NULL OR "
        + SHA256_CHECK.format(column="before_evidence_fingerprint"),
    )
    op.create_check_constraint(
        "ck_cluster_event_projection_after_fingerprint_sha256",
        "cluster_event_projections",
        SHA256_CHECK.format(column="after_evidence_fingerprint"),
    )
    op.create_check_constraint(
        "ck_cluster_event_projection_reconciliation_shape",
        "cluster_event_projections",
        "(reconciliation_kind = 'initial' "
        " AND predecessor_projection_id IS NULL "
        " AND reconciliation_rule_version IS NULL "
        " AND before_evidence_fingerprint IS NULL) OR "
        "(reconciliation_kind = 'continued' "
        " AND predecessor_projection_id IS NOT NULL "
        " AND reconciliation_rule_version IS NOT NULL "
        " AND reconciliation_rule_version <> '' "
        " AND before_evidence_fingerprint IS NOT NULL)",
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
        "Event continuation 历史不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
