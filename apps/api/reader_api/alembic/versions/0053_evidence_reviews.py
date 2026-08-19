"""Add immutable Evidence Reviews and the reviewed watermark."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0053_evidence_reviews"
down_revision: str | None = "0052_synthesis_provenance_audit"
branch_labels: str | None = None
depends_on: str | None = None

UUID_CHECK = (
    "uid ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    "[89ab][0-9a-f]{3}-[0-9a-f]{12}$'"
)


def upgrade() -> None:
    op.drop_constraint(
        "ck_llm_task_active_synthesis_fingerprint",
        "llm_tasks",
        type_="check",
    )
    op.create_check_constraint(
        "ck_llm_task_active_synthesis_fingerprint",
        "llm_tasks",
        "task_type NOT IN ('event-synthesis', 'evidence-review') "
        "OR status NOT IN ('pending', 'running') "
        "OR input_fingerprint IS NOT NULL",
    )
    op.create_table(
        "evidence_reviews",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uid", sa.String(length=36), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("baseline_revision_id", sa.Integer(), nullable=False),
        sa.Column("baseline_snapshot_id", sa.Integer(), nullable=False),
        sa.Column("target_revision_id", sa.Integer(), nullable=False),
        sa.Column("target_snapshot_id", sa.Integer(), nullable=False),
        sa.Column("comparison_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("result", sa.String(length=24), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("model", sa.String(length=120), nullable=False),
        sa.Column("policy_version", sa.String(length=120), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(UUID_CHECK, name="ck_evidence_review_uid_uuid"),
        sa.CheckConstraint(
            "comparison_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_evidence_review_comparison_fingerprint_sha256",
        ),
        sa.CheckConstraint(
            "result IN ('ordinary', 'material', 'uncertain')",
            name="ck_evidence_review_result",
        ),
        sa.CheckConstraint("btrim(reason) <> ''", name="ck_evidence_review_reason"),
        sa.CheckConstraint("provider <> ''", name="ck_evidence_review_provider"),
        sa.CheckConstraint("model <> ''", name="ck_evidence_review_model"),
        sa.CheckConstraint(
            "policy_version <> ''", name="ck_evidence_review_policy_version"
        ),
        sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["event_id", "baseline_revision_id", "baseline_snapshot_id"],
            [
                "evidence_snapshots.event_id",
                "evidence_snapshots.target_revision_id",
                "evidence_snapshots.id",
            ],
            name="fk_evidence_review_baseline_snapshot",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["event_id", "target_revision_id", "target_snapshot_id"],
            [
                "evidence_snapshots.event_id",
                "evidence_snapshots.target_revision_id",
                "evidence_snapshots.id",
            ],
            name="fk_evidence_review_target_snapshot",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uid", name="uq_evidence_reviews_uid"),
        sa.UniqueConstraint("event_id", "id", name="uq_evidence_review_event_id"),
        sa.UniqueConstraint(
            "event_id",
            "comparison_fingerprint",
            name="uq_evidence_review_comparison_fingerprint",
        ),
        sa.UniqueConstraint(
            "id", "target_snapshot_id", name="uq_evidence_review_target_snapshot"
        ),
    )
    op.execute(
        "ALTER TABLE evidence_reviews ADD COLUMN created_transaction_id xid8 "
        "NOT NULL DEFAULT pg_current_xact_id()"
    )
    op.create_table(
        "evidence_review_citations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("review_id", sa.Integer(), nullable=False),
        sa.Column("target_snapshot_id", sa.Integer(), nullable=False),
        sa.Column("evidence_version_id", sa.Integer(), nullable=False),
        sa.Column("evidence_type", sa.String(length=24), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "position >= 1", name="ck_evidence_review_citation_position"
        ),
        sa.ForeignKeyConstraint(
            ["review_id", "target_snapshot_id"],
            ["evidence_reviews.id", "evidence_reviews.target_snapshot_id"],
            name="fk_evidence_review_citation_review",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["target_snapshot_id", "evidence_version_id", "evidence_type", "role"],
            [
                "evidence_snapshot_members.snapshot_id",
                "evidence_snapshot_members.evidence_version_id",
                "evidence_snapshot_members.evidence_type",
                "evidence_snapshot_members.role",
            ],
            name="fk_evidence_review_citation_snapshot_member",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "review_id",
            "evidence_version_id",
            name="uq_evidence_review_citation_evidence",
        ),
        sa.UniqueConstraint(
            "review_id", "position", name="uq_evidence_review_citation_position"
        ),
    )
    op.add_column(
        "events",
        sa.Column("reviewed_evidence_review_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_event_reviewed_evidence_review",
        "events",
        "evidence_reviews",
        ["id", "reviewed_evidence_review_id"],
        ["event_id", "id"],
        ondelete="RESTRICT",
        deferrable=True,
        initially="DEFERRED",
    )
    _install_integrity_triggers()


def _install_integrity_triggers() -> None:
    op.execute(
        "CREATE TRIGGER trg_00_evidence_review_created_transaction "
        "BEFORE INSERT ON evidence_reviews FOR EACH ROW "
        "EXECUTE FUNCTION reader_force_created_transaction_id()"
    )
    op.execute(
        """
        CREATE FUNCTION reader_check_evidence_review_citation_insert() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM evidence_reviews review
                 WHERE review.id = NEW.review_id
                   AND review.created_transaction_id = pg_current_xact_id()
            ) THEN
                RAISE EXCEPTION 'immutable_evidence_review_citations: %', NEW.review_id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE FUNCTION reader_check_evidence_review() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM evidence_review_citations citation
                 WHERE citation.review_id = NEW.id
            ) THEN
                RAISE EXCEPTION 'evidence_review_without_citations: %', NEW.id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END
        $reader$;
        """
    )
    for table in ("evidence_reviews", "evidence_review_citations"):
        op.execute(
            f"CREATE TRIGGER {table}_immutable "
            f"BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW "
            "EXECUTE FUNCTION reader_reject_synthesis_mutation()"
        )
    op.execute(
        "CREATE TRIGGER evidence_review_citations_insert_guard "
        "BEFORE INSERT ON evidence_review_citations FOR EACH ROW "
        "EXECUTE FUNCTION reader_check_evidence_review_citation_insert()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER evidence_review_citations_guard "
        "AFTER INSERT ON evidence_reviews DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION reader_check_evidence_review()"
    )


def downgrade() -> None:
    raise RuntimeError(
        "P0.3 Evidence Review 契约不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
