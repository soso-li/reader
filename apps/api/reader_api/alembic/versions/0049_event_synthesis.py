"""Add immutable, cited Event synthesis versions."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0049_event_synthesis"
down_revision: str | None = "0048_ambiguous_audit_fast_path"
branch_labels: str | None = None
depends_on: str | None = None

UUID_CHECK = (
    "uid ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    "[89ab][0-9a-f]{3}-[0-9a-f]{12}$'"
)
SHA256_CHECK = "{column} ~ '^[0-9a-f]{{64}}$'"


def upgrade() -> None:
    op.add_column(
        "events",
        sa.Column("current_synthesis_version_id", sa.Integer(), nullable=True),
    )
    op.create_unique_constraint(
        "uq_event_revision_evidence_snapshot_role",
        "event_revision_evidence",
        ["revision_id", "evidence_version_id", "evidence_type", "role"],
    )
    op.create_table(
        "evidence_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uid", sa.String(length=36), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("target_revision_id", sa.Integer(), nullable=False),
        sa.Column("source_coverage_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("content_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("policy_version", sa.String(length=120), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(UUID_CHECK, name="ck_evidence_snapshot_uid_uuid"),
        sa.CheckConstraint(
            SHA256_CHECK.format(column="source_coverage_fingerprint"),
            name="ck_evidence_snapshot_source_fingerprint_sha256",
        ),
        sa.CheckConstraint(
            SHA256_CHECK.format(column="content_fingerprint"),
            name="ck_evidence_snapshot_content_fingerprint_sha256",
        ),
        sa.CheckConstraint(
            "policy_version <> ''", name="ck_evidence_snapshot_policy_nonempty"
        ),
        sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["event_id", "target_revision_id"],
            ["event_revisions.event_id", "event_revisions.id"],
            name="fk_evidence_snapshot_target_revision",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uid", name="uq_evidence_snapshots_uid"),
        sa.UniqueConstraint(
            "event_id", "target_revision_id", "id", name="uq_evidence_snapshot_owner"
        ),
        sa.UniqueConstraint(
            "id", "target_revision_id", name="uq_evidence_snapshot_target"
        ),
    )
    op.create_table(
        "evidence_snapshot_members",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("snapshot_id", sa.Integer(), nullable=False),
        sa.Column("target_revision_id", sa.Integer(), nullable=False),
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
            "position >= 1", name="ck_evidence_snapshot_member_position"
        ),
        sa.CheckConstraint(
            "evidence_type IN ('article', 'social', 'notification')",
            name="ck_evidence_snapshot_member_type",
        ),
        sa.CheckConstraint(
            "role IN ('primary_source', 'corroboration', 'challenge', 'opinion', "
            "'social_reaction', 'material')",
            name="ck_evidence_snapshot_member_role",
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "target_revision_id"],
            ["evidence_snapshots.id", "evidence_snapshots.target_revision_id"],
            name="fk_evidence_snapshot_member_snapshot",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["target_revision_id", "evidence_version_id", "evidence_type", "role"],
            [
                "event_revision_evidence.revision_id",
                "event_revision_evidence.evidence_version_id",
                "event_revision_evidence.evidence_type",
                "event_revision_evidence.role",
            ],
            name="fk_evidence_snapshot_member_revision_evidence",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["evidence_version_id"],
            ["event_evidence_versions.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "snapshot_id",
            "evidence_version_id",
            name="uq_evidence_snapshot_member_version",
        ),
        sa.UniqueConstraint(
            "snapshot_id", "position", name="uq_evidence_snapshot_member_position"
        ),
        sa.UniqueConstraint(
            "snapshot_id",
            "evidence_version_id",
            "evidence_type",
            "role",
            name="uq_evidence_snapshot_member_citation_target",
        ),
    )
    op.execute(
        "ALTER TABLE evidence_snapshots ADD COLUMN created_transaction_id xid8 "
        "NOT NULL DEFAULT pg_current_xact_id()"
    )
    op.create_table(
        "synthesis_versions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uid", sa.String(length=36), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("target_revision_id", sa.Integer(), nullable=False),
        sa.Column("snapshot_id", sa.Integer(), nullable=False),
        sa.Column("source_count", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("model", sa.String(length=120), nullable=False),
        sa.Column("prompt_version", sa.String(length=120), nullable=False),
        sa.Column("schema_version", sa.String(length=120), nullable=False),
        sa.Column("generation_fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(UUID_CHECK, name="ck_synthesis_version_uid_uuid"),
        sa.CheckConstraint(
            "source_count >= 1", name="ck_synthesis_version_source_count"
        ),
        sa.CheckConstraint(
            "provider <> ''", name="ck_synthesis_version_provider_nonempty"
        ),
        sa.CheckConstraint("model <> ''", name="ck_synthesis_version_model_nonempty"),
        sa.CheckConstraint(
            "prompt_version <> ''", name="ck_synthesis_version_prompt_nonempty"
        ),
        sa.CheckConstraint(
            "schema_version <> ''", name="ck_synthesis_version_schema_nonempty"
        ),
        sa.CheckConstraint(
            SHA256_CHECK.format(column="generation_fingerprint"),
            name="ck_synthesis_version_generation_fingerprint_sha256",
        ),
        sa.ForeignKeyConstraint(
            ["event_id", "target_revision_id", "snapshot_id"],
            [
                "evidence_snapshots.event_id",
                "evidence_snapshots.target_revision_id",
                "evidence_snapshots.id",
            ],
            name="fk_synthesis_version_snapshot_owner",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uid", name="uq_synthesis_versions_uid"),
        sa.UniqueConstraint("event_id", "id", name="uq_synthesis_version_event_id"),
        sa.UniqueConstraint("id", "snapshot_id", name="uq_synthesis_version_snapshot"),
    )
    op.create_table(
        "synthesis_blocks",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uid", sa.String(length=36), nullable=False),
        sa.Column("synthesis_version_id", sa.Integer(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=24), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("attribution", sa.Text(), server_default="", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(UUID_CHECK, name="ck_synthesis_block_uid_uuid"),
        sa.CheckConstraint("position >= 1", name="ck_synthesis_block_position"),
        sa.CheckConstraint(
            "kind IN ('summary', 'fact', 'viewpoint', 'disagreement', 'uncertainty')",
            name="ck_synthesis_block_kind",
        ),
        sa.CheckConstraint(
            "btrim(body) <> ''", name="ck_synthesis_block_body_nonempty"
        ),
        sa.CheckConstraint(
            "kind <> 'viewpoint' OR btrim(attribution) <> ''",
            name="ck_synthesis_block_viewpoint_attribution",
        ),
        sa.ForeignKeyConstraint(
            ["synthesis_version_id"],
            ["synthesis_versions.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uid", name="uq_synthesis_blocks_uid"),
        sa.UniqueConstraint(
            "synthesis_version_id", "position", name="uq_synthesis_block_position"
        ),
        sa.UniqueConstraint(
            "id", "synthesis_version_id", name="uq_synthesis_block_version_id"
        ),
    )
    op.execute(
        "ALTER TABLE synthesis_versions ADD COLUMN created_transaction_id xid8 "
        "NOT NULL DEFAULT pg_current_xact_id()"
    )
    op.create_table(
        "synthesis_citations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("block_id", sa.Integer(), nullable=False),
        sa.Column("synthesis_version_id", sa.Integer(), nullable=False),
        sa.Column("snapshot_id", sa.Integer(), nullable=False),
        sa.Column("evidence_version_id", sa.Integer(), nullable=False),
        sa.Column("evidence_type", sa.String(length=24), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=40), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint("position >= 1", name="ck_synthesis_citation_position"),
        sa.CheckConstraint(
            "btrim(side) <> ''", name="ck_synthesis_citation_side_nonempty"
        ),
        sa.ForeignKeyConstraint(
            ["block_id", "synthesis_version_id"],
            ["synthesis_blocks.id", "synthesis_blocks.synthesis_version_id"],
            name="fk_synthesis_citation_block",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["synthesis_version_id", "snapshot_id"],
            ["synthesis_versions.id", "synthesis_versions.snapshot_id"],
            name="fk_synthesis_citation_version_snapshot",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "evidence_version_id", "evidence_type", "role"],
            [
                "evidence_snapshot_members.snapshot_id",
                "evidence_snapshot_members.evidence_version_id",
                "evidence_snapshot_members.evidence_type",
                "evidence_snapshot_members.role",
            ],
            name="fk_synthesis_citation_snapshot_member",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "block_id", "evidence_version_id", name="uq_synthesis_citation_evidence"
        ),
        sa.UniqueConstraint(
            "block_id", "position", name="uq_synthesis_citation_position"
        ),
    )
    op.create_foreign_key(
        "fk_event_current_synthesis_version",
        "events",
        "synthesis_versions",
        ["id", "current_synthesis_version_id"],
        ["event_id", "id"],
        ondelete="RESTRICT",
        deferrable=True,
        initially="DEFERRED",
    )
    _install_integrity_triggers()


def _install_integrity_triggers() -> None:
    op.execute(
        "CREATE TRIGGER trg_00_evidence_snapshot_created_transaction "
        "BEFORE INSERT ON evidence_snapshots FOR EACH ROW "
        "EXECUTE FUNCTION reader_force_created_transaction_id()"
    )
    op.execute(
        "CREATE TRIGGER trg_00_synthesis_version_created_transaction "
        "BEFORE INSERT ON synthesis_versions FOR EACH ROW "
        "EXECUTE FUNCTION reader_force_created_transaction_id()"
    )
    op.execute(
        """
        CREATE FUNCTION reader_reject_synthesis_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            RAISE EXCEPTION 'immutable_synthesis_artifact: %', TG_TABLE_NAME
                USING ERRCODE = '23514';
        END
        $reader$;

        CREATE FUNCTION reader_check_snapshot_members() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM evidence_snapshot_members member
                WHERE member.snapshot_id = NEW.id
            ) THEN
                RAISE EXCEPTION 'evidence_snapshot_without_members: %', NEW.id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_check_snapshot_member_insert() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM evidence_snapshots snapshot
                 WHERE snapshot.id = NEW.snapshot_id
                   AND snapshot.created_transaction_id = pg_current_xact_id()
            ) THEN
                RAISE EXCEPTION 'immutable_evidence_snapshot_members: %', NEW.snapshot_id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE FUNCTION reader_check_synthesis_block_insert() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM synthesis_versions version
                 WHERE version.id = NEW.synthesis_version_id
                   AND version.created_transaction_id = pg_current_xact_id()
            ) THEN
                RAISE EXCEPTION 'immutable_synthesis_blocks: %', NEW.synthesis_version_id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE FUNCTION reader_check_synthesis_citation_insert() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM synthesis_versions version
                 WHERE version.id = NEW.synthesis_version_id
                   AND version.created_transaction_id = pg_current_xact_id()
            ) THEN
                RAISE EXCEPTION 'immutable_synthesis_citations: %', NEW.synthesis_version_id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE FUNCTION reader_check_synthesis_version() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        DECLARE actual_source_count integer;
        BEGIN
            SELECT count(DISTINCT version.source_id)
              INTO actual_source_count
              FROM evidence_snapshot_members member
              JOIN event_evidence_versions version
                ON version.id = member.evidence_version_id
             WHERE member.snapshot_id = NEW.snapshot_id;
            IF actual_source_count <> NEW.source_count THEN
                RAISE EXCEPTION 'synthesis_source_count_mismatch: expected %, got %',
                    actual_source_count, NEW.source_count USING ERRCODE = '23514';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM synthesis_blocks block
                WHERE block.synthesis_version_id = NEW.id
            ) THEN
                RAISE EXCEPTION 'synthesis_version_without_blocks: %', NEW.id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_check_synthesis_block_citations(target_id bigint) RETURNS void
        LANGUAGE plpgsql AS $reader$
        DECLARE block_kind text;
        DECLARE citation_count integer;
        DECLARE side_count integer;
        BEGIN
            SELECT kind INTO block_kind FROM synthesis_blocks WHERE id = target_id;
            IF block_kind IS NULL THEN
                RETURN;
            END IF;
            SELECT count(*), count(DISTINCT side)
              INTO citation_count, side_count
              FROM synthesis_citations WHERE block_id = target_id;
            IF citation_count < 1 THEN
                RAISE EXCEPTION 'synthesis_block_without_citation: %', target_id
                    USING ERRCODE = '23514';
            END IF;
            IF block_kind = 'disagreement'
               AND (citation_count < 2 OR side_count < 2) THEN
                RAISE EXCEPTION 'synthesis_disagreement_requires_two_sides: %', target_id
                    USING ERRCODE = '23514';
            END IF;
            RETURN;
        END
        $reader$;

        CREATE FUNCTION reader_check_synthesis_block_row() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            PERFORM reader_check_synthesis_block_citations(NEW.id);
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_check_synthesis_citation_row() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            PERFORM reader_check_synthesis_block_citations(NEW.block_id);
            RETURN NULL;
        END
        $reader$;
        """
    )
    for table in (
        "evidence_snapshots",
        "evidence_snapshot_members",
        "synthesis_versions",
        "synthesis_blocks",
        "synthesis_citations",
    ):
        op.execute(
            f"CREATE TRIGGER {table}_immutable "
            f"BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW "
            "EXECUTE FUNCTION reader_reject_synthesis_mutation()"
        )
    op.execute(
        "CREATE TRIGGER evidence_snapshot_members_insert_guard "
        "BEFORE INSERT ON evidence_snapshot_members FOR EACH ROW "
        "EXECUTE FUNCTION reader_check_snapshot_member_insert()"
    )
    op.execute(
        "CREATE TRIGGER synthesis_blocks_insert_guard "
        "BEFORE INSERT ON synthesis_blocks FOR EACH ROW "
        "EXECUTE FUNCTION reader_check_synthesis_block_insert()"
    )
    op.execute(
        "CREATE TRIGGER synthesis_citations_insert_guard "
        "BEFORE INSERT ON synthesis_citations FOR EACH ROW "
        "EXECUTE FUNCTION reader_check_synthesis_citation_insert()"
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER evidence_snapshot_members_guard
        AFTER INSERT ON evidence_snapshots DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_check_snapshot_members();
        CREATE CONSTRAINT TRIGGER synthesis_version_guard
        AFTER INSERT ON synthesis_versions DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_check_synthesis_version();
        CREATE CONSTRAINT TRIGGER synthesis_block_citations_guard
        AFTER INSERT ON synthesis_blocks DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_check_synthesis_block_row();
        CREATE CONSTRAINT TRIGGER synthesis_citation_block_guard
        AFTER INSERT ON synthesis_citations DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_check_synthesis_citation_row();
        """
    )


def downgrade() -> None:
    raise RuntimeError("P0.3 合成稿契约不支持原地 downgrade；回滚必须恢复迁移前备份")
