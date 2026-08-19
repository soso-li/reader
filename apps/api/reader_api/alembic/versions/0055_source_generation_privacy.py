"""Add fail-closed source privacy policy for external generation."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0055_source_generation_privacy"
down_revision: str | None = "0054_generation_lifecycle"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "sources",
        sa.Column(
            "privacy_class",
            sa.String(length=20),
            server_default="unclassified",
            nullable=False,
        ),
    )
    op.add_column(
        "sources",
        sa.Column(
            "external_generation_allowed",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )
    op.add_column(
        "sources",
        sa.Column(
            "generation_policy_version",
            sa.Integer(),
            server_default=sa.text("1"),
            nullable=False,
        ),
    )
    op.create_check_constraint(
        "ck_source_privacy_class",
        "sources",
        "privacy_class IN ('unclassified', 'public', 'private')",
    )
    op.create_check_constraint(
        "ck_source_external_generation_public",
        "sources",
        "external_generation_allowed = false OR privacy_class = 'public'",
    )
    op.create_check_constraint(
        "ck_source_generation_policy_version_positive",
        "sources",
        "generation_policy_version >= 1",
    )
    op.execute(
        """
        CREATE FUNCTION reader_bump_source_generation_policy_version() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NEW.privacy_class IS DISTINCT FROM OLD.privacy_class
               OR NEW.external_generation_allowed IS DISTINCT FROM OLD.external_generation_allowed THEN
                NEW.generation_policy_version := OLD.generation_policy_version + 1;
            ELSIF NEW.generation_policy_version IS DISTINCT FROM OLD.generation_policy_version THEN
                RAISE EXCEPTION 'source_generation_policy_version_is_managed: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $reader$;
        """
    )
    op.execute(
        "CREATE TRIGGER sources_generation_policy_version "
        "BEFORE UPDATE OF privacy_class, external_generation_allowed, generation_policy_version "
        "ON sources FOR EACH ROW "
        "EXECUTE FUNCTION reader_bump_source_generation_policy_version()"
    )
    op.add_column(
        "generation_requests",
        sa.Column(
            "privacy_status",
            sa.String(length=20),
            server_default="local",
            nullable=False,
        ),
    )
    op.add_column(
        "generation_requests",
        sa.Column(
            "privacy_reason", sa.Text(), server_default="", nullable=False
        ),
    )
    op.add_column(
        "generation_requests",
        sa.Column("source_policy_fingerprint", sa.String(length=64), nullable=True),
    )
    op.execute(
        "ALTER TABLE generation_requests ADD COLUMN source_snapshot_transaction_id xid8 "
        "NOT NULL DEFAULT pg_current_xact_id()"
    )
    op.execute(
        """
        CREATE FUNCTION reader_force_generation_request_source_transaction_id()
        RETURNS trigger LANGUAGE plpgsql AS $reader$
        BEGIN
            NEW.source_snapshot_transaction_id := pg_current_xact_id();
            RETURN NEW;
        END
        $reader$;
        """
    )
    op.execute(
        "CREATE TRIGGER generation_requests_force_source_transaction_id "
        "BEFORE INSERT ON generation_requests FOR EACH ROW "
        "EXECUTE FUNCTION reader_force_generation_request_source_transaction_id()"
    )
    op.create_check_constraint(
        "ck_generation_request_privacy_status",
        "generation_requests",
        "privacy_status IN ('local', 'eligible', 'blocked')",
    )
    op.create_check_constraint(
        "ck_generation_request_privacy_fields",
        "generation_requests",
        "(privacy_status = 'local' AND source_policy_fingerprint IS NULL AND privacy_reason = '') OR "
        "(privacy_status = 'eligible' AND length(source_policy_fingerprint) = 64 AND privacy_reason = '') OR "
        "(privacy_status = 'blocked' AND length(source_policy_fingerprint) = 64 AND btrim(privacy_reason) <> '')",
    )
    op.create_check_constraint(
        "ck_generation_request_external_provider_privacy",
        "generation_requests",
        "provider NOT IN ('legacy', 'openai_compatible') OR privacy_status <> 'local'",
    )
    op.create_table(
        "generation_request_sources",
        sa.Column("request_id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("source_name", sa.String(length=320), nullable=False),
        sa.Column("privacy_class", sa.String(length=20), nullable=False),
        sa.Column("external_generation_allowed", sa.Boolean(), nullable=False),
        sa.Column("source_policy_version", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "privacy_class IN ('unclassified', 'public', 'private')",
            name="ck_generation_request_source_privacy_class",
        ),
        sa.CheckConstraint(
            "source_policy_version >= 1",
            name="ck_generation_request_source_policy_version_positive",
        ),
        sa.CheckConstraint(
            "external_generation_allowed = false OR privacy_class = 'public'",
            name="ck_generation_request_source_external_public",
        ),
        sa.ForeignKeyConstraint(
            ["request_id"], ["generation_requests.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("request_id", "source_id"),
    )
    op.execute(
        """
        CREATE FUNCTION reader_check_generation_request_source_insert() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM generation_requests request
                 WHERE request.id = NEW.request_id
                   AND request.source_snapshot_transaction_id = pg_current_xact_id()
            ) THEN
                RAISE EXCEPTION 'immutable_generation_request_sources: %', NEW.request_id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $reader$;
        """
    )
    op.execute(
        "CREATE TRIGGER generation_request_sources_insert_guard "
        "BEFORE INSERT ON generation_request_sources FOR EACH ROW "
        "EXECUTE FUNCTION reader_check_generation_request_source_insert()"
    )
    op.execute(
        "CREATE TRIGGER generation_request_sources_immutable "
        "BEFORE UPDATE OR DELETE ON generation_request_sources FOR EACH ROW "
        "EXECUTE FUNCTION reader_reject_generation_mutation()"
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION reader_check_generation_request_payload() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NEW.privacy_status = 'blocked' AND EXISTS (
                SELECT 1 FROM generation_request_payloads payload
                 WHERE payload.request_id = NEW.id
            ) THEN
                RAISE EXCEPTION 'blocked_generation_request_with_payload: %', NEW.id
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.privacy_status <> 'blocked' AND NOT EXISTS (
                SELECT 1 FROM generation_request_payloads payload
                 WHERE payload.request_id = NEW.id
            ) THEN
                RAISE EXCEPTION 'generation_request_without_payload: %', NEW.id
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.privacy_status = 'eligible' AND NOT EXISTS (
                SELECT 1 FROM generation_request_sources source
                 WHERE source.request_id = NEW.id
            ) THEN
                RAISE EXCEPTION 'external_generation_request_without_sources: %', NEW.id
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.privacy_status = 'eligible' AND EXISTS (
                SELECT 1 FROM generation_request_sources source
                 WHERE source.request_id = NEW.id
                   AND (source.privacy_class <> 'public'
                        OR source.external_generation_allowed = false)
            ) THEN
                RAISE EXCEPTION 'external_generation_request_ineligible_source: %', NEW.id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_reject_blocked_generation_payload() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM generation_requests request
                 WHERE request.id = NEW.request_id
                   AND request.privacy_status = 'blocked'
            ) THEN
                RAISE EXCEPTION 'blocked_generation_request_with_payload: %', NEW.request_id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END
        $reader$;
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER generation_blocked_payload_guard "
        "AFTER INSERT ON generation_request_payloads DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION reader_reject_blocked_generation_payload()"
    )


def downgrade() -> None:
    raise RuntimeError(
        "P0.4 Source 隐私契约不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
