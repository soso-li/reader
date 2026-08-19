"""Add replay-safe Result metadata and application attempt audit."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0061_result_apply_contract"
down_revision: str | None = "0059_legacy_runner_audit"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "generation_results",
        sa.Column("output_fingerprint", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "generation_results",
        sa.Column("schema_version", sa.String(length=120), nullable=True),
    )
    op.create_check_constraint(
        "ck_generation_result_output_fingerprint_sha256",
        "generation_results",
        "output_fingerprint IS NULL OR output_fingerprint ~ '^[0-9a-f]{64}$'",
    )
    op.create_check_constraint(
        "ck_generation_result_schema_version",
        "generation_results",
        "schema_version IS NULL OR btrim(schema_version) <> ''",
    )
    op.execute(
        """
        CREATE FUNCTION reader_guard_generation_result_metadata() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NEW.output_fingerprint IS NULL OR NEW.schema_version IS NULL THEN
                RAISE EXCEPTION 'incomplete_generation_result_metadata: %', NEW.id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE TRIGGER generation_results_metadata_guard
        BEFORE INSERT ON generation_results FOR EACH ROW
        EXECUTE FUNCTION reader_guard_generation_result_metadata();
        """
    )
    op.add_column(
        "generation_applications",
        sa.Column(
            "apply_attempt_count",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "generation_applications",
        sa.Column("last_error", sa.Text(), server_default="", nullable=False),
    )
    op.create_check_constraint(
        "ck_generation_application_attempt_count",
        "generation_applications",
        "apply_attempt_count >= 0",
    )
    _replace_application_guard()


def _replace_application_guard() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION reader_guard_generation_application() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'immutable_generation_application: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.request_id IS DISTINCT FROM OLD.request_id
               OR NEW.result_id IS DISTINCT FROM OLD.result_id
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION 'immutable_generation_application_identity: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            IF OLD.status = 'applied'
               OR (OLD.status = 'pending' AND NEW.status NOT IN ('pending', 'applied', 'failed'))
               OR (OLD.status = 'failed' AND NEW.status NOT IN ('failed', 'pending', 'applied')) THEN
                RAISE EXCEPTION 'invalid_generation_application_transition: % -> %', OLD.status, NEW.status
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.apply_attempt_count < OLD.apply_attempt_count
               OR NEW.apply_attempt_count > OLD.apply_attempt_count + 1
               OR (NEW.apply_attempt_count = OLD.apply_attempt_count AND (
                    NEW.status IS DISTINCT FROM OLD.status
                    OR NEW.artifact_type IS DISTINCT FROM OLD.artifact_type
                    OR NEW.artifact_id IS DISTINCT FROM OLD.artifact_id
                    OR NEW.error IS DISTINCT FROM OLD.error
                    OR NEW.last_error IS DISTINCT FROM OLD.last_error
                    OR NEW.applied_at IS DISTINCT FROM OLD.applied_at
               )) THEN
                RAISE EXCEPTION 'invalid_generation_application_attempt: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.last_error IS DISTINCT FROM OLD.last_error
               AND NEW.status <> 'failed' THEN
                RAISE EXCEPTION 'invalid_generation_application_last_error: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "P0.4 Result/apply 合同不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
