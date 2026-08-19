"""Add the frozen legacy runner identity and filtered audit."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0059_legacy_runner_audit"
down_revision: str | None = "0058_runner_claim_lease"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "generation_attempts",
        sa.Column("runner_cli_version", sa.String(length=120), nullable=True),
    )
    op.add_column(
        "generation_attempts",
        sa.Column("runner_exit_code", sa.Integer(), nullable=True),
    )
    op.add_column(
        "generation_attempts",
        sa.Column("runner_events_json", sa.JSON(), nullable=True),
    )
    op.create_check_constraint(
        "ck_generation_attempt_runner_audit_terminal_only",
        "generation_attempts",
        "status IN ('failed', 'complete') OR "
        "(runner_exit_code IS NULL AND runner_events_json IS NULL)",
    )
    op.create_check_constraint(
        "ck_generation_attempt_runner_cli_version",
        "generation_attempts",
        "runner_cli_version IS NULL OR "
        "(runner_id IS NOT NULL AND btrim(runner_cli_version) <> '')",
    )
    op.create_check_constraint(
        "ck_generation_attempt_runner_exit_code",
        "generation_attempts",
        "runner_exit_code IS NULL OR runner_exit_code BETWEEN -255 AND 255",
    )
    op.create_check_constraint(
        "ck_generation_attempt_runner_events_array",
        "generation_attempts",
        "runner_events_json IS NULL OR json_typeof(runner_events_json) = 'array'",
    )
    _replace_attempt_guard()


def _replace_attempt_guard() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION reader_guard_generation_attempt() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'immutable_generation_attempt: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.uid IS DISTINCT FROM OLD.uid
               OR NEW.request_id IS DISTINCT FROM OLD.request_id
               OR NEW.attempt_no IS DISTINCT FROM OLD.attempt_no
               OR NEW.estimator_version IS DISTINCT FROM OLD.estimator_version
               OR NEW.input_tokens_estimated IS DISTINCT FROM OLD.input_tokens_estimated
               OR NEW.output_tokens_reserved IS DISTINCT FROM OLD.output_tokens_reserved
               OR NEW.runner_id IS DISTINCT FROM OLD.runner_id
               OR NEW.runner_environment_id IS DISTINCT FROM OLD.runner_environment_id
               OR NEW.runner_cli_version IS DISTINCT FROM OLD.runner_cli_version
               OR NEW.lease_token_hash IS DISTINCT FROM OLD.lease_token_hash
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION 'immutable_generation_attempt_identity: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            IF OLD.status IN ('complete', 'failed')
               OR (OLD.status = 'pending' AND NEW.status NOT IN ('pending', 'running', 'failed'))
               OR (OLD.status = 'running' AND NEW.status NOT IN ('running', 'complete', 'failed')) THEN
                RAISE EXCEPTION 'invalid_generation_attempt_transition: % -> %', OLD.status, NEW.status
                    USING ERRCODE = '23514';
            END IF;
            IF (OLD.input_tokens_actual IS NOT NULL
                AND NEW.input_tokens_actual IS DISTINCT FROM OLD.input_tokens_actual)
               OR (OLD.output_tokens_actual IS NOT NULL
                AND NEW.output_tokens_actual IS DISTINCT FROM OLD.output_tokens_actual)
               OR ((NEW.input_tokens_actual IS DISTINCT FROM OLD.input_tokens_actual
                    OR NEW.output_tokens_actual IS DISTINCT FROM OLD.output_tokens_actual)
                   AND NEW.status NOT IN ('complete', 'failed')) THEN
                RAISE EXCEPTION 'immutable_generation_attempt_actual_usage: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            IF (OLD.runner_exit_code IS NOT NULL
                AND NEW.runner_exit_code IS DISTINCT FROM OLD.runner_exit_code)
               OR (OLD.runner_events_json IS NOT NULL
                AND NEW.runner_events_json::jsonb
                    IS DISTINCT FROM OLD.runner_events_json::jsonb)
               OR ((NEW.runner_exit_code IS DISTINCT FROM OLD.runner_exit_code
                    OR NEW.runner_events_json::jsonb
                        IS DISTINCT FROM OLD.runner_events_json::jsonb)
                   AND NEW.status NOT IN ('complete', 'failed')) THEN
                RAISE EXCEPTION 'immutable_generation_attempt_runner_audit: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            IF (NEW.lease_expires_at IS DISTINCT FROM OLD.lease_expires_at
                OR NEW.last_heartbeat_at IS DISTINCT FROM OLD.last_heartbeat_at)
               AND (OLD.status <> 'running' OR NEW.status <> 'running'
                    OR NEW.lease_expires_at <= OLD.lease_expires_at
                    OR NEW.last_heartbeat_at <= OLD.last_heartbeat_at) THEN
                RAISE EXCEPTION 'invalid_generation_attempt_heartbeat: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "P0.4 历史执行器审计契约不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
