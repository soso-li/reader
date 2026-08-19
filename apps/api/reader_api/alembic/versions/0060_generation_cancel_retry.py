"""Add cancellation, lease expiry, and bounded retry audit."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0060_generation_cancel_retry"
down_revision: str | None = "0059_legacy_runner_audit"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.drop_constraint(
        "ck_generation_admission_status", "generation_admissions", type_="check"
    )
    op.create_check_constraint(
        "ck_generation_admission_status",
        "generation_admissions",
        "admission_status IN "
        "('awaiting', 'blocked_paused', 'blocked_budget_unconfigured', "
        " 'blocked_budget', 'blocked_concurrency', 'admitted', 'canceled')",
    )
    op.add_column(
        "generation_admissions",
        sa.Column(
            "next_attempt_kind",
            sa.String(length=20),
            server_default="initial",
            nullable=True,
        ),
    )
    op.add_column(
        "generation_admissions",
        sa.Column("canceled_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_generation_admission_next_attempt_kind",
        "generation_admissions",
        "next_attempt_kind IS NULL OR "
        "next_attempt_kind IN ('initial', 'automatic', 'manual')",
    )
    op.execute(
        "UPDATE generation_admissions AS admission "
        "SET next_attempt_kind = NULL "
        "WHERE EXISTS (SELECT 1 FROM generation_attempts AS attempt "
        "              WHERE attempt.request_id = admission.request_id) "
        "   OR EXISTS (SELECT 1 FROM generation_results AS result "
        "              WHERE result.request_id = admission.request_id)"
    )

    for name in (
        "ck_generation_attempt_status",
        "ck_generation_attempt_state_fields",
        "ck_generation_attempt_actual_terminal_only",
        "ck_generation_attempt_runner_audit_terminal_only",
    ):
        op.drop_constraint(name, "generation_attempts", type_="check")
    op.add_column(
        "generation_attempts",
        sa.Column(
            "retry_kind",
            sa.String(length=20),
            server_default="initial",
            nullable=False,
        ),
    )
    op.add_column(
        "generation_attempts",
        sa.Column("failure_class", sa.String(length=20), nullable=True),
    )
    op.add_column(
        "generation_attempts",
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Existing failures predate stable classification; validation is the
    # fail-closed category because it cannot trigger automatic provider use.
    op.execute(
        "UPDATE generation_attempts SET failure_class = 'validation' "
        "WHERE status = 'failed'"
    )
    op.create_check_constraint(
        "ck_generation_attempt_retry_kind",
        "generation_attempts",
        "retry_kind IN ('initial', 'automatic', 'manual')",
    )
    op.create_check_constraint(
        "ck_generation_attempt_status",
        "generation_attempts",
        "status IN ('pending', 'running', 'failed', 'complete', 'canceled', 'expired')",
    )
    op.create_check_constraint(
        "ck_generation_attempt_state_fields",
        "generation_attempts",
        "(status = 'pending' AND started_at IS NULL AND finished_at IS NULL AND error = '') OR "
        "(status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL AND error = '') OR "
        "(status IN ('failed', 'canceled', 'expired') AND started_at IS NOT NULL "
        " AND finished_at IS NOT NULL AND btrim(error) <> '') OR "
        "(status = 'complete' AND started_at IS NOT NULL AND finished_at IS NOT NULL AND error = '')",
    )
    op.create_check_constraint(
        "ck_generation_attempt_failure_class",
        "generation_attempts",
        "(status = 'failed' AND failure_class IN ('transport', 'validation')) OR "
        "(status = 'expired' AND failure_class = 'transport') OR "
        "(status = 'canceled' AND failure_class = 'canceled') OR "
        "(status IN ('pending', 'running', 'complete') AND failure_class IS NULL)",
    )
    op.create_check_constraint(
        "ck_generation_attempt_cancel_request",
        "generation_attempts",
        "(cancel_requested_at IS NULL AND status <> 'canceled') OR "
        "(cancel_requested_at IS NOT NULL AND status IN ('running', 'canceled'))",
    )
    op.create_check_constraint(
        "ck_generation_attempt_actual_terminal_only",
        "generation_attempts",
        "status IN ('failed', 'complete', 'canceled', 'expired') OR "
        "(input_tokens_actual IS NULL AND output_tokens_actual IS NULL)",
    )
    op.create_check_constraint(
        "ck_generation_attempt_runner_audit_terminal_only",
        "generation_attempts",
        "status IN ('failed', 'complete', 'canceled', 'expired') OR "
        "(runner_exit_code IS NULL AND runner_events_json IS NULL)",
    )
    op.create_index(
        "uq_generation_attempt_single_automatic_retry",
        "generation_attempts",
        ["request_id"],
        unique=True,
        postgresql_where=sa.text("retry_kind = 'automatic'"),
    )
    _replace_attempt_guard()
    op.execute(
        """
        CREATE FUNCTION reader_guard_generation_admission_cancel() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF OLD.canceled_at IS NOT NULL
               AND NEW.canceled_at IS DISTINCT FROM OLD.canceled_at THEN
                RAISE EXCEPTION 'immutable_generation_cancellation: %', OLD.request_id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE TRIGGER generation_admissions_cancel_guard
        BEFORE UPDATE ON generation_admissions FOR EACH ROW
        EXECUTE FUNCTION reader_guard_generation_admission_cancel();
        """
    )


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
               OR NEW.retry_kind IS DISTINCT FROM OLD.retry_kind
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
            IF OLD.status IN ('complete', 'failed', 'canceled', 'expired')
               OR (OLD.status = 'pending'
                   AND NEW.status NOT IN ('pending', 'running', 'failed', 'canceled'))
               OR (OLD.status = 'running'
                   AND NEW.status NOT IN ('running', 'complete', 'failed', 'canceled', 'expired')) THEN
                RAISE EXCEPTION 'invalid_generation_attempt_transition: % -> %', OLD.status, NEW.status
                    USING ERRCODE = '23514';
            END IF;
            IF (OLD.input_tokens_actual IS NOT NULL
                AND NEW.input_tokens_actual IS DISTINCT FROM OLD.input_tokens_actual)
               OR (OLD.output_tokens_actual IS NOT NULL
                AND NEW.output_tokens_actual IS DISTINCT FROM OLD.output_tokens_actual)
               OR ((NEW.input_tokens_actual IS DISTINCT FROM OLD.input_tokens_actual
                    OR NEW.output_tokens_actual IS DISTINCT FROM OLD.output_tokens_actual)
                   AND NEW.status NOT IN ('complete', 'failed', 'canceled', 'expired')) THEN
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
                   AND NEW.status NOT IN ('complete', 'failed', 'canceled', 'expired')) THEN
                RAISE EXCEPTION 'immutable_generation_attempt_runner_audit: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            IF (OLD.failure_class IS NOT NULL
                AND NEW.failure_class IS DISTINCT FROM OLD.failure_class)
               OR (NEW.failure_class IS DISTINCT FROM OLD.failure_class
                   AND NEW.status NOT IN ('failed', 'canceled', 'expired')) THEN
                RAISE EXCEPTION 'immutable_generation_attempt_failure_class: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            IF OLD.cancel_requested_at IS NOT NULL
               AND NEW.cancel_requested_at IS DISTINCT FROM OLD.cancel_requested_at THEN
                RAISE EXCEPTION 'immutable_generation_attempt_cancel_request: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            IF OLD.cancel_requested_at IS NULL
               AND NEW.cancel_requested_at IS NOT NULL
               AND (OLD.status <> 'running' OR NEW.status <> 'running') THEN
                RAISE EXCEPTION 'invalid_generation_attempt_cancel_request: %', OLD.id
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
        "P0.4 cancel/retry 契约不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
