"""Add the single-runner claim, lease, and presence contract."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0058_runner_claim_lease"
down_revision: str | None = "0057_privacy_admission_merge"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.drop_constraint(
        "ck_generation_admission_status",
        "generation_admissions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_generation_admission_status",
        "generation_admissions",
        "admission_status IN "
        "('awaiting', 'blocked_paused', 'blocked_budget_unconfigured', "
        " 'blocked_budget', 'blocked_concurrency', 'admitted')",
    )
    for column in (
        sa.Column("runner_id", sa.String(length=120), nullable=True),
        sa.Column("runner_environment_id", sa.String(length=120), nullable=True),
        sa.Column("lease_token_hash", sa.String(length=64), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
    ):
        op.add_column("generation_attempts", column)
    op.create_check_constraint(
        "ck_generation_attempt_runner_lease_shape",
        "generation_attempts",
        "(runner_id IS NULL AND runner_environment_id IS NULL "
        " AND lease_token_hash IS NULL AND lease_expires_at IS NULL "
        " AND last_heartbeat_at IS NULL) OR "
        "(runner_id IS NOT NULL AND btrim(runner_id) <> '' "
        " AND runner_environment_id IS NOT NULL "
        " AND btrim(runner_environment_id) <> '' "
        " AND lease_token_hash IS NOT NULL AND length(lease_token_hash) = 64 "
        " AND lease_expires_at IS NOT NULL AND last_heartbeat_at IS NOT NULL "
        " AND last_heartbeat_at <= lease_expires_at)",
    )
    op.create_index(
        "uq_generation_attempt_single_active",
        "generation_attempts",
        ["status"],
        unique=True,
        postgresql_where=sa.text("status = 'running'"),
    )
    op.create_table(
        "generation_runner_presences",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("environment_id", sa.String(length=120), nullable=False),
        sa.Column("runner_id", sa.String(length=120), nullable=False),
        sa.Column("runner_version", sa.String(length=120), nullable=False),
        sa.Column("cli_version", sa.String(length=120), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("current_attempt_id", sa.Integer(), nullable=True),
        sa.Column(
            "last_heartbeat_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint("id = 1", name="ck_generation_runner_presence_singleton"),
        sa.CheckConstraint(
            "btrim(environment_id) <> ''",
            name="ck_generation_runner_environment",
        ),
        sa.CheckConstraint("btrim(runner_id) <> ''", name="ck_generation_runner_id"),
        sa.CheckConstraint(
            "btrim(runner_version) <> ''",
            name="ck_generation_runner_version",
        ),
        sa.CheckConstraint(
            "btrim(cli_version) <> ''",
            name="ck_generation_runner_cli_version",
        ),
        sa.CheckConstraint(
            "status IN ('idle', 'running')",
            name="ck_generation_runner_status",
        ),
        sa.CheckConstraint(
            "(status = 'idle' AND current_attempt_id IS NULL) OR "
            "(status = 'running' AND current_attempt_id IS NOT NULL)",
            name="ck_generation_runner_state_fields",
        ),
        sa.ForeignKeyConstraint(
            ["current_attempt_id"],
            ["generation_attempts.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "current_attempt_id", name="uq_generation_runner_current_attempt"
        ),
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
        "P0.4 Runner claim/lease 契约不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
