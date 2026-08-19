"""Add fail-closed generation admission control and per-attempt token ledger."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0056_generation_admission"
down_revision: str | None = "0054_generation_lifecycle"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "generation_controls",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "global_pause", sa.Boolean(), server_default=sa.true(), nullable=False
        ),
        sa.Column("auto_run", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("daily_budget_tokens", sa.Integer(), nullable=True),
        sa.Column(
            "input_estimator",
            sa.String(length=40),
            server_default="unicode-codepoints-v1",
            nullable=False,
        ),
        sa.Column(
            "output_reserve_tokens",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "day_timezone",
            sa.String(length=80),
            server_default="Asia/Shanghai",
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint("id = 1", name="ck_generation_control_singleton"),
        sa.CheckConstraint(
            "daily_budget_tokens IS NULL OR daily_budget_tokens >= 0",
            name="ck_generation_control_daily_budget_nonnegative",
        ),
        sa.CheckConstraint(
            "output_reserve_tokens >= 0",
            name="ck_generation_control_output_reserve_nonnegative",
        ),
        sa.CheckConstraint(
            "input_estimator IN ('unicode-codepoints-v1', 'utf8-bytes-v1')",
            name="ck_generation_control_input_estimator",
        ),
        sa.CheckConstraint(
            "btrim(day_timezone) <> ''",
            name="ck_generation_control_day_timezone",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute(
        "INSERT INTO generation_controls "
        "(id, global_pause, auto_run, daily_budget_tokens, input_estimator, "
        " output_reserve_tokens, day_timezone, updated_at) "
        "VALUES (1, true, false, NULL, 'unicode-codepoints-v1', 0, "
        "'Asia/Shanghai', CURRENT_TIMESTAMP)"
    )
    op.create_table(
        "generation_admissions",
        sa.Column("request_id", sa.Integer(), nullable=False),
        sa.Column(
            "approval_status",
            sa.String(length=24),
            server_default="awaiting",
            nullable=False,
        ),
        sa.Column(
            "admission_status",
            sa.String(length=40),
            server_default="awaiting",
            nullable=False,
        ),
        sa.Column("admission_reason", sa.Text(), server_default="", nullable=False),
        sa.Column("input_tokens_estimated", sa.Integer(), nullable=True),
        sa.Column("output_tokens_reserved", sa.Integer(), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "approval_status IN ('awaiting', 'approved', 'consumed')",
            name="ck_generation_admission_approval_status",
        ),
        sa.CheckConstraint(
            "admission_status IN "
            "('awaiting', 'blocked_paused', 'blocked_budget_unconfigured', "
            " 'blocked_budget', 'admitted')",
            name="ck_generation_admission_status",
        ),
        sa.CheckConstraint(
            "(approval_status = 'awaiting' AND approved_at IS NULL "
            " AND consumed_at IS NULL) OR "
            "(approval_status = 'approved' AND approved_at IS NOT NULL "
            " AND consumed_at IS NULL) OR "
            "(approval_status = 'consumed' AND approved_at IS NOT NULL "
            " AND consumed_at IS NOT NULL)",
            name="ck_generation_admission_approval_times",
        ),
        sa.CheckConstraint(
            "input_tokens_estimated IS NULL OR input_tokens_estimated >= 0",
            name="ck_generation_admission_input_estimate",
        ),
        sa.CheckConstraint(
            "output_tokens_reserved IS NULL OR output_tokens_reserved >= 0",
            name="ck_generation_admission_output_reserve",
        ),
        sa.CheckConstraint(
            "(input_tokens_estimated IS NULL) = "
            "(output_tokens_reserved IS NULL)",
            name="ck_generation_admission_estimate_pair",
        ),
        sa.CheckConstraint(
            "admission_status NOT LIKE 'blocked_%' OR btrim(admission_reason) <> ''",
            name="ck_generation_admission_block_reason",
        ),
        sa.ForeignKeyConstraint(
            ["request_id"], ["generation_requests.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("request_id"),
    )
    op.execute(
        "INSERT INTO generation_admissions "
        "(request_id, approval_status, admission_status, admission_reason, "
        " updated_at) "
        "SELECT id, 'awaiting', 'awaiting', '', CURRENT_TIMESTAMP "
        "FROM generation_requests"
    )
    op.execute(
        "DROP TRIGGER generation_attempts_state_guard ON generation_attempts"
    )
    for name in (
        "estimator_version",
        "input_tokens_estimated",
        "output_tokens_reserved",
        "input_tokens_actual",
        "output_tokens_actual",
    ):
        column = (
            sa.Column(name, sa.String(length=40), nullable=True)
            if name == "estimator_version"
            else sa.Column(name, sa.Integer(), nullable=True)
        )
        op.add_column("generation_attempts", column)
    op.execute(
        "UPDATE generation_attempts AS attempt "
        "SET input_tokens_actual = result.input_tokens, "
        "    output_tokens_actual = result.output_tokens "
        "FROM generation_results AS result "
        "WHERE result.attempt_id = attempt.id"
    )
    op.execute("SET CONSTRAINTS ALL IMMEDIATE")
    for name, expression in (
        ("input_estimate", "input_tokens_estimated IS NULL OR input_tokens_estimated >= 0"),
        ("output_reserve", "output_tokens_reserved IS NULL OR output_tokens_reserved >= 0"),
        ("input_actual", "input_tokens_actual IS NULL OR input_tokens_actual >= 0"),
        ("output_actual", "output_tokens_actual IS NULL OR output_tokens_actual >= 0"),
    ):
        op.create_check_constraint(
            f"ck_generation_attempt_{name}", "generation_attempts", expression
        )
    op.create_check_constraint(
        "ck_generation_attempt_estimate_shape",
        "generation_attempts",
        "(estimator_version IS NULL AND input_tokens_estimated IS NULL "
        " AND output_tokens_reserved IS NULL) OR "
        "(btrim(estimator_version) <> '' AND input_tokens_estimated IS NOT NULL "
        " AND output_tokens_reserved IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_generation_attempt_actual_terminal_only",
        "generation_attempts",
        "status IN ('failed', 'complete') OR "
        "(input_tokens_actual IS NULL AND output_tokens_actual IS NULL)",
    )
    _replace_attempt_guard()
    op.execute(
        "CREATE TRIGGER generation_attempts_state_guard "
        "BEFORE UPDATE OR DELETE ON generation_attempts FOR EACH ROW "
        "EXECUTE FUNCTION reader_guard_generation_attempt()"
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
               OR NEW.estimator_version IS DISTINCT FROM OLD.estimator_version
               OR NEW.input_tokens_estimated IS DISTINCT FROM OLD.input_tokens_estimated
               OR NEW.output_tokens_reserved IS DISTINCT FROM OLD.output_tokens_reserved
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
            RETURN NEW;
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "P0.4 Generation admission 契约不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
