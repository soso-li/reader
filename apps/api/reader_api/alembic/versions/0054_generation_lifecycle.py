"""Add the first immutable Generation lifecycle tracer."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0054_generation_lifecycle"
down_revision: str | None = "0053_evidence_reviews"
branch_labels: str | None = None
depends_on: str | None = None

UUID_CHECK = (
    "uid ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    "[89ab][0-9a-f]{3}-[0-9a-f]{12}$'"
)


def upgrade() -> None:
    op.create_table(
        "generation_requests",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uid", sa.String(length=36), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("input_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("task_type", sa.String(length=80), nullable=False),
        sa.Column("reason", sa.String(length=80), nullable=False),
        sa.Column("target_type", sa.String(length=40), nullable=False),
        sa.Column("target_id", sa.Integer(), nullable=False),
        sa.Column("target_uid", sa.String(length=36), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("model", sa.String(length=120), nullable=False),
        sa.Column("prompt_version", sa.String(length=120), nullable=False),
        sa.Column("schema_version", sa.String(length=120), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(UUID_CHECK, name="ck_generation_request_uid_uuid"),
        sa.CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_generation_request_fingerprint_sha256",
        ),
        sa.CheckConstraint(
            "input_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_generation_request_input_fingerprint_sha256",
        ),
        sa.CheckConstraint("btrim(task_type) <> ''", name="ck_generation_request_task_type"),
        sa.CheckConstraint("btrim(reason) <> ''", name="ck_generation_request_reason"),
        sa.CheckConstraint("btrim(target_type) <> ''", name="ck_generation_request_target_type"),
        sa.CheckConstraint("target_id >= 1", name="ck_generation_request_target_id"),
        sa.CheckConstraint("btrim(target_uid) <> ''", name="ck_generation_request_target_uid"),
        sa.CheckConstraint("btrim(provider) <> ''", name="ck_generation_request_provider"),
        sa.CheckConstraint("btrim(model) <> ''", name="ck_generation_request_model"),
        sa.CheckConstraint("btrim(prompt_version) <> ''", name="ck_generation_request_prompt_version"),
        sa.CheckConstraint("btrim(schema_version) <> ''", name="ck_generation_request_schema_version"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uid", name="uq_generation_requests_uid"),
        sa.UniqueConstraint(
            "request_fingerprint", name="uq_generation_request_fingerprint"
        ),
    )
    op.create_index(
        "ix_generation_request_target_input",
        "generation_requests",
        ["task_type", "target_type", "target_id", "input_fingerprint"],
    )
    op.create_table(
        "generation_request_payloads",
        sa.Column("request_id", sa.Integer(), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("payload_fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "payload_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_generation_request_payload_fingerprint_sha256",
        ),
        sa.ForeignKeyConstraint(
            ["request_id"], ["generation_requests.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("request_id"),
    )
    op.create_table(
        "generation_attempts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uid", sa.String(length=36), nullable=False),
        sa.Column("request_id", sa.Integer(), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("error", sa.Text(), server_default="", nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.CheckConstraint(UUID_CHECK, name="ck_generation_attempt_uid_uuid"),
        sa.CheckConstraint("attempt_no >= 1", name="ck_generation_attempt_positive"),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'failed', 'complete')",
            name="ck_generation_attempt_status",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND started_at IS NULL AND finished_at IS NULL AND error = '') OR "
            "(status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL AND error = '') OR "
            "(status = 'failed' AND started_at IS NOT NULL AND finished_at IS NOT NULL AND btrim(error) <> '') OR "
            "(status = 'complete' AND started_at IS NOT NULL AND finished_at IS NOT NULL AND error = '')",
            name="ck_generation_attempt_state_fields",
        ),
        sa.ForeignKeyConstraint(
            ["request_id"], ["generation_requests.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uid", name="uq_generation_attempts_uid"),
        sa.UniqueConstraint(
            "request_id", "attempt_no", name="uq_generation_attempt_number"
        ),
        sa.UniqueConstraint("id", "request_id", name="uq_generation_attempt_request"),
    )
    op.create_table(
        "generation_results",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uid", sa.String(length=36), nullable=False),
        sa.Column("request_id", sa.Integer(), nullable=False),
        sa.Column("attempt_id", sa.Integer(), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("payload_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(UUID_CHECK, name="ck_generation_result_uid_uuid"),
        sa.CheckConstraint(
            "payload_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_generation_result_payload_fingerprint_sha256",
        ),
        sa.CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name="ck_generation_result_input_tokens",
        ),
        sa.CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name="ck_generation_result_output_tokens",
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id", "request_id"],
            ["generation_attempts.id", "generation_attempts.request_id"],
            name="fk_generation_result_attempt_request",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uid", name="uq_generation_results_uid"),
        sa.UniqueConstraint("attempt_id", name="uq_generation_result_attempt"),
        sa.UniqueConstraint("id", "request_id", name="uq_generation_result_request"),
    )
    op.create_table(
        "generation_applications",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("request_id", sa.Integer(), nullable=False),
        sa.Column("result_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=24), server_default="pending", nullable=False),
        sa.Column("artifact_type", sa.String(length=40), server_default="", nullable=False),
        sa.Column("artifact_id", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), server_default="", nullable=False),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.CheckConstraint(
            "status IN ('pending', 'applied', 'failed')",
            name="ck_generation_application_status",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND artifact_type = '' AND artifact_id IS NULL AND error = '' AND applied_at IS NULL) OR "
            "(status = 'applied' AND btrim(artifact_type) <> '' AND artifact_id IS NOT NULL AND error = '' AND applied_at IS NOT NULL) OR "
            "(status = 'failed' AND artifact_type = '' AND artifact_id IS NULL AND btrim(error) <> '' AND applied_at IS NULL)",
            name="ck_generation_application_state_fields",
        ),
        sa.ForeignKeyConstraint(
            ["result_id", "request_id"],
            ["generation_results.id", "generation_results.request_id"],
            name="fk_generation_application_result_request",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("result_id", name="uq_generation_application_result"),
    )
    _install_generation_triggers()


def _install_generation_triggers() -> None:
    op.execute(
        """
        CREATE FUNCTION reader_reject_generation_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            RAISE EXCEPTION 'immutable_generation_artifact: %', TG_TABLE_NAME
                USING ERRCODE = '23514';
        END
        $reader$;

        CREATE FUNCTION reader_guard_generation_attempt() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'immutable_generation_attempt: %', OLD.id
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.uid IS DISTINCT FROM OLD.uid
               OR NEW.request_id IS DISTINCT FROM OLD.request_id
               OR NEW.attempt_no IS DISTINCT FROM OLD.attempt_no
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
            RETURN NEW;
        END
        $reader$;

        CREATE FUNCTION reader_guard_generation_application() RETURNS trigger
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
            RETURN NEW;
        END
        $reader$;

        CREATE FUNCTION reader_check_generation_request_payload() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM generation_request_payloads payload
                 WHERE payload.request_id = NEW.id
            ) THEN
                RAISE EXCEPTION 'generation_request_without_payload: %', NEW.id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_check_generation_complete_attempt() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NEW.status = 'complete' AND NOT EXISTS (
                SELECT 1 FROM generation_results result
                 WHERE result.attempt_id = NEW.id
            ) THEN
                RAISE EXCEPTION 'complete_generation_attempt_without_result: %', NEW.id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END
        $reader$;

        CREATE FUNCTION reader_check_generation_result_application() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM generation_attempts attempt
                 WHERE attempt.id = NEW.attempt_id
                   AND attempt.request_id = NEW.request_id
                   AND attempt.status = 'complete'
            ) OR NOT EXISTS (
                SELECT 1 FROM generation_applications application
                 WHERE application.result_id = NEW.id
                   AND application.request_id = NEW.request_id
            ) THEN
                RAISE EXCEPTION 'incomplete_generation_result_contract: %', NEW.id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END
        $reader$;
        """
    )
    for table in (
        "generation_requests",
        "generation_request_payloads",
        "generation_results",
    ):
        op.execute(
            f"CREATE TRIGGER {table}_immutable "
            f"BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW "
            "EXECUTE FUNCTION reader_reject_generation_mutation()"
        )
    op.execute(
        "CREATE TRIGGER generation_attempts_state_guard "
        "BEFORE UPDATE OR DELETE ON generation_attempts FOR EACH ROW "
        "EXECUTE FUNCTION reader_guard_generation_attempt()"
    )
    op.execute(
        "CREATE TRIGGER generation_applications_state_guard "
        "BEFORE UPDATE OR DELETE ON generation_applications FOR EACH ROW "
        "EXECUTE FUNCTION reader_guard_generation_application()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER generation_request_payload_guard "
        "AFTER INSERT ON generation_requests DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION reader_check_generation_request_payload()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER generation_complete_attempt_guard "
        "AFTER INSERT OR UPDATE ON generation_attempts DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION reader_check_generation_complete_attempt()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER generation_result_application_guard "
        "AFTER INSERT ON generation_results DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION reader_check_generation_result_application()"
    )


def downgrade() -> None:
    raise RuntimeError(
        "P0.4 Generation lifecycle 契约不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
