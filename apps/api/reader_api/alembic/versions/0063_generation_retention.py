"""Separate and expire Generation payloads and filtered Runner JSONL."""

from __future__ import annotations

import hashlib
import json

from alembic import context, op
import sqlalchemy as sa


revision: str = "0063_generation_retention"
down_revision: str | None = "0062_w5_contract_merge"
branch_labels: str | None = None
depends_on: str | None = None

SYNTHESIS_APPLICATION_CONTEXT_KEYS = (
    "event_uid",
    "target_revision_uid",
    "snapshot_uid",
    "policy_version",
    "prompt_version",
    "schema_version",
    "source_coverage_fingerprint",
    "content_fingerprint",
    "generation_fingerprint",
)
EVIDENCE_REVIEW_APPLICATION_CONTEXT_KEYS = (
    "event_uid",
    "baseline_snapshot_uid",
    "baseline_revision_uid",
    "target_snapshot_uid",
    "target_revision_uid",
    "policy_version",
    "prompt_version",
    "schema_version",
    "comparison_fingerprint",
)


def _fingerprint(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _application_context(
    task_type: str,
    payload: object,
) -> dict[str, object] | None:
    if not isinstance(payload, dict):
        return None
    input_data = payload.get("input_data")
    if not isinstance(input_data, dict):
        return None
    if task_type == "item-summary":
        return {}
    if task_type == "cluster-synthesis":
        citations = input_data.get("citations")
        if not isinstance(citations, list) or not citations:
            return None
        retained: list[dict[str, object]] = []
        for citation in citations:
            if not isinstance(citation, dict):
                return None
            retained.append(
                {
                    key: citation.get(key)
                    for key in (
                        "source_id",
                        "source_name",
                        "url",
                        "published_at",
                    )
                }
            )
        return {"citations": retained}
    keys = (
        SYNTHESIS_APPLICATION_CONTEXT_KEYS
        if task_type == "event-synthesis"
        else EVIDENCE_REVIEW_APPLICATION_CONTEXT_KEYS
        if task_type == "evidence-review"
        else ()
    )
    if not keys:
        return None
    retained = {key: input_data.get(key) for key in keys}
    if not all(
        isinstance(value, str) and value.strip() for value in retained.values()
    ) or not retained:
        return None
    return retained


def upgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError(
            "0063_generation_retention 必须在线迁移存量 Runner JSONL；"
            "离线 SQL 会丢失审计正文，已在执行任何 DDL 前停止"
        )

    op.drop_constraint(
        "uq_generation_request_fingerprint",
        "generation_requests",
        type_="unique",
    )
    op.create_index(
        "ix_generation_request_fingerprint",
        "generation_requests",
        ["request_fingerprint"],
    )
    op.add_column(
        "maintenance_runs",
        sa.Column(
            "scanned_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.alter_column(
        "generation_request_payloads",
        "payload_json",
        existing_type=sa.JSON(),
        nullable=True,
    )
    op.add_column(
        "generation_request_payloads",
        sa.Column("application_context_json", sa.JSON(), nullable=True),
    )
    op.create_check_constraint(
        "ck_generation_request_payload_application_context_object",
        "generation_request_payloads",
        "application_context_json IS NULL OR "
        "json_typeof(application_context_json) = 'object'",
    )
    op.add_column(
        "generation_request_payloads",
        sa.Column("purged_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_generation_request_payload_retention_state",
        "generation_request_payloads",
        "(payload_json IS NULL) = (purged_at IS NOT NULL)",
    )
    op.create_index(
        "ix_generation_request_payload_retention",
        "generation_request_payloads",
        ["created_at", "request_id"],
        postgresql_where=sa.text("payload_json IS NOT NULL"),
    )
    _replace_payload_guard()

    op.create_table(
        "generation_attempt_runner_audits",
        sa.Column("attempt_id", sa.Integer(), nullable=False),
        sa.Column("events_json", sa.JSON(), nullable=True),
        sa.Column("events_fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("purged_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "events_json IS NULL OR json_typeof(events_json) = 'array'",
            name="ck_generation_attempt_runner_audit_events_array",
        ),
        sa.CheckConstraint(
            "events_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_generation_attempt_runner_audit_fingerprint_sha256",
        ),
        sa.CheckConstraint(
            "(events_json IS NULL) = (purged_at IS NOT NULL)",
            name="ck_generation_attempt_runner_audit_retention_state",
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id"],
            ["generation_attempts.id"],
            name="fk_generation_attempt_runner_audit_attempt",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("attempt_id"),
    )
    op.create_index(
        "ix_generation_attempt_runner_audit_retention",
        "generation_attempt_runner_audits",
        ["created_at", "attempt_id"],
        postgresql_where=sa.text("events_json IS NOT NULL"),
    )
    _move_runner_events()
    _install_runner_audit_guards()

    op.drop_constraint(
        "ck_generation_attempt_runner_audit_terminal_only",
        "generation_attempts",
        type_="check",
    )
    op.drop_constraint(
        "ck_generation_attempt_runner_events_array",
        "generation_attempts",
        type_="check",
    )
    op.create_check_constraint(
        "ck_generation_attempt_runner_exit_terminal_only",
        "generation_attempts",
        "status IN ('failed', 'complete', 'canceled', 'expired') OR "
        "runner_exit_code IS NULL",
    )
    _replace_attempt_guard()
    op.drop_column("generation_attempts", "runner_events_json")


def _move_runner_events() -> None:
    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            "SELECT id, runner_events_json, "
            "COALESCE(finished_at, updated_at, created_at) AS created_at "
            "FROM generation_attempts WHERE runner_events_json IS NOT NULL "
            "ORDER BY id"
        )
    ).mappings()
    for row in rows:
        events = row["runner_events_json"]
        if isinstance(events, str):
            events = json.loads(events)
        connection.execute(
            sa.text(
                "INSERT INTO generation_attempt_runner_audits "
                "(attempt_id, events_json, events_fingerprint, created_at) "
                "VALUES (:attempt_id, CAST(:events_json AS json), "
                ":events_fingerprint, :created_at)"
            ),
            {
                "attempt_id": row["id"],
                "events_json": json.dumps(events, ensure_ascii=False),
                "events_fingerprint": _fingerprint(events),
                "created_at": row["created_at"],
            },
        )


def _replace_payload_guard() -> None:
    op.execute(
        "DROP TRIGGER generation_request_payloads_immutable "
        "ON generation_request_payloads"
    )
    _backfill_application_context()
    op.execute(
        """
        CREATE FUNCTION reader_guard_generation_request_payload() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'immutable_generation_request_payload: %', OLD.request_id
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.request_id IS DISTINCT FROM OLD.request_id
               OR NEW.payload_fingerprint IS DISTINCT FROM OLD.payload_fingerprint
               OR NEW.application_context_json::jsonb
                  IS DISTINCT FROM OLD.application_context_json::jsonb
               OR NEW.created_at IS DISTINCT FROM OLD.created_at
               OR OLD.payload_json IS NULL
               OR NEW.payload_json IS NOT NULL
               OR OLD.purged_at IS NOT NULL
               OR NEW.purged_at IS NULL THEN
                RAISE EXCEPTION 'invalid_generation_request_payload_retention: %', OLD.request_id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE TRIGGER generation_request_payloads_immutable
        BEFORE UPDATE OR DELETE ON generation_request_payloads FOR EACH ROW
        EXECUTE FUNCTION reader_guard_generation_request_payload();
        """
    )


def _backfill_application_context() -> None:
    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            "SELECT payload.request_id, request.task_type, payload.payload_json "
            "FROM generation_request_payloads payload "
            "JOIN generation_requests request ON request.id = payload.request_id "
            "ORDER BY payload.request_id"
        )
    ).mappings()
    for row in rows:
        payload = row["payload_json"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        retained = _application_context(str(row["task_type"]), payload)
        if retained is None:
            continue
        connection.execute(
            sa.text(
                "UPDATE generation_request_payloads "
                "SET application_context_json = CAST(:retained AS json) "
                "WHERE request_id = :request_id"
            ),
            {
                "request_id": row["request_id"],
                "retained": json.dumps(retained, ensure_ascii=False),
            },
        )


def _install_runner_audit_guards() -> None:
    op.execute(
        """
        CREATE FUNCTION reader_guard_generation_attempt_runner_audit() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'immutable_generation_attempt_runner_audit: %', OLD.attempt_id
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.attempt_id IS DISTINCT FROM OLD.attempt_id
               OR NEW.events_fingerprint IS DISTINCT FROM OLD.events_fingerprint
               OR NEW.created_at IS DISTINCT FROM OLD.created_at
               OR OLD.events_json IS NULL
               OR NEW.events_json IS NOT NULL
               OR OLD.purged_at IS NOT NULL
               OR NEW.purged_at IS NULL THEN
                RAISE EXCEPTION 'invalid_generation_attempt_runner_audit_retention: %', OLD.attempt_id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE FUNCTION reader_check_generation_attempt_runner_audit() RETURNS trigger
        LANGUAGE plpgsql AS $reader$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM generation_attempts attempt
                 WHERE attempt.id = NEW.attempt_id
                   AND attempt.status IN ('failed', 'complete', 'canceled', 'expired')
            ) THEN
                RAISE EXCEPTION 'runner_audit_for_nonterminal_attempt: %', NEW.attempt_id
                    USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END
        $reader$;

        CREATE TRIGGER generation_attempt_runner_audits_immutable
        BEFORE UPDATE OR DELETE ON generation_attempt_runner_audits FOR EACH ROW
        EXECUTE FUNCTION reader_guard_generation_attempt_runner_audit();

        CREATE CONSTRAINT TRIGGER generation_attempt_runner_audit_terminal_guard
        AFTER INSERT ON generation_attempt_runner_audits
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
        EXECUTE FUNCTION reader_check_generation_attempt_runner_audit();
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
               OR (NEW.runner_exit_code IS DISTINCT FROM OLD.runner_exit_code
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
        "Generation 保留策略不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
