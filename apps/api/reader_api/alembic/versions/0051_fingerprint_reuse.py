"""Deduplicate generation results and active fingerprinted tasks."""

from __future__ import annotations

import hashlib
import json

from alembic import op
import sqlalchemy as sa


revision: str = "0051_fingerprint_reuse"
down_revision: str | None = "0050_synthesis_source_count"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "llm_tasks",
        sa.Column("input_fingerprint", sa.String(length=64), nullable=True),
    )
    op.execute(
        """
        UPDATE llm_tasks
           SET input_fingerprint = CASE
               WHEN result_json IS JSON THEN
                   result_json::jsonb #>>
                       '{request,input_data,generation_fingerprint}'
               ELSE NULL
           END
         WHERE task_type = 'event-synthesis'
           AND CASE
               WHEN result_json IS JSON THEN
                   result_json::jsonb #>>
                       '{request,input_data,generation_fingerprint}'
                       ~ '^[0-9a-f]{64}$'
               ELSE FALSE
           END
        """
    )
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                  FROM llm_tasks
                 WHERE task_type = 'event-synthesis'
                   AND status IN ('pending', 'running')
                   AND CASE
                       WHEN result_json IS JSON THEN
                           COALESCE(
                               result_json::jsonb #>>
                                   '{request,input_data,policy_version}',
                               ''
                           ) <> 'event-synthesis-policy-v2'
                       ELSE TRUE
                   END
            ) THEN
                RAISE EXCEPTION 'active_synthesis_task_policy_mismatch'
                    USING ERRCODE = '23514';
            END IF;
            IF EXISTS (
                SELECT 1
                  FROM llm_tasks
                 WHERE task_type = 'event-synthesis'
                   AND status IN ('pending', 'running')
                   AND input_fingerprint IS NULL
            ) THEN
                RAISE EXCEPTION 'active_synthesis_task_without_fingerprint'
                    USING ERRCODE = '23514';
            END IF;
            IF EXISTS (
                SELECT 1
                  FROM llm_tasks
                 WHERE input_fingerprint IS NOT NULL
                   AND status IN ('pending', 'running')
                 GROUP BY task_type, object_type, object_id, input_fingerprint
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION 'duplicate_active_input_fingerprint'
                    USING ERRCODE = '23505';
            END IF;
            IF EXISTS (
                SELECT 1
                  FROM synthesis_versions
                 GROUP BY event_id, generation_fingerprint
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION 'duplicate_synthesis_generation_fingerprint'
                    USING ERRCODE = '23505';
            END IF;
        END
        $reader$
        """
    )
    if op.get_context().as_sql:
        op.execute(
            """
            DO $reader$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM llm_tasks
                    WHERE task_type = 'event-synthesis'
                      AND status IN ('pending', 'running')
                ) THEN
                    RAISE EXCEPTION
                        'active_synthesis_provenance_online_validation_required';
                END IF;
            END
            $reader$
            """
        )
    else:
        _validate_active_synthesis_provenance(op.get_bind())
    op.create_check_constraint(
        "ck_llm_task_input_fingerprint_length",
        "llm_tasks",
        "input_fingerprint IS NULL OR input_fingerprint ~ '^[0-9a-f]{64}$'",
    )
    op.create_check_constraint(
        "ck_llm_task_active_synthesis_fingerprint",
        "llm_tasks",
        "task_type <> 'event-synthesis' "
        "OR status NOT IN ('pending', 'running') "
        "OR input_fingerprint IS NOT NULL",
    )
    op.create_unique_constraint(
        "uq_synthesis_version_generation_fingerprint",
        "synthesis_versions",
        ["event_id", "generation_fingerprint"],
    )
    op.create_index(
        "uq_llm_task_active_input_fingerprint",
        "llm_tasks",
        ["task_type", "object_type", "object_id", "input_fingerprint"],
        unique=True,
        postgresql_where=sa.text(
            "input_fingerprint IS NOT NULL AND status IN ('pending', 'running')"
        ),
    )


def _validate_active_synthesis_provenance(bind: sa.Connection) -> None:
    tasks = bind.execute(
        sa.text(
            "SELECT id, object_type, object_id, input_fingerprint, result_json "
            "FROM llm_tasks WHERE task_type = 'event-synthesis' "
            "AND status IN ('pending', 'running') ORDER BY id"
        )
    ).mappings()
    for task in tasks:
        task_id = int(task["id"])
        try:
            payload = json.loads(str(task["result_json"]))
        except (TypeError, json.JSONDecodeError):
            _invalid_active_synthesis_task(task_id)
        request = payload.get("request") if isinstance(payload, dict) else None
        input_data = request.get("input_data") if isinstance(request, dict) else None
        if task["object_type"] != "event" or not isinstance(input_data, dict):
            _invalid_active_synthesis_task(task_id)

        snapshot_uid = str(input_data.get("snapshot_uid") or "").strip()
        owner = bind.execute(
            sa.text(
                "SELECT event.uid AS event_uid, snapshot.event_id AS snapshot_event_id, "
                "snapshot.policy_version, snapshot.source_coverage_fingerprint, "
                "snapshot.content_fingerprint, snapshot.id AS snapshot_id, "
                "revision.uid AS revision_uid, revision.event_id AS revision_event_id "
                "FROM events event "
                "JOIN evidence_snapshots snapshot ON snapshot.uid = :snapshot_uid "
                "JOIN event_revisions revision "
                "ON revision.id = snapshot.target_revision_id "
                "WHERE event.id = :event_id"
            ),
            {"event_id": task["object_id"], "snapshot_uid": snapshot_uid},
        ).mappings().one_or_none()
        if owner is None:
            _invalid_active_synthesis_task(task_id)

        source_fingerprint = str(owner["source_coverage_fingerprint"])
        content_fingerprint = str(owner["content_fingerprint"])
        expected_fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "source_coverage_fingerprint": source_fingerprint,
                    "content_fingerprint": content_fingerprint,
                    "policy_version": "event-synthesis-policy-v2",
                    "prompt_version": "event-synthesis-prompt-v1",
                    "schema_version": "event-synthesis-schema-v1",
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        if (
            owner["snapshot_event_id"] != task["object_id"]
            or owner["revision_event_id"] != task["object_id"]
            or input_data.get("event_uid") != owner["event_uid"]
            or input_data.get("target_revision_uid") != owner["revision_uid"]
            or owner["policy_version"] != "event-synthesis-policy-v2"
            or input_data.get("policy_version") != "event-synthesis-policy-v2"
            or input_data.get("prompt_version") != "event-synthesis-prompt-v1"
            or input_data.get("schema_version") != "event-synthesis-schema-v1"
            or input_data.get("source_coverage_fingerprint") != source_fingerprint
            or input_data.get("content_fingerprint") != content_fingerprint
            or input_data.get("generation_fingerprint") != expected_fingerprint
            or task["input_fingerprint"] != expected_fingerprint
        ):
            _invalid_active_synthesis_task(task_id)

        members = bind.execute(
            sa.text(
                "SELECT version.uid, version.source_id "
                "FROM evidence_snapshot_members member "
                "JOIN event_evidence_versions version "
                "ON version.id = member.evidence_version_id "
                "WHERE member.snapshot_id = :snapshot_id"
            ),
            {"snapshot_id": owner["snapshot_id"]},
        ).mappings().all()
        allowed_version_uids = {str(member["uid"]) for member in members}
        source_ids = {int(member["source_id"]) for member in members}
        requested_evidence = input_data.get("evidence")
        requested_version_uids = (
            {
                str(row.get("evidence_version_uid") or "")
                for row in requested_evidence
                if isinstance(row, dict)
            }
            if isinstance(requested_evidence, list)
            else set()
        )
        if (
            not allowed_version_uids
            or requested_version_uids != allowed_version_uids
            or len(source_ids) < 2
        ):
            _invalid_active_synthesis_task(task_id)


def _invalid_active_synthesis_task(task_id: int) -> None:
    raise RuntimeError(f"active_synthesis_task_provenance_invalid: task_id={task_id}")


def downgrade() -> None:
    raise RuntimeError(
        "P0.3 fingerprint 复用契约不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
