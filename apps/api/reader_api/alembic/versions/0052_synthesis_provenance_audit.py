"""Audit active synthesis task provenance without mutating facts."""

from __future__ import annotations

import hashlib
import json

from alembic import op
import sqlalchemy as sa


revision: str = "0052_synthesis_provenance_audit"
down_revision: str | None = "0051_fingerprint_reuse"
branch_labels: str | None = None
depends_on: str | None = None

SYNTHESIS_POLICY_VERSION = "event-synthesis-policy-v2"
SYNTHESIS_PROMPT_VERSION = "event-synthesis-prompt-v1"
SYNTHESIS_SCHEMA_VERSION = "event-synthesis-schema-v1"
SYNTHESIS_SYSTEM_PROMPT = (
    "你是个人信息阅读器的事件合成助手。只使用输入证据，输出严格 JSON。"
    "结果字段 blocks 是有序数组；kind 只允许 summary、fact、viewpoint、"
    "disagreement、uncertainty，body 使用中文。每个 block 必须引用输入中的"
    " evidence_version_uid；viewpoint 必须填写 attribution；disagreement 必须用"
    "不同 side 分别引用至少两份冲突证据。不得补充输入外事实。"
)
SYNTHESIS_OUTPUT_SCHEMA: dict[str, object] = {
    "type": "object",
    "required": ["blocks"],
    "additionalProperties": False,
    "properties": {
        "blocks": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["kind", "body", "citations"],
                "additionalProperties": False,
                "properties": {
                    "kind": {
                        "enum": [
                            "summary",
                            "fact",
                            "viewpoint",
                            "disagreement",
                            "uncertainty",
                        ]
                    },
                    "body": {"type": "string", "minLength": 1},
                    "attribution": {"type": "string"},
                    "citations": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "required": ["evidence_version_uid", "side"],
                            "additionalProperties": False,
                            "properties": {
                                "evidence_version_uid": {"type": "string"},
                                "side": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": 40,
                                },
                            },
                        },
                    },
                },
            },
        }
    },
}


def _stable_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def upgrade() -> None:
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


def _validate_active_synthesis_provenance(bind: sa.Connection) -> None:
    tasks = bind.execute(
        sa.text(
            "SELECT id, object_type, object_id, prompt_version, "
            "input_fingerprint, result_json "
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
                "snapshot.uid AS snapshot_uid, snapshot.policy_version, "
                "snapshot.source_coverage_fingerprint, "
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

        if (
            owner["snapshot_event_id"] != task["object_id"]
            or owner["revision_event_id"] != task["object_id"]
            or owner["policy_version"] != SYNTHESIS_POLICY_VERSION
            or task["prompt_version"] != SYNTHESIS_PROMPT_VERSION
        ):
            _invalid_active_synthesis_task(task_id)

        members = bind.execute(
            sa.text(
                "SELECT evidence.uid AS evidence_uid, "
                "evidence.identity_fingerprint, "
                "version.uid AS evidence_version_uid, member.evidence_type, "
                "version.version_fingerprint, "
                "member.role, source.id AS source_id, source.name AS source_name, "
                "source.media_type, version.title_snapshot AS title, "
                "version.url_snapshot AS url, version.author_snapshot AS author, "
                "version.published_at_snapshot AS published_at, "
                "version.content_snapshot AS content "
                "FROM evidence_snapshot_members member "
                "JOIN event_evidence_versions version "
                "ON version.id = member.evidence_version_id "
                "JOIN event_evidence evidence ON evidence.id = version.evidence_id "
                "JOIN sources source ON source.id = version.source_id "
                "WHERE member.snapshot_id = :snapshot_id "
                "ORDER BY member.position"
            ),
            {"snapshot_id": owner["snapshot_id"]},
        ).mappings().all()
        fingerprint_members = sorted(
            members,
            key=lambda member: (
                str(member["identity_fingerprint"]),
                str(member["version_fingerprint"]),
                str(member["evidence_type"]),
                str(member["role"]),
            ),
        )
        source_fingerprint = _stable_hash(
            [
                [
                    str(member["identity_fingerprint"]),
                    str(member["evidence_type"]),
                    str(member["role"]),
                ]
                for member in fingerprint_members
            ]
        )
        content_fingerprint = _stable_hash(
            [
                [
                    str(member["identity_fingerprint"]),
                    str(member["version_fingerprint"]),
                    str(member["evidence_type"]),
                    str(member["role"]),
                ]
                for member in fingerprint_members
            ]
        )
        expected_fingerprint = _stable_hash(
            {
                "source_coverage_fingerprint": source_fingerprint,
                "content_fingerprint": content_fingerprint,
                "policy_version": SYNTHESIS_POLICY_VERSION,
                "prompt_version": SYNTHESIS_PROMPT_VERSION,
                "schema_version": SYNTHESIS_SCHEMA_VERSION,
            }
        )
        if (
            owner["source_coverage_fingerprint"] != source_fingerprint
            or owner["content_fingerprint"] != content_fingerprint
            or task["input_fingerprint"] != expected_fingerprint
        ):
            _invalid_active_synthesis_task(task_id)
        evidence = [
            {
                "evidence_uid": str(member["evidence_uid"]),
                "evidence_version_uid": str(member["evidence_version_uid"]),
                "evidence_type": str(member["evidence_type"]),
                "role": str(member["role"]),
                "source_id": int(member["source_id"]),
                "source_name": str(member["source_name"]),
                "media_type": str(member["media_type"]),
                "title": str(member["title"]),
                "url": str(member["url"]),
                "author": str(member["author"]),
                "published_at": (
                    member["published_at"].isoformat()
                    if member["published_at"] is not None
                    else None
                ),
                "content": str(member["content"]),
            }
            for member in members
        ]
        expected_input_data = {
            "event_uid": str(owner["event_uid"]),
            "target_revision_uid": str(owner["revision_uid"]),
            "snapshot_uid": str(owner["snapshot_uid"]),
            "policy_version": SYNTHESIS_POLICY_VERSION,
            "prompt_version": SYNTHESIS_PROMPT_VERSION,
            "schema_version": SYNTHESIS_SCHEMA_VERSION,
            "source_coverage_fingerprint": source_fingerprint,
            "content_fingerprint": content_fingerprint,
            "generation_fingerprint": expected_fingerprint,
            "evidence": evidence,
        }
        expected_request = {
            "system_prompt": SYNTHESIS_SYSTEM_PROMPT,
            "input": json.dumps(
                expected_input_data,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "input_data": expected_input_data,
            "output_schema": SYNTHESIS_OUTPUT_SCHEMA,
        }
        if (
            len({int(member["source_id"]) for member in members}) < 2
            or input_data != expected_input_data
            or request != expected_request
        ):
            _invalid_active_synthesis_task(task_id)


def _invalid_active_synthesis_task(task_id: int) -> None:
    raise RuntimeError(f"active_synthesis_task_provenance_invalid: task_id={task_id}")


def downgrade() -> None:
    raise RuntimeError(
        "P0.3 synthesis provenance audit 不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
