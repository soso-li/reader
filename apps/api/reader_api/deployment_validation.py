from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlsplit

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from .evidence_io import (
    EvidenceError,
    canonical_json as _canonical_json,
    write_evidence_json as _write_json,
)
from .isolated_stage_target import (
    IsolatedStageTargetError,
    IsolatedStageTargetPolicy,
    dedicated_stage_policy_for_database,
)
from .production_target import (
    KNOWN_PRODUCTION_HOSTS,
    credential_secrets,
    production_target_identity,
    sanitized_exception_message,
)
from .queues import (
    belongs_to_reader_queue_namespace,
    reader_queue_names,
)


EVIDENCE_FORMAT = "reader-deployment-evidence/v1"
DATABASE_URL_ENV = "READER_DEPLOYMENT_DATABASE_URL"
REDIS_URL_ENV = "READER_DEPLOYMENT_REDIS_URL"
PRODUCTION_AUTH_ENV = "READER_DEPLOYMENT_ALLOW_PRODUCTION"
REHEARSAL_DATABASE_RE = re.compile(
    r"^(?:reader_p0[12]_rehearsal|reader_test)_[a-z0-9][a-z0-9_]{0,62}$"
)
RQ_WORKER_HEARTBEAT_MAX_AGE_SECONDS = 600

COUNT_TABLES = (
    "sources",
    "raw_entries",
    "documents",
    "content_items",
    "user_states",
)
LEGACY_RAW_ENTRY_FIELDS = (
    "id",
    "source_id",
    "external_id",
    "title",
    "url",
    "author",
    "published_at",
    "fetched_at",
    "raw_summary",
    "raw_content",
    "content_hash",
)
LEGACY_USER_STATE_FIELDS = (
    "id",
    "object_type",
    "object_id",
    "read_status",
    "read_later",
    "starred",
    "updated_at",
)
PRESERVED_TABLES = (
    "sources",
    "raw_entries",
    "source_entry_identities",
    "source_entry_keys",
    "documents",
    "content_items",
    "clusters",
    "cluster_items",
)
PRESERVED_TABLE_IGNORED_COLUMNS = {
    "sources": (
        "fetch_etag",
        "fetch_last_modified",
        "last_successful_payload_hash",
    ),
}
LEGACY_PRESERVED_TABLE_FIELDS = {
    "sources": (
        "id",
        "folder_id",
        "name",
        "url",
        "site_url",
        "media_type",
        "status",
        "enabled",
        "fetch_full_content",
        "feed_trust_score",
        "last_fetched_at",
        "last_error",
        "created_at",
        "status_changed_at",
    ),
    "documents": (
        "id",
        "raw_entry_id",
        "document_type",
        "title",
        "summary",
        "content_text",
        "digest_score",
        "created_at",
    ),
    "content_items": (
        "id",
        "document_id",
        "source_id",
        "title",
        "summary",
        "content_text",
        "url",
        "published_at",
        "content_hash",
        "canonical_url",
        "normalized_title",
        "lsh_signature",
        "media_url",
        "media_kind",
        "media_duration",
        "embedding_vector",
        "embedding_model",
        "cluster_score",
        "created_at",
    ),
    "clusters": (
        "id",
        "cluster_key",
        "title",
        "generated_title",
        "generated_summary",
        "generated_content",
        "citations",
        "model_version",
        "prompt_version",
        "first_seen_at",
        "last_seen_at",
        "created_at",
    ),
    "cluster_items": (
        "id",
        "cluster_id",
        "content_item_id",
        "duplicate_score",
        "created_at",
    ),
}
P02_PROJECTION_TABLES = (
    "events",
    "event_revisions",
    "event_evidence",
    "event_evidence_versions",
    "event_revision_evidence",
    "cluster_event_projections",
    "migration_baselines",
    "event_user_states",
    "interaction_events",
    "event_lineages",
)
P02_PROJECTION_IGNORED_COLUMNS = {
    "events": ("current_synthesis_version_id", "reviewed_evidence_review_id"),
    # These columns do not exist before 0070. Projection verify protects their
    # values before and after deployment; the migration manifest stays
    # comparable across the additive schema change.
    "event_user_states": (
        "uninterested",
        "uninterested_reason",
        "uninterested_note",
        "uninterested_at",
    ),
}
RUNTIME_USER_STATE_IGNORED_COLUMNS = {
    "user_states": (
        "uninterested",
        "uninterested_reason",
        "uninterested_note",
        "uninterested_at",
    )
}
P03_ARTIFACT_TABLES = (
    "evidence_snapshots",
    "evidence_snapshot_members",
    "synthesis_versions",
    "synthesis_blocks",
    "synthesis_citations",
    "evidence_reviews",
    "evidence_review_citations",
)
P03_MIGRATION_EVIDENCE_MIN_REVISION = 53
P02_P03_UPGRADE_HEAD = ["0048_ambiguous_audit_fast_path"]
P03_UPGRADE_HEAD = ["0053_evidence_reviews"]
P04_UPGRADE_EVIDENCE_START_REVISION = 53
P03_P04_UPGRADE_HEAD = ["0053_evidence_reviews"]
P04_UPGRADE_HEAD = ["0066_content_filter_projection"]
P04_GENERATION_TABLE_KEYS = {
    "generation_requests": "id",
    "generation_request_sources": "request_id::text || ':' || source_id::text",
    "generation_controls": "id",
    "generation_admissions": "request_id",
    "generation_request_payloads": "request_id",
    "generation_attempts": "id",
    "generation_attempt_runner_audits": "attempt_id",
    "generation_runner_presences": "id",
    "generation_results": "id",
    "generation_applications": "id",
}
P04_GENERATION_TABLES = tuple(P04_GENERATION_TABLE_KEYS)
P04_GENERATION_IGNORED_COLUMNS = {
    # The singleton is created with CURRENT_TIMESTAMP by the frozen 0056
    # revision.  Its operational timestamp is not a business fact and differs
    # between independent restores of the same P0.3 dump.
    "generation_controls": ("updated_at",),
}
P04_SOURCE_POLICY_COUNT_FIELDS = (
    "source_count",
    "unclassified_count",
    "public_count",
    "private_count",
    "external_allowed_count",
    "policy_version_one_count",
)
P04_SOURCE_POLICY_FIELDS = (
    "privacy_class",
    "external_generation_allowed",
    "generation_policy_version",
)


def _requires_p04_upgrade_evidence(revision_ordinal: int | None) -> bool:
    return (
        revision_ordinal is not None
        and revision_ordinal >= P04_UPGRADE_EVIDENCE_START_REVISION
    )
PRESERVED_EVIDENCE_MIN_REVISION = 4
P02_PROJECTION_EVIDENCE_MIN_REVISION = 31
EVENT_AUTHORITY_MIN_REVISION = 47
SOURCE_GENERATION_PRIVACY_MIN_REVISION = 55
SOURCE_GENERATION_PRIVACY_REVIEWED_THROUGH_REVISION = 71
READING_BODY_CONTRACT_REVISION = 72
READING_BODY_CONTRACT_EVIDENCE_COUNTS = {
    "source_selectors_all_null_count": "sources",
    "document_body_all_null_count": "documents",
}
ALEMBIC_REVISION_RE = re.compile(r"^(\d{4})_[a-z0-9_]+$")


class DatabaseSafetyError(RuntimeError):
    """Raised before a database operation whose target is not provably safe."""


@dataclass(frozen=True)
class DatabaseTarget:
    database_url: str = field(repr=False)
    host: str
    port: int
    database: str
    username: str
    production_authorized: bool
    maintenance_id: str

    def public_identity(self) -> dict[str, object]:
        return {
            "database": self.database,
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "production_authorized": self.production_authorized,
            "maintenance_id": self.maintenance_id,
        }


def database_target(
    database_url: str,
    *,
    production_maintenance: bool = False,
    maintenance_id: str = "",
    environ: Mapping[str, str] | None = None,
) -> DatabaseTarget:
    """Parse a PostgreSQL target and require three explicit production signals."""
    try:
        identity = production_target_identity(database_url)
    except Exception as exc:
        raise DatabaseSafetyError(str(exc)) from exc
    url = identity.url
    if url.query:
        raise DatabaseSafetyError(
            "部署证据数据库地址不允许查询参数；"
            "请使用明确的 host、port 与 database"
        )

    environment = os.environ if environ is None else environ
    production_authorized = identity.is_authorized(
        command_confirmed=production_maintenance,
        authorization_env=PRODUCTION_AUTH_ENV,
        maintenance_id=maintenance_id,
        environ=environment,
    )
    if identity.looks_like_production and not production_authorized:
        raise DatabaseSafetyError(
            "拒绝疑似生产数据库；生产维护授权必须同时包含命令行确认、"
            f"{PRODUCTION_AUTH_ENV}=1 与有效 maintenance id"
        )
    if not identity.database:
        raise DatabaseSafetyError("数据库地址必须显式包含数据库名称")

    return DatabaseTarget(
        database_url=database_url,
        host=identity.host,
        port=identity.port,
        database=identity.database,
        username=identity.username,
        production_authorized=production_authorized,
        maintenance_id=maintenance_id if production_authorized else "",
    )


def validate_rehearsal_target(target: DatabaseTarget) -> DatabaseTarget:
    """Require an unmistakably disposable target before external rehearsal steps."""
    if target.host in KNOWN_PRODUCTION_HOSTS or target.production_authorized:
        raise DatabaseSafetyError("演练数据库必须与生产 PostgreSQL host 完全隔离")
    _reject_dedicated_stage_database(target.database)
    if len(target.database) > 63 or not REHEARSAL_DATABASE_RE.fullmatch(
        target.database
    ):
        raise DatabaseSafetyError(
            "恢复目标数据库必须匹配 reader_p01_rehearsal_*、"
            "reader_p02_rehearsal_* 或 reader_test_*，"
            "前缀后必须有小写字母或数字，且总长不得超过 63 字节"
        )
    return target


def _reject_dedicated_stage_database(database: str) -> None:
    policy = dedicated_stage_policy_for_database(database)
    if policy is not None:
        raise DatabaseSafetyError(policy.validation_entrypoint_error())


def _canonical_scalar(value: object) -> object:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise EvidenceError("Raw Entry 时间字段缺少时区，无法生成确定性证据")
        utc_value = value.astimezone(timezone.utc)
        return utc_value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise EvidenceError(f"Raw Entry 字段包含不支持的类型：{type(value).__name__}")


def _sha256_lines(lines: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for line in lines:
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def build_table_evidence(rows: Iterable[tuple[object, str]]) -> dict[str, object]:
    normalized: list[tuple[object, str]] = []
    for row_id, row_json in rows:
        if isinstance(row_id, bool) or not isinstance(row_id, (int, str)):
            raise EvidenceError("证据表主键必须为整数或字符串")
        canonical_id = _canonical_scalar(row_id)
        try:
            canonical_row = _canonical_json(json.loads(row_json))
        except (TypeError, json.JSONDecodeError) as exc:
            raise EvidenceError("证据表行不是有效 JSON") from exc
        normalized.append((canonical_id, canonical_row))
    normalized.sort(key=lambda row: _canonical_json(row[0]))
    canonical_ids = [_canonical_json(row[0]) for row in normalized]
    if len(set(canonical_ids)) != len(normalized):
        raise EvidenceError("证据表主键重复")
    return {
        "row_count": len(normalized),
        "ordered_rows_sha256": _sha256_lines(
            _canonical_json([row_id, json.loads(row_json)])
            for row_id, row_json in normalized
        ),
    }


def build_legacy_user_state_evidence(
    rows: Iterable[Mapping[str, object]],
    *,
    storage: str,
) -> dict[str, object]:
    if storage not in {"user_states", "migration_baselines"}:
        raise EvidenceError("未知 UserState 证据存储")
    normalized: list[dict[str, object]] = []
    for row in rows:
        if set(row) != set(LEGACY_USER_STATE_FIELDS):
            raise EvidenceError("UserState 证据字段不完整")
        normalized.append(
            {
                field_name: _canonical_scalar(row[field_name])
                for field_name in LEGACY_USER_STATE_FIELDS
            }
        )
    normalized.sort(key=lambda row: int(row["id"]))
    ids = [row["id"] for row in normalized]
    if len(ids) != len(set(ids)):
        raise EvidenceError("UserState 证据 id 重复")
    object_type_counts: dict[str, int] = {}
    for row in normalized:
        object_type = row["object_type"]
        if not isinstance(object_type, str) or not object_type:
            raise EvidenceError("UserState object_type 无效")
        object_type_counts[object_type] = object_type_counts.get(object_type, 0) + 1
    return {
        "storage": storage,
        "columns": list(LEGACY_USER_STATE_FIELDS),
        "row_count": len(normalized),
        "object_type_counts": dict(sorted(object_type_counts.items())),
        "ordered_rows_sha256": _sha256_lines(
            _canonical_json([row[field] for field in LEGACY_USER_STATE_FIELDS])
            for row in normalized
        ),
        "field_sha256": {
            field: _sha256_lines(
                _canonical_json([row["id"], row[field]]) for row in normalized
            )
            for field in LEGACY_USER_STATE_FIELDS
        },
    }


def build_database_snapshot(
    *,
    counts: Mapping[str, int],
    raw_rows: Iterable[Mapping[str, object]],
    alembic_revisions: Sequence[str],
) -> dict[str, object]:
    """Build the deterministic, timestamp-free portion of a database manifest."""
    if set(counts) != set(COUNT_TABLES):
        missing = sorted(set(COUNT_TABLES) - set(counts))
        extra = sorted(set(counts) - set(COUNT_TABLES))
        raise EvidenceError(f"计数表集合不完整：missing={missing} extra={extra}")
    normalized_counts: dict[str, int] = {}
    for table in COUNT_TABLES:
        value = counts[table]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise EvidenceError(f"{table} 计数必须为非负整数")
        normalized_counts[table] = value

    normalized_rows: list[dict[str, object]] = []
    for row in raw_rows:
        if set(row) != set(LEGACY_RAW_ENTRY_FIELDS):
            missing = sorted(set(LEGACY_RAW_ENTRY_FIELDS) - set(row))
            extra = sorted(set(row) - set(LEGACY_RAW_ENTRY_FIELDS))
            raise EvidenceError(
                f"Raw Entry 证据字段不完整：missing={missing} extra={extra}"
            )
        normalized_rows.append(
            {
                field_name: _canonical_scalar(row[field_name])
                for field_name in LEGACY_RAW_ENTRY_FIELDS
            }
        )
    normalized_rows.sort(key=lambda row: int(row["id"]))
    ids = [row["id"] for row in normalized_rows]
    if len(ids) != len(set(ids)):
        raise EvidenceError("Raw Entry id 重复，无法生成确定性证据")
    if normalized_counts["raw_entries"] != len(normalized_rows):
        raise EvidenceError(
            "raw_entries 计数与证据行数不一致："
            f"count={normalized_counts['raw_entries']} rows={len(normalized_rows)}"
        )

    ordered_rows_sha256 = _sha256_lines(
        _canonical_json([row[field_name] for field_name in LEGACY_RAW_ENTRY_FIELDS])
        for row in normalized_rows
    )
    field_sha256 = {
        field_name: _sha256_lines(
            _canonical_json([row["id"], row[field_name]]) for row in normalized_rows
        )
        for field_name in LEGACY_RAW_ENTRY_FIELDS
    }
    return {
        "counts": normalized_counts,
        "alembic_revisions": sorted(set(alembic_revisions)),
        "raw_entry_legacy_evidence": {
            "columns": list(LEGACY_RAW_ENTRY_FIELDS),
            "row_count": len(normalized_rows),
            "ordered_rows_sha256": ordered_rows_sha256,
            "field_sha256": field_sha256,
        },
    }


def _snapshot_sha256(snapshot: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical_json(snapshot).encode("utf-8")).hexdigest()


def _highest_revision_ordinal(revisions: Sequence[str]) -> int | None:
    if not revisions:
        return None
    ordinals: list[int] = []
    for revision in revisions:
        match = ALEMBIC_REVISION_RE.fullmatch(revision)
        if match is None:
            raise EvidenceError(
                f"database-manifest Alembic revision 无法识别：{revision}"
            )
        ordinals.append(int(match.group(1)))
    return max(ordinals)


def _validated_snapshot(manifest: Mapping[str, object]) -> Mapping[str, object]:
    if manifest.get("format") != EVIDENCE_FORMAT:
        raise EvidenceError("证据格式不受支持")
    if manifest.get("kind") != "database-manifest":
        raise EvidenceError("输入不是 database-manifest")
    snapshot = manifest.get("snapshot")
    if not isinstance(snapshot, Mapping):
        raise EvidenceError("database-manifest 缺少 snapshot")
    expected_hash = _snapshot_sha256(snapshot)
    if manifest.get("snapshot_sha256") != expected_hash:
        raise EvidenceError("database-manifest snapshot_sha256 校验失败")
    counts = snapshot.get("counts")
    if not isinstance(counts, Mapping) or set(counts) != set(COUNT_TABLES):
        raise EvidenceError("database-manifest counts 表集合不完整")
    for table in COUNT_TABLES:
        count = counts[table]
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise EvidenceError(f"database-manifest counts.{table} 无效")
    revisions = snapshot.get("alembic_revisions")
    if (
        not isinstance(revisions, list)
        or not all(isinstance(value, str) and value for value in revisions)
        or revisions != sorted(set(revisions))
    ):
        raise EvidenceError("database-manifest alembic_revisions 无效")
    revision_ordinal = _highest_revision_ordinal(revisions)
    raw_evidence = snapshot.get("raw_entry_legacy_evidence")
    if not isinstance(raw_evidence, Mapping):
        raise EvidenceError("database-manifest 缺少 Raw Entry 证据")
    if raw_evidence.get("columns") != list(LEGACY_RAW_ENTRY_FIELDS):
        raise EvidenceError("database-manifest Raw Entry columns 不完整")
    if raw_evidence.get("row_count") != counts["raw_entries"]:
        raise EvidenceError("database-manifest Raw Entry row_count 与计数不一致")
    digest_pattern = re.compile(r"^[0-9a-f]{64}$")
    if not digest_pattern.fullmatch(
        str(raw_evidence.get("ordered_rows_sha256") or "")
    ):
        raise EvidenceError("database-manifest Raw Entry 总摘要无效")
    field_digests = raw_evidence.get("field_sha256")
    if not isinstance(field_digests, Mapping) or set(field_digests) != set(
        LEGACY_RAW_ENTRY_FIELDS
    ):
        raise EvidenceError("database-manifest Raw Entry 字段摘要集合不完整")
    for field_name in LEGACY_RAW_ENTRY_FIELDS:
        if not digest_pattern.fullmatch(str(field_digests[field_name])):
            raise EvidenceError(
                f"database-manifest Raw Entry 字段摘要无效：{field_name}"
            )
    legacy_preserved = snapshot.get("legacy_preserved_table_evidence")
    if not isinstance(legacy_preserved, Mapping) or set(
        legacy_preserved
    ) != set(LEGACY_PRESERVED_TABLE_FIELDS):
        raise EvidenceError(
            "database-manifest legacy_preserved_table_evidence 表集合不完整"
        )
    count_fields = {
        "sources": "sources",
        "documents": "documents",
        "content_items": "content_items",
    }
    for table, columns in LEGACY_PRESERVED_TABLE_FIELDS.items():
        evidence = legacy_preserved[table]
        if not isinstance(evidence, Mapping) or set(evidence) != {
            "columns",
            "row_count",
            "ordered_rows_sha256",
        }:
            raise EvidenceError(
                "database-manifest legacy_preserved_table_evidence."
                f"{table} 字段不完整"
            )
        if evidence.get("columns") != list(columns):
            raise EvidenceError(
                "database-manifest legacy_preserved_table_evidence."
                f"{table}.columns 不完整"
            )
        row_count = evidence.get("row_count")
        if (
            isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
        ):
            raise EvidenceError(
                "database-manifest legacy_preserved_table_evidence."
                f"{table}.row_count 无效"
            )
        count_field = count_fields.get(table)
        if count_field is not None and row_count != counts[count_field]:
            raise EvidenceError(
                "database-manifest legacy_preserved_table_evidence."
                f"{table}.row_count 与计数不一致"
            )
        if not digest_pattern.fullmatch(
            str(evidence.get("ordered_rows_sha256") or "")
        ):
            raise EvidenceError(
                "database-manifest legacy_preserved_table_evidence."
                f"{table} 摘要无效"
            )
    legacy_state = snapshot.get("legacy_user_state_evidence")
    if not isinstance(legacy_state, Mapping):
        raise EvidenceError("database-manifest 缺少 UserState 证据")
    storage = legacy_state.get("storage")
    if storage not in {"user_states", "migration_baselines"}:
        raise EvidenceError("database-manifest UserState storage 无效")
    if set(legacy_state) != {
        "storage",
        "columns",
        "row_count",
        "object_type_counts",
        "ordered_rows_sha256",
        "field_sha256",
    }:
        raise EvidenceError("database-manifest UserState 证据字段不完整")
    if legacy_state.get("columns") != list(LEGACY_USER_STATE_FIELDS):
        raise EvidenceError("database-manifest UserState columns 不完整")
    row_count = legacy_state.get("row_count")
    if (
        isinstance(row_count, bool)
        or not isinstance(row_count, int)
        or row_count < 0
    ):
        raise EvidenceError("database-manifest UserState row_count 无效")
    if storage == "user_states" and row_count != counts["user_states"]:
        raise EvidenceError(
            "database-manifest UserState row_count 与 user_states 计数不一致"
        )
    object_type_counts = legacy_state.get("object_type_counts")
    if not isinstance(object_type_counts, Mapping):
        raise EvidenceError("database-manifest UserState object_type 计数无效")
    normalized_type_count = 0
    for object_type, type_count in object_type_counts.items():
        if (
            not isinstance(object_type, str)
            or not object_type
            or isinstance(type_count, bool)
            or not isinstance(type_count, int)
            or type_count <= 0
        ):
            raise EvidenceError(
                "database-manifest UserState object_type 计数无效"
            )
        normalized_type_count += type_count
    if normalized_type_count != row_count:
        raise EvidenceError(
            "database-manifest UserState object_type 计数与总数不一致"
        )
    if not digest_pattern.fullmatch(
        str(legacy_state.get("ordered_rows_sha256") or "")
    ):
        raise EvidenceError("database-manifest UserState 总摘要无效")
    legacy_field_digests = legacy_state.get("field_sha256")
    if not isinstance(legacy_field_digests, Mapping) or set(
        legacy_field_digests
    ) != set(LEGACY_USER_STATE_FIELDS):
        raise EvidenceError("database-manifest UserState 字段摘要集合不完整")
    for field_name in LEGACY_USER_STATE_FIELDS:
        if not digest_pattern.fullmatch(str(legacy_field_digests[field_name])):
            raise EvidenceError(
                f"database-manifest UserState 字段摘要无效：{field_name}"
            )

    def validate_table_evidence_rows(
        evidence_name: str,
        table_evidence: object,
        expected_tables: tuple[str, ...],
    ) -> None:
        if not isinstance(table_evidence, Mapping) or set(table_evidence) != set(
            expected_tables
        ):
            raise EvidenceError(
                f"database-manifest {evidence_name} 表集合不完整"
            )
        for table in expected_tables:
            evidence = table_evidence[table]
            if not isinstance(evidence, Mapping):
                raise EvidenceError(
                    f"database-manifest {evidence_name}.{table} 无效"
                )
            if set(evidence) != {"row_count", "ordered_rows_sha256"}:
                raise EvidenceError(
                    f"database-manifest {evidence_name}.{table} 字段不完整"
                )
            row_count = evidence["row_count"]
            if (
                isinstance(row_count, bool)
                or not isinstance(row_count, int)
                or row_count < 0
            ):
                raise EvidenceError(
                    f"database-manifest {evidence_name}.{table}.row_count 无效"
                )
            if not digest_pattern.fullmatch(
                str(evidence["ordered_rows_sha256"])
            ):
                raise EvidenceError(
                    f"database-manifest {evidence_name}.{table} 摘要无效"
                )

    def validate_table_evidence(
        evidence_name: str,
        expected_tables: tuple[str, ...],
    ) -> None:
        table_evidence = snapshot.get(evidence_name)
        if table_evidence is not None:
            validate_table_evidence_rows(
                evidence_name,
                table_evidence,
                expected_tables,
            )

    validate_table_evidence("preserved_table_evidence", PRESERVED_TABLES)
    validate_table_evidence("p02_projection_evidence", P02_PROJECTION_TABLES)
    validate_table_evidence(
        "runtime_user_state_evidence", ("user_states",)
    )
    p03_evidence = snapshot.get("p03_migration_evidence")
    requires_p03_evidence = (
        revision_ordinal is not None
        and revision_ordinal >= P03_MIGRATION_EVIDENCE_MIN_REVISION
    )
    if requires_p03_evidence and not isinstance(p03_evidence, Mapping):
        raise EvidenceError(
            "database-manifest P0.3 head 缺少 p03_migration_evidence"
        )
    if not requires_p03_evidence and p03_evidence is not None:
        raise EvidenceError(
            "database-manifest p03_migration_evidence 与 Alembic 阶段不一致"
        )
    if isinstance(p03_evidence, Mapping):
        if set(p03_evidence) != {
            "artifact_tables",
            "event_pointer_counts",
            "event_pointer_evidence",
            "event_task_evidence",
        }:
            raise EvidenceError("database-manifest P0.3 migration 证据字段不完整")
        artifacts = p03_evidence["artifact_tables"]
        if not isinstance(artifacts, Mapping) or set(artifacts) != set(
            P03_ARTIFACT_TABLES
        ):
            raise EvidenceError("database-manifest P0.3 派生表集合不完整")
        for table in P03_ARTIFACT_TABLES:
            evidence = artifacts[table]
            if not isinstance(evidence, Mapping) or set(evidence) != {
                "row_count",
                "ordered_rows_sha256",
            }:
                raise EvidenceError(
                    f"database-manifest P0.3 {table} 证据字段不完整"
                )
            row_count = evidence["row_count"]
            if (
                isinstance(row_count, bool)
                or not isinstance(row_count, int)
                or row_count < 0
                or not digest_pattern.fullmatch(
                    str(evidence["ordered_rows_sha256"])
                )
            ):
                raise EvidenceError(f"database-manifest P0.3 {table} 证据无效")
        pointer_counts = p03_evidence["event_pointer_counts"]
        if not isinstance(pointer_counts, Mapping) or set(pointer_counts) != {
            "current_synthesis",
            "reviewed_evidence",
        }:
            raise EvidenceError("database-manifest P0.3 Event 指针计数不完整")
        for name, count in pointer_counts.items():
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise EvidenceError(
                    f"database-manifest P0.3 Event 指针计数无效：{name}"
                )
        pointer_evidence = p03_evidence["event_pointer_evidence"]
        if not isinstance(pointer_evidence, Mapping) or set(pointer_evidence) != {
            "row_count",
            "ordered_rows_sha256",
        }:
            raise EvidenceError("database-manifest P0.3 Event 指针证据不完整")
        pointer_row_count = pointer_evidence["row_count"]
        if (
            isinstance(pointer_row_count, bool)
            or not isinstance(pointer_row_count, int)
            or pointer_row_count < 0
            or not digest_pattern.fullmatch(
                str(pointer_evidence["ordered_rows_sha256"])
            )
        ):
            raise EvidenceError("database-manifest P0.3 Event 指针证据无效")
        task_evidence = p03_evidence["event_task_evidence"]
        if not isinstance(task_evidence, Mapping) or set(task_evidence) != {
            "row_count",
            "ordered_rows_sha256",
        }:
            raise EvidenceError("database-manifest P0.3 Event 任务证据不完整")
        task_count = task_evidence["row_count"]
        if (
            isinstance(task_count, bool)
            or not isinstance(task_count, int)
            or task_count < 0
            or not digest_pattern.fullmatch(
                str(task_evidence["ordered_rows_sha256"])
            )
        ):
            raise EvidenceError("database-manifest P0.3 Event 任务证据无效")
    p04_evidence = snapshot.get("p04_upgrade_evidence")
    requires_p04_evidence = _requires_p04_upgrade_evidence(revision_ordinal)
    if requires_p04_evidence and not isinstance(p04_evidence, Mapping):
        raise EvidenceError(
            "database-manifest P0.3/P0.4 head 缺少 p04_upgrade_evidence"
        )
    if not requires_p04_evidence and p04_evidence is not None:
        raise EvidenceError(
            "database-manifest p04_upgrade_evidence 与 Alembic 阶段不一致"
        )
    if isinstance(p04_evidence, Mapping):
        if set(p04_evidence) != {
            "llm_task_evidence",
            "generation_tables_present",
            "generation_tables",
            "generation_control_defaults",
            "source_policy_columns_present",
            "source_policy_counts",
            "source_policy_evidence",
        }:
            raise EvidenceError("database-manifest P0.4 upgrade 证据字段不完整")
        llm_tasks = p04_evidence["llm_task_evidence"]
        if not isinstance(llm_tasks, Mapping) or set(llm_tasks) != {
            "row_count",
            "ordered_rows_sha256",
        }:
            raise EvidenceError("database-manifest P0.4 LLMTask 证据字段不完整")
        generation_tables = p04_evidence["generation_tables"]
        validate_table_evidence_rows(
            "P0.4 generation",
            generation_tables,
            P04_GENERATION_TABLES,
        )
        llm_task_count = llm_tasks["row_count"]
        if (
            isinstance(llm_task_count, bool)
            or not isinstance(llm_task_count, int)
            or llm_task_count < 0
            or not digest_pattern.fullmatch(
                str(llm_tasks["ordered_rows_sha256"])
            )
        ):
            raise EvidenceError("database-manifest P0.4 LLMTask 证据无效")
        generation_tables_present = p04_evidence["generation_tables_present"]
        source_policy_present = p04_evidence["source_policy_columns_present"]
        if not isinstance(generation_tables_present, bool) or not isinstance(
            source_policy_present, bool
        ):
            raise EvidenceError("database-manifest P0.4 schema presence 无效")
        if not generation_tables_present and any(
            evidence["row_count"] != 0 for evidence in generation_tables.values()
        ):
            raise EvidenceError(
                "database-manifest P0.4 缺少 generation schema 却存在 generation 行"
            )
        control_defaults = p04_evidence["generation_control_defaults"]
        if not isinstance(control_defaults, Mapping) or set(control_defaults) != {
            "row_count",
            "safe_default_count",
        }:
            raise EvidenceError("database-manifest P0.4 control 证据字段不完整")
        for field_name, value in control_defaults.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise EvidenceError(
                    f"database-manifest P0.4 control 计数无效：{field_name}"
                )
        source_policy_counts = p04_evidence["source_policy_counts"]
        source_policy_evidence = p04_evidence["source_policy_evidence"]
        if (
            not isinstance(source_policy_evidence, Mapping)
            or set(source_policy_evidence)
            != {"row_count", "ordered_rows_sha256"}
            or isinstance(source_policy_evidence["row_count"], bool)
            or not isinstance(source_policy_evidence["row_count"], int)
            or source_policy_evidence["row_count"] < 0
            or not digest_pattern.fullmatch(
                str(source_policy_evidence["ordered_rows_sha256"])
            )
        ):
            raise EvidenceError(
                "database-manifest P0.4 Source policy 逐行证据无效"
            )
        if not isinstance(source_policy_counts, Mapping) or set(
            source_policy_counts
        ) != set(P04_SOURCE_POLICY_COUNT_FIELDS):
            raise EvidenceError("database-manifest P0.4 Source policy 计数不完整")
        for field_name, value in source_policy_counts.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise EvidenceError(
                    f"database-manifest P0.4 Source policy 计数无效：{field_name}"
                )
        if source_policy_counts["source_count"] != counts["sources"]:
            raise EvidenceError(
                "database-manifest P0.4 Source policy 数量与 Source 总数不一致"
            )
        expected_policy_rows = counts["sources"] if source_policy_present else 0
        if source_policy_evidence["row_count"] != expected_policy_rows:
            raise EvidenceError(
                "database-manifest P0.4 Source policy 逐行证据数量不一致"
            )
    reading_body_evidence = snapshot.get("reading_body_contract_evidence")
    requires_reading_body_evidence = (
        revision_ordinal is not None
        and revision_ordinal >= READING_BODY_CONTRACT_REVISION
    )
    if requires_reading_body_evidence and not isinstance(
        reading_body_evidence, Mapping
    ):
        raise EvidenceError(
            "database-manifest 0072+ 缺少 reading_body_contract_evidence"
        )
    if not requires_reading_body_evidence and reading_body_evidence is not None:
        raise EvidenceError(
            "database-manifest reading_body_contract_evidence "
            "与 Alembic 阶段不一致"
        )
    if isinstance(reading_body_evidence, Mapping):
        if set(reading_body_evidence) != set(
            READING_BODY_CONTRACT_EVIDENCE_COUNTS
        ):
            raise EvidenceError(
                "database-manifest reading_body_contract_evidence 字段不完整"
            )
        for field_name, count_field in (
            READING_BODY_CONTRACT_EVIDENCE_COUNTS.items()
        ):
            value = reading_body_evidence[field_name]
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 0 <= value <= counts[count_field]
            ):
                raise EvidenceError(
                    "database-manifest reading_body_contract_evidence."
                    f"{field_name} 无效"
                )
    preserved_evidence = snapshot.get("preserved_table_evidence")
    projection_evidence = snapshot.get("p02_projection_evidence")
    runtime_user_state_evidence = snapshot.get(
        "runtime_user_state_evidence"
    )
    requires_preserved_evidence = (
        revision_ordinal is not None
        and revision_ordinal >= PRESERVED_EVIDENCE_MIN_REVISION
    )
    requires_projection_evidence = (
        revision_ordinal is not None
        and revision_ordinal >= P02_PROJECTION_EVIDENCE_MIN_REVISION
    )
    requires_runtime_user_state_evidence = (
        revision_ordinal is not None
        and revision_ordinal >= EVENT_AUTHORITY_MIN_REVISION
    )
    if requires_preserved_evidence and not isinstance(
        preserved_evidence, Mapping
    ):
        raise EvidenceError(
            "database-manifest 已登记数据库缺少 preserved_table_evidence"
        )
    if not requires_preserved_evidence and preserved_evidence is not None:
        raise EvidenceError(
            "database-manifest preserved_table_evidence 与 Alembic 阶段不一致"
        )
    if isinstance(preserved_evidence, Mapping):
        raw_table = preserved_evidence["raw_entries"]
        assert isinstance(raw_table, Mapping)
        if raw_table.get("row_count") != counts["raw_entries"]:
            raise EvidenceError(
                "database-manifest preserved raw_entries row_count 与计数不一致"
            )
        for table in LEGACY_PRESERVED_TABLE_FIELDS:
            full_table = preserved_evidence[table]
            legacy_table = legacy_preserved[table]
            assert isinstance(full_table, Mapping)
            assert isinstance(legacy_table, Mapping)
            if full_table.get("row_count") != legacy_table.get("row_count"):
                raise EvidenceError(
                    "database-manifest preserved 与 legacy preserved "
                    f"row_count 不一致：{table}"
                )
    if requires_projection_evidence and not isinstance(
        projection_evidence, Mapping
    ):
        raise EvidenceError(
            "database-manifest P0.2 数据库缺少 p02_projection_evidence"
        )
    if not requires_projection_evidence and projection_evidence is not None:
        raise EvidenceError(
            "database-manifest p02_projection_evidence 与 Alembic 阶段不一致"
        )
    if requires_runtime_user_state_evidence and not isinstance(
        runtime_user_state_evidence, Mapping
    ):
        raise EvidenceError(
            "database-manifest Event 权威数据库缺少 runtime_user_state_evidence"
        )
    if (
        not requires_runtime_user_state_evidence
        and runtime_user_state_evidence is not None
    ):
        raise EvidenceError(
            "database-manifest runtime_user_state_evidence 与 Alembic 阶段不一致"
        )
    if isinstance(runtime_user_state_evidence, Mapping):
        runtime_user_states = runtime_user_state_evidence["user_states"]
        assert isinstance(runtime_user_states, Mapping)
        if runtime_user_states.get("row_count") != counts["user_states"]:
            raise EvidenceError(
                "database-manifest runtime UserState row_count 与计数不一致"
            )
    expected_legacy_storage = (
        "migration_baselines" if requires_projection_evidence else "user_states"
    )
    if legacy_state.get("storage") != expected_legacy_storage:
        raise EvidenceError(
            "database-manifest UserState storage 与 Alembic 阶段不一致"
        )
    if legacy_state.get("storage") == "migration_baselines":
        if not isinstance(projection_evidence, Mapping):
            raise EvidenceError(
                "database-manifest MigrationBaseline UserState 证据缺少 P0.2 投影证据"
            )
        baseline_evidence = projection_evidence.get("migration_baselines")
        if (
            not isinstance(baseline_evidence, Mapping)
            or legacy_state.get("row_count") != baseline_evidence.get("row_count")
        ):
            raise EvidenceError(
                "database-manifest UserState row_count 与 migration_baselines 不一致"
            )
        object_type_counts = legacy_state["object_type_counts"]
        assert isinstance(object_type_counts, Mapping)
        expected_runtime_states = (
            sum(
                int(type_count)
                for object_type, type_count in object_type_counts.items()
                if object_type != "cluster"
            )
            if revision_ordinal is not None
            and revision_ordinal >= EVENT_AUTHORITY_MIN_REVISION
            else int(legacy_state["row_count"])
        )
        if counts["user_states"] < expected_runtime_states:
            raise EvidenceError(
                "database-manifest user_states 少于当前 Alembic 阶段 baseline 数量"
            )
    return snapshot


def compare_database_manifests(
    before: Mapping[str, object],
    after: Mapping[str, object],
) -> dict[str, object]:
    """Compare the migration-preserved facts while allowing schema revision changes."""
    before_snapshot = _validated_snapshot(before)
    after_snapshot = _validated_snapshot(after)
    mismatches: list[dict[str, object]] = []

    before_counts = before_snapshot.get("counts")
    after_counts = after_snapshot.get("counts")
    if not isinstance(before_counts, Mapping) or not isinstance(after_counts, Mapping):
        raise EvidenceError("database-manifest 缺少 counts")
    before_legacy_state = before_snapshot.get("legacy_user_state_evidence")
    after_legacy_state = after_snapshot.get("legacy_user_state_evidence")
    compare_logical_user_state = isinstance(
        before_legacy_state, Mapping
    ) and isinstance(after_legacy_state, Mapping)
    before_revision_ordinal = _highest_revision_ordinal(
        before_snapshot["alembic_revisions"]
    )
    after_revision_ordinal = _highest_revision_ordinal(
        after_snapshot["alembic_revisions"]
    )
    compare_runtime_user_state_count = bool(
        before_revision_ordinal is not None
        and before_revision_ordinal >= EVENT_AUTHORITY_MIN_REVISION
        and after_revision_ordinal is not None
        and after_revision_ordinal >= EVENT_AUTHORITY_MIN_REVISION
    )
    for table in COUNT_TABLES:
        if (
            table == "user_states"
            and compare_logical_user_state
            and not compare_runtime_user_state_count
        ):
            continue
        if before_counts.get(table) != after_counts.get(table):
            mismatches.append(
                {
                    "field": f"counts.{table}",
                    "before": before_counts.get(table),
                    "after": after_counts.get(table),
                }
            )

    before_runtime_states = before_snapshot.get(
        "runtime_user_state_evidence"
    )
    after_runtime_states = after_snapshot.get(
        "runtime_user_state_evidence"
    )
    if isinstance(before_runtime_states, Mapping):
        if not isinstance(after_runtime_states, Mapping):
            mismatches.append(
                {
                    "field": "runtime_user_state_evidence",
                    "before": True,
                    "after": False,
                }
            )
        else:
            before_runtime_table = before_runtime_states["user_states"]
            after_runtime_table = after_runtime_states["user_states"]
            assert isinstance(before_runtime_table, Mapping)
            assert isinstance(after_runtime_table, Mapping)
            for field_name in ("row_count", "ordered_rows_sha256"):
                if before_runtime_table.get(
                    field_name
                ) != after_runtime_table.get(field_name):
                    mismatches.append(
                        {
                            "field": (
                                "runtime_user_state_evidence.user_states."
                                f"{field_name}"
                            ),
                            "before": before_runtime_table.get(field_name),
                            "after": after_runtime_table.get(field_name),
                        }
                    )

    before_raw = before_snapshot.get("raw_entry_legacy_evidence")
    after_raw = after_snapshot.get("raw_entry_legacy_evidence")
    if not isinstance(before_raw, Mapping) or not isinstance(after_raw, Mapping):
        raise EvidenceError("database-manifest 缺少 Raw Entry 证据")
    for field_name in ("columns", "row_count", "ordered_rows_sha256"):
        if before_raw.get(field_name) != after_raw.get(field_name):
            mismatches.append(
                {
                    "field": f"raw_entry_legacy_evidence.{field_name}",
                    "before": before_raw.get(field_name),
                    "after": after_raw.get(field_name),
                }
            )
    before_fields = before_raw.get("field_sha256")
    after_fields = after_raw.get("field_sha256")
    if not isinstance(before_fields, Mapping) or not isinstance(after_fields, Mapping):
        raise EvidenceError("database-manifest 缺少 Raw Entry 字段摘要")
    for field_name in LEGACY_RAW_ENTRY_FIELDS:
        if before_fields.get(field_name) != after_fields.get(field_name):
            mismatches.append(
                {
                    "field": f"raw_entry_legacy_evidence.field_sha256.{field_name}",
                    "before": before_fields.get(field_name),
                    "after": after_fields.get(field_name),
                }
            )

    before_legacy_preserved = before_snapshot.get(
        "legacy_preserved_table_evidence"
    )
    after_legacy_preserved = after_snapshot.get(
        "legacy_preserved_table_evidence"
    )
    assert isinstance(before_legacy_preserved, Mapping)
    assert isinstance(after_legacy_preserved, Mapping)
    for table in LEGACY_PRESERVED_TABLE_FIELDS:
        before_table = before_legacy_preserved[table]
        after_table = after_legacy_preserved[table]
        assert isinstance(before_table, Mapping)
        assert isinstance(after_table, Mapping)
        for field_name in ("columns", "row_count", "ordered_rows_sha256"):
            if before_table.get(field_name) != after_table.get(field_name):
                mismatches.append(
                    {
                        "field": (
                            "legacy_preserved_table_evidence."
                            f"{table}.{field_name}"
                        ),
                        "before": before_table.get(field_name),
                        "after": after_table.get(field_name),
                    }
                )

    if (before_legacy_state is None) != (after_legacy_state is None):
        mismatches.append(
            {
                "field": "legacy_user_state_evidence",
                "before": before_legacy_state is not None,
                "after": after_legacy_state is not None,
            }
        )
    elif compare_logical_user_state:
        assert isinstance(before_legacy_state, Mapping)
        assert isinstance(after_legacy_state, Mapping)
        for field_name in (
            "columns",
            "row_count",
            "object_type_counts",
            "ordered_rows_sha256",
        ):
            if before_legacy_state.get(field_name) != after_legacy_state.get(
                field_name
            ):
                mismatches.append(
                    {
                        "field": f"legacy_user_state_evidence.{field_name}",
                        "before": before_legacy_state.get(field_name),
                        "after": after_legacy_state.get(field_name),
                    }
                )
        before_state_fields = before_legacy_state.get("field_sha256")
        after_state_fields = after_legacy_state.get("field_sha256")
        assert isinstance(before_state_fields, Mapping)
        assert isinstance(after_state_fields, Mapping)
        for field_name in LEGACY_USER_STATE_FIELDS:
            if before_state_fields.get(field_name) != after_state_fields.get(
                field_name
            ):
                mismatches.append(
                    {
                        "field": (
                            "legacy_user_state_evidence.field_sha256."
                            f"{field_name}"
                        ),
                        "before": before_state_fields.get(field_name),
                        "after": after_state_fields.get(field_name),
                    }
                )

    before_preserved = before_snapshot.get("preserved_table_evidence")
    after_preserved = after_snapshot.get("preserved_table_evidence")
    before_is_unregistered_legacy = not before_snapshot["alembic_revisions"]
    before_revision_ordinal = _highest_revision_ordinal(
        before_snapshot["alembic_revisions"]
    )
    after_revision_ordinal = _highest_revision_ordinal(
        after_snapshot["alembic_revisions"]
    )
    crossed_source_generation_privacy_revision = (
        before_revision_ordinal is not None
        and before_revision_ordinal < SOURCE_GENERATION_PRIVACY_MIN_REVISION
        and after_revision_ordinal is not None
        and SOURCE_GENERATION_PRIVACY_MIN_REVISION
        <= after_revision_ordinal
        <= SOURCE_GENERATION_PRIVACY_REVIEWED_THROUGH_REVISION
    )
    after_source_policy = (
        after_snapshot.get("p04_upgrade_evidence", {})
        if isinstance(after_snapshot.get("p04_upgrade_evidence"), Mapping)
        else {}
    )
    after_source_policy_counts = after_source_policy.get("source_policy_counts", {})
    expected_default_source_policy_counts = {
        "source_count": after_counts.get("sources"),
        "unclassified_count": after_counts.get("sources"),
        "public_count": 0,
        "private_count": 0,
        "external_allowed_count": 0,
        "policy_version_one_count": after_counts.get("sources"),
    }
    crossed_default_source_generation_privacy_revision = (
        crossed_source_generation_privacy_revision
        and after_source_policy.get("source_policy_columns_present") is True
        and after_source_policy_counts == expected_default_source_policy_counts
    )
    crossed_reading_body_contract_revision = (
        (
            before_revision_ordinal is None
            or before_revision_ordinal < READING_BODY_CONTRACT_REVISION
        )
        and after_revision_ordinal is not None
        and after_revision_ordinal >= READING_BODY_CONTRACT_REVISION
    )
    added_preserved_evidence_after_legacy_upgrade = (
        before_preserved is None
        and isinstance(after_preserved, Mapping)
        and before_is_unregistered_legacy
        and after_revision_ordinal is not None
        and after_revision_ordinal >= PRESERVED_EVIDENCE_MIN_REVISION
    )
    if (
        (before_preserved is None) != (after_preserved is None)
        and not added_preserved_evidence_after_legacy_upgrade
    ):
        mismatches.append(
            {
                "field": "preserved_table_evidence",
                "before": before_preserved is not None,
                "after": after_preserved is not None,
            }
        )
    elif isinstance(before_preserved, Mapping) and isinstance(
        after_preserved, Mapping
    ):
        for table in before_preserved:
            if table not in after_preserved:
                mismatches.append(
                    {
                        "field": f"preserved_table_evidence.{table}",
                        "before": True,
                        "after": False,
                    }
                )
                continue
            before_table = before_preserved[table]
            after_table = after_preserved[table]
            assert isinstance(before_table, Mapping)
            assert isinstance(after_table, Mapping)
            for field_name in ("row_count", "ordered_rows_sha256"):
                if (
                    table in {"sources", "documents"}
                    and field_name == "ordered_rows_sha256"
                    and crossed_reading_body_contract_revision
                ):
                    # The legacy-field evidence already proves the existing
                    # Source and Document facts. Revision 0072 only appends
                    # nullable contract columns, so the one cross-revision
                    # whole-row hash is structurally incomparable.
                    continue
                if (
                    table == "sources"
                    and field_name == "ordered_rows_sha256"
                    and crossed_default_source_generation_privacy_revision
                ):
                    # The legacy-field source evidence above already proves the
                    # preserved subscription data.  Revision 0055 legitimately
                    # adds defaulted policy columns, so only its
                    # cross-revision whole-row hash is incomparable. This is
                    # not an allowance for 0069: a 0068-to-0069 comparison
                    # does not cross 0055 and therefore stays strict.
                    continue
                if before_table.get(field_name) != after_table.get(field_name):
                    mismatches.append(
                        {
                            "field": (
                                f"preserved_table_evidence.{table}.{field_name}"
                            ),
                            "before": before_table.get(field_name),
                            "after": after_table.get(field_name),
                        }
                    )

    before_reading_body = before_snapshot.get(
        "reading_body_contract_evidence"
    )
    after_reading_body = after_snapshot.get("reading_body_contract_evidence")
    if isinstance(before_reading_body, Mapping) and not isinstance(
        after_reading_body, Mapping
    ):
        mismatches.append(
            {
                "field": "reading_body_contract_evidence",
                "before": True,
                "after": False,
            }
        )
    elif isinstance(after_reading_body, Mapping):
        for field_name, count_field in (
            READING_BODY_CONTRACT_EVIDENCE_COUNTS.items()
        ):
            expected = (
                before_counts[count_field]
                if crossed_reading_body_contract_revision
                else (
                    before_reading_body[field_name]
                    if isinstance(before_reading_body, Mapping)
                    else after_reading_body[field_name]
                )
            )
            if after_reading_body[field_name] != expected:
                mismatches.append(
                    {
                        "field": (
                            f"reading_body_contract_evidence.{field_name}"
                        ),
                        "before": expected,
                        "after": after_reading_body[field_name],
                    }
                )

    before_projection = before_snapshot.get("p02_projection_evidence")
    after_projection = after_snapshot.get("p02_projection_evidence")
    if isinstance(before_projection, Mapping) and not isinstance(
        after_projection, Mapping
    ):
        mismatches.append(
            {
                "field": "p02_projection_evidence",
                "before": True,
                "after": False,
            }
        )
    elif isinstance(after_projection, Mapping):
        empty_interactions = build_table_evidence(())
        projection_tables = (
            P02_PROJECTION_TABLES
            if isinstance(before_projection, Mapping)
            else ("interaction_events",)
        )
        for table in projection_tables:
            before_table = (
                before_projection[table]
                if isinstance(before_projection, Mapping)
                else empty_interactions
            )
            after_table = after_projection[table]
            assert isinstance(before_table, Mapping)
            assert isinstance(after_table, Mapping)
            for field_name in ("row_count", "ordered_rows_sha256"):
                if before_table.get(field_name) != after_table.get(field_name):
                    mismatches.append(
                        {
                            "field": (
                                f"p02_projection_evidence.{table}.{field_name}"
                            ),
                            "before": before_table.get(field_name),
                            "after": after_table.get(field_name),
                        }
                    )
    before_p03 = before_snapshot.get("p03_migration_evidence")
    after_p03 = after_snapshot.get("p03_migration_evidence")
    if isinstance(before_p03, Mapping) and not isinstance(after_p03, Mapping):
        mismatches.append(
            {"field": "p03_migration_evidence", "before": True, "after": False}
        )
    elif isinstance(after_p03, Mapping):
        before_artifacts = (
            before_p03["artifact_tables"]
            if isinstance(before_p03, Mapping)
            else {table: build_table_evidence(()) for table in P03_ARTIFACT_TABLES}
        )
        after_artifacts = after_p03["artifact_tables"]
        assert isinstance(before_artifacts, Mapping)
        assert isinstance(after_artifacts, Mapping)
        for table in P03_ARTIFACT_TABLES:
            before_table = before_artifacts[table]
            after_table = after_artifacts[table]
            assert isinstance(before_table, Mapping)
            assert isinstance(after_table, Mapping)
            for field_name in ("row_count", "ordered_rows_sha256"):
                if before_table.get(field_name) != after_table.get(field_name):
                    mismatches.append(
                        {
                            "field": (
                                "p03_migration_evidence.artifact_tables."
                                f"{table}.{field_name}"
                            ),
                            "before": before_table.get(field_name),
                            "after": after_table.get(field_name),
                        }
                    )
        before_pointers = (
            before_p03["event_pointer_counts"]
            if isinstance(before_p03, Mapping)
            else {"current_synthesis": 0, "reviewed_evidence": 0}
        )
        after_pointers = after_p03["event_pointer_counts"]
        assert isinstance(before_pointers, Mapping)
        assert isinstance(after_pointers, Mapping)
        for name in ("current_synthesis", "reviewed_evidence"):
            if before_pointers.get(name) != after_pointers.get(name):
                mismatches.append(
                    {
                        "field": f"p03_migration_evidence.event_pointer_counts.{name}",
                        "before": before_pointers.get(name),
                        "after": after_pointers.get(name),
                    }
                )
        if isinstance(before_p03, Mapping):
            before_pointer_evidence = before_p03["event_pointer_evidence"]
            after_pointer_evidence = after_p03["event_pointer_evidence"]
            assert isinstance(before_pointer_evidence, Mapping)
            assert isinstance(after_pointer_evidence, Mapping)
            for field_name in ("row_count", "ordered_rows_sha256"):
                if before_pointer_evidence.get(
                    field_name
                ) != after_pointer_evidence.get(field_name):
                    mismatches.append(
                        {
                            "field": (
                                "p03_migration_evidence.event_pointer_evidence."
                                f"{field_name}"
                            ),
                            "before": before_pointer_evidence.get(field_name),
                            "after": after_pointer_evidence.get(field_name),
                        }
                    )
        before_tasks = (
            before_p03["event_task_evidence"]
            if isinstance(before_p03, Mapping)
            else build_table_evidence(())
        )
        after_tasks = after_p03["event_task_evidence"]
        assert isinstance(before_tasks, Mapping)
        assert isinstance(after_tasks, Mapping)
        for field_name in ("row_count", "ordered_rows_sha256"):
            if before_tasks.get(field_name) != after_tasks.get(field_name):
                mismatches.append(
                    {
                        "field": (
                            "p03_migration_evidence.event_task_evidence."
                            f"{field_name}"
                        ),
                        "before": before_tasks.get(field_name),
                        "after": after_tasks.get(field_name),
                    }
                )
    before_p04 = before_snapshot.get("p04_upgrade_evidence")
    after_p04 = after_snapshot.get("p04_upgrade_evidence")
    if isinstance(before_p04, Mapping) and not isinstance(after_p04, Mapping):
        mismatches.append(
            {"field": "p04_upgrade_evidence", "before": True, "after": False}
        )
    elif isinstance(after_p04, Mapping):
        after_llm_tasks = after_p04["llm_task_evidence"]
        assert isinstance(after_llm_tasks, Mapping)
        if isinstance(before_p04, Mapping):
            before_llm_tasks = before_p04["llm_task_evidence"]
            assert isinstance(before_llm_tasks, Mapping)
            for field_name in ("row_count", "ordered_rows_sha256"):
                if before_llm_tasks.get(field_name) != after_llm_tasks.get(
                    field_name
                ):
                    mismatches.append(
                        {
                            "field": (
                                "p04_upgrade_evidence.llm_task_evidence."
                                f"{field_name}"
                            ),
                            "before": before_llm_tasks.get(field_name),
                            "after": after_llm_tasks.get(field_name),
                        }
                    )
        before_generation_present = bool(
            isinstance(before_p04, Mapping)
            and before_p04["generation_tables_present"]
        )
        after_generation_present = bool(after_p04["generation_tables_present"])
        if before_generation_present and not after_generation_present:
            mismatches.append(
                {
                    "field": "p04_upgrade_evidence.generation_tables_present",
                    "before": True,
                    "after": False,
                }
            )
        elif (
            before_generation_present == after_generation_present
            and isinstance(before_p04, Mapping)
        ):
            before_tables = (
                before_p04["generation_tables"]
                if isinstance(before_p04, Mapping)
                else {table: build_table_evidence(()) for table in P04_GENERATION_TABLES}
            )
            after_tables = after_p04["generation_tables"]
            assert isinstance(before_tables, Mapping)
            assert isinstance(after_tables, Mapping)
            for table in P04_GENERATION_TABLES:
                before_table = before_tables[table]
                after_table = after_tables[table]
                assert isinstance(before_table, Mapping)
                assert isinstance(after_table, Mapping)
                for field_name in ("row_count", "ordered_rows_sha256"):
                    if before_table.get(field_name) != after_table.get(field_name):
                        mismatches.append(
                            {
                                "field": (
                                    "p04_upgrade_evidence.generation_tables."
                                    f"{table}.{field_name}"
                                ),
                                "before": before_table.get(field_name),
                                "after": after_table.get(field_name),
                            }
                        )
            before_control = (
                before_p04["generation_control_defaults"]
                if isinstance(before_p04, Mapping)
                else {"row_count": 0, "safe_default_count": 0}
            )
            after_control = after_p04["generation_control_defaults"]
            assert isinstance(before_control, Mapping)
            assert isinstance(after_control, Mapping)
            for field_name in ("row_count", "safe_default_count"):
                if before_control.get(field_name) != after_control.get(field_name):
                    mismatches.append(
                        {
                            "field": (
                                "p04_upgrade_evidence.generation_control_defaults."
                                f"{field_name}"
                            ),
                            "before": before_control.get(field_name),
                            "after": after_control.get(field_name),
                        }
                    )
        before_source_policy_present = bool(
            isinstance(before_p04, Mapping)
            and before_p04["source_policy_columns_present"]
        )
        after_source_policy_present = bool(
            after_p04["source_policy_columns_present"]
        )
        if before_source_policy_present and not after_source_policy_present:
            mismatches.append(
                {
                    "field": "p04_upgrade_evidence.source_policy_columns_present",
                    "before": True,
                    "after": False,
                }
            )
        elif (
            before_source_policy_present == after_source_policy_present
            and isinstance(before_p04, Mapping)
        ):
            before_counts = (
                before_p04["source_policy_counts"]
                if isinstance(before_p04, Mapping)
                else {
                    field_name: 0
                    for field_name in P04_SOURCE_POLICY_COUNT_FIELDS
                }
            )
            after_counts = after_p04["source_policy_counts"]
            assert isinstance(before_counts, Mapping)
            assert isinstance(after_counts, Mapping)
            for field_name in P04_SOURCE_POLICY_COUNT_FIELDS:
                if before_counts.get(field_name) != after_counts.get(field_name):
                    mismatches.append(
                        {
                            "field": (
                                "p04_upgrade_evidence.source_policy_counts."
                                f"{field_name}"
                            ),
                            "before": before_counts.get(field_name),
                            "after": after_counts.get(field_name),
                        }
                    )
            before_policy_rows = before_p04["source_policy_evidence"]
            after_policy_rows = after_p04["source_policy_evidence"]
            assert isinstance(before_policy_rows, Mapping)
            assert isinstance(after_policy_rows, Mapping)
            for field_name in ("row_count", "ordered_rows_sha256"):
                if before_policy_rows.get(field_name) != after_policy_rows.get(
                    field_name
                ):
                    mismatches.append(
                        {
                            "field": (
                                "p04_upgrade_evidence.source_policy_evidence."
                                f"{field_name}"
                            ),
                            "before": before_policy_rows.get(field_name),
                            "after": after_policy_rows.get(field_name),
                        }
                    )
    return {"ok": not mismatches, "mismatches": mismatches}


def compare_p03_upgrade_manifests(
    before: Mapping[str, object],
    after: Mapping[str, object],
) -> dict[str, object]:
    result = compare_database_manifests(before, after)
    before_snapshot = _validated_snapshot(before)
    after_snapshot = _validated_snapshot(after)
    mismatches = list(result["mismatches"])
    for field_name, actual, expected in (
        (
            "alembic_revisions.before_p02_head",
            before_snapshot["alembic_revisions"],
            P02_P03_UPGRADE_HEAD,
        ),
        (
            "alembic_revisions.after_p03_head",
            after_snapshot["alembic_revisions"],
            P03_UPGRADE_HEAD,
        ),
    ):
        if actual != expected:
            mismatches.append(
                {"field": field_name, "before": actual, "after": expected}
            )
    return {"ok": not mismatches, "mismatches": mismatches}


def compare_p04_upgrade_manifests(
    before: Mapping[str, object],
    after: Mapping[str, object],
) -> dict[str, object]:
    result = compare_database_manifests(before, after)
    before_snapshot = _validated_snapshot(before)
    after_snapshot = _validated_snapshot(after)
    mismatches = list(result["mismatches"])
    for field_name, actual, expected in (
        (
            "alembic_revisions.before_p03_head",
            before_snapshot["alembic_revisions"],
            P03_P04_UPGRADE_HEAD,
        ),
        (
            "alembic_revisions.after_p04_head",
            after_snapshot["alembic_revisions"],
            P04_UPGRADE_HEAD,
        ),
    ):
        if actual != expected:
            mismatches.append(
                {"field": field_name, "before": actual, "after": expected}
            )

    before_evidence = before_snapshot.get("p04_upgrade_evidence")
    after_evidence = after_snapshot.get("p04_upgrade_evidence")
    if not isinstance(before_evidence, Mapping) or not isinstance(
        after_evidence, Mapping
    ):
        mismatches.append(
            {
                "field": "p04_upgrade_evidence.required_heads",
                "before": isinstance(before_evidence, Mapping),
                "after": isinstance(after_evidence, Mapping),
            }
        )
        return {"ok": False, "mismatches": mismatches}
    for field_name, actual, expected in (
        (
            "p04_upgrade_evidence.before_generation_tables_present",
            before_evidence["generation_tables_present"],
            False,
        ),
        (
            "p04_upgrade_evidence.generation_tables_present",
            after_evidence["generation_tables_present"],
            True,
        ),
        (
            "p04_upgrade_evidence.before_source_policy_columns_present",
            before_evidence["source_policy_columns_present"],
            False,
        ),
        (
            "p04_upgrade_evidence.source_policy_columns_present",
            after_evidence["source_policy_columns_present"],
            True,
        ),
    ):
        if actual != expected:
            mismatches.append(
                {"field": field_name, "before": actual, "after": expected}
            )

    generation_tables = after_evidence["generation_tables"]
    assert isinstance(generation_tables, Mapping)
    for table in P04_GENERATION_TABLES:
        evidence = generation_tables[table]
        assert isinstance(evidence, Mapping)
        expected_count = 1 if table == "generation_controls" else 0
        if evidence["row_count"] != expected_count:
            mismatches.append(
                {
                    "field": (
                        "p04_upgrade_evidence.generation_tables."
                        f"{table}.row_count"
                    ),
                    "before": evidence["row_count"],
                    "after": expected_count,
                }
            )
    control_defaults = after_evidence["generation_control_defaults"]
    assert isinstance(control_defaults, Mapping)
    for field_name in ("row_count", "safe_default_count"):
        if control_defaults[field_name] != 1:
            mismatches.append(
                {
                    "field": (
                        "p04_upgrade_evidence.generation_control_defaults."
                        f"{field_name}"
                    ),
                    "before": control_defaults[field_name],
                    "after": 1,
                }
            )
    source_counts = after_evidence["source_policy_counts"]
    assert isinstance(source_counts, Mapping)
    source_total = after_snapshot["counts"]["sources"]
    assert isinstance(source_total, int)
    expected_source_counts = {
        "source_count": source_total,
        "unclassified_count": source_total,
        "public_count": 0,
        "private_count": 0,
        "external_allowed_count": 0,
        "policy_version_one_count": source_total,
    }
    for field_name, expected in expected_source_counts.items():
        if source_counts[field_name] != expected:
            mismatches.append(
                {
                    "field": (
                        "p04_upgrade_evidence.source_policy_counts."
                        f"{field_name}"
                    ),
                    "before": source_counts[field_name],
                    "after": expected,
                }
            )
    return {"ok": not mismatches, "mismatches": mismatches}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EvidenceError(f"无法读取证据文件 {path}：{exc}") from exc
    if not isinstance(payload, dict):
        raise EvidenceError(f"证据文件顶层必须是 JSON object：{path}")
    return payload


def build_runtime_health(
    *,
    database_probe: Mapping[str, object],
    redis_probe: Mapping[str, object],
    queue_prefix: str,
) -> dict[str, object]:
    """Evaluate DB, Redis and RQ isolation without delegating to HTTP /health."""
    if not re.fullmatch(r"reader(?:-[a-z0-9]+)*", queue_prefix):
        raise EvidenceError("RQ queue prefix 必须是 reader 或 reader-* 的小写命名")
    expected_queue_names = list(reader_queue_names(queue_prefix))
    current_revisions = sorted(
        str(value) for value in database_probe.get("alembic_revisions", [])
    )
    code_heads = sorted(
        str(value) for value in database_probe.get("code_head_revisions", [])
    )
    database = dict(database_probe)
    database["alembic_revisions"] = current_revisions
    database["code_head_revisions"] = code_heads
    database["at_head"] = bool(current_revisions and current_revisions == code_heads)

    registered_queues = sorted(
        {str(value) for value in redis_probe.get("registered_queues", [])}
    )
    worker_rows = redis_probe.get("workers", [])
    if not isinstance(worker_rows, Sequence):
        raise EvidenceError("Redis probe workers 必须是数组")
    queue_workers: dict[str, list[str]] = {
        queue_name: [] for queue_name in expected_queue_names
    }
    worker_queue_names: set[str] = set()
    unexpected_worker_queues: set[str] = set()
    normalized_workers: list[dict[str, object]] = []
    for worker in worker_rows:
        if not isinstance(worker, Mapping):
            raise EvidenceError("Redis probe worker 必须是 object")
        name = str(worker.get("name") or "")
        queue_names = sorted({str(value) for value in worker.get("queue_names", [])})
        healthy = worker.get("healthy", True) is True
        normalized_worker: dict[str, object] = {
            "name": name,
            "queue_names": queue_names,
            "healthy": healthy,
        }
        for field_name in (
            "state",
            "last_heartbeat",
            "heartbeat_age_seconds",
        ):
            if field_name in worker:
                normalized_worker[field_name] = worker[field_name]
        normalized_workers.append(normalized_worker)
        for queue_name in queue_names:
            worker_queue_names.add(queue_name)
            if queue_name in queue_workers:
                if healthy:
                    queue_workers[queue_name].append(name)
            elif belongs_to_reader_queue_namespace(queue_name):
                unexpected_worker_queues.add(queue_name)
    normalized_workers.sort(key=lambda worker: str(worker["name"]))
    for names in queue_workers.values():
        names.sort()
    observed_queue_names = sorted(set(registered_queues) | worker_queue_names)
    missing_queues = sorted(set(expected_queue_names) - set(observed_queue_names))
    queues_without_workers = sorted(
        queue_name for queue_name, names in queue_workers.items() if not names
    )
    unexpected_reader_queues = sorted(
        queue_name
        for queue_name in registered_queues
        if belongs_to_reader_queue_namespace(queue_name)
        and queue_name not in expected_queue_names
    )
    redis = dict(redis_probe)
    redis["registered_queues"] = registered_queues
    redis["workers"] = normalized_workers
    ok = bool(
        database.get("ping") is True
        and database["at_head"] is True
        and redis.get("ping") is True
        and not missing_queues
        and not queues_without_workers
        and not unexpected_reader_queues
        and not unexpected_worker_queues
    )
    return {
        "ok": ok,
        "database": database,
        "redis": redis,
        "expected_queue_names": expected_queue_names,
        "observed_queue_names": observed_queue_names,
        "queue_workers": queue_workers,
        "missing_queues": missing_queues,
        "queues_without_workers": queues_without_workers,
        "unexpected_reader_queues": unexpected_reader_queues,
        "unexpected_worker_queues": sorted(unexpected_worker_queues),
    }


def build_namespace_isolation(
    stable: Mapping[str, object],
    p03: Mapping[str, object],
) -> dict[str, object]:
    """Prove that successful main and P0.3 probes use distinct resources."""
    for label, evidence in (("stable", stable), ("p03", p03)):
        if evidence.get("kind") != "runtime-health" or evidence.get("ok") is not True:
            raise EvidenceError(f"{label} 必须是成功的 runtime-health 证据")

    def database_identity(evidence: Mapping[str, object]) -> tuple[str, str, int]:
        database = evidence.get("database")
        observed = database.get("observed_database") if isinstance(database, Mapping) else None
        if not isinstance(observed, Mapping):
            raise EvidenceError("runtime-health 缺少 observed_database")
        identity = (
            str(observed.get("database") or ""),
            str(observed.get("server_address") or ""),
            observed.get("server_port"),
        )
        if not identity[0] or not identity[1] or isinstance(identity[2], bool) or not isinstance(identity[2], int):
            raise EvidenceError("runtime-health database identity 不完整")
        return identity

    def redis_identity(evidence: Mapping[str, object]) -> tuple[str, str, int, int]:
        redis = evidence.get("redis")
        identity = redis.get("identity") if isinstance(redis, Mapping) else None
        if not isinstance(identity, Mapping):
            raise EvidenceError("runtime-health 缺少 Redis identity")
        value = (
            str(identity.get("run_id") or ""),
            str(identity.get("host") or ""),
            identity.get("port"),
            identity.get("database"),
        )
        if not value[0]:
            raise EvidenceError("runtime-health Redis identity 缺少 server run_id")
        if not value[1] or any(
            isinstance(part, bool) or not isinstance(part, int)
            for part in value[2:]
        ):
            raise EvidenceError("runtime-health Redis identity 不完整")
        return value

    stable_database = database_identity(stable)
    p03_database = database_identity(p03)
    stable_redis = redis_identity(stable)
    p03_redis = redis_identity(p03)
    stable_expected = set(stable.get("expected_queue_names", []))
    p03_expected = set(p03.get("expected_queue_names", []))
    stable_observed = set(stable.get("observed_queue_names", []))
    p03_observed = set(p03.get("observed_queue_names", []))
    checks = {
        "database": bool(
            stable_database[0] == "reader"
            and p03_database[0].startswith("reader_p03")
            and stable_database != p03_database
            and stable_database[1] != p03_database[1]
        ),
        "redis": stable_redis[0] != p03_redis[0],
        "expected_queues": bool(
            stable_expected == {"reader-fetch", "reader-llm"}
            and p03_expected == {"reader-p03-fetch", "reader-p03-llm"}
            and stable_expected.isdisjoint(p03_expected)
        ),
        "observed_queues": bool(
            stable_expected <= stable_observed
            and p03_expected <= p03_observed
            and stable_observed.isdisjoint(p03_observed)
        ),
    }
    return {
        "ok": all(checks.values()),
        "checks": checks,
        "stable": {"database": stable_database, "redis": stable_redis, "queues": sorted(stable_observed)},
        "p03": {"database": p03_database, "redis": p03_redis, "queues": sorted(p03_observed)},
    }


def _begin_read_only(connection: Any) -> Any:
    transaction = connection.begin()
    connection.exec_driver_sql(
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    )
    connection.exec_driver_sql("SET LOCAL search_path = pg_catalog, public")
    return transaction


def _observed_database(connection: Any) -> dict[str, object]:
    row = connection.execute(
        text(
            """
            SELECT current_database() AS database,
                   COALESCE(inet_server_addr()::text, '') AS server_address,
                   inet_server_port() AS server_port,
                   current_user AS username,
                   current_setting('server_version') AS server_version,
                   current_setting('transaction_read_only') AS transaction_read_only
            """
        )
    ).mappings().one()
    return dict(row)


def _assert_observed_target(
    target: DatabaseTarget, observed: Mapping[str, object]
) -> None:
    if observed.get("database") != target.database:
        raise DatabaseSafetyError(
            "实际连接数据库与已校验目标不一致："
            f"expected={target.database} observed={observed.get('database')}"
        )
    if observed.get("transaction_read_only") != "on":
        raise DatabaseSafetyError("证据查询未处于 PostgreSQL read-only transaction")


def _current_alembic_revisions(connection: Any) -> list[str]:
    version_table = connection.execute(
        text("SELECT to_regclass('public.alembic_version')")
    ).scalar_one()
    if version_table is None:
        return []
    return sorted(
        str(value)
        for value in connection.execute(
            text("SELECT version_num FROM alembic_version ORDER BY version_num")
        ).scalars()
    )


def _table_exists(connection: Any, table: str) -> bool:
    return (
        connection.execute(
            text("SELECT to_regclass(:table_name)"),
            {"table_name": f"public.{table}"},
        ).scalar_one()
        is not None
    )


def _columns_exist(connection: Any, table: str, columns: Sequence[str]) -> bool:
    existing = set(
        connection.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :table_name"
            ),
            {"table_name": table},
        ).scalars()
    )
    return set(columns).issubset(existing)


def _collect_table_evidence(
    connection: Any,
    tables: Sequence[str],
    *,
    ignored_columns: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, object]:
    evidence: dict[str, object] = {}
    ignored_columns = ignored_columns or {}
    for table in tables:
        row_json = "to_jsonb(row_value)" + "".join(
            f" - '{column}'" for column in ignored_columns.get(table, ())
        )
        evidence[table] = build_table_evidence(
            (
                (row["id"], row["row_json"])
                for row in connection.execute(
                    text(
                        f"SELECT id, ({row_json})::text AS row_json "
                        f'FROM "{table}" AS row_value ORDER BY id'
                    )
                ).mappings()
            )
        )
    return evidence


def _collect_p04_generation_table_evidence(
    connection: Any,
) -> dict[str, object]:
    evidence: dict[str, object] = {}
    for table, key_expression in P04_GENERATION_TABLE_KEYS.items():
        row_json = "to_jsonb(row_value)" + "".join(
            f" - '{column}'"
            for column in P04_GENERATION_IGNORED_COLUMNS.get(table, ())
        )
        evidence[table] = build_table_evidence(
            (
                (row["row_id"], row["row_json"])
                for row in connection.execute(
                    text(
                        f"SELECT {key_expression} AS row_id, "
                        f"({row_json})::text AS row_json "
                        f'FROM "{table}" AS row_value ORDER BY 1'
                    )
                ).mappings()
            )
        )
    return evidence


def _collect_legacy_preserved_table_evidence(
    connection: Any,
) -> dict[str, object]:
    evidence: dict[str, object] = {}
    for table, columns in LEGACY_PRESERVED_TABLE_FIELDS.items():
        json_arguments = ", ".join(
            f"'{column}', row_value.\"{column}\"" for column in columns
        )
        table_evidence = build_table_evidence(
            (
                (row["id"], row["row_json"])
                for row in connection.execute(
                    text(
                        f'SELECT id, jsonb_build_object({json_arguments})::text '
                        f'AS row_json FROM "{table}" AS row_value ORDER BY id'
                    )
                ).mappings()
            )
        )
        evidence[table] = {
            "columns": list(columns),
            **table_evidence,
        }
    return evidence


@contextmanager
def _validated_read_only_connection(
    target: DatabaseTarget,
) -> Iterator[tuple[Any, dict[str, object]]]:
    """Open one fail-closed, repeatable-read transaction for evidence queries."""
    engine = create_engine(target.database_url, poolclass=NullPool)
    try:
        with engine.connect() as connection:
            transaction = _begin_read_only(connection)
            try:
                observed = _observed_database(connection)
                _assert_observed_target(target, observed)
                yield connection, observed
                transaction.commit()
            except Exception:
                if transaction.is_active:
                    transaction.rollback()
                raise
    finally:
        engine.dispose()


def collect_database_manifest(target: DatabaseTarget) -> dict[str, object]:
    """Capture counts and immutable legacy evidence in one repeatable-read snapshot."""
    with _validated_read_only_connection(target) as (connection, observed):
        counts = {
            table: int(
                connection.execute(
                    text(f'SELECT count(*) FROM "{table}"')
                ).scalar_one()
            )
            for table in COUNT_TABLES
        }
        revisions = _current_alembic_revisions(connection)
        revision_ordinal = _highest_revision_ordinal(revisions)
        raw_rows = connection.execute(
            text(
                """
                SELECT id, source_id, external_id, title, url, author,
                       published_at, fetched_at, raw_summary, raw_content,
                       content_hash
                FROM raw_entries
                ORDER BY id
                """
            )
        ).mappings()
        snapshot = build_database_snapshot(
            counts=counts,
            raw_rows=raw_rows,
            alembic_revisions=revisions,
        )
        snapshot["legacy_preserved_table_evidence"] = (
            _collect_legacy_preserved_table_evidence(connection)
        )
        if (
            revision_ordinal is not None
            and revision_ordinal >= PRESERVED_EVIDENCE_MIN_REVISION
        ):
            missing_preserved_tables = [
                table
                for table in PRESERVED_TABLES
                if not _table_exists(connection, table)
            ]
            if missing_preserved_tables:
                raise EvidenceError(
                    "数据库缺少必须保留的迁移表："
                    + ", ".join(missing_preserved_tables)
                )
            snapshot["preserved_table_evidence"] = _collect_table_evidence(
                connection,
                PRESERVED_TABLES,
                ignored_columns=PRESERVED_TABLE_IGNORED_COLUMNS,
            )

        projection_tables = [
            table for table in P02_PROJECTION_TABLES if _table_exists(connection, table)
        ]
        if projection_tables and set(projection_tables) != set(P02_PROJECTION_TABLES):
            missing_projection_tables = sorted(
                set(P02_PROJECTION_TABLES) - set(projection_tables)
            )
            raise EvidenceError(
                "P0.2 投影表集合不完整：" + ", ".join(missing_projection_tables)
            )
        if projection_tables:
            snapshot["p02_projection_evidence"] = _collect_table_evidence(
                connection,
                P02_PROJECTION_TABLES,
                ignored_columns=P02_PROJECTION_IGNORED_COLUMNS,
            )
            if (
                revision_ordinal is not None
                and revision_ordinal >= EVENT_AUTHORITY_MIN_REVISION
            ):
                snapshot["runtime_user_state_evidence"] = (
                    _collect_table_evidence(
                        connection,
                        ("user_states",),
                        ignored_columns=RUNTIME_USER_STATE_IGNORED_COLUMNS,
                    )
                )
            legacy_state_rows = connection.execute(
                text(
                    "SELECT legacy_user_state_id AS id, "
                    "       legacy_object_type AS object_type, "
                    "       legacy_object_id AS object_id, read_status, "
                    "       read_later, starred, source_updated_at AS updated_at "
                    "FROM migration_baselines ORDER BY legacy_user_state_id"
                )
            ).mappings()
            legacy_state_storage = "migration_baselines"
        else:
            legacy_state_rows = connection.execute(
                text(
                    "SELECT id, object_type, object_id, read_status, read_later, "
                    "       starred, updated_at FROM user_states ORDER BY id"
                )
            ).mappings()
            legacy_state_storage = "user_states"
        if (
            revision_ordinal is not None
            and revision_ordinal >= P03_MIGRATION_EVIDENCE_MIN_REVISION
        ):
            missing_artifact_tables = [
                table
                for table in P03_ARTIFACT_TABLES
                if not _table_exists(connection, table)
            ]
            if missing_artifact_tables:
                raise EvidenceError(
                    "P0.3 派生表集合不完整："
                    + ", ".join(missing_artifact_tables)
                )
            pointer_row = connection.execute(
                text(
                    "SELECT count(*) FILTER (WHERE current_synthesis_version_id "
                    "IS NOT NULL) AS current_synthesis, "
                    "count(*) FILTER (WHERE reviewed_evidence_review_id "
                    "IS NOT NULL) AS reviewed_evidence FROM events"
                )
            ).mappings().one()
            event_tasks = build_table_evidence(
                (
                    (row["id"], row["row_json"])
                    for row in connection.execute(
                        text(
                            "SELECT id, to_jsonb(row_value)::text AS row_json "
                            "FROM llm_tasks AS row_value WHERE task_type IN "
                            "('event-synthesis', 'evidence-review') ORDER BY id"
                        )
                    ).mappings()
                )
            )
            snapshot["p03_migration_evidence"] = {
                "artifact_tables": _collect_table_evidence(
                    connection, P03_ARTIFACT_TABLES
                ),
                "event_pointer_counts": {
                    "current_synthesis": int(pointer_row["current_synthesis"]),
                    "reviewed_evidence": int(pointer_row["reviewed_evidence"]),
                },
                "event_pointer_evidence": build_table_evidence(
                    (
                        (row["id"], row["row_json"])
                        for row in connection.execute(
                            text(
                                "SELECT id, jsonb_build_object("
                                "'current_synthesis_version_id', "
                                "current_synthesis_version_id, "
                                "'reviewed_evidence_review_id', "
                                "reviewed_evidence_review_id)::text AS row_json "
                                "FROM events ORDER BY id"
                            )
                        ).mappings()
                    )
                ),
                "event_task_evidence": event_tasks,
            }
        if _requires_p04_upgrade_evidence(revision_ordinal):
            generation_tables_found = {
                table
                for table in P04_GENERATION_TABLES
                if _table_exists(connection, table)
            }
            generation_tables_present = generation_tables_found == set(
                P04_GENERATION_TABLES
            )
            generation_tables = (
                _collect_p04_generation_table_evidence(connection)
                if generation_tables_present
                else {
                    table: build_table_evidence(())
                    for table in P04_GENERATION_TABLES
                }
            )
            if generation_tables_present:
                control_row = connection.execute(
                    text(
                        "SELECT count(*) AS row_count, "
                        "count(*) FILTER (WHERE id = 1 AND global_pause "
                        "AND NOT auto_run AND daily_budget_tokens IS NULL "
                        "AND input_estimator = 'unicode-codepoints-v1' "
                        "AND output_reserve_tokens = 0 "
                        "AND day_timezone = 'Asia/Shanghai') AS safe_default_count "
                        "FROM generation_controls"
                    )
                ).mappings().one()
            else:
                control_row = {"row_count": 0, "safe_default_count": 0}
            source_policy_columns_present = _columns_exist(
                connection,
                "sources",
                (
                    "privacy_class",
                    "external_generation_allowed",
                    "generation_policy_version",
                ),
            )
            if source_policy_columns_present:
                source_policy_row = connection.execute(
                    text(
                        "SELECT count(*) AS source_count, "
                        "count(*) FILTER (WHERE privacy_class = 'unclassified') "
                        "AS unclassified_count, "
                        "count(*) FILTER (WHERE privacy_class = 'public') "
                        "AS public_count, "
                        "count(*) FILTER (WHERE privacy_class = 'private') "
                        "AS private_count, "
                        "count(*) FILTER (WHERE external_generation_allowed) "
                        "AS external_allowed_count, "
                        "count(*) FILTER (WHERE generation_policy_version = 1) "
                        "AS policy_version_one_count FROM sources"
                    )
                ).mappings().one()
                policy_json_arguments = ", ".join(
                    f"'{column}', row_value.\"{column}\""
                    for column in P04_SOURCE_POLICY_FIELDS
                )
                source_policy_evidence = build_table_evidence(
                    (
                        (row["id"], row["row_json"])
                        for row in connection.execute(
                            text(
                                "SELECT id, jsonb_build_object("
                                f"{policy_json_arguments})::text AS row_json "
                                "FROM sources AS row_value ORDER BY id"
                            )
                        ).mappings()
                    )
                )
            else:
                source_policy_row = {
                    "source_count": counts["sources"],
                    "unclassified_count": 0,
                    "public_count": 0,
                    "private_count": 0,
                    "external_allowed_count": 0,
                    "policy_version_one_count": 0,
                }
                source_policy_evidence = build_table_evidence(())
            snapshot["p04_upgrade_evidence"] = {
                "llm_task_evidence": _collect_table_evidence(
                    connection, ("llm_tasks",)
                )["llm_tasks"],
                "generation_tables_present": generation_tables_present,
                "generation_tables": generation_tables,
                "generation_control_defaults": {
                    "row_count": int(control_row["row_count"]),
                    "safe_default_count": int(control_row["safe_default_count"]),
                },
                "source_policy_columns_present": source_policy_columns_present,
                "source_policy_counts": {
                    field_name: int(source_policy_row[field_name])
                    for field_name in P04_SOURCE_POLICY_COUNT_FIELDS
                },
                "source_policy_evidence": source_policy_evidence,
            }
        if (
            revision_ordinal is not None
            and revision_ordinal >= READING_BODY_CONTRACT_REVISION
        ):
            reading_body_row = connection.execute(
                text(
                    "SELECT "
                    "(SELECT count(*) FROM sources "
                    " WHERE article_selector IS NULL "
                    "   AND remove_selector IS NULL) "
                    "AS source_selectors_all_null_count, "
                    "(SELECT count(*) FROM documents "
                    " WHERE reading_html IS NULL "
                    "   AND body_source IS NULL "
                    "   AND web_fetch_status IS NULL) "
                    "AS document_body_all_null_count"
                )
            ).mappings().one()
            snapshot["reading_body_contract_evidence"] = {
                field_name: int(reading_body_row[field_name])
                for field_name in READING_BODY_CONTRACT_EVIDENCE_COUNTS
            }
        snapshot["legacy_user_state_evidence"] = build_legacy_user_state_evidence(
            legacy_state_rows,
            storage=legacy_state_storage,
        )
    return {
        "format": EVIDENCE_FORMAT,
        "kind": "database-manifest",
        "captured_at": _now_iso(),
        "database": target.public_identity(),
        "observed_database": observed,
        "snapshot": snapshot,
        "snapshot_sha256": _snapshot_sha256(snapshot),
    }


def collect_database_activity(target: DatabaseTarget) -> dict[str, object]:
    """Inspect all other client backends without exposing their SQL text."""
    with _validated_read_only_connection(target) as (connection, observed):
        rows = connection.execute(
            text(
                """
                SELECT pid,
                       COALESCE(usename, '') AS username,
                       COALESCE(application_name, '') AS application_name,
                       COALESCE(client_addr::text, '') AS client_address,
                       COALESCE(state, '') AS state,
                       backend_start,
                       xact_start,
                       query_start
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND backend_type = 'client backend'
                ORDER BY pid
                """
            )
        ).mappings()
        other_clients = [
            {
                key: _canonical_scalar(value)
                for key, value in dict(row).items()
            }
            for row in rows
        ]
    return {
        "format": EVIDENCE_FORMAT,
        "kind": "database-activity",
        "captured_at": _now_iso(),
        "database": target.public_identity(),
        "observed_database": observed,
        "inspection_policy": "zero-other-client-backends",
        "quiescent": not other_clients,
        "other_client_count": len(other_clients),
        "active_transaction_count": sum(
            1 for client in other_clients if client["xact_start"] is not None
        ),
        "other_clients": other_clients,
    }


def collect_runtime_database_probe(target: DatabaseTarget) -> dict[str, object]:
    with _validated_read_only_connection(target) as (connection, observed):
        ping = connection.execute(text("SELECT 1")).scalar_one() == 1
        revisions = _current_alembic_revisions(connection)
    from .migrations.alembic_config import code_head_revisions

    return {
        "ping": ping,
        "observed_database": observed,
        "alembic_revisions": revisions,
        "code_head_revisions": list(code_head_revisions()),
    }


def _command_secrets(environ: Mapping[str, str]) -> tuple[str, ...]:
    """Return credentials from the two dedicated evidence connections."""
    return credential_secrets(
        environ.get(env_name, "")
        for env_name in (DATABASE_URL_ENV, REDIS_URL_ENV)
    )


def collect_redis_probe(redis_url: str) -> dict[str, object]:
    from redis import Redis
    from rq import Queue, Worker

    parsed = urlsplit(redis_url)
    public_identity = {
        "host": parsed.hostname or "",
        "port": parsed.port or 6379,
        "database": int((parsed.path or "/0").lstrip("/") or 0),
        "username": parsed.username or "",
    }
    connection = Redis.from_url(
        redis_url,
        socket_connect_timeout=3,
        socket_timeout=3,
    )
    try:
        ping = bool(connection.ping())
        server_info = connection.info("server")
        run_id = str(server_info.get("run_id") or "")
        if not run_id:
            raise EvidenceError("Redis INFO server 缺少 run_id")
        public_identity.update(
            {
                "run_id": run_id,
                "redis_version": str(server_info.get("redis_version") or ""),
            }
        )
        queues = Queue.all(connection=connection)
        workers = Worker.all(connection=connection)
        now = datetime.now(timezone.utc)

        def worker_evidence(worker: Any) -> dict[str, object]:
            heartbeat = worker.last_heartbeat
            if heartbeat is not None and heartbeat.tzinfo is None:
                heartbeat = heartbeat.replace(tzinfo=timezone.utc)
            heartbeat_age = (
                (now - heartbeat.astimezone(timezone.utc)).total_seconds()
                if heartbeat is not None
                else None
            )
            state = worker.get_state()
            healthy = bool(
                state in {"idle", "busy"}
                and heartbeat_age is not None
                and heartbeat_age <= RQ_WORKER_HEARTBEAT_MAX_AGE_SECONDS
            )
            return {
                "name": worker.name,
                "queue_names": sorted(worker.queue_names()),
                "state": state,
                "last_heartbeat": _canonical_scalar(heartbeat),
                "heartbeat_age_seconds": heartbeat_age,
                "healthy": healthy,
            }

        return {
            "ping": ping,
            "identity": public_identity,
            "registered_queues": sorted(queue.name for queue in queues),
            "queue_depths": {
                queue.name: int(queue.count)
                for queue in sorted(queues, key=lambda value: value.name)
            },
            "workers": [
                worker_evidence(worker)
                for worker in sorted(workers, key=lambda value: value.name)
            ],
        }
    except Exception as exc:
        return {
            "ping": False,
            "identity": public_identity,
            "registered_queues": [],
            "queue_depths": {},
            "workers": [],
            "error": sanitized_exception_message(
                exc,
                credential_values=(redis_url,),
            ),
        }
    finally:
        connection.close()


def _database_target_from_args(
    args: argparse.Namespace,
    environ: Mapping[str, str],
    *,
    stage_target_policy: IsolatedStageTargetPolicy | None = None,
) -> DatabaseTarget:
    database_url = environ.get(DATABASE_URL_ENV, "").strip()
    if not database_url:
        raise DatabaseSafetyError(
            f"必须显式设置 {DATABASE_URL_ENV}；不会回退到应用 DATABASE_URL"
        )
    target = database_target(
        database_url,
        production_maintenance=args.production_maintenance,
        maintenance_id=args.maintenance_id,
        environ=environ,
    )
    if stage_target_policy is not None:
        _validate_stage_ambient_database_url(
            environ=environ,
            target=target,
            stage_target_policy=stage_target_policy,
            require_rehearsal=args.require_rehearsal_target,
        )
        try:
            stage_target_policy.validate(
                host=target.host,
                port=target.port,
                database=target.database,
                username=target.username,
                require_rehearsal=args.require_rehearsal_target,
            )
        except IsolatedStageTargetError as exc:
            raise DatabaseSafetyError(str(exc)) from exc
    else:
        if args.require_rehearsal_target:
            validate_rehearsal_target(target)
        else:
            _reject_dedicated_stage_database(target.database)
    return target


def _validate_stage_ambient_database_url(
    *,
    environ: Mapping[str, str],
    target: DatabaseTarget,
    stage_target_policy: IsolatedStageTargetPolicy,
    require_rehearsal: bool,
) -> None:
    ambient_database_url = environ.get("DATABASE_URL", "").strip()
    if not ambient_database_url:
        return

    error_message = (
        f"{stage_target_policy.stage_label} 证据拒绝越界或与显式目标不一致的 "
        "DATABASE_URL"
    )
    try:
        identity = production_target_identity(ambient_database_url)
        if identity.url.query:
            raise IsolatedStageTargetError(error_message)
        stage_target_policy.validate(
            host=identity.host,
            port=identity.port,
            database=identity.database,
            username=identity.username,
            require_rehearsal=require_rehearsal,
        )
    except (IsolatedStageTargetError, ValueError) as exc:
        raise DatabaseSafetyError(error_message) from exc

    ambient_identity = (
        identity.host,
        identity.port,
        identity.database,
        identity.username,
    )
    explicit_identity = (
        target.host,
        target.port,
        target.database,
        target.username,
    )
    if ambient_identity != explicit_identity:
        raise DatabaseSafetyError(error_message)


def _add_database_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--production-maintenance",
        action="store_true",
        help="确认本命令属于受控生产维护窗口",
    )
    parser.add_argument(
        "--maintenance-id",
        default="",
        help="生产维护审计标识；还必须设置授权环境变量",
    )
    parser.add_argument(
        "--require-rehearsal-target",
        action="store_true",
        help="额外要求数据库名匹配 reader_p01/p02_rehearsal_* 或 reader_test_*",
    )


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reader PostgreSQL 部署演练与证据工具"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    manifest = subparsers.add_parser("manifest", help="采集确定性数据清单")
    _add_database_arguments(manifest)
    manifest.add_argument("--output", type=Path, required=True)

    activity = subparsers.add_parser("activity", help="只读检查其它数据库客户端")
    _add_database_arguments(activity)
    activity.add_argument("--output", type=Path, required=True)
    activity.add_argument("--require-quiescent", action="store_true")

    compare = subparsers.add_parser("compare", help="对账两个 database manifest")
    compare.add_argument("--before", type=Path, required=True)
    compare.add_argument("--after", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--require-p03-upgrade", action="store_true")
    compare.add_argument("--require-p04-upgrade", action="store_true")

    runtime = subparsers.add_parser(
        "runtime-health", help="独立核验 DB、Alembic、Redis 与 RQ worker"
    )
    _add_database_arguments(runtime)
    runtime.add_argument("--queue-prefix", default="")
    runtime.add_argument("--output", type=Path, required=True)
    isolation = subparsers.add_parser(
        "namespace-isolation", help="比较稳定环境与 P0.3 runtime-health 证据"
    )
    isolation.add_argument("--stable", type=Path, required=True)
    isolation.add_argument("--p03", type=Path, required=True)
    isolation.add_argument("--output", type=Path, required=True)
    return parser


def _print_json(payload: Mapping[str, object], *, stream: Any = sys.stdout) -> None:
    print(_canonical_json(payload), file=stream)


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    stage_target_policy: IsolatedStageTargetPolicy | None = None,
) -> int:
    args = _argument_parser().parse_args(argv)
    environment = os.environ if environ is None else environ
    try:
        if args.command == "namespace-isolation":
            result = build_namespace_isolation(
                _read_json(args.stable), _read_json(args.p03)
            )
            evidence = {
                "format": EVIDENCE_FORMAT,
                "kind": "namespace-isolation",
                "captured_at": _now_iso(),
                **result,
            }
            _write_json(args.output, evidence)
            _print_json(evidence)
            return 0 if result["ok"] is True else 1
        if args.command == "compare":
            before = _read_json(args.before)
            after = _read_json(args.after)
            if args.require_p03_upgrade and args.require_p04_upgrade:
                raise EvidenceError("P0.3/P0.4 upgrade 对账模式不能同时启用")
            if args.require_p03_upgrade:
                result = compare_p03_upgrade_manifests(before, after)
            elif args.require_p04_upgrade:
                result = compare_p04_upgrade_manifests(before, after)
            else:
                result = compare_database_manifests(before, after)
            evidence = {
                "format": EVIDENCE_FORMAT,
                "kind": "database-reconciliation",
                "captured_at": _now_iso(),
                "before_path": str(args.before.resolve()),
                "after_path": str(args.after.resolve()),
                **result,
            }
            _write_json(args.output, evidence)
            _print_json(evidence)
            return 0 if result["ok"] is True else 1

        target = _database_target_from_args(
            args,
            environment,
            stage_target_policy=stage_target_policy,
        )
        if args.command == "manifest":
            evidence = collect_database_manifest(target)
            _write_json(args.output, evidence)
            _print_json(evidence)
            return 0
        if args.command == "activity":
            evidence = collect_database_activity(target)
            _write_json(args.output, evidence)
            _print_json(evidence)
            return (
                1
                if args.require_quiescent and evidence["quiescent"] is not True
                else 0
            )
        if args.command == "runtime-health":
            redis_url = environment.get(REDIS_URL_ENV, "").strip()
            if not redis_url:
                raise DatabaseSafetyError(f"必须显式设置 {REDIS_URL_ENV}")
            queue_prefix = args.queue_prefix or environment.get("RQ_QUEUE_PREFIX", "")
            if stage_target_policy is not None:
                try:
                    stage_target_policy.validate_runtime_target(
                        redis_url=redis_url,
                        queue_prefix=queue_prefix,
                    )
                except IsolatedStageTargetError as exc:
                    raise DatabaseSafetyError(str(exc)) from exc
            health = build_runtime_health(
                database_probe=collect_runtime_database_probe(target),
                redis_probe=collect_redis_probe(redis_url),
                queue_prefix=queue_prefix,
            )
            evidence = {
                "format": EVIDENCE_FORMAT,
                "kind": "runtime-health",
                "captured_at": _now_iso(),
                "database": target.public_identity(),
                **health,
            }
            _write_json(args.output, evidence)
            _print_json(evidence)
            return 0 if health["ok"] is True else 1
        raise EvidenceError(f"未知命令：{args.command}")
    except Exception as exc:
        _print_json(
            {
                "format": EVIDENCE_FORMAT,
                "kind": "command-error",
                "command": args.command,
                "error": sanitized_exception_message(
                    exc,
                    extra_secrets=_command_secrets(environment),
                ),
            },
            stream=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
