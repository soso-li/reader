from __future__ import annotations

from dataclasses import replace
import hashlib
import importlib
from io import StringIO
from pathlib import Path
import sys

import pytest
from alembic import command

from reader_api.migrations import __main__ as migration_cli
from reader_api.migrations.__main__ import database_url_from_environment
from reader_api.deployment_validation import DatabaseSafetyError
from reader_api.migrations.alembic_config import code_head_revisions, make_alembic_config
from reader_api.migrations.preflight import migration_engine
from reader_api.migrations.schema_contract import (
    EXPLICIT_FOLDER_MEDIA_TYPES,
    EXPLICIT_FOLDER_UNIQUES,
    EXPLICIT_SOURCE_FOLDER_FOREIGN_KEY,
    EXPECTED_LEGACY_SCHEMA,
    IndexState,
    PrimaryKeyConstraint,
    RestrictedConstraint,
    SchemaSnapshot,
    UniqueConstraint,
    compare_legacy_schema,
)
from reader_api.migrations.testing import validate_isolated_test_database_url
from reader_api.models import (
    ClusterCurrentEventProjection,
    ClusterEventProjection,
    Source,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def test_complete_legacy_snapshot_passes_preflight() -> None:
    assert compare_legacy_schema(EXPECTED_LEGACY_SCHEMA) == []


def test_explicit_folder_media_contract_is_separate_from_the_legacy_baseline() -> None:
    assert EXPLICIT_FOLDER_MEDIA_TYPES == {
        "article", "social", "image", "video", "podcast", "notification"
    }
    assert {constraint.columns for constraint in EXPLICIT_FOLDER_UNIQUES} == {
        ("media_type", "name"),
        ("id", "media_type"),
    }
    assert EXPLICIT_SOURCE_FOLDER_FOREIGN_KEY.constrained_columns == (
        "folder_id",
        "media_type",
    )


def test_architecture_records_current_migration_head() -> None:
    architecture = (REPOSITORY_ROOT / "docs/ARCHITECTURE.md").read_text(
        encoding="utf-8"
    )

    assert code_head_revisions() == ("0072_reading_body_contract",)
    assert "`0072_reading_body_contract`" in architecture
    assert "0072_reading_body_contract" in architecture.partition(
        "当前为唯一代码 head"
    )[0]


def test_privacy_admission_merge_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0057_privacy_admission_merge"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_generation_runner_claim_lease_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0058_runner_claim_lease"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_mac_runner_audit_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0059_legacy_runner_audit"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_generation_cancel_retry_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0060_generation_cancel_retry"

    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_result_apply_contract_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0061_result_apply_contract"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_w5_contract_merge_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0062_w5_contract_merge"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_generation_retention_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0063_generation_retention"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_ambiguous_topology_set_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0064_ambiguous_topology_set"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_ambiguous_topology_plan_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0065_ambiguous_topology_plan"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_content_filter_projection_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0066_content_filter_projection"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_source_deletion_tombstone_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0067_source_deletion_tombstone"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_cluster_current_projection_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0068_cluster_current_projection"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_folder_media_types_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0069_folder_media_types"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_uninterested_projection_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0070_uninterested_projection"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_source_fetch_validators_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0071_source_fetch_validators"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_reading_body_contract_rejects_in_place_downgrade() -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0072_reading_body_contract"
    )

    with pytest.raises(RuntimeError, match="恢复迁移前备份"):
        migration.downgrade()


def test_source_model_declares_live_url_partial_unique_index() -> None:
    index = next(
        index for index in Source.__table__.indexes if index.name == "uq_sources_live_url"
    )

    assert index.unique is True
    assert str(index.dialect_options["postgresql"]["where"]) == (
        "status <> 'deleted'"
    )
    assert str(index.dialect_options["sqlite"]["where"]) == "status <> 'deleted'"


def test_generation_retention_rejects_offline_upgrade_before_ddl(
    monkeypatch,
) -> None:
    migration = importlib.import_module(
        "reader_api.alembic.versions.0063_generation_retention"
    )
    ddl_started = False

    def record_ddl(*_args, **_kwargs) -> None:
        nonlocal ddl_started
        ddl_started = True

    monkeypatch.setattr(migration.context, "is_offline_mode", lambda: True)
    monkeypatch.setattr(migration.op, "drop_constraint", record_ddl)

    with pytest.raises(RuntimeError, match="任何 DDL 前停止"):
        migration.upgrade()

    assert ddl_started is False


def test_event_read_indexes_are_declared_in_model_metadata() -> None:
    indexes = {
        index.name: tuple(column.name for column in index.columns)
        for index in ClusterEventProjection.__table__.indexes
    }

    assert indexes["ix_cluster_event_projections_cluster_snapshot_id"] == (
        "cluster_id_snapshot",
        "id",
    )
    assert indexes["ix_cluster_event_projections_event_id"] == (
        "event_id",
        "id",
    )


def test_cluster_current_projection_model_is_a_two_column_mapping() -> None:
    table = ClusterCurrentEventProjection.__table__

    assert tuple(table.columns) == (table.c.cluster_id, table.c.projection_id)
    assert tuple(table.primary_key.columns) == (table.c.cluster_id,)
    assert {
        constraint.name
        for constraint in table.constraints
        if constraint.name is not None
    } >= {"uq_cluster_current_event_projection_projection"}


def test_ambiguous_audit_lookup_index_is_declared_in_model_metadata() -> None:
    from reader_api.models import ClusteringRunMembership

    indexes = {
        index.name: tuple(column.name for column in index.columns)
        for index in ClusteringRunMembership.__table__.indexes
    }

    assert indexes["ix_clustering_run_membership_evidence_lookup"] == (
        "run_id",
        "snapshot_phase",
        "evidence_anchor",
        "evidence_occurrence",
        "cluster_anchor",
        "cluster_occurrence",
    )


def test_deployed_0023_event_projection_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0023_event_projection.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "08d6713816804f65907709153a15569a615397f40dc47f06c1ef52a646a33b64"
    )


def test_deployed_0024_event_evidence_integrity_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0024_event_evidence_integrity.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "bf2a801f442e25789ac7eaa5e912042f8afd6d6afdb2e5b8a273ed52d314874d"
    )


def test_deployed_0051_fingerprint_reuse_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0051_fingerprint_reuse.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "2f639bc995a70b3c5038783ea8b2caddfd33fbe8ab367a39f1d3f9a253bd8e12"
    )


def test_deployed_0026_legacy_cluster_events_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0026_legacy_cluster_events.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "547955ce4bb1a3303c7720aaad32673c64308eb3d07db4595940d822fd08adcf"
    )


def test_deployed_0027_user_state_baseline_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0027_user_state_baseline.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "e923e6300c46163a6dfa6eb56d8ab0d44cf9852bcb4c62357295023eba66815b"
    )


def test_deployed_0028_event_continuation_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0028_event_continuation.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "1bc72d8f55ffc17a44410179c3766ee4948d7f2df536e213db7ff762dd7b9eef"
    )


def test_deployed_0029_run_projection_predecessors_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0029_run_projection_predecessors.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "a419800c8a81a344099099373ee05b5b783c5ee168574d567bcbe75acd0e71a1"
    )


def test_0030_event_revision_recurrence_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0030_event_revision_recurrence.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "7bcd7c653e4c3f42d522d34ad38ba34878b05c966673d7d7af5638a8012a1455"
    )


def test_0031_event_split_lineage_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0031_event_split_lineage.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "35dae37acd8c252b7ff5d69239cfe120fcdc813ebbefea569f48bdf40e28281d"
    )


def test_0032_split_lineage_integrity_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0032_split_lineage_integrity.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "3cf412f5ecc7a04be0a94cdb785fc62537b70755e2e42f484b5b5b7977dc8cd9"
    )


def test_0033_split_target_newborn_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0033_split_target_newborn.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "59bfdd2e365b81763952351a09932278fdaef76f7f7793e9d35e4c1b186ea6eb"
    )


def test_0034_split_terminal_integrity_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0034_split_terminal_integrity.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "8aed621a8af38898b66f0ff6f101bfdcfbf1705776fc67f85f06e5c96e79d66f"
    )


def test_0035_split_transaction_integrity_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0035_split_transaction_integrity.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "d248eae5cbb0f66215d7ba587691beb8b97f936450260dc22cd89485641f07f1"
    )


def test_0036_split_projection_guard_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0036_split_projection_guard.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "03badd43eb3282b65871c46f28476ca32010eb3b8557c02b4a1f7e8e2241cdc7"
    )


def test_0037_split_projection_audit_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0037_split_projection_audit.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "343c5825076b0dc48c6c80657ae35c60b209f029e56a0df3b807af202ad2a55a"
    )


def test_0038_split_xid_provenance_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0038_split_xid_provenance.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "0d3a2cc5859a6446d861a683fabc3310598dd243c6e946240b16d62aeed7587c"
    )


def test_0039_event_merge_lineage_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0039_event_merge_lineage.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "1aa214209ad0fe967a206789e24f2ce1fda2dc1c09feb4ee723b4b0167dfc890"
    )


def test_0040_merge_transaction_index_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0040_merge_transaction_index.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "588f55e8b5ea69c4adea3b71661d88ebed2403c0d898f9ba42e421da9d577974"
    )


def test_0041_event_ambiguous_lineage_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0041_event_ambiguous_lineage.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "808f469e02fb88c446e53249281225a5b0879fff2b69e2a46eb24e4088816a12"
    )


def test_0042_ambiguous_topology_guard_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0042_ambiguous_topology_guard.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "cbdf575714a3ba779af3612d116e38164d932e9c2bf31a0182ebfa6134b17c14"
    )


def test_0043_ambiguous_run_guard_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0043_ambiguous_run_guard.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "ace8f02230f473178936a8e8d63e91d4f59309fca1a5ef16b524f9e89e4f9c5b"
    )


def test_0044_ambiguous_upgrade_audit_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0044_ambiguous_upgrade_audit.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "66b0713aeb4aaf1b9e73cc6f1c61b7c0c857babdd48823644fe57fe20fe411b2"
    )


def test_0045_feed_metric_source_unique_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0045_feed_metric_source_unique.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "ff35e6816b2abd663b9a5b34c704f2c6f1688511b930d727dd5f1fe6d10f9e2c"
    )


def test_0047_event_authority_contract_migration_is_frozen() -> None:
    migration = (
        REPOSITORY_ROOT
        / "apps/api/reader_api/alembic/versions/0047_event_authority_contract.py"
    )

    assert hashlib.sha256(migration.read_bytes()).hexdigest() == (
        "366e1dd9b19d34449cc8b8fffcdd7a0eaf7c8c2ccb80595dfb891c3af059bb70"
    )


def test_0048_ambiguous_audit_fast_path_is_frozen() -> None:
    files = (
        (
            "apps/api/reader_api/alembic/versions/"
            "0048_ambiguous_audit_fast_path.py"
        ),
        "apps/api/reader_api/migrations/ambiguous_audit_fast_path.py",
    )

    assert tuple(
        hashlib.sha256((REPOSITORY_ROOT / file_name).read_bytes()).hexdigest()
        for file_name in files
    ) == (
        "59dc671856e98ce00fb349bdda885b906f55a01be544d58157cb75a119fab6fe",
        "0d0e41c13be7383a6be8670bafe9a8414dbf3f31ca9c24446fe4cccd186bada0",
    )


def test_preflight_rejects_missing_critical_table() -> None:
    tables = dict(EXPECTED_LEGACY_SCHEMA.tables)
    tables.pop("raw_entries")

    errors = compare_legacy_schema(replace(EXPECTED_LEGACY_SCHEMA, tables=tables))

    assert any("缺少表 raw_entries" in error for error in errors)


def test_preflight_rejects_missing_column_and_wrong_type() -> None:
    tables = {name: dict(columns) for name, columns in EXPECTED_LEGACY_SCHEMA.tables.items()}
    tables["sources"].pop("url")
    tables["content_items"]["embedding_vector"] = "text"

    errors = compare_legacy_schema(replace(EXPECTED_LEGACY_SCHEMA, tables=tables))

    assert any("sources.url" in error and "缺少列" in error for error in errors)
    assert any("content_items.embedding_vector" in error and "halfvec(2560)" in error for error in errors)


def test_preflight_rejects_wrong_column_nullability() -> None:
    nullability = {
        table: dict(columns)
        for table, columns in EXPECTED_LEGACY_SCHEMA.column_nullability.items()
    }
    nullability["raw_entries"]["content_hash"] = True

    errors = compare_legacy_schema(
        replace(EXPECTED_LEGACY_SCHEMA, column_nullability=nullability)
    )

    assert any(
        "raw_entries.content_hash" in error and "不可为空" in error
        for error in errors
    )


def test_preflight_accepts_identity_backed_id_autoincrement() -> None:
    defaults = {
        table: dict(columns)
        for table, columns in EXPECTED_LEGACY_SCHEMA.column_defaults.items()
    }
    defaults["folders"]["id"] = None
    identity_columns = EXPECTED_LEGACY_SCHEMA.identity_columns | {("folders", "id")}

    errors = compare_legacy_schema(
        replace(
            EXPECTED_LEGACY_SCHEMA,
            column_defaults=defaults,
            identity_columns=identity_columns,
        )
    )

    assert errors == []


def test_preflight_rejects_default_using_unrelated_sequence() -> None:
    defaults = {
        table: dict(columns)
        for table, columns in EXPECTED_LEGACY_SCHEMA.column_defaults.items()
    }
    defaults["folders"]["id"] = "nextval('unrelated_id_seq'::regclass)"

    errors = compare_legacy_schema(
        replace(EXPECTED_LEGACY_SCHEMA, column_defaults=defaults)
    )

    assert any("folders.id" in error and "自增" in error for error in errors)


def test_preflight_fails_closed_when_column_nullability_is_unavailable() -> None:
    nullability = {
        table: dict(columns)
        for table, columns in EXPECTED_LEGACY_SCHEMA.column_nullability.items()
    }
    nullability["raw_entries"].pop("content_hash")

    errors = compare_legacy_schema(
        replace(EXPECTED_LEGACY_SCHEMA, column_nullability=nullability)
    )

    assert any(
        "raw_entries.content_hash" in error and "无法核对 nullability" in error
        for error in errors
    )


def test_preflight_rejects_missing_unique_foreign_key_extension_and_index() -> None:
    uniques = dict(EXPECTED_LEGACY_SCHEMA.unique_constraints)
    uniques["raw_entries"] = ()
    foreign_keys = dict(EXPECTED_LEGACY_SCHEMA.foreign_keys)
    foreign_keys["documents"] = ()
    indexes = dict(EXPECTED_LEGACY_SCHEMA.indexes)
    indexes.pop("ix_content_items_embedding_hnsw")
    snapshot = replace(
        EXPECTED_LEGACY_SCHEMA,
        unique_constraints=uniques,
        foreign_keys=foreign_keys,
        extensions=frozenset(),
        indexes=indexes,
    )

    errors = compare_legacy_schema(snapshot)

    assert any("raw_entries" in error and "唯一约束" in error for error in errors)
    assert any("documents" in error and "外键" in error for error in errors)
    assert any("vector" in error and "扩展" in error for error in errors)
    assert any("ix_content_items_embedding_hnsw" in error and "索引" in error for error in errors)


def test_preflight_rejects_unexpected_unique_constraint() -> None:
    uniques = dict(EXPECTED_LEGACY_SCHEMA.unique_constraints)
    uniques["raw_entries"] = uniques["raw_entries"] + (
        UniqueConstraint(("title",)),
    )

    errors = compare_legacy_schema(
        replace(EXPECTED_LEGACY_SCHEMA, unique_constraints=uniques)
    )

    assert any(
        "raw_entries" in error
        and "额外唯一约束" in error
        and "title" in error
        for error in errors
    )


def test_preflight_rejects_deferred_primary_key() -> None:
    primary_keys = dict(EXPECTED_LEGACY_SCHEMA.primary_keys)
    primary_keys["llm_tasks"] = PrimaryKeyConstraint(
        ("id",),
        is_deferrable=True,
        initially="DEFERRED",
    )

    errors = compare_legacy_schema(
        replace(EXPECTED_LEGACY_SCHEMA, primary_keys=primary_keys)
    )

    assert any(
        "llm_tasks" in error and "主键" in error and "DEFERRED" in error
        for error in errors
    )


def test_preflight_rejects_invalid_unique_backing_index() -> None:
    unique_constraints = dict(EXPECTED_LEGACY_SCHEMA.unique_constraints)
    unique_constraints["folders"] = (
        UniqueConstraint(
            ("name",),
            index_state=IndexState(is_valid=False, is_ready=False),
        ),
    )

    errors = compare_legacy_schema(
        replace(
            EXPECTED_LEGACY_SCHEMA,
            unique_constraints=unique_constraints,
        )
    )

    assert any(
        "folders" in error
        and "IndexState" in error
        and "is_valid=False" in error
        and "is_ready=False" in error
        for error in errors
    )


def test_preflight_rejects_check_and_exclusion_constraints() -> None:
    restricted_constraints = {
        "llm_tasks": (
            RestrictedConstraint("CHECK", "CHECK (object_id >= 0)", False),
            RestrictedConstraint("EXCLUDE", "EXCLUDE USING btree (id WITH =)", True),
        )
    }

    errors = compare_legacy_schema(
        replace(
            EXPECTED_LEGACY_SCHEMA,
            restricted_constraints=restricted_constraints,
        )
    )

    assert any("llm_tasks" in error and "CHECK" in error for error in errors)
    assert any("llm_tasks" in error and "EXCLUDE" in error for error in errors)


def test_preflight_rejects_standalone_unique_index() -> None:
    errors = compare_legacy_schema(
        replace(
            EXPECTED_LEGACY_SCHEMA,
            standalone_unique_indexes={
                "llm_tasks": ("llm_tasks_object_id_unique_idx",)
            },
        )
    )

    assert any(
        "llm_tasks" in error
        and "独立 UNIQUE 索引" in error
        and "llm_tasks_object_id_unique_idx" in error
        for error in errors
    )


def test_preflight_rejects_wrong_index_definition() -> None:
    indexes = dict(EXPECTED_LEGACY_SCHEMA.indexes)
    indexes["ix_content_embeddings_zh_hnsw"] = "create index using btree (vector)"
    snapshot = replace(EXPECTED_LEGACY_SCHEMA, indexes=indexes)

    errors = compare_legacy_schema(snapshot)

    assert any("ix_content_embeddings_zh_hnsw" in error and "halfvec_cosine_ops" in error for error in errors)


def test_preflight_rejects_fts_index_with_incomplete_expression_or_wrong_config() -> None:
    indexes = dict(EXPECTED_LEGACY_SCHEMA.indexes)
    indexes["ix_content_items_fts"] = (
        "CREATE INDEX ix_content_items_fts ON content_items "
        "USING GIN (to_tsvector('english', content_text))"
    )

    errors = compare_legacy_schema(
        replace(EXPECTED_LEGACY_SCHEMA, indexes=indexes)
    )

    assert any(
        "ix_content_items_fts" in error and "定义不兼容" in error
        for error in errors
    )


def test_preflight_rejects_hnsw_index_with_incomplete_predicate() -> None:
    indexes = dict(EXPECTED_LEGACY_SCHEMA.indexes)
    indexes["ix_content_embeddings_zh_hnsw"] = (
        "CREATE INDEX ix_content_embeddings_zh_hnsw ON content_embeddings "
        "USING HNSW (vector halfvec_cosine_ops) "
        "WHERE representation = 'zh_canonical'"
    )

    errors = compare_legacy_schema(
        replace(EXPECTED_LEGACY_SCHEMA, indexes=indexes)
    )

    assert any(
        "ix_content_embeddings_zh_hnsw" in error and "定义不兼容" in error
        for error in errors
    )


@pytest.mark.parametrize(
    "wrong_literal",
    ("zh_ canonical", "public.zh_canonical", "ZH_CANONICAL"),
)
def test_preflight_never_normalizes_hnsw_predicate_literals(
    wrong_literal: str,
) -> None:
    indexes = dict(EXPECTED_LEGACY_SCHEMA.indexes)
    indexes["ix_content_embeddings_zh_hnsw"] = (
        "CREATE INDEX ix_content_embeddings_zh_hnsw ON content_embeddings "
        "USING HNSW (vector halfvec_cosine_ops) "
        f"WHERE vector IS NOT NULL AND representation = '{wrong_literal}'"
    )

    errors = compare_legacy_schema(
        replace(EXPECTED_LEGACY_SCHEMA, indexes=indexes)
    )

    assert any(
        "ix_content_embeddings_zh_hnsw" in error and "定义不兼容" in error
        for error in errors
    )


def test_preflight_accepts_postgres_canonical_index_formatting() -> None:
    indexes = dict(EXPECTED_LEGACY_SCHEMA.indexes)
    indexes["ix_content_embeddings_zh_hnsw"] = (
        "CREATE INDEX ix_content_embeddings_zh_hnsw ON public.content_embeddings "
        "USING hnsw (vector halfvec_cosine_ops) "
        "WHERE ((vector IS NOT NULL) AND ((representation)::text = "
        "'zh_canonical'::text))"
    )

    assert compare_legacy_schema(
        replace(EXPECTED_LEGACY_SCHEMA, indexes=indexes)
    ) == []


def test_preflight_rejects_unexpected_table_and_managed_column() -> None:
    tables = {name: dict(columns) for name, columns in EXPECTED_LEGACY_SCHEMA.tables.items()}
    tables["sources"]["legacy_note"] = "text"
    tables["legacy_audit"] = {"id": "integer"}
    tables["alembic_version"] = {"version_num": "character varying(32)"}
    nullability = {
        name: dict(columns)
        for name, columns in EXPECTED_LEGACY_SCHEMA.column_nullability.items()
    }
    nullability["alembic_version"] = {"version_num": False}
    primary_keys = dict(EXPECTED_LEGACY_SCHEMA.primary_keys)
    primary_keys["alembic_version"] = PrimaryKeyConstraint(("version_num",))

    errors = compare_legacy_schema(
        replace(
            EXPECTED_LEGACY_SCHEMA,
            tables=tables,
            column_nullability=nullability,
            primary_keys=primary_keys,
        )
    )

    assert errors == ["存在额外表 legacy_audit", "sources 存在额外列 legacy_note"]


def test_isolated_postgres_test_url_guard_accepts_only_explicit_local_test_database() -> None:
    url = "postgresql+psycopg://reader:reader@127.0.0.1:55439/reader_test_migrations"

    assert validate_isolated_test_database_url(url) == url


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+psycopg://reader:reader@127.0.0.1:55438/reader",
        "postgresql+psycopg://reader:reader@192.0.2.6:5432/reader_test_migrations",
        "sqlite:///reader_test_migrations.db",
    ],
)
def test_isolated_postgres_test_url_guard_rejects_production_or_non_postgres_targets(url: str) -> None:
    with pytest.raises(ValueError, match="隔离测试数据库"):
        validate_isolated_test_database_url(url)


def test_isolated_postgres_test_url_guard_rejects_query_parameter_target_override() -> None:
    disguised_production_url = (
        "postgresql+psycopg://reader_test:reader_test@127.0.0.1:55439/reader_test_migrations"
        "?host=192.0.2.6&port=5432&dbname=reader"
    )

    with pytest.raises(ValueError, match="查询参数"):
        validate_isolated_test_database_url(disguised_production_url)


def test_schema_snapshot_type_is_immutable() -> None:
    snapshot = SchemaSnapshot(
        tables={},
        column_nullability={},
        column_defaults={},
        identity_columns=frozenset(),
        serial_sequences={},
        primary_keys={},
        unique_constraints={},
        foreign_keys={},
        restricted_constraints={},
        standalone_unique_indexes={},
        extensions=frozenset(),
        indexes={},
        index_states={},
    )

    with pytest.raises(AttributeError):
        snapshot.extensions = frozenset({"vector"})  # type: ignore[misc]


def test_baseline_renders_complete_postgres_ddl_through_last_offline_safe_revision() -> None:
    output = StringIO()
    config = make_alembic_config(
        "postgresql+psycopg://reader:reader@127.0.0.1:55439/reader_test_migrations"
    )
    config.output_buffer = output

    command.upgrade(config, "0062_w5_contract_merge", sql=True)

    sql = output.getvalue()
    assert "CREATE EXTENSION IF NOT EXISTS vector" in sql
    for table in EXPECTED_LEGACY_SCHEMA.tables:
        assert f"CREATE TABLE {table}" in sql
    assert "CREATE TABLE maintenance_runs" in sql
    assert "CREATE TABLE source_entry_identities" in sql
    assert "CREATE TABLE source_entry_keys" in sql
    assert "CREATE TABLE source_entry_relations" in sql
    assert "CREATE TABLE clustering_runs" in sql
    assert "CREATE TABLE clustering_run_scope_evidence" in sql
    assert "CREATE TABLE clustering_run_memberships" in sql
    assert "ADD COLUMN cluster_occurrence INTEGER DEFAULT '1' NOT NULL" in sql
    assert "ADD COLUMN evidence_occurrence INTEGER DEFAULT '1' NOT NULL" in sql
    assert "ck_clustering_run_membership_occurrence" in sql
    assert "ck_clustering_run_scope_occurrence" in sql
    assert "FOR UPDATE" in sql
    assert "after_snapshot_finalized" in sql
    assert "clustering_run_snapshot_incomplete" in sql
    assert "clustering_run_snapshot_seals" in sql
    assert "clustering_run_snapshot_unsealed" in sql
    assert "clustering_snapshot_sealed" in sql
    assert "ARRAY['before', 'after']" in sql
    assert "ADD COLUMN source_entry_id" in sql
    assert "ADD COLUMN revision_no" in sql
    assert "ADD COLUMN payload_fingerprint" in sql
    assert "CREATE TEMPORARY TABLE source_entry_backfill_map" in sql
    assert "UPDATE raw_entries" in sql
    assert "uq_raw_source_entry_revision" in sql
    assert "ALTER COLUMN source_entry_id SET NOT NULL" in sql
    assert "ALTER COLUMN revision_no SET NOT NULL" in sql
    assert "raw_revision_offline_backfill_unsupported" in sql
    assert "legacy_cluster_offline_backfill_unsupported" in sql
    assert "legacy_user_state_offline_backfill_unsupported" in sql
    assert "CREATE TABLE migration_baselines" in sql
    assert "CREATE TABLE event_user_states" in sql
    assert "CREATE TABLE interaction_events" in sql
    assert "ck_raw_payload_fingerprint_sha256" in sql
    assert "ck_raw_payload_fingerprint_canonical" in sql
    assert "ck_source_entry_key_kind" in sql
    assert "trg_raw_revision_immutable" in sql
    assert "trg_identity_revision_consistency" in sql
    assert "trg_identity_key_presence" in sql
    assert "trg_source_entry_key_presence" in sql
    assert "trg_clustering_run_lifecycle" in sql
    assert "trg_clustering_scope_immutable" in sql
    assert "trg_clustering_membership_immutable" in sql
    assert "ALTER COLUMN payload_fingerprint SET NOT NULL" in sql
    assert "halfvec(2560)" in sql
    for index in EXPECTED_LEGACY_SCHEMA.indexes:
        assert f"CREATE INDEX {index}" in sql


def test_migration_cli_requires_dedicated_url_without_database_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("READER_MIGRATION_DATABASE_URL", raising=False)
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://reader:reader@192.0.2.6:5432/reader",
    )

    with pytest.raises(RuntimeError, match="不会回退"):
        database_url_from_environment()


def test_migration_cli_rejects_production_target_without_explicit_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    production_url = "postgresql+psycopg://reader:reader@postgres:5432/reader"
    monkeypatch.setenv("READER_MIGRATION_DATABASE_URL", production_url)
    monkeypatch.delenv("READER_MIGRATION_ALLOW_PRODUCTION", raising=False)

    with pytest.raises(RuntimeError, match="拒绝迁移疑似生产数据库"):
        database_url_from_environment()

    monkeypatch.setenv("READER_MIGRATION_ALLOW_PRODUCTION", "1")
    assert database_url_from_environment() == production_url


@pytest.mark.parametrize(
    "host",
    (
        "postgres",
        "reader-postgres",
        "postgres.",
        "reader-postgres.",
        "postgres。",
        "postgres．",
        "postgres｡",
    ),
)
def test_migration_cli_rejects_known_production_host_for_rehearsal_database(
    monkeypatch: pytest.MonkeyPatch,
    host: str,
) -> None:
    database_url = (
        f"postgresql+psycopg://reader:reader@{host}:5432/"
        "reader_p01_rehearsal_issue15"
    )
    monkeypatch.setenv("READER_MIGRATION_DATABASE_URL", database_url)
    monkeypatch.delenv("READER_MIGRATION_ALLOW_PRODUCTION", raising=False)

    with pytest.raises(RuntimeError, match="拒绝迁移疑似生产数据库"):
        database_url_from_environment()


@pytest.mark.parametrize(
    "host",
    (
        "3221225990",
        "0xc0000206",
        "192.000.002.006",
        "0300.000.002.006",
        "192.2.6",
    ),
)
def test_migration_cli_rejects_ambiguous_numeric_host(
    monkeypatch: pytest.MonkeyPatch,
    host: str,
) -> None:
    monkeypatch.setenv(
        "READER_MIGRATION_DATABASE_URL",
        f"postgresql+psycopg://reader:reader@{host}:5432/"
        "reader_p01_rehearsal_issue15",
    )

    with pytest.raises(ValueError, match="非标准数值"):
        database_url_from_environment()


def test_migration_cli_requires_explicit_database_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "READER_MIGRATION_DATABASE_URL",
        "postgresql+psycopg:///reader_p01_rehearsal_issue15",
    )

    with pytest.raises(ValueError, match="显式包含 host"):
        database_url_from_environment()


def test_read_only_head_check_allows_production_without_migration_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    production_url = "postgresql+psycopg://reader:reader@postgres:5432/reader"
    monkeypatch.setenv("READER_MIGRATION_DATABASE_URL", production_url)
    monkeypatch.delenv("READER_MIGRATION_ALLOW_PRODUCTION", raising=False)

    assert (
        database_url_from_environment(require_production_override=False)
        == production_url
    )


def test_folder_type_prepare_cli_requires_three_production_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    production_url = "postgresql+psycopg://reader:reader@postgres:5432/reader"
    monkeypatch.setenv("READER_MIGRATION_DATABASE_URL", production_url)
    monkeypatch.delenv("READER_MIGRATION_ALLOW_PRODUCTION", raising=False)
    monkeypatch.delenv("READER_DEPLOYMENT_ALLOW_PRODUCTION", raising=False)
    for command in ([], ["--apply"]):
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "reader-migrations",
                "prepare-folder-media-types",
                *command,
            ],
        )
        with pytest.raises(DatabaseSafetyError, match="生产维护授权必须同时包含"):
            migration_cli.main()

    calls: list[dict[str, object]] = []

    def fake_prepare(database_url: str, **kwargs: object) -> dict[str, object]:
        calls.append({"database_url": database_url, **kwargs})
        return {"ok": True}

    monkeypatch.setenv("READER_DEPLOYMENT_ALLOW_PRODUCTION", "1")
    monkeypatch.setattr(migration_cli, "prepare_folder_media_types", fake_prepare)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "reader-migrations",
            "prepare-folder-media-types",
            "--apply",
            "--production-maintenance",
            "--maintenance-id",
            "change-0069",
        ],
    )

    assert migration_cli.main() == 0
    assert calls == [
        {
            "database_url": production_url,
            "apply": True,
            "target": {
                "database": "reader",
                "host": "postgres",
                "port": 5432,
                "username": "reader",
                "production_authorized": True,
                "maintenance_id": "change-0069",
            },
        }
    ]


def test_migration_cli_rejects_query_parameter_target_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disguised_production_url = (
        "postgresql+psycopg://reader_test:reader_test@127.0.0.1:55439/reader_test_migrations"
        "?host=192.0.2.6&port=5432&dbname=reader"
    )
    monkeypatch.setenv("READER_MIGRATION_DATABASE_URL", disguised_production_url)
    monkeypatch.setenv("READER_MIGRATION_ALLOW_PRODUCTION", "1")

    with pytest.raises(ValueError, match="查询参数"):
        database_url_from_environment()


def test_internal_migration_engine_rejects_query_parameter_target_override() -> None:
    disguised_production_url = (
        "postgresql+psycopg://reader_test:reader_test@127.0.0.1:55439/reader_test_migrations"
        "?host=192.0.2.6&port=5432&dbname=reader"
    )

    with pytest.raises(ValueError, match="查询参数"):
        migration_engine(disguised_production_url)


def test_migration_cli_rejects_authority_multi_host_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    multi_host_url = (
        "postgresql+psycopg://reader:reader@192.0.2.6,127.0.0.1/reader_copy"
    )
    monkeypatch.setenv("READER_MIGRATION_DATABASE_URL", multi_host_url)
    monkeypatch.setenv("READER_MIGRATION_ALLOW_PRODUCTION", "1")

    with pytest.raises(ValueError, match="多主机"):
        database_url_from_environment()
