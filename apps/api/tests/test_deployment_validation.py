from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
from typing import Any
from unittest.mock import patch

import pytest

from reader_api.deployment_validation import (
    LEGACY_PRESERVED_TABLE_FIELDS,
    P02_PROJECTION_TABLES,
    P04_GENERATION_TABLES,
    PRESERVED_TABLES,
    PRODUCTION_AUTH_ENV,
    DatabaseSafetyError,
    EvidenceError,
    _write_json,
    build_database_snapshot,
    build_legacy_user_state_evidence,
    build_namespace_isolation,
    build_runtime_health,
    build_table_evidence,
    compare_database_manifests,
    compare_p03_upgrade_manifests,
    compare_p04_upgrade_manifests,
    database_target,
    validate_rehearsal_target,
)


PRODUCTION_URL = "postgresql+psycopg://reader:secret@postgres:5432/reader"
REHEARSAL_URL = (
    "postgresql+psycopg://reader:secret@postgres-p01:5432/"
    "reader_p01_rehearsal_issue15"
)
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def _race_evidence_writer(
    output: str,
    writer: str,
    barrier: Any,
    results: Any,
) -> None:
    output_path = Path(output).resolve()
    original_exists = Path.exists
    synchronized = False

    def synchronized_exists(candidate: Path) -> bool:
        nonlocal synchronized
        if candidate == output_path and not synchronized:
            synchronized = True
            barrier.wait(timeout=10)
            return False
        return original_exists(candidate)

    try:
        with patch.object(Path, "exists", synchronized_exists):
            _write_json(output_path, {"writer": writer})
    except Exception as exc:
        results.put(("error", writer, type(exc).__name__, str(exc)))
    else:
        results.put(("ok", writer, "", ""))


def _manifest(snapshot: dict[str, object]) -> dict[str, object]:
    return {
        "format": "reader-deployment-evidence/v1",
        "kind": "database-manifest",
        "snapshot": snapshot,
        "snapshot_sha256": hashlib.sha256(
            json.dumps(
                snapshot,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
    }


def _table_evidence(
    overrides: dict[str, dict[str, object]] | None = None,
    *,
    tables: tuple[str, ...] = PRESERVED_TABLES,
) -> dict[str, dict[str, object]]:
    evidence = {table: build_table_evidence(()) for table in tables}
    evidence.update(overrides or {})
    return evidence


def _legacy_state_evidence(
    *, storage: str = "user_states"
) -> dict[str, object]:
    return build_legacy_user_state_evidence((), storage=storage)


def _runtime_user_state_evidence(
    rows: tuple[tuple[object, str], ...] = (),
) -> dict[str, dict[str, object]]:
    return {"user_states": build_table_evidence(rows)}


def _legacy_preserved_evidence(
    overrides: dict[str, dict[str, object]] | None = None,
) -> dict[str, dict[str, object]]:
    evidence = {
        table: {
            "columns": list(columns),
            **build_table_evidence(()),
        }
        for table, columns in LEGACY_PRESERVED_TABLE_FIELDS.items()
    }
    evidence.update(overrides or {})
    return evidence


def _legacy_table_evidence(
    table: str,
    rows: tuple[tuple[object, str], ...] = (),
) -> dict[str, object]:
    return {
        "columns": list(LEGACY_PRESERVED_TABLE_FIELDS[table]),
        **build_table_evidence(rows),
    }


def _registered_snapshot(revision: str) -> dict[str, object]:
    counts = {
        table: 0
        for table in ("sources", "raw_entries", "documents", "content_items", "user_states")
    }
    snapshot = build_database_snapshot(
        counts=counts, raw_rows=(), alembic_revisions=(revision,)
    )
    snapshot["legacy_user_state_evidence"] = _legacy_state_evidence(
        storage="migration_baselines"
    )
    snapshot["legacy_preserved_table_evidence"] = _legacy_preserved_evidence()
    snapshot["preserved_table_evidence"] = _table_evidence()
    snapshot["p02_projection_evidence"] = _table_evidence(
        tables=P02_PROJECTION_TABLES
    )
    snapshot["runtime_user_state_evidence"] = _runtime_user_state_evidence()
    revision_ordinal = int(revision.split("_", 1)[0])
    if revision_ordinal >= 53:
        snapshot["p03_migration_evidence"] = {
            "artifact_tables": {
                table: build_table_evidence(())
                for table in (
                    "evidence_snapshots", "evidence_snapshot_members",
                    "synthesis_versions", "synthesis_blocks", "synthesis_citations",
                    "evidence_reviews", "evidence_review_citations",
                )
            },
            "event_pointer_counts": {
                "current_synthesis": 0, "reviewed_evidence": 0,
            },
            "event_pointer_evidence": build_table_evidence(()),
            "event_task_evidence": build_table_evidence(()),
        }
    if revision_ordinal >= 53:
        generation_tables_present = revision_ordinal >= 63
        generation_tables = _table_evidence(tables=P04_GENERATION_TABLES)
        if generation_tables_present:
            generation_tables["generation_controls"] = build_table_evidence(
                ((1, '{"id":1,"global_pause":true,"auto_run":false}'),)
            )
        source_policy_columns_present = revision_ordinal >= 55
        snapshot["p04_upgrade_evidence"] = {
            "llm_task_evidence": build_table_evidence(()),
            "generation_tables_present": generation_tables_present,
            "generation_tables": generation_tables,
            "generation_control_defaults": {
                "row_count": 1 if generation_tables_present else 0,
                "safe_default_count": 1 if generation_tables_present else 0,
            },
            "source_policy_columns_present": source_policy_columns_present,
            "source_policy_counts": {
                "source_count": 0,
                "unclassified_count": 0,
                "public_count": 0,
                "private_count": 0,
                "external_allowed_count": 0,
                "policy_version_one_count": 0,
            },
            "source_policy_evidence": build_table_evidence(()),
        }
    if revision_ordinal >= 72:
        snapshot["reading_body_contract_evidence"] = {
            "source_selectors_all_null_count": 0,
            "document_body_all_null_count": 0,
        }
    return snapshot


def test_production_database_and_host_require_explicit_maintenance_authorization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(PRODUCTION_AUTH_ENV, raising=False)

    with pytest.raises(DatabaseSafetyError, match="生产维护授权"):
        database_target(PRODUCTION_URL)

    monkeypatch.setenv(PRODUCTION_AUTH_ENV, "1")
    target = database_target(
        PRODUCTION_URL,
        production_maintenance=True,
        maintenance_id="issue-15-20260712",
    )

    assert target.database == "reader"
    assert target.production_authorized is True
    assert target.maintenance_id == "issue-15-20260712"


def test_database_target_rejects_connection_query_options() -> None:
    with pytest.raises(DatabaseSafetyError, match="查询参数"):
        database_target(
            REHEARSAL_URL + "?options=-c%20search_path%3Dshadow"
        )


@pytest.mark.parametrize(
    "database_url",
    (
        "postgresql+psycopg://reader:secret@postgres-p01:5432/reader_p01",
        "postgresql+psycopg://reader:secret@postgres-p01:5432/reader_rehearsal_issue15",
        "postgresql+psycopg://reader:secret@postgres-p01:5432/reader_p01_rehearsal_",
        "postgresql+psycopg://reader:secret@postgres-p01:5432/"
        "reader_p01_rehearsal_" + "a" * 64,
    ),
)
def test_rehearsal_guard_only_accepts_named_disposable_databases(
    database_url: str,
) -> None:
    with pytest.raises(
        DatabaseSafetyError,
        match="reader_p0[12]_rehearsal_|reader_test_",
    ):
        validate_rehearsal_target(database_target(database_url))


def test_rehearsal_guard_accepts_named_p02_database() -> None:
    target = database_target(
        "postgresql+psycopg://reader_p02:secret@postgres-p02:5432/"
        "reader_p02_rehearsal_issue34"
    )

    assert validate_rehearsal_target(target) is target


def test_shared_rehearsal_guard_rejects_p03_database() -> None:
    target = database_target(
        "postgresql+psycopg://reader_p03:secret@postgres-p03:5432/"
        "reader_p03_rehearsal_issue37"
    )

    with pytest.raises(DatabaseSafetyError, match="P0.3.*专属"):
        validate_rehearsal_target(target)


def test_production_authorization_never_makes_a_production_database_a_restore_target() -> None:
    target = database_target(
        "postgresql+psycopg://reader:secret@postgres-p01:5432/reader",
        production_maintenance=True,
        maintenance_id="issue-15-20260712",
        environ={PRODUCTION_AUTH_ENV: "1"},
    )

    with pytest.raises(DatabaseSafetyError, match="生产.*host|隔离"):
        validate_rehearsal_target(target)


@pytest.mark.parametrize(
    ("host", "canonical_host"),
    (
        ("postgres", "postgres"),
        ("reader-postgres", "reader-postgres"),
        ("postgres.", "postgres"),
        ("reader-postgres.", "reader-postgres"),
        ("postgres。", "postgres"),
        ("postgres．", "postgres"),
        ("postgres｡", "postgres"),
    ),
)
def test_production_authorization_never_turns_a_production_host_into_rehearsal(
    host: str,
    canonical_host: str,
) -> None:
    target = database_target(
        f"postgresql+psycopg://reader:secret@{host}:5432/"
        "reader_p01_rehearsal_issue15",
        production_maintenance=True,
        maintenance_id="issue-15-20260712",
        environ={PRODUCTION_AUTH_ENV: "1"},
    )
    assert target.host == canonical_host

    with pytest.raises(DatabaseSafetyError, match="生产.*host|隔离"):
        validate_rehearsal_target(target)


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
def test_deployment_evidence_rejects_ambiguous_numeric_host(host: str) -> None:
    with pytest.raises(DatabaseSafetyError, match="非标准数值"):
        database_target(
            f"postgresql+psycopg://reader:secret@{host}:5432/"
            "reader_p01_rehearsal_issue15"
        )


def test_deployment_evidence_requires_explicit_database_host() -> None:
    with pytest.raises(DatabaseSafetyError, match="显式包含 host"):
        database_target(
            "postgresql+psycopg:///reader_p01_rehearsal_issue15"
        )


def test_database_snapshot_is_deterministic_and_covers_all_legacy_raw_fields() -> None:
    counts = {
        "sources": 1,
        "raw_entries": 1,
        "documents": 1,
        "content_items": 2,
        "user_states": 3,
    }
    raw_rows = [
        {
            "id": 7,
            "source_id": 2,
            "external_id": "guid-7",
            "title": "标题",
            "url": "https://example.com/7",
            "author": "作者",
            "published_at": datetime(
                2026, 7, 12, 1, 2, 3, 456789, tzinfo=timezone.utc
            ),
            "fetched_at": datetime(
                2026, 7, 12, 1, 3, 4, 5, tzinfo=timezone.utc
            ),
            "raw_summary": "摘要\r\n保持原样",
            "raw_content": "正文",
            "content_hash": "a" * 64,
        }
    ]

    snapshot = build_database_snapshot(
        counts=counts,
        raw_rows=raw_rows,
        alembic_revisions=(),
    )

    assert snapshot["counts"] == counts
    assert snapshot["alembic_revisions"] == []
    raw_evidence = snapshot["raw_entry_legacy_evidence"]
    assert raw_evidence["columns"] == [
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
    ]
    assert raw_evidence["row_count"] == 1
    assert raw_evidence["ordered_rows_sha256"] == (
        "f0db16f154bcbb041b2f5f6ef35e1f6434a815119568e6835b00c017319f92ae"
    )
    assert raw_evidence["field_sha256"]["raw_summary"] == (
        "8e566ff327cf2b33f48c62a25590a6fab9c7b5b01160db9ba38ab871ea7f99ed"
    )


def test_manifest_compare_tracks_logical_user_state_across_event_cutover() -> None:
    before_snapshot = build_database_snapshot(
        counts={
            "sources": 0,
            "raw_entries": 0,
            "documents": 0,
            "content_items": 0,
            "user_states": 2,
        },
        raw_rows=(),
        alembic_revisions=("0014_identity_key_kind_unique",),
    )
    after_snapshot = build_database_snapshot(
        counts={
            "sources": 0,
            "raw_entries": 0,
            "documents": 0,
            "content_items": 0,
            "user_states": 1,
        },
        raw_rows=(),
        alembic_revisions=("0047_event_authority_contract",),
    )
    states = (
        {
            "id": 7,
            "object_type": "cluster",
            "object_id": 11,
            "read_status": "summary_seen",
            "read_later": True,
            "starred": False,
            "updated_at": datetime(2026, 7, 12, tzinfo=timezone.utc),
        },
        {
            "id": 8,
            "object_type": "item",
            "object_id": 12,
            "read_status": "unread",
            "read_later": False,
            "starred": True,
            "updated_at": datetime(2026, 7, 13, tzinfo=timezone.utc),
        },
    )
    before_snapshot["legacy_user_state_evidence"] = (
        build_legacy_user_state_evidence(states, storage="user_states")
    )
    after_snapshot["legacy_user_state_evidence"] = (
        build_legacy_user_state_evidence(states, storage="migration_baselines")
    )
    after_snapshot["p02_projection_evidence"] = _table_evidence(
        {
            "migration_baselines": build_table_evidence(
                (
                    (7, '{"legacy_user_state_id":7}'),
                    (8, '{"legacy_user_state_id":8}'),
                )
            )
        },
        tables=P02_PROJECTION_TABLES,
    )
    after_snapshot["runtime_user_state_evidence"] = (
        _runtime_user_state_evidence(
            ((8, '{"id":8,"object_type":"item","object_id":12}'),)
        )
    )
    before_snapshot["preserved_table_evidence"] = _table_evidence()
    after_snapshot["preserved_table_evidence"] = _table_evidence()
    before_snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )
    after_snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )

    assert compare_database_manifests(
        _manifest(before_snapshot), _manifest(after_snapshot)
    )["ok"] is True

    changed_after_snapshot = {
        **after_snapshot,
        "legacy_user_state_evidence": build_legacy_user_state_evidence(
            ({**states[0], "starred": True}, states[1]),
            storage="migration_baselines",
        ),
    }
    changed = compare_database_manifests(
        _manifest(before_snapshot), _manifest(changed_after_snapshot)
    )
    assert changed["ok"] is False
    assert any(
        mismatch["field"]
        == "legacy_user_state_evidence.field_sha256.starred"
        for mismatch in changed["mismatches"]
    )


def test_manifest_compare_detects_preserved_table_changes() -> None:
    counts = {
        "sources": 0,
        "raw_entries": 0,
        "documents": 0,
        "content_items": 0,
        "user_states": 0,
    }
    before_snapshot = build_database_snapshot(
        counts=counts,
        raw_rows=(),
        alembic_revisions=("0014_identity_key_kind_unique",),
    )
    after_snapshot = dict(before_snapshot)
    before_snapshot["legacy_user_state_evidence"] = _legacy_state_evidence()
    after_snapshot["legacy_user_state_evidence"] = _legacy_state_evidence()
    shared_cluster_evidence = _legacy_table_evidence(
        "clusters", ((1, '{"id":1,"title":"shared"}'),)
    )
    before_snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence({"clusters": shared_cluster_evidence})
    )
    after_snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence({"clusters": shared_cluster_evidence})
    )
    before_snapshot["preserved_table_evidence"] = _table_evidence(
        {
            "clusters": build_table_evidence(
                ((1, '{"id":1,"title":"before"}'),)
            )
        }
    )
    after_snapshot["preserved_table_evidence"] = _table_evidence(
        {
            "clusters": build_table_evidence(
                ((1, '{"id":1,"title":"after"}'),)
            )
        }
    )

    result = compare_database_manifests(
        _manifest(before_snapshot), _manifest(after_snapshot)
    )

    assert result["ok"] is False
    assert result["mismatches"][0]["field"].startswith(
        "preserved_table_evidence.clusters."
    )


def source_policy_transition_snapshot(
    revision: str,
    *,
    legacy_name: str = "Source",
    privacy_class: str | None = None,
) -> dict[str, object]:
    snapshot = _registered_snapshot(revision)
    counts = snapshot["counts"]
    assert isinstance(counts, dict)
    counts["sources"] = 1
    p04_evidence = snapshot["p04_upgrade_evidence"]
    assert isinstance(p04_evidence, dict)
    policy_counts = p04_evidence["source_policy_counts"]
    assert isinstance(policy_counts, dict)
    policy_counts["source_count"] = 1
    if privacy_class is not None:
        policy_counts[f"{privacy_class}_count"] = 1
        policy_counts["external_allowed_count"] = int(privacy_class == "public")
        policy_counts["policy_version_one_count"] = 1
        p04_evidence["source_policy_evidence"] = build_table_evidence(
            (
                (
                    1,
                    json.dumps(
                        {
                            "privacy_class": privacy_class,
                            "external_generation_allowed": privacy_class
                            == "public",
                            "generation_policy_version": 1,
                        }
                    ),
                ),
            )
        )
    legacy_evidence = snapshot["legacy_preserved_table_evidence"]
    preserved_evidence = snapshot["preserved_table_evidence"]
    assert isinstance(legacy_evidence, dict)
    assert isinstance(preserved_evidence, dict)
    legacy_evidence["sources"] = _legacy_table_evidence(
        "sources",
        ((1, json.dumps({"id": 1, "name": legacy_name})),),
    )
    source_row: dict[str, object] = {"id": 1, "name": legacy_name}
    if privacy_class is not None:
        source_row.update(
            {
                "privacy_class": privacy_class,
                "external_generation_allowed": privacy_class == "public",
                "generation_policy_version": 1,
            }
        )
    preserved_evidence["sources"] = build_table_evidence(
        ((1, json.dumps(source_row)),)
    )
    return snapshot


def test_manifest_compare_allows_only_default_0055_policy_through_0069() -> None:
    before = source_policy_transition_snapshot("0054_generation_lifecycle")
    after = source_policy_transition_snapshot(
        "0069_folder_media_types",
        privacy_class="unclassified",
    )

    assert compare_database_manifests(_manifest(before), _manifest(after)) == {
        "ok": True,
        "mismatches": [],
    }

    changed_legacy = source_policy_transition_snapshot(
        "0069_folder_media_types",
        legacy_name="Changed source",
        privacy_class="unclassified",
    )
    result = compare_database_manifests(_manifest(before), _manifest(changed_legacy))
    assert result["ok"] is False
    assert any(
        mismatch["field"].startswith(
            "legacy_preserved_table_evidence.sources."
        )
        for mismatch in result["mismatches"]
    )

    before_0068 = source_policy_transition_snapshot(
        "0068_cluster_current_projection",
        privacy_class="unclassified",
    )
    changed_0069 = source_policy_transition_snapshot(
        "0069_folder_media_types",
        legacy_name="Changed source",
        privacy_class="unclassified",
    )
    result = compare_database_manifests(_manifest(before_0068), _manifest(changed_0069))
    assert result["ok"] is False
    assert any(
        mismatch["field"].startswith(
            "legacy_preserved_table_evidence.sources."
        )
        for mismatch in result["mismatches"]
    )


def test_manifest_compare_fails_closed_for_later_source_policy_transitions() -> None:
    current = source_policy_transition_snapshot(
        "0055_source_generation_privacy",
        privacy_class="unclassified",
    )
    changed_current = source_policy_transition_snapshot(
        "0055_source_generation_privacy",
        privacy_class="public",
    )
    future = source_policy_transition_snapshot(
        "0069_folder_media_types",
        privacy_class="public",
    )
    pre_privacy = source_policy_transition_snapshot("0054_generation_lifecycle")

    for before, after in ((current, changed_current), (pre_privacy, future)):
        result = compare_database_manifests(_manifest(before), _manifest(after))
        assert result["ok"] is False
        assert any(
            mismatch["field"]
            == "preserved_table_evidence.sources.ordered_rows_sha256"
            for mismatch in result["mismatches"]
        )


def test_reading_body_upgrade_keeps_per_source_policy_identity() -> None:
    before = _registered_snapshot("0071_source_fetch_validators")
    after = _registered_snapshot("0072_reading_body_contract")
    for snapshot, policies in (
        (before, ("public", "private")),
        (after, ("private", "public")),
    ):
        counts = snapshot["counts"]
        p04 = snapshot["p04_upgrade_evidence"]
        legacy = snapshot["legacy_preserved_table_evidence"]
        preserved = snapshot["preserved_table_evidence"]
        assert isinstance(counts, dict)
        assert isinstance(p04, dict)
        assert isinstance(legacy, dict)
        assert isinstance(preserved, dict)
        counts["sources"] = 2
        p04["source_policy_counts"] = {
            "source_count": 2,
            "unclassified_count": 0,
            "public_count": 1,
            "private_count": 1,
            "external_allowed_count": 1,
            "policy_version_one_count": 2,
        }
        policy_rows = tuple(
            (
                source_id,
                json.dumps(
                    {
                        "privacy_class": privacy_class,
                        "external_generation_allowed": privacy_class == "public",
                        "generation_policy_version": 1,
                    }
                ),
            )
            for source_id, privacy_class in enumerate(policies, start=1)
        )
        p04["source_policy_evidence"] = build_table_evidence(policy_rows)
        legacy["sources"] = _legacy_table_evidence(
            "sources",
            tuple(
                (source_id, json.dumps({"id": source_id, "name": f"S{source_id}"}))
                for source_id in (1, 2)
            ),
        )
        preserved["sources"] = build_table_evidence(policy_rows)

    result = compare_database_manifests(_manifest(before), _manifest(after))

    assert result["ok"] is False
    assert any(
        mismatch["field"]
        == "p04_upgrade_evidence.source_policy_evidence.ordered_rows_sha256"
        for mismatch in result["mismatches"]
    )


def test_reading_body_upgrade_rejects_rewritten_source_selector() -> None:
    before = source_policy_transition_snapshot(
        "0071_source_fetch_validators",
        privacy_class="unclassified",
    )
    after = source_policy_transition_snapshot(
        "0072_reading_body_contract",
        privacy_class="unclassified",
    )
    body_evidence = after["reading_body_contract_evidence"]
    preserved = after["preserved_table_evidence"]
    assert isinstance(body_evidence, dict)
    assert isinstance(preserved, dict)
    body_evidence["source_selectors_all_null_count"] = 0
    preserved["sources"] = build_table_evidence(
        (
            (
                1,
                json.dumps(
                    {
                        "privacy_class": "unclassified",
                        "external_generation_allowed": False,
                        "generation_policy_version": 1,
                        "article_selector": "article",
                        "remove_selector": None,
                    }
                ),
            ),
        )
    )

    result = compare_database_manifests(_manifest(before), _manifest(after))

    assert result["ok"] is False
    assert any(
        mismatch["field"]
        == (
            "reading_body_contract_evidence."
            "source_selectors_all_null_count"
        )
        for mismatch in result["mismatches"]
    )


def test_reading_body_upgrade_rejects_rewritten_legacy_document() -> None:
    before = _registered_snapshot("0071_source_fetch_validators")
    after = _registered_snapshot("0072_reading_body_contract")
    for snapshot in (before, after):
        counts = snapshot["counts"]
        legacy = snapshot["legacy_preserved_table_evidence"]
        assert isinstance(counts, dict)
        assert isinstance(legacy, dict)
        counts["documents"] = 1
        legacy["documents"] = _legacy_table_evidence(
            "documents",
            ((1, json.dumps({"id": 1, "content_text": "RSS"})),),
        )
    before_preserved = before["preserved_table_evidence"]
    after_preserved = after["preserved_table_evidence"]
    body_evidence = after["reading_body_contract_evidence"]
    assert isinstance(before_preserved, dict)
    assert isinstance(after_preserved, dict)
    assert isinstance(body_evidence, dict)
    before_preserved["documents"] = build_table_evidence(
        ((1, json.dumps({"id": 1, "content_text": "RSS"})),)
    )
    after_preserved["documents"] = build_table_evidence(
        (
            (
                1,
                json.dumps(
                    {
                        "id": 1,
                        "content_text": "RSS",
                        "reading_html": "<p>RSS</p>",
                        "body_source": "rss",
                        "web_fetch_status": "not_requested",
                    }
                ),
            ),
        )
    )
    body_evidence["document_body_all_null_count"] = 0

    result = compare_database_manifests(_manifest(before), _manifest(after))

    assert result["ok"] is False
    assert any(
        mismatch["field"]
        == "reading_body_contract_evidence.document_body_all_null_count"
        for mismatch in result["mismatches"]
    )


def test_registered_manifest_compare_detects_raw_revision_metadata_changes() -> None:
    counts = {
        "sources": 0,
        "raw_entries": 1,
        "documents": 0,
        "content_items": 0,
        "user_states": 0,
    }
    raw_rows = (
        {
            "id": 1,
            "source_id": 7,
            "external_id": "entry-1",
            "title": "Entry",
            "url": "https://example.com/entry-1",
            "author": "Author",
            "published_at": datetime(2026, 7, 15, tzinfo=timezone.utc),
            "fetched_at": datetime(2026, 7, 15, tzinfo=timezone.utc),
            "raw_summary": "Summary",
            "raw_content": "Content",
            "content_hash": "a" * 64,
        },
    )
    before_snapshot = build_database_snapshot(
        counts=counts,
        raw_rows=raw_rows,
        alembic_revisions=("0014_identity_key_kind_unique",),
    )
    after_snapshot = build_database_snapshot(
        counts=counts,
        raw_rows=raw_rows,
        alembic_revisions=("0048_ambiguous_audit_fast_path",),
    )
    before_snapshot["legacy_user_state_evidence"] = _legacy_state_evidence()
    after_snapshot["legacy_user_state_evidence"] = _legacy_state_evidence(
        storage="migration_baselines"
    )
    before_snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )
    after_snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )
    before_snapshot["preserved_table_evidence"] = _table_evidence(
        {
            "raw_entries": build_table_evidence(
                ((1, '{"id":1,"source_entry_id":11,"revision_no":1}'),)
            )
        }
    )
    after_snapshot["preserved_table_evidence"] = _table_evidence(
        {
            "raw_entries": build_table_evidence(
                ((1, '{"id":1,"source_entry_id":99,"revision_no":1}'),)
            )
        }
    )
    after_snapshot["p02_projection_evidence"] = _table_evidence(
        tables=P02_PROJECTION_TABLES
    )
    after_snapshot["runtime_user_state_evidence"] = (
        _runtime_user_state_evidence()
    )

    result = compare_database_manifests(
        _manifest(before_snapshot), _manifest(after_snapshot)
    )

    assert result["ok"] is False
    assert result["mismatches"] == [
        {
            "field": (
                "preserved_table_evidence.raw_entries.ordered_rows_sha256"
            ),
            "before": before_snapshot["preserved_table_evidence"][
                "raw_entries"
            ]["ordered_rows_sha256"],
            "after": after_snapshot["preserved_table_evidence"][
                "raw_entries"
            ]["ordered_rows_sha256"],
        }
    ]


def test_manifest_compare_detects_legacy_common_field_changes() -> None:
    counts = {
        "sources": 0,
        "raw_entries": 0,
        "documents": 0,
        "content_items": 0,
        "user_states": 0,
    }
    before_snapshot = build_database_snapshot(
        counts=counts,
        raw_rows=(),
        alembic_revisions=(),
    )
    after_snapshot = dict(before_snapshot)
    before_snapshot["legacy_user_state_evidence"] = _legacy_state_evidence()
    after_snapshot["legacy_user_state_evidence"] = _legacy_state_evidence()
    before_snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence(
            {
                "clusters": _legacy_table_evidence(
                    "clusters", ((1, '{"id":1,"title":"before"}'),)
                )
            }
        )
    )
    after_snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence(
            {
                "clusters": _legacy_table_evidence(
                    "clusters", ((1, '{"id":1,"title":"after"}'),)
                )
            }
        )
    )

    result = compare_database_manifests(
        _manifest(before_snapshot), _manifest(after_snapshot)
    )

    assert result["ok"] is False
    assert result["mismatches"] == [
        {
            "field": (
                "legacy_preserved_table_evidence.clusters."
                "ordered_rows_sha256"
            ),
            "before": before_snapshot["legacy_preserved_table_evidence"][
                "clusters"
            ]["ordered_rows_sha256"],
            "after": after_snapshot["legacy_preserved_table_evidence"][
                "clusters"
            ]["ordered_rows_sha256"],
        }
    ]


def test_pre_event_authority_manifest_keeps_cluster_user_states() -> None:
    cluster_state = {
        "id": 7,
        "object_type": "cluster",
        "object_id": 11,
        "read_status": "summary_seen",
        "read_later": False,
        "starred": True,
        "updated_at": datetime(2026, 7, 15, tzinfo=timezone.utc),
    }
    snapshot = build_database_snapshot(
        counts={
            "sources": 0,
            "raw_entries": 0,
            "documents": 0,
            "content_items": 0,
            "user_states": 1,
        },
        raw_rows=(),
        alembic_revisions=("0046_event_read_indexes",),
    )
    snapshot["legacy_user_state_evidence"] = build_legacy_user_state_evidence(
        (cluster_state,), storage="migration_baselines"
    )
    snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )
    snapshot["preserved_table_evidence"] = _table_evidence()
    snapshot["p02_projection_evidence"] = _table_evidence(
        {
            "migration_baselines": build_table_evidence(
                ((7, '{"legacy_user_state_id":7}'),)
            )
        },
        tables=P02_PROJECTION_TABLES,
    )

    assert compare_database_manifests(
        _manifest(snapshot), _manifest(snapshot)
    ) == {"ok": True, "mismatches": []}

    post_cutover = {
        **snapshot,
        "alembic_revisions": ["0047_event_authority_contract"],
        "runtime_user_state_evidence": _runtime_user_state_evidence(
            ((7, '{"id":7,"object_type":"cluster"}'),)
        ),
    }
    assert compare_database_manifests(
        _manifest(post_cutover), _manifest(post_cutover)
    ) == {"ok": True, "mismatches": []}

    item_baseline = {
        **cluster_state,
        "object_type": "item",
    }
    missing_runtime_projection = {
        **post_cutover,
        "counts": {**post_cutover["counts"], "user_states": 0},
        "runtime_user_state_evidence": _runtime_user_state_evidence(),
        "legacy_user_state_evidence": build_legacy_user_state_evidence(
            (item_baseline,), storage="migration_baselines"
        ),
    }
    with pytest.raises(EvidenceError, match="少于当前 Alembic 阶段 baseline"):
        compare_database_manifests(
            _manifest(missing_runtime_projection),
            _manifest(missing_runtime_projection),
        )


def test_post_event_authority_manifest_allows_new_runtime_user_states() -> None:
    baseline_state = {
        "id": 7,
        "object_type": "item",
        "object_id": 11,
        "read_status": "summary_seen",
        "read_later": False,
        "starred": True,
        "updated_at": datetime(2026, 7, 15, tzinfo=timezone.utc),
    }
    snapshot = build_database_snapshot(
        counts={
            "sources": 0,
            "raw_entries": 0,
            "documents": 0,
            "content_items": 0,
            "user_states": 2,
        },
        raw_rows=(),
        alembic_revisions=("0048_ambiguous_audit_fast_path",),
    )
    snapshot["legacy_user_state_evidence"] = build_legacy_user_state_evidence(
        (baseline_state,), storage="migration_baselines"
    )
    snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )
    snapshot["preserved_table_evidence"] = _table_evidence()
    snapshot["p02_projection_evidence"] = _table_evidence(
        {
            "migration_baselines": build_table_evidence(
                ((7, '{"legacy_user_state_id":7}'),)
            )
        },
        tables=P02_PROJECTION_TABLES,
    )
    snapshot["runtime_user_state_evidence"] = _runtime_user_state_evidence(
        (
            (7, '{"id":7,"read_status":"summary_seen"}'),
            (8, '{"id":8,"read_status":"unread"}'),
        )
    )

    assert compare_database_manifests(
        _manifest(snapshot), _manifest(snapshot)
    ) == {"ok": True, "mismatches": []}


@pytest.mark.parametrize("table", P02_PROJECTION_TABLES)
def test_post_event_authority_compare_rejects_projection_mutation(
    table: str,
) -> None:
    snapshot = build_database_snapshot(
        counts={
            "sources": 0,
            "raw_entries": 0,
            "documents": 0,
            "content_items": 0,
            "user_states": 0,
        },
        raw_rows=(),
        alembic_revisions=("0048_ambiguous_audit_fast_path",),
    )
    snapshot["legacy_user_state_evidence"] = _legacy_state_evidence(
        storage="migration_baselines"
    )
    snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )
    snapshot["preserved_table_evidence"] = _table_evidence()
    projection = _table_evidence(tables=P02_PROJECTION_TABLES)
    snapshot["p02_projection_evidence"] = projection
    snapshot["runtime_user_state_evidence"] = _runtime_user_state_evidence()
    changed_projection = {
        name: dict(evidence) for name, evidence in projection.items()
    }
    changed_projection[table]["ordered_rows_sha256"] = "b" * 64
    changed = {
        **snapshot,
        "p02_projection_evidence": changed_projection,
    }

    result = compare_database_manifests(
        _manifest(snapshot), _manifest(changed)
    )

    assert result == {
        "ok": False,
        "mismatches": [
            {
                "field": (
                    f"p02_projection_evidence.{table}.ordered_rows_sha256"
                ),
                "before": projection[table]["ordered_rows_sha256"],
                "after": "b" * 64,
            }
        ],
    }


def test_post_event_authority_compare_rejects_runtime_user_state_count_change() -> None:
    snapshot = build_database_snapshot(
        counts={
            "sources": 0,
            "raw_entries": 0,
            "documents": 0,
            "content_items": 0,
            "user_states": 2,
        },
        raw_rows=(),
        alembic_revisions=("0048_ambiguous_audit_fast_path",),
    )
    snapshot["legacy_user_state_evidence"] = _legacy_state_evidence(
        storage="migration_baselines"
    )
    snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )
    snapshot["preserved_table_evidence"] = _table_evidence()
    snapshot["p02_projection_evidence"] = _table_evidence(
        tables=P02_PROJECTION_TABLES
    )
    snapshot["runtime_user_state_evidence"] = _runtime_user_state_evidence(
        (
            (7, '{"id":7,"read_status":"summary_seen"}'),
            (8, '{"id":8,"read_status":"unread"}'),
        )
    )
    changed = {
        **snapshot,
        "counts": {**snapshot["counts"], "user_states": 3},
        "runtime_user_state_evidence": _runtime_user_state_evidence(
            (
                (7, '{"id":7,"read_status":"summary_seen"}'),
                (8, '{"id":8,"read_status":"unread"}'),
                (9, '{"id":9,"read_status":"unread"}'),
            )
        ),
    }

    result = compare_database_manifests(
        _manifest(snapshot), _manifest(changed)
    )

    assert result["ok"] is False
    assert result["mismatches"][0] == {
        "field": "counts.user_states",
        "before": 2,
        "after": 3,
    }


def test_post_event_authority_compare_rejects_runtime_user_state_mutation() -> None:
    snapshot = build_database_snapshot(
        counts={
            "sources": 0,
            "raw_entries": 0,
            "documents": 0,
            "content_items": 0,
            "user_states": 1,
        },
        raw_rows=(),
        alembic_revisions=("0048_ambiguous_audit_fast_path",),
    )
    snapshot["legacy_user_state_evidence"] = _legacy_state_evidence(
        storage="migration_baselines"
    )
    snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )
    snapshot["preserved_table_evidence"] = _table_evidence()
    snapshot["p02_projection_evidence"] = _table_evidence(
        tables=P02_PROJECTION_TABLES
    )
    snapshot["runtime_user_state_evidence"] = _runtime_user_state_evidence(
        ((7, '{"id":7,"starred":false}'),)
    )
    changed = {
        **snapshot,
        "runtime_user_state_evidence": _runtime_user_state_evidence(
            ((7, '{"id":7,"starred":true}'),)
        ),
    }

    result = compare_database_manifests(
        _manifest(snapshot), _manifest(changed)
    )

    assert result == {
        "ok": False,
        "mismatches": [
            {
                "field": (
                    "runtime_user_state_evidence.user_states."
                    "ordered_rows_sha256"
                ),
                "before": snapshot["runtime_user_state_evidence"][
                    "user_states"
                ]["ordered_rows_sha256"],
                "after": changed["runtime_user_state_evidence"][
                    "user_states"
                ]["ordered_rows_sha256"],
            }
        ],
    }


def test_manifest_compare_rejects_interactions_fabricated_by_migration() -> None:
    counts = {
        "sources": 0,
        "raw_entries": 0,
        "documents": 0,
        "content_items": 0,
        "user_states": 0,
    }
    before_snapshot = build_database_snapshot(
        counts=counts,
        raw_rows=(),
        alembic_revisions=("0014_identity_key_kind_unique",),
    )
    after_snapshot = build_database_snapshot(
        counts=counts,
        raw_rows=(),
        alembic_revisions=("0048_ambiguous_audit_fast_path",),
    )
    before_snapshot["legacy_user_state_evidence"] = _legacy_state_evidence()
    after_snapshot["legacy_user_state_evidence"] = _legacy_state_evidence(
        storage="migration_baselines"
    )
    before_snapshot["preserved_table_evidence"] = _table_evidence()
    after_snapshot["preserved_table_evidence"] = _table_evidence()
    before_snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )
    after_snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )
    after_snapshot["p02_projection_evidence"] = _table_evidence(
        {
            "interaction_events": build_table_evidence(
                (("fabricated", '{"id":"fabricated"}'),)
            )
        },
        tables=P02_PROJECTION_TABLES,
    )
    after_snapshot["runtime_user_state_evidence"] = (
        _runtime_user_state_evidence()
    )

    result = compare_database_manifests(
        _manifest(before_snapshot), _manifest(after_snapshot)
    )

    assert result["ok"] is False
    assert result["mismatches"] == [
        {
            "field": "p02_projection_evidence.interaction_events.row_count",
            "before": 0,
            "after": 1,
        },
        {
            "field": (
                "p02_projection_evidence.interaction_events."
                "ordered_rows_sha256"
            ),
            "before": hashlib.sha256(b"").hexdigest(),
            "after": after_snapshot["p02_projection_evidence"][
                "interaction_events"
            ]["ordered_rows_sha256"],
        },
    ]


def test_registered_manifests_require_stage_specific_evidence() -> None:
    counts = {
        "sources": 0,
        "raw_entries": 0,
        "documents": 0,
        "content_items": 0,
        "user_states": 0,
    }
    incomplete_p01 = build_database_snapshot(
        counts=counts,
        raw_rows=(),
        alembic_revisions=("0014_identity_key_kind_unique",),
    )
    incomplete_p02 = build_database_snapshot(
        counts=counts,
        raw_rows=(),
        alembic_revisions=("0048_ambiguous_audit_fast_path",),
    )
    incomplete_p01["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )
    incomplete_p02["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )

    with pytest.raises(EvidenceError, match="UserState"):
        compare_database_manifests(
            _manifest(incomplete_p01), _manifest(incomplete_p02)
        )

    incomplete_p01["legacy_user_state_evidence"] = _legacy_state_evidence()
    with pytest.raises(EvidenceError, match="preserved_table_evidence"):
        compare_database_manifests(
            _manifest(incomplete_p01), _manifest(incomplete_p02)
        )

    incomplete_p01["preserved_table_evidence"] = _table_evidence()
    incomplete_p02["legacy_user_state_evidence"] = _legacy_state_evidence(
        storage="migration_baselines"
    )
    incomplete_p02["preserved_table_evidence"] = _table_evidence()
    with pytest.raises(EvidenceError, match="p02_projection_evidence"):
        compare_database_manifests(
            _manifest(incomplete_p01), _manifest(incomplete_p02)
        )

    incomplete_p02["p02_projection_evidence"] = _table_evidence(
        tables=P02_PROJECTION_TABLES
    )
    with pytest.raises(EvidenceError, match="runtime_user_state_evidence"):
        compare_database_manifests(
            _manifest(incomplete_p01), _manifest(incomplete_p02)
        )


def test_p03_upgrade_compare_rejects_derived_artifacts_created_by_migration() -> None:
    before = _registered_snapshot("0048_ambiguous_audit_fast_path")
    after = _registered_snapshot("0053_evidence_reviews")
    after["p03_migration_evidence"]["artifact_tables"]["evidence_snapshots"] = (
        build_table_evidence(((1, '{"id":1}'),))
    )
    after["p03_migration_evidence"]["event_pointer_counts"]["current_synthesis"] = 1
    result = compare_database_manifests(_manifest(before), _manifest(after))
    assert result["ok"] is False
    assert {mismatch["field"] for mismatch in result["mismatches"]} >= {
        "p03_migration_evidence.artifact_tables.evidence_snapshots.row_count",
        "p03_migration_evidence.event_pointer_counts.current_synthesis",
    }


def test_p03_upgrade_compare_requires_exact_p02_and_p03_heads() -> None:
    before = _registered_snapshot("0047_event_authority_contract")
    after = _registered_snapshot("0053_evidence_reviews")
    result = compare_p03_upgrade_manifests(_manifest(before), _manifest(after))
    assert result["ok"] is False
    assert result["mismatches"][-1]["field"] == "alembic_revisions.before_p02_head"


def test_p03_upgrade_does_not_compare_pointer_rows_absent_from_old_schema() -> None:
    before = _registered_snapshot("0048_ambiguous_audit_fast_path")
    after = _registered_snapshot("0053_evidence_reviews")
    after_p03 = after["p03_migration_evidence"]
    assert isinstance(after_p03, dict)
    after_p03["event_pointer_evidence"] = build_table_evidence(
        ((1, '{"current_synthesis_version_id":null,"reviewed_evidence_review_id":null}'),)
    )

    assert compare_p03_upgrade_manifests(_manifest(before), _manifest(after)) == {
        "ok": True,
        "mismatches": [],
    }


def test_p04_upgrade_compare_accepts_only_empty_generation_facts_and_safe_defaults() -> None:
    before = _registered_snapshot("0053_evidence_reviews")
    after = _registered_snapshot("0066_content_filter_projection")

    assert compare_p04_upgrade_manifests(_manifest(before), _manifest(after)) == {
        "ok": True,
        "mismatches": [],
    }


def test_p04_upgrade_compare_rejects_generated_rows_or_unsafe_defaults() -> None:
    before = _registered_snapshot("0053_evidence_reviews")
    after = _registered_snapshot("0066_content_filter_projection")
    evidence = after["p04_upgrade_evidence"]
    assert isinstance(evidence, dict)
    generation_tables = evidence["generation_tables"]
    assert isinstance(generation_tables, dict)
    generation_tables["generation_requests"] = build_table_evidence(
        ((1, '{"id":1}'),)
    )
    control = evidence["generation_control_defaults"]
    assert isinstance(control, dict)
    control["safe_default_count"] = 0

    result = compare_p04_upgrade_manifests(_manifest(before), _manifest(after))

    assert result["ok"] is False
    assert {mismatch["field"] for mismatch in result["mismatches"]} >= {
        "p04_upgrade_evidence.generation_tables.generation_requests.row_count",
        "p04_upgrade_evidence.generation_control_defaults.safe_default_count",
    }


def test_p04_upgrade_compare_rejects_swapped_event_artifact_pointers() -> None:
    before = _registered_snapshot("0053_evidence_reviews")
    after = _registered_snapshot("0066_content_filter_projection")
    before_p03 = before["p03_migration_evidence"]
    after_p03 = after["p03_migration_evidence"]
    assert isinstance(before_p03, dict)
    assert isinstance(after_p03, dict)
    before_p03["event_pointer_counts"] = {
        "current_synthesis": 2,
        "reviewed_evidence": 2,
    }
    after_p03["event_pointer_counts"] = {
        "current_synthesis": 2,
        "reviewed_evidence": 2,
    }
    before_p03["event_pointer_evidence"] = build_table_evidence(
        ((1, '{"current_synthesis_version_id":10,"reviewed_evidence_review_id":20}'),)
    )
    after_p03["event_pointer_evidence"] = build_table_evidence(
        ((1, '{"current_synthesis_version_id":20,"reviewed_evidence_review_id":10}'),)
    )

    result = compare_p04_upgrade_manifests(_manifest(before), _manifest(after))

    assert result["ok"] is False
    assert any(
        mismatch["field"]
        == "p03_migration_evidence.event_pointer_evidence.ordered_rows_sha256"
        for mismatch in result["mismatches"]
    )


def test_p04_upgrade_compare_requires_exact_p03_and_p04_heads() -> None:
    before = _registered_snapshot("0054_generation_lifecycle")
    after = _registered_snapshot("0066_content_filter_projection")

    result = compare_p04_upgrade_manifests(_manifest(before), _manifest(after))

    assert result["ok"] is False
    assert any(
        mismatch["field"] == "alembic_revisions.before_p03_head"
        for mismatch in result["mismatches"]
    )


def test_unregistered_manifest_rejects_current_schema_evidence() -> None:
    snapshot = build_database_snapshot(
        counts={
            "sources": 0,
            "raw_entries": 0,
            "documents": 0,
            "content_items": 0,
            "user_states": 0,
        },
        raw_rows=(),
        alembic_revisions=(),
    )
    snapshot["legacy_user_state_evidence"] = _legacy_state_evidence()
    snapshot["legacy_preserved_table_evidence"] = (
        _legacy_preserved_evidence()
    )
    unexpected_after = {
        **snapshot,
        "preserved_table_evidence": _table_evidence(),
    }

    with pytest.raises(EvidenceError, match="Alembic 阶段"):
        compare_database_manifests(
            _manifest(snapshot), _manifest(unexpected_after)
        )


def test_manifest_comparison_ignores_revision_change_but_rejects_evidence_change() -> None:
    counts = {
        "sources": 1,
        "raw_entries": 0,
        "documents": 1,
        "content_items": 1,
        "user_states": 1,
    }
    before_snapshot = build_database_snapshot(
        counts=counts,
        raw_rows=[],
        alembic_revisions=(),
    )
    after_snapshot = {
        **before_snapshot,
        "alembic_revisions": ["0014_identity_key_kind_unique"],
    }
    state = {
        "id": 1,
        "object_type": "item",
        "object_id": 1,
        "read_status": "unread",
        "read_later": False,
        "starred": False,
        "updated_at": datetime(2026, 7, 15, tzinfo=timezone.utc),
    }
    before_snapshot["legacy_user_state_evidence"] = (
        build_legacy_user_state_evidence((state,), storage="user_states")
    )
    after_snapshot["legacy_user_state_evidence"] = (
        before_snapshot["legacy_user_state_evidence"]
    )
    after_snapshot["preserved_table_evidence"] = _table_evidence(
        {
            "sources": build_table_evidence(((1, '{"id":1}'),)),
            "documents": build_table_evidence(((1, '{"id":1}'),)),
            "content_items": build_table_evidence(((1, '{"id":1}'),)),
        }
    )
    shared_evidence = _legacy_preserved_evidence(
        {
            "sources": _legacy_table_evidence(
                "sources", ((1, '{"id":1}'),)
            ),
            "documents": _legacy_table_evidence(
                "documents", ((1, '{"id":1}'),)
            ),
            "content_items": _legacy_table_evidence(
                "content_items", ((1, '{"id":1}'),)
            ),
        }
    )
    before_snapshot["legacy_preserved_table_evidence"] = shared_evidence
    after_snapshot["legacy_preserved_table_evidence"] = shared_evidence

    successful = compare_database_manifests(
        _manifest(before_snapshot), _manifest(after_snapshot)
    )
    assert successful == {"ok": True, "mismatches": []}

    changed_snapshot = {
        **after_snapshot,
        "raw_entry_legacy_evidence": {
            **after_snapshot["raw_entry_legacy_evidence"],
            "ordered_rows_sha256": "b" * 64,
        },
    }
    failed = compare_database_manifests(
        _manifest(before_snapshot), _manifest(changed_snapshot)
    )

    assert failed["ok"] is False
    assert failed["mismatches"] == [
        {
            "field": "raw_entry_legacy_evidence.ordered_rows_sha256",
            "before": hashlib.sha256(b"").hexdigest(),
            "after": "b" * 64,
        }
    ]


def test_manifest_comparison_rejects_incomplete_evidence_even_when_hash_matches() -> None:
    incomplete_snapshot = {
        "counts": {
            "sources": 0,
            "raw_entries": 0,
            "documents": 0,
            "content_items": 0,
            "user_states": 0,
        },
        "alembic_revisions": [],
        "raw_entry_legacy_evidence": {
            "columns": ["id"],
            "row_count": 0,
            "ordered_rows_sha256": hashlib.sha256(b"").hexdigest(),
            "field_sha256": {"id": hashlib.sha256(b"").hexdigest()},
        },
    }

    with pytest.raises(EvidenceError, match="columns"):
        compare_database_manifests(
            _manifest(incomplete_snapshot), _manifest(incomplete_snapshot)
        )


def test_evidence_publish_is_atomic_when_two_processes_target_same_path(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("fork")
    barrier = context.Barrier(2)
    results = context.Queue()
    output = tmp_path / "manifest.json"
    processes = [
        context.Process(
            target=_race_evidence_writer,
            args=(str(output), writer, barrier, results),
        )
        for writer in ("first", "second")
    ]

    started: list[multiprocessing.Process] = []
    try:
        for process in processes:
            process.start()
            started.append(process)
        for process in started:
            process.join(timeout=15)
        assert not [process for process in started if process.is_alive()]
        assert [process.exitcode for process in started] == [0, 0]
        outcomes = [results.get(timeout=5) for _ in started]
    finally:
        for process in started:
            if process.is_alive():
                process.terminate()
        for process in started:
            process.join(timeout=5)
            if process.is_alive():
                process.kill()
                process.join(timeout=5)
        results.close()
        results.join_thread()

    successful = [outcome for outcome in outcomes if outcome[0] == "ok"]
    rejected = [outcome for outcome in outcomes if outcome[0] == "error"]

    assert len(successful) == 1
    assert len(rejected) == 1
    assert rejected[0][2] == "EvidenceError"
    assert "拒绝覆盖既有证据文件" in rejected[0][3]
    assert json.loads(output.read_text(encoding="utf-8")) == {
        "writer": successful[0][1]
    }
    assert output.stat().st_mode & 0o777 == 0o600
    assert list(tmp_path.glob(".manifest.json.*.tmp")) == []


def test_runtime_health_requires_head_and_one_worker_for_each_isolated_queue() -> None:
    healthy = build_runtime_health(
        database_probe={
            "ping": True,
            "alembic_revisions": ["0014_identity_key_kind_unique"],
            "code_head_revisions": ["0014_identity_key_kind_unique"],
        },
        redis_probe={
            "ping": True,
            "registered_queues": ["reader-p01-fetch", "reader-p01-llm"],
            "queue_depths": {"reader-p01-fetch": 0, "reader-p01-llm": 2},
            "workers": [
                {"name": "fetch@p01", "queue_names": ["reader-p01-fetch"]},
                {"name": "llm@p01", "queue_names": ["reader-p01-llm"]},
            ],
        },
        queue_prefix="reader-p01",
    )

    assert healthy["ok"] is True
    assert healthy["expected_queue_names"] == ["reader-p01-fetch", "reader-p01-llm"]
    assert healthy["queue_workers"] == {
        "reader-p01-fetch": ["fetch@p01"],
        "reader-p01-llm": ["llm@p01"],
    }

    unhealthy = build_runtime_health(
        database_probe=healthy["database"],
        redis_probe={
            "ping": True,
            "registered_queues": ["reader-p01-fetch", "reader-fetch"],
            "queue_depths": {},
            "workers": [
                {"name": "fetch@p01", "queue_names": ["reader-p01-fetch"]}
            ],
        },
        queue_prefix="reader-p01",
    )

    assert unhealthy["ok"] is False
    assert unhealthy["missing_queues"] == ["reader-p01-llm"]
    assert unhealthy["queues_without_workers"] == ["reader-p01-llm"]
    assert unhealthy["unexpected_reader_queues"] == ["reader-fetch"]


def test_runtime_health_accepts_authorized_main_queue_prefix() -> None:
    health = build_runtime_health(
        database_probe={
            "ping": True,
            "alembic_revisions": ["0014_identity_key_kind_unique"],
            "code_head_revisions": ["0014_identity_key_kind_unique"],
        },
        redis_probe={
            "ping": True,
            "registered_queues": ["reader-fetch", "reader-llm"],
            "queue_depths": {"reader-fetch": 0, "reader-llm": 0},
            "workers": [
                {"name": "fetch@main", "queue_names": ["reader-fetch"]},
                {"name": "llm@main", "queue_names": ["reader-llm"]},
            ],
        },
        queue_prefix="reader",
    )

    assert health["ok"] is True
    assert health["expected_queue_names"] == ["reader-fetch", "reader-llm"]


def test_namespace_isolation_requires_distinct_database_redis_and_queues() -> None:
    stable = {
        "kind": "runtime-health", "ok": True,
        "database": {"observed_database": {
            "database": "reader", "server_address": "172.20.0.2", "server_port": 5432,
        }},
        "redis": {"identity": {
            "host": "redis", "port": 6379, "database": 0, "run_id": "stable-run",
        }},
        "expected_queue_names": ["reader-fetch", "reader-llm"],
        "observed_queue_names": ["reader-fetch", "reader-llm"],
    }
    p03 = {
        "kind": "runtime-health", "ok": True,
        "database": {"observed_database": {
            "database": "reader_p03", "server_address": "172.21.0.2", "server_port": 5432,
        }},
        "redis": {"identity": {
            "host": "redis-p03", "port": 6379, "database": 0, "run_id": "p03-run",
        }},
        "expected_queue_names": ["reader-p03-fetch", "reader-p03-llm"],
        "observed_queue_names": ["reader-p03-fetch", "reader-p03-llm"],
    }
    isolated = build_namespace_isolation(stable, p03)
    shared_redis = build_namespace_isolation(stable, {**p03, "redis": stable["redis"]})
    aliased_same_server = build_namespace_isolation(stable, {
        **p03,
        "redis": {"identity": {
            "host": "redis-p03-alias", "port": 6380, "database": 9,
            "run_id": "stable-run",
        }},
    })
    assert isolated["ok"] is True
    assert all(isolated["checks"].values())
    assert shared_redis["ok"] is False
    assert shared_redis["checks"]["redis"] is False
    assert aliased_same_server["ok"] is False
    assert aliased_same_server["checks"]["redis"] is False


def test_namespace_isolation_rejects_missing_redis_run_id() -> None:
    stable = {
        "kind": "runtime-health", "ok": True,
        "database": {"observed_database": {
            "database": "reader", "server_address": "1", "server_port": 5432,
        }},
        "redis": {"identity": {"host": "redis", "port": 6379, "database": 0}},
        "expected_queue_names": ["reader-fetch", "reader-llm"],
        "observed_queue_names": ["reader-fetch", "reader-llm"],
    }
    p03 = {
        "kind": "runtime-health", "ok": True,
        "database": {"observed_database": {
            "database": "reader_p03", "server_address": "2", "server_port": 5432,
        }},
        "redis": {"identity": {
            "host": "redis-p03", "port": 6379, "database": 0, "run_id": "p03",
        }},
        "expected_queue_names": ["reader-p03-fetch", "reader-p03-llm"],
        "observed_queue_names": ["reader-p03-fetch", "reader-p03-llm"],
    }

    with pytest.raises(EvidenceError, match="run_id"):
        build_namespace_isolation(stable, p03)


def test_runtime_health_accepts_empty_queue_proven_by_healthy_worker() -> None:
    health = build_runtime_health(
        database_probe={
            "ping": True,
            "alembic_revisions": ["0014_identity_key_kind_unique"],
            "code_head_revisions": ["0014_identity_key_kind_unique"],
        },
        redis_probe={
            "ping": True,
            "registered_queues": ["reader-p01-fetch"],
            "queue_depths": {"reader-p01-fetch": 0},
            "workers": [
                {
                    "name": "fetch@p01",
                    "queue_names": ["reader-p01-fetch"],
                    "healthy": True,
                },
                {
                    "name": "llm@p01",
                    "queue_names": ["reader-p01-llm"],
                    "healthy": True,
                },
            ],
        },
        queue_prefix="reader-p01",
    )

    assert health["ok"] is True
    assert health["redis"]["registered_queues"] == ["reader-p01-fetch"]
    assert health["observed_queue_names"] == [
        "reader-p01-fetch",
        "reader-p01-llm",
    ]
    assert health["missing_queues"] == []


def test_runtime_health_reports_stale_expected_worker_as_unavailable_not_cross_namespace() -> None:
    health = build_runtime_health(
        database_probe={
            "ping": True,
            "alembic_revisions": ["0014_identity_key_kind_unique"],
            "code_head_revisions": ["0014_identity_key_kind_unique"],
        },
        redis_probe={
            "ping": True,
            "registered_queues": ["reader-p01-fetch", "reader-p01-llm"],
            "workers": [
                {
                    "name": "stale-fetch@p01",
                    "queue_names": ["reader-p01-fetch"],
                    "healthy": False,
                },
                {
                    "name": "llm@p01",
                    "queue_names": ["reader-p01-llm"],
                    "healthy": True,
                },
            ],
        },
        queue_prefix="reader-p01",
    )

    assert health["ok"] is False
    assert health["queues_without_workers"] == ["reader-p01-fetch"]
    assert health["unexpected_worker_queues"] == []


def test_runtime_health_does_not_count_stale_worker_registration_as_healthy() -> None:
    health = build_runtime_health(
        database_probe={
            "ping": True,
            "alembic_revisions": ["head"],
            "code_head_revisions": ["head"],
        },
        redis_probe={
            "ping": True,
            "registered_queues": ["reader-p01-fetch", "reader-p01-llm"],
            "queue_depths": {},
            "workers": [
                {
                    "name": "fetch@stale",
                    "queue_names": ["reader-p01-fetch"],
                    "healthy": False,
                },
                {
                    "name": "llm@p01",
                    "queue_names": ["reader-p01-llm"],
                    "healthy": True,
                },
            ],
        },
        queue_prefix="reader-p01",
    )

    assert health["ok"] is False
    assert health["queues_without_workers"] == ["reader-p01-fetch"]


def test_cli_exposes_unraid_suitable_evidence_commands_and_rejects_unsafe_url() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "apps/api")
    help_result = subprocess.run(
        [sys.executable, "-m", "reader_api.deployment_validation", "--help"],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )

    assert help_result.returncode == 0
    for command in (
        "manifest",
        "activity",
        "compare",
        "runtime-health",
        "namespace-isolation",
    ):
        assert command in help_result.stdout

    environment["READER_DEPLOYMENT_DATABASE_URL"] = PRODUCTION_URL
    environment.pop(PRODUCTION_AUTH_ENV, None)
    rejected = subprocess.run(
        [
            sys.executable,
            "-m",
            "reader_api.deployment_validation",
            "manifest",
            "--output",
            "/tmp/reader-unsafe-manifest-must-not-exist.json",
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )

    assert rejected.returncode != 0
    assert "生产维护授权" in rejected.stderr
    assert not Path("/tmp/reader-unsafe-manifest-must-not-exist.json").exists()


@pytest.mark.parametrize(
    "target_requirement",
    ((), ("--require-rehearsal-target",)),
)
@pytest.mark.parametrize(
    "database_url",
    (
        "postgresql+psycopg://reader_p03:secret@127.0.0.1:1/"
        "reader_p02_rehearsal_issue37",
        "postgresql+psycopg://reader_p03:secret@postgres-p02:5432/"
        "reader_p03_rehearsal_issue37",
        "postgresql+psycopg://reader_p02:secret@postgres-p03:5432/"
        "reader_p03_rehearsal_issue37",
        "postgresql+psycopg://reader_p03:secret@127.0.0.1:55441/"
        "reader_p03_rehearsal_issue37",
    ),
)
def test_p03_evidence_cli_locks_every_target_to_p03(
    tmp_path: Path,
    target_requirement: tuple[str, ...],
    database_url: str,
) -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "apps/api")
    environment["READER_DEPLOYMENT_DATABASE_URL"] = database_url
    output = tmp_path / "must-not-exist.json"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "reader_api.p03_deployment_validation",
            "manifest",
            *target_requirement,
            "--output",
            str(output),
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "P0.3" in result.stderr or "reader_p03" in result.stderr
    assert "connection refused" not in result.stderr.lower()
    assert not output.exists()


def test_p03_evidence_cli_cannot_use_production_authorization(
    tmp_path: Path,
) -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "apps/api")
    environment["READER_DEPLOYMENT_DATABASE_URL"] = PRODUCTION_URL
    environment[PRODUCTION_AUTH_ENV] = "1"
    output = tmp_path / "must-not-exist.json"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "reader_api.p03_deployment_validation",
            "manifest",
            "--production-maintenance",
            "--maintenance-id",
            "issue37-production-bypass",
            "--output",
            str(output),
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "P0.3" in result.stderr
    assert "connection" not in result.stderr.lower()
    assert not output.exists()


@pytest.mark.parametrize(
    ("database", "extra_args"),
    (
        ("reader_p03", ()),
        ("reader_p03_rehearsal_issue37", ("--require-rehearsal-target",)),
        ("reader_p03_shadow", ()),
    ),
)
def test_shared_evidence_cli_cannot_bypass_p03_entrypoint(
    tmp_path: Path,
    database: str,
    extra_args: tuple[str, ...],
) -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "apps/api")
    environment["READER_DEPLOYMENT_DATABASE_URL"] = (
        f"postgresql+psycopg://reader_p03:secret@postgres-p03:5432/{database}"
    )
    output = tmp_path / "must-not-exist.json"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "reader_api.deployment_validation",
            "manifest",
            *extra_args,
            "--output",
            str(output),
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "P0.3" in result.stderr and "专属" in result.stderr
    assert "connection" not in result.stderr.lower()
    assert not output.exists()


def test_cli_sanitizes_unexpected_connection_errors(
    tmp_path: Path,
) -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "apps/api")
    secret = "must-not-appear-in-evidence"
    environment["READER_DEPLOYMENT_DATABASE_URL"] = (
        f"postgresql+psycopg://reader_test:{secret}@127.0.0.1:1/"
        "reader_test_unreachable"
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "reader_api.deployment_validation",
            "manifest",
            "--require-rehearsal-target",
            "--output",
            str(tmp_path / "must-not-exist.json"),
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert secret not in result.stdout + result.stderr
    assert "Traceback" not in result.stderr
    assert "command-error" in result.stderr
    assert not (tmp_path / "must-not-exist.json").exists()


def test_cli_rejects_connection_environment_override_options(
    tmp_path: Path,
) -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "apps/api")
    for command, option_name in (
        ("manifest", "--database-url-env"),
        ("runtime-health", "--database-url-env"),
        ("runtime-health", "--redis-url-env"),
    ):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "reader_api.deployment_validation",
                command,
                option_name,
                "ISSUE15_CONNECTION_URL",
                "--output",
                str(tmp_path / f"{command}.json"),
            ],
            cwd=REPOSITORY_ROOT,
            env=environment,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 2
        assert "unrecognized arguments" in result.stderr
