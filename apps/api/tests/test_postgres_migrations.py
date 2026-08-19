from __future__ import annotations

from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
from threading import Barrier, Event
import time
from unittest.mock import patch
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, event, func, inspect, select, text, true
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError, IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, aliased, sessionmaker

from reader_api.migrations.alembic_config import (
    BASELINE_REVISION,
    PRODUCTION_LEGACY_REVISION,
    code_head_revisions,
    make_alembic_config,
)
from reader_api.migrations.__main__ import upgrade_database
from reader_api.migrations.preflight import preflight_legacy_database, stamp_legacy_database
from reader_api.migrations.folder_type_prepare import prepare_folder_media_types
from reader_api.migrations.runtime import SchemaRevisionError, assert_database_at_head
from reader_api.migrations.schema_contract import (
    EXPLICIT_FOLDER_MEDIA_TYPES,
    EXPLICIT_FOLDER_UNIQUES,
    EXPLICIT_SOURCE_FOLDER_FOREIGN_KEY,
    compare_legacy_schema,
    read_postgres_schema,
)
from reader_api.migrations.testing import validate_isolated_test_database_url
from reader_api import event_projection as event_projection_module
from reader_api.cluster import (
    assign_cluster as assign_runtime_cluster,
    cluster_source_items,
    decluster_source_items,
)
from reader_api.ai_runtime import runtime_ai_settings
from reader_api.bulk_read import confirm_bulk_read_manifest
from reader_api.clustering_run import (
    clustering_run,
    clustering_run_execution_lock,
)
from reader_api.event_interactions import (
    apply_event_user_state_mutation,
    lock_operation_id,
)
from reader_api.event_synthesis import (
    EVIDENCE_REVIEW_POLICY_VERSION,
    SYNTHESIS_OUTPUT_SCHEMA,
    SYNTHESIS_SYSTEM_PROMPT,
    create_evidence_snapshot,
    evidence_review_input_data,
    event_synthesis_freshness_for,
    event_synthesis_state,
    save_evidence_review,
    save_synthesis_version,
    synthesis_generation_fingerprint,
    synthesis_input_data,
    synthesis_model_request,
    synthesis_task_snapshot_from_request,
)
from reader_api.generation_lifecycle import (
    approve_generation_request,
    current_generation_control,
    external_generation_policy,
    fail_generation_attempt,
    get_or_create_generation_request,
    locked_generation_control,
    start_generation_attempt,
    stable_hash,
)
from reader_api.event_projection import (
    cluster_event_identities_for,
    event_material_updates_for,
    project_completed_clustering_run,
)
from reader_api.object_interactions import apply_object_user_state_mutation
from reader_api.maintenance import (
    GENERATION_RETENTION,
    REBUILD_PROJECTIONS,
    run_explicit_maintenance,
)
from reader_api.projection_rebuild import inspect_projection_rebuild, rebuild_projections
from reader_api.digest import content_hash
from reader_api.db import get_session
from reader_api.deployment_validation import (
    collect_database_manifest,
    compare_database_manifests,
    database_target,
)
from reader_api.models import (
    AppSetting,
    Cluster,
    ClusterCurrentEventProjection,
    ClusterItem,
    ClusteringRun,
    ClusteringRunMembership,
    ClusteringRunProjectionPredecessor,
    ClusteringRunScopeEvidence,
    ClusterEventProjection,
    ContentItem,
    Document,
    EvidenceSnapshot,
    EvidenceSnapshotMember,
    EvidenceReview,
    EvidenceReviewCitation,
    Event as ReaderEvent,
    EventEvidence,
    EventEvidenceVersion,
    EventLineage,
    EventRevision,
    EventRevisionEvidence,
    EventUserState,
    FeedMetric,
    GenerationApplication,
    GenerationAdmission,
    GenerationAttempt,
    GenerationAttemptRunnerAudit,
    GenerationControl,
    GenerationRequest,
    GenerationRequestPayload,
    GenerationResult,
    GenerationRunnerPresence,
    InteractionEvent,
    LLMTask,
    MigrationBaseline,
    RawEntry,
    Source,
    SourceEntryIdentity,
    SourceEntryKey,
    SynthesisBlock,
    SynthesisCitation,
    SynthesisVersion,
    UserState,
)
from reader_api.source_entry_revision import (
    RawEntryRevisionInput,
    RawEntryRevisionOutcome,
    allocate_raw_entry_revision,
    calculate_payload_fingerprint,
    raw_entry_revision_values,
)
from reader_api.source_entry_identity import (
    SourceEntryIdentityInput,
    SourceEntryResolutionOutcome,
    resolve_source_entry_identity,
)
from reader_api.source_ingest import (
    IngestEntry,
    ingest_source_entries,
)
from reader_api.rss import fetch_source
from reader_api.main import (
    app,
    apply_source_update,
    bulk_update_sources,
    create_source,
    delete_source,
    generate_event_synthesis,
    report_bounds,
    report_key,
    update_ai_settings,
)
from reader_api.opml import import_opml
from reader_api.translations import ensure_translation
from reader_api.schemas import (
    AISettingsPatch,
    BulkReadManifest,
    BulkReadTarget,
    EventUserStateMutationIn,
    SourceBulkPatch,
    SourceCreate,
    SourcePatch,
    SynthesisGenerateIn,
    UserStatePatch,
)
from tests.factories import (
    INVALID_SOURCE_ENTRY_KEYS,
    POSTGRES_TEST_VECTOR,
    add_raw_revision_seed,
    assign_publishable_cluster as assign_cluster,
    legacy_source_entry_key,
    make_raw_entry,
    make_revision_input,
)


@pytest.fixture(autouse=True)
def support_runtime_helpers_on_historical_postgres_heads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_query = event_projection_module.latest_live_cluster_projection

    def compatible_latest_projection(
        session: Session,
        cluster_ids: set[int] | None = None,
    ):
        if session.scalar(
            select(func.to_regclass("cluster_current_event_projections"))
        ) is not None:
            return runtime_query(session, cluster_ids)

        projection = aliased(ClusterEventProjection)
        latest = (
            select(projection.id.label("projection_id"))
            .where(projection.cluster_id == Cluster.id)
            .order_by(projection.id.desc())
            .limit(1)
            .lateral("latest_cluster_event_projection")
        )
        statement = (
            select(
                Cluster.id.label("cluster_id"),
                latest.c.projection_id,
            )
            .select_from(Cluster)
            .join(latest, true())
        )
        if cluster_ids is not None:
            statement = statement.where(Cluster.id.in_(cluster_ids))
        return statement.subquery()

    monkeypatch.setattr(
        "reader_api.clustering_run.latest_live_cluster_projection",
        compatible_latest_projection,
    )


pytestmark = pytest.mark.postgres
LEGACY_SCHEMA_SQL = Path(__file__).parent / "fixtures" / "pre_alembic_legacy_schema.sql"
MutationStep = str | tuple[str, Mapping[str, object]]
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
STRICT_LEGACY_NULLABILITY_COLUMNS = (
    ("clusters", "generated_content"),
    ("content_items", "media_url"),
    ("content_items", "media_kind"),
    ("content_items", "embedding_model"),
)
PRODUCTION_LEGACY_NOT_NULL_COLUMNS = (
    ("clusters", "generated_content"),
    ("content_items", "embedding_model"),
)


@dataclass(frozen=True)
class EventProjectionFixture:
    cluster_id: int
    event_id: int
    revision_id: int
    evidence_id: int
    version_id: int
    raw_id: int
    source_entry_id: int
    source_id: int
    item_id: int
    mapping_id: int
    fragment_fingerprint: str
RUNTIME_STARTUP_PROBE = """
import os
import sys

expected_head = os.environ.get("READER_TEST_EXPECTED_HEAD", "")
if expected_head:
    import reader_api.migrations.runtime as runtime
    runtime.code_head_revisions = lambda: (expected_head,)

target = sys.argv[1]
if target == "api":
    from fastapi.testclient import TestClient
    from reader_api.main import app
    with TestClient(app):
        pass
else:
    import reader_api.worker as worker

    class StubRedis:
        @classmethod
        def from_url(cls, _url):
            return object()

    class StubWorker:
        def __init__(self, *_args, **_kwargs):
            pass

        def work(self):
            pass

    worker.Redis = StubRedis
    worker.Worker = StubWorker
    worker.start_fetch_scheduler = lambda: None
    worker.run_worker(target)
"""


@pytest.fixture()
def postgres_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL", "")
    if not url:
        pytest.skip("设置 TEST_DATABASE_URL 才运行真实 PostgreSQL migration 测试")
    if os.environ.get("ALLOW_DESTRUCTIVE_TEST_DATABASE") != "1":
        pytest.skip("设置 ALLOW_DESTRUCTIVE_TEST_DATABASE=1 才允许重建隔离测试 schema")
    return validate_isolated_test_database_url(url)


def insert_pre_privacy_source(
    session: Session | Connection,
    *,
    name: str,
    url: str,
    created_at: datetime | None = None,
) -> int:
    """Seed a Source through the schema shared by revisions before 0055."""

    return int(
        session.scalar(
            text(
                "INSERT INTO sources "
                "(name, url, site_url, status, media_type, enabled, "
                "fetch_full_content, feed_trust_score, last_error, created_at) "
                "VALUES (:name, :url, '', 'active', 'article', true, false, "
                "0, '', :created_at) RETURNING id"
            ),
            {
                "name": name,
                "url": url,
                "created_at": created_at or datetime.now(timezone.utc),
            },
        )
    )


def add_pre_privacy_raw_revision_seed(
    session: Session,
    *,
    revision: RawEntryRevisionInput,
    payload_fingerprint: str,
) -> tuple[SourceEntryIdentity, RawEntry]:
    source_id = insert_pre_privacy_source(
        session,
        name="Revision source",
        url="https://example.com/revision.xml",
    )
    identity = SourceEntryIdentity(source_id=source_id, current_revision_no=1)
    session.add(identity)
    session.flush()
    session.add(
        SourceEntryKey(
            source_entry_id=identity.id,
            source_id=source_id,
            identity_kind="legacy",
            identity_key=legacy_source_entry_key(revision.external_id),
        )
    )
    raw_values = raw_entry_revision_values(
        source_id=source_id,
        source_entry_id=identity.id,
        revision_no=1,
        revision=revision,
    )
    raw_values["payload_fingerprint"] = payload_fingerprint
    raw = RawEntry(**raw_values)
    session.add(raw)
    session.flush()
    return identity, raw


def add_valid_v2_synthesis_task_at_0050(
    engine: Engine,
    *,
    status: str = "pending",
) -> dict[str, object]:
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        items: list[ContentItem] = []
        for index in range(2):
            source_id = insert_pre_privacy_source(
                session,
                name=f"Migration synthesis source {index}",
                url=f"https://example.com/migration-synthesis-{index}.xml",
            )
            raw = make_raw_entry(
                source_id=source_id,
                external_id=f"migration-synthesis-{index}",
                title=f"Migration synthesis evidence {index}",
                url=f"https://example.com/migration-synthesis-{index}",
                raw_content=f"Migration evidence body {index}",
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                document_type="normal_article",
                title=raw.title,
                content_text=raw.raw_content,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source_id,
                title=raw.title,
                content_text=raw.raw_content,
                url=raw.url,
                canonical_url=raw.url,
                content_hash=raw.content_hash,
                embedding_vector=POSTGRES_TEST_VECTOR,
                embedding_model="migration-synthesis-model",
            )
            session.add(item)
            session.flush()
            items.append(item)
        with clustering_run(
            session,
            scope_type="migration-synthesis-v2-task",
            item_ids=[item.id for item in items],
            rule_version="migration-synthesis-v2-task-v1",
        ):
            for item in items:
                assign_cluster(session, item)
        event = session.scalar(select(ReaderEvent).order_by(ReaderEvent.id.desc()))
        assert event is not None
        revision = session.get(EventRevision, event.current_revision_id)
        assert revision is not None
        # Revision 0050 predates FilterRule/FilterMatch.  This historical
        # migration fixture must not compile the current-head visibility clause.
        with patch(
            "reader_api.event_synthesis.unfiltered_content_clause",
            lambda _content_item_id: true(),
        ):
            snapshot, evidence, _source_count, _current_fingerprint = (
                create_evidence_snapshot(session, event, revision)
            )
        historical_contract = importlib.import_module(
            "reader_api.alembic.versions.0052_synthesis_provenance_audit"
        )
        fingerprint = stable_hash(
            {
                "source_coverage_fingerprint": snapshot.source_coverage_fingerprint,
                "content_fingerprint": snapshot.content_fingerprint,
                "policy_version": historical_contract.SYNTHESIS_POLICY_VERSION,
                "prompt_version": historical_contract.SYNTHESIS_PROMPT_VERSION,
                "schema_version": historical_contract.SYNTHESIS_SCHEMA_VERSION,
            }
        )
        input_data = {
            "event_uid": event.uid,
            "target_revision_uid": revision.uid,
            "snapshot_uid": snapshot.uid,
            "policy_version": historical_contract.SYNTHESIS_POLICY_VERSION,
            "prompt_version": historical_contract.SYNTHESIS_PROMPT_VERSION,
            "schema_version": historical_contract.SYNTHESIS_SCHEMA_VERSION,
            "source_coverage_fingerprint": snapshot.source_coverage_fingerprint,
            "content_fingerprint": snapshot.content_fingerprint,
            "generation_fingerprint": fingerprint,
            "evidence": evidence,
        }
        input_text = json.dumps(
            input_data, ensure_ascii=False, separators=(",", ":")
        )
        result_json = json.dumps(
            {
                "request": {
                    "system_prompt": historical_contract.SYNTHESIS_SYSTEM_PROMPT,
                    "input": input_text,
                    "input_data": input_data,
                    "output_schema": historical_contract.SYNTHESIS_OUTPUT_SCHEMA,
                }
            },
            ensure_ascii=False,
        )
        task_id = session.scalar(
            text(
                "INSERT INTO llm_tasks "
                "(task_type, provider, object_type, object_id, status, "
                " prompt_version, model_version, result_json, created_at, updated_at) "
                "VALUES ('event-synthesis', 'legacy', 'event', :event_id, :status, "
                "'event-synthesis-prompt-v1', 'migration-model', :result_json, "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id"
            ),
            {
                "event_id": event.id,
                "status": status,
                "result_json": result_json,
            },
        )
        assert task_id is not None
        session.commit()
        return {
            "task_id": task_id,
            "event_id": event.id,
            "status": status,
            "result_json": result_json,
            "input_data": input_data,
            "fingerprint": fingerprint,
        }


@pytest.fixture(autouse=True)
def reset_postgres_schema(postgres_url: str) -> Iterator[None]:
    engine = create_engine(postgres_url)

    def reset() -> None:
        with engine.begin() as connection:
            username = connection.scalar(text("SELECT current_user"))
            user_schema = connection.dialect.identifier_preparer.quote(username)
            connection.execute(text(f"DROP SCHEMA IF EXISTS {user_schema} CASCADE"))
            connection.execute(text("DROP EXTENSION IF EXISTS vector CASCADE"))
            connection.execute(
                text("DROP SCHEMA IF EXISTS foreign_key_shadow CASCADE")
            )
            connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
            connection.execute(text("CREATE SCHEMA public"))

    reset()
    try:
        yield None
    finally:
        reset()
        engine.dispose()


def upgrade_baseline(postgres_url: str) -> None:
    command.upgrade(make_alembic_config(postgres_url), "head")


def test_deleted_source_url_can_be_reused_but_live_duplicates_are_rejected(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)

    with Session(engine) as session:
        original = Source(
            name="Original source",
            url="https://example.com/reusable-source.xml",
            media_type="social",
            privacy_class="public",
            external_generation_allowed=True,
        )
        session.add(original)
        session.commit()
        original_id = original.id

        session.add(
            Source(
                name="Live duplicate",
                url="https://example.com/reusable-source.xml",
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()

        assert delete_source(original_id, session).status_code == 204
        deleted_source = session.get(Source, original_id)
        assert deleted_source is not None
        assert deleted_source.status == "deleted"
        assert deleted_source.enabled is False
        assert deleted_source.external_generation_allowed is False
        assert deleted_source.generation_policy_version == 2

        replacement = Source(
            name="Replacement source",
            url="https://example.com/reusable-source.xml",
        )
        session.add(replacement)
        session.commit()
        assert replacement.id != original_id

    engine.dispose()


def test_ai_model_setting_boundary_matches_postgres_artifacts(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        lengths = {
            row.table_name: row.character_maximum_length
            for row in connection.execute(
                text(
                    "SELECT table_name, character_maximum_length "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND "
                    "(table_name, column_name) IN "
                    "(('llm_tasks', 'model_version'), "
                    " ('synthesis_versions', 'model'))"
                )
            )
        }
    assert lengths == {"llm_tasks": 120, "synthesis_versions": 120}

    with Session(engine) as session:
        for payload in (
            AISettingsPatch(llm_model="m" * 121),
            AISettingsPatch(
                synthesis_provider="openai_compatible",
                synthesis_remote_base_url="https://synthesis.example/v1",
                synthesis_remote_model="m" * 121,
                synthesis_remote_api_key="synthesis-secret",
            ),
        ):
            with pytest.raises(HTTPException) as error:
                update_ai_settings(payload, session)
            assert error.value.status_code == 400

        accepted = update_ai_settings(
            AISettingsPatch(
                llm_model="l" * 120,
                synthesis_provider="openai_compatible",
                synthesis_remote_base_url="https://synthesis.example/v1",
                synthesis_remote_model="s" * 120,
                synthesis_remote_api_key="synthesis-secret",
            ),
            session,
        )
        assert len(accepted.llm_model) == 120
        assert len(accepted.synthesis_remote_model) == 120
        stored = {
            key: value
            for key, value in session.execute(
                select(AppSetting.key, AppSetting.value).where(
                    AppSetting.key.in_(("llm_model", "synthesis_remote_model"))
                )
            )
        }
        assert stored == {
            "llm_model": "l" * 120,
            "synthesis_remote_model": "s" * 120,
        }
        for key in ("llm_model", "synthesis_remote_model"):
            setting = session.get(AppSetting, key)
            assert setting is not None
            setting.value = "m" * 121
        session.commit()
        session.info.pop("runtime_ai_settings", None)

        historical = runtime_ai_settings(session)
        assert historical.llm_model != "m" * 121
        assert historical.synthesis_remote_model == ""
    engine.dispose()


def test_event_synthesis_migration_preserves_p02_facts_and_does_not_backfill(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    with Session(engine) as session:
        add_user_state_baseline_seed(session)
    command.upgrade(config, "0048_ambiguous_audit_fast_path")

    with Session(engine) as session:
        projection = session.scalar(select(ClusterEventProjection).limit(1))
        cluster = session.scalar(select(Cluster).order_by(Cluster.id).limit(1))
        assert projection is not None and cluster is not None
        cluster.generated_title = "Legacy generated title must remain"
        cluster.generated_summary = "Legacy generated summary must remain"
        cluster.generated_content = "Legacy generated content must remain"
        cluster.citations = '[{"url":"https://example.com/legacy"}]'
        cluster.model_version = "legacy-model"
        cluster.prompt_version = "legacy-prompt"
        session.add(
            InteractionEvent(
                id="49000000-0000-4000-8000-000000000001",
                operation_id="synthesis-migration-preserved-interaction",
                target_kind="event",
                event_id=projection.event_id,
                observed_revision_id=projection.event_revision_id,
                action="starred_set",
                set_value=True,
                payload={"metric_source_ids": []},
                occurred_at=datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc),
                recorded_at=datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc),
            )
        )
        session.commit()
        session.execute(
            text(
                "INSERT INTO llm_tasks "
                "(task_type, provider, object_type, object_id, status, "
                " prompt_version, model_version, result_json, created_at, updated_at) "
                "VALUES ('cluster-synthesis', 'legacy', 'cluster', :cluster_id, "
                "'complete', 'legacy-prompt', 'legacy-model', :result_json, "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            ),
            {"cluster_id": cluster.id, "result_json": '{"legacy":true}'},
        )
        session.commit()

    target = database_target(postgres_url)
    before = collect_database_manifest(target)

    def llm_task_snapshot() -> list[str]:
        with engine.connect() as connection:
            return list(
                connection.scalars(
                    text(
                        "SELECT to_jsonb(snapshot_row)::text FROM "
                        "(SELECT id, task_type, provider, object_type, object_id, "
                        " status, prompt_version, model_version, result_json, "
                        " created_at, updated_at FROM llm_tasks ORDER BY id) "
                        "snapshot_row"
                    )
                )
            )

    llm_before = llm_task_snapshot()
    with engine.connect() as connection:
        event_facts_before = list(
            connection.scalars(
                text(
                    "SELECT to_jsonb(snapshot_row)::text FROM "
                    "(SELECT id, uid, status, current_revision_id, created_at, "
                    " superseded_at FROM events ORDER BY id) snapshot_row"
                )
            )
        )
    command.upgrade(config, "0049_event_synthesis")
    command.upgrade(config, "0050_synthesis_source_count")
    command.upgrade(config, "0051_fingerprint_reuse")
    # This regression proves the P0.3 synthesis/lifecycle migrations do not
    # rewrite preserved facts.  Stop at their final revision, 0054.  The
    # following 0055 adds defaulted source policy columns, which changes a
    # whole-row source manifest without changing the preserved source data.
    command.upgrade(config, "0054_generation_lifecycle")
    after = collect_database_manifest(target)
    assert compare_database_manifests(before, after) == {
        "ok": True,
        "mismatches": [],
    }
    assert (
        before["snapshot"]["legacy_preserved_table_evidence"]
        == after["snapshot"]["legacy_preserved_table_evidence"]
    )
    assert (
        before["snapshot"]["preserved_table_evidence"]
        == after["snapshot"]["preserved_table_evidence"]
    )
    before_p02 = before["snapshot"]["p02_projection_evidence"]
    after_p02 = after["snapshot"]["p02_projection_evidence"]
    for table_name in set(before_p02) - {"events"}:
        assert before_p02[table_name] == after_p02[table_name]
    assert llm_task_snapshot() == llm_before
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0054_generation_lifecycle"
        )
        for table_name in (
            "evidence_snapshots",
            "evidence_snapshot_members",
            "synthesis_versions",
            "synthesis_blocks",
            "synthesis_citations",
            "evidence_reviews",
            "evidence_review_citations",
        ):
            assert connection.scalar(text(f"SELECT count(*) FROM {table_name}")) == 0
        assert (
            connection.scalar(
                text(
                    "SELECT count(*) FROM events "
                    "WHERE current_synthesis_version_id IS NOT NULL"
                )
            )
            == 0
        )
        assert (
            connection.scalar(
                text(
                    "SELECT count(*) FROM events "
                    "WHERE reviewed_evidence_review_id IS NOT NULL"
                )
            )
            == 0
        )
        assert (
            list(
                connection.scalars(
                    text(
                        "SELECT to_jsonb(snapshot_row)::text FROM "
                        "(SELECT id, uid, status, current_revision_id, created_at, "
                        " superseded_at FROM events ORDER BY id) snapshot_row"
                    )
                )
            )
            == event_facts_before
        )

    command.upgrade(config, "0054_generation_lifecycle")
    repeated = collect_database_manifest(target)
    assert repeated["snapshot"] == after["snapshot"]
    assert llm_task_snapshot() == llm_before
    engine.dispose()


def test_p03_manifest_ignores_user_schema_shadow_tables(postgres_url: str) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        username = connection.scalar(text("SELECT current_user"))
        schema = connection.dialect.identifier_preparer.quote(username)
        connection.execute(text(f"CREATE SCHEMA {schema} AUTHORIZATION CURRENT_USER"))
        connection.execute(text(
            f"CREATE TABLE {schema}.events "
            "(id bigint, current_synthesis_version_id bigint, "
            "reviewed_evidence_review_id bigint)"
        ))
        connection.execute(text(
            f"CREATE TABLE {schema}.llm_tasks (id bigint, task_type text)"
        ))
        connection.execute(text(
            f"INSERT INTO {schema}.llm_tasks VALUES (1, 'event-synthesis')"
        ))
        connection.execute(text(f"CREATE TABLE {schema}.sources (id bigint)"))
        connection.execute(text(f"INSERT INTO {schema}.sources VALUES (1)"))
        for table_name in (
            "evidence_snapshots",
            "evidence_snapshot_members",
            "synthesis_versions",
            "synthesis_blocks",
            "synthesis_citations",
            "evidence_reviews",
            "evidence_review_citations",
        ):
            connection.execute(text(
                f"CREATE TABLE {schema}.{table_name} (id bigint)"
            ))
            connection.execute(text(
                f"INSERT INTO {schema}.{table_name} VALUES (1)"
            ))

    manifest = collect_database_manifest(database_target(postgres_url))
    snapshot = manifest["snapshot"]
    with engine.connect() as connection:
        public_event_count = connection.scalar(text(
            "SELECT count(*) FROM public.events"
        ))
    assert (
        snapshot["p02_projection_evidence"]["events"]["row_count"]
        == public_event_count
    )
    assert snapshot["preserved_table_evidence"]["sources"]["row_count"] == 0
    assert snapshot["p03_migration_evidence"]["event_task_evidence"]["row_count"] == 0
    assert {
        table: evidence["row_count"]
        for table, evidence in snapshot["p03_migration_evidence"][
            "artifact_tables"
        ].items()
    } == {
        "evidence_snapshots": 0,
        "evidence_snapshot_members": 0,
        "synthesis_versions": 0,
        "synthesis_blocks": 0,
        "synthesis_citations": 0,
        "evidence_reviews": 0,
        "evidence_review_citations": 0,
    }
    engine.dispose()


def test_fingerprint_reuse_migration_backfills_and_guards_active_tasks(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0050_synthesis_source_count")
    fingerprint = "3" * 64
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        task_id = connection.scalar(
            text(
                "INSERT INTO llm_tasks "
                "(task_type, provider, object_type, object_id, status, "
                " prompt_version, model_version, result_json, created_at, updated_at) "
                "VALUES ('event-synthesis', 'legacy', 'event', 39, 'pending', "
                "'v1', 'm1', CAST(:result_json AS text), CURRENT_TIMESTAMP, "
                "CURRENT_TIMESTAMP) RETURNING id"
            ),
            {
                "result_json": (
                    '{"request":{"input_data":{"policy_version":'
                    '"event-synthesis-policy-v1","generation_fingerprint":"'
                    + fingerprint
                    + '"}}}'
                )
            },
        )
        assert task_id is not None
    engine.dispose()

    with pytest.raises(DBAPIError, match="active_synthesis_task_policy_mismatch"):
        command.upgrade(config, "head")
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0050_synthesis_source_count"
        )
        assert "input_fingerprint" not in {
            column["name"] for column in inspect(connection).get_columns("llm_tasks")
        }
        assert connection.execute(
            text(
                "SELECT status, result_json FROM llm_tasks WHERE id = :id"
            ),
            {"id": task_id},
        ).one() == (
            "pending",
            '{"request":{"input_data":{"policy_version":'
            '"event-synthesis-policy-v1","generation_fingerprint":"'
            + fingerprint
            + '"}}}',
        )
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE llm_tasks SET status = 'complete' WHERE id = :id"),
            {"id": task_id},
        )
    valid_task = add_valid_v2_synthesis_task_at_0050(engine)
    engine.dispose()

    command.upgrade(config, "head")
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        assert connection.scalar(
            text("SELECT input_fingerprint FROM llm_tasks WHERE id = :id"),
            {"id": task_id},
        ) == fingerprint
        assert connection.scalar(
            text("SELECT status FROM llm_tasks WHERE id = :id"),
            {"id": task_id},
        ) == "complete"
        assert connection.execute(
            text(
                "SELECT object_id, provider, status, prompt_version, model_version, "
                "result_json, input_fingerprint FROM llm_tasks WHERE id = :id"
            ),
            {"id": valid_task["task_id"]},
        ).one() == (
            valid_task["event_id"],
            "legacy",
            "pending",
            "event-synthesis-prompt-v1",
            "migration-model",
            valid_task["result_json"],
            valid_task["fingerprint"],
        )
        constraints = {
            constraint["name"]
            for table_name in ("synthesis_versions", "llm_tasks")
            for constraint in inspect(connection).get_unique_constraints(table_name)
        }
        assert "uq_synthesis_version_generation_fingerprint" in constraints
        indexes = {
            index["name"]: index
            for index in inspect(connection).get_indexes("llm_tasks")
        }
        assert indexes["uq_llm_task_active_input_fingerprint"]["unique"] is True

    with Session(engine) as session:
        session.add(
            LLMTask(
                task_type="evidence-review",
                provider="legacy",
                object_type="event",
                object_id=39,
                status="pending",
                input_fingerprint=None,
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        active = LLMTask(
            task_type="event-synthesis",
            provider="legacy",
            object_type="event",
            object_id=39,
            status="pending",
            input_fingerprint=fingerprint,
        )
        session.add(active)
        session.commit()
        active_id = active.id
        session.add(
            LLMTask(
                task_type="event-synthesis",
                provider="local",
                object_type="event",
                object_id=39,
                status="running",
                input_fingerprint=fingerprint,
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()

        active = session.get(LLMTask, active_id)
        assert active is not None
        active.status = "complete"
        session.commit()
        session.add(
            LLMTask(
                task_type="event-synthesis",
                provider="local",
                object_type="event",
                object_id=39,
                status="running",
                input_fingerprint=fingerprint,
            )
        )
        session.commit()

        session.add(
            LLMTask(
                task_type="evidence-comparison",
                provider="legacy",
                object_type="event",
                object_id=40,
                status="pending",
                input_fingerprint=fingerprint,
            )
        )
        session.commit()
        session.add(
            LLMTask(
                task_type="evidence-comparison",
                provider="local",
                object_type="event",
                object_id=40,
                status="running",
                input_fingerprint=fingerprint,
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()

        session.add(
            LLMTask(
                task_type="item-summary",
                provider="local",
                object_type="item",
                object_id=1,
                status="complete",
                input_fingerprint="not-a-sha256",
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()

    barrier = Barrier(2)

    def insert_same_comparison(provider: str) -> str:
        with SessionLocal() as session:
            session.add(
                LLMTask(
                    task_type="evidence-comparison",
                    provider=provider,
                    object_type="event",
                    object_id=41,
                    status="pending",
                    input_fingerprint=fingerprint,
                )
            )
            barrier.wait()
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                return "duplicate"
            return "created"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = sorted(
            executor.map(insert_same_comparison, ("legacy", "local"))
        )
    assert outcomes == ["created", "duplicate"]
    with SessionLocal() as session:
        assert session.scalar(
            select(func.count(LLMTask.id)).where(
                LLMTask.task_type == "evidence-comparison",
                LLMTask.object_id == 41,
                LLMTask.status.in_(("pending", "running")),
            )
        ) == 1
    engine.dispose()


def test_fingerprint_reuse_migration_safely_handles_invalid_legacy_task_json(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0050_synthesis_source_count")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO llm_tasks "
                "(task_type, provider, object_type, object_id, status, "
                " prompt_version, model_version, result_json, created_at, updated_at) "
                "VALUES "
                "('event-synthesis', 'legacy', 'event', 51, 'complete', "
                " 'v1', 'm1', 'not-json', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP), "
                "('event-synthesis', 'legacy', 'event', 52, 'failed', "
                " 'v1', 'm1', '{', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP), "
                "('event-synthesis', 'legacy', 'event', 53, 'pending', "
                " 'v1', 'm1', 'active-not-json', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
        )

    def task_facts() -> list[tuple[int, str, str]]:
        with engine.connect() as connection:
            return [
                (int(row.object_id), str(row.status), str(row.result_json))
                for row in connection.execute(
                    text(
                        "SELECT object_id, status, result_json FROM llm_tasks "
                        "ORDER BY object_id"
                    )
                )
            ]

    before = task_facts()
    with pytest.raises(DBAPIError, match="active_synthesis_task_policy_mismatch"):
        command.upgrade(config, "head")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0050_synthesis_source_count"
        )
        assert "input_fingerprint" not in {
            column["name"] for column in inspect(connection).get_columns("llm_tasks")
        }
    assert task_facts() == before

    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE llm_tasks SET status = 'complete' WHERE object_id = 53"
            )
        )
    command.upgrade(config, "head")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        rows = connection.execute(
            text(
                "SELECT object_id, status, result_json, input_fingerprint "
                "FROM llm_tasks ORDER BY object_id"
            )
        ).all()
    assert rows == [
        (51, "complete", "not-json", None),
        (52, "failed", "{", None),
        (53, "complete", "active-not-json", None),
    ]
    engine.dispose()


@pytest.mark.parametrize(
    "corruption",
    [
        "missing_snapshot_uid",
        "missing_input",
        "tampered_input",
        "tampered_system_prompt",
        "tampered_output_schema",
        "tampered_evidence_content",
    ],
)
def test_fingerprint_reuse_migration_rejects_active_incomplete_provenance(
    postgres_url: str,
    corruption: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0050_synthesis_source_count")
    engine = create_engine(postgres_url)
    task = add_valid_v2_synthesis_task_at_0050(engine)
    payload = json.loads(str(task["result_json"]))
    request = payload["request"]
    input_data = request["input_data"]
    if corruption == "missing_snapshot_uid":
        input_data.pop("snapshot_uid")
    elif corruption == "missing_input":
        request.pop("input")
    elif corruption == "tampered_input":
        request["input"] = "{}"
    elif corruption == "tampered_system_prompt":
        request["system_prompt"] = "tampered"
    elif corruption == "tampered_output_schema":
        request["output_schema"] = {"type": "object"}
    else:
        input_data["evidence"][0]["content"] = "tampered"
        request["input"] = json.dumps(
            input_data, ensure_ascii=False, separators=(",", ":")
        )
    invalid_result = json.dumps(payload)
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE llm_tasks SET result_json = :result WHERE id = :id"),
            {"id": task["task_id"], "result": invalid_result},
        )
    engine.dispose()

    with pytest.raises(
        RuntimeError, match="active_synthesis_task_provenance_invalid"
    ):
        command.upgrade(config, "head")
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0050_synthesis_source_count"
        )
        assert "input_fingerprint" not in {
            column["name"] for column in inspect(connection).get_columns("llm_tasks")
        }
        assert connection.execute(
            text("SELECT status, result_json FROM llm_tasks WHERE id = :id"),
            {"id": task["task_id"]},
        ).one() == ("pending", invalid_result)
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE llm_tasks SET status = 'complete' WHERE id = :id"),
            {"id": task["task_id"]},
        )
    engine.dispose()

    command.upgrade(config, "head")
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.execute(
            text(
                "SELECT status, result_json, input_fingerprint "
                "FROM llm_tasks WHERE id = :id"
            ),
            {"id": task["task_id"]},
        ).one() == ("complete", invalid_result, task["fingerprint"])
    engine.dispose()


def test_fingerprint_reuse_migration_rejects_self_consistent_forged_snapshot_fingerprints(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0050_synthesis_source_count")
    engine = create_engine(postgres_url)
    task = add_valid_v2_synthesis_task_at_0050(engine)
    input_data = json.loads(json.dumps(task["input_data"]))
    source_fingerprint = "a" * 64
    content_fingerprint = "b" * 64
    assert source_fingerprint != input_data["source_coverage_fingerprint"]
    assert content_fingerprint != input_data["content_fingerprint"]
    generation_fingerprint = synthesis_generation_fingerprint(
        source_fingerprint,
        content_fingerprint,
    )
    input_data.update(
        {
            "source_coverage_fingerprint": source_fingerprint,
            "content_fingerprint": content_fingerprint,
            "generation_fingerprint": generation_fingerprint,
        }
    )
    result_json = json.dumps(
        {"request": synthesis_model_request(input_data)}, ensure_ascii=False
    )
    with engine.begin() as connection:
        connection.execute(
            text("ALTER TABLE evidence_snapshots DISABLE TRIGGER USER")
        )
        connection.execute(
            text(
                "UPDATE evidence_snapshots "
                "SET source_coverage_fingerprint = :source_fingerprint, "
                "content_fingerprint = :content_fingerprint "
                "WHERE uid = :snapshot_uid"
            ),
            {
                "source_fingerprint": source_fingerprint,
                "content_fingerprint": content_fingerprint,
                "snapshot_uid": input_data["snapshot_uid"],
            },
        )
        connection.execute(
            text("ALTER TABLE evidence_snapshots ENABLE TRIGGER USER")
        )
        connection.execute(
            text("UPDATE llm_tasks SET result_json = :result WHERE id = :id"),
            {"id": task["task_id"], "result": result_json},
        )
    engine.dispose()

    with pytest.raises(
        RuntimeError, match="active_synthesis_task_provenance_invalid"
    ):
        command.upgrade(config, "head")
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0050_synthesis_source_count"
        )
        assert "input_fingerprint" not in {
            column["name"] for column in inspect(connection).get_columns("llm_tasks")
        }
    engine.dispose()


def test_synthesis_provenance_audit_upgrades_deployed_0051_without_rewriting_valid_task(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0050_synthesis_source_count")
    engine = create_engine(postgres_url)
    task = add_valid_v2_synthesis_task_at_0050(engine)
    engine.dispose()
    command.upgrade(config, "0051_fingerprint_reuse")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        before = connection.execute(
            text(
                "SELECT status, prompt_version, result_json, input_fingerprint "
                "FROM llm_tasks WHERE id = :id"
            ),
            {"id": task["task_id"]},
        ).one()
    engine.dispose()

    command.upgrade(config, "head")
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        assert connection.execute(
            text(
                "SELECT status, prompt_version, result_json, input_fingerprint "
                "FROM llm_tasks WHERE id = :id"
            ),
            {"id": task["task_id"]},
        ).one() == before
    engine.dispose()

    command.upgrade(config, "head")
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        assert connection.execute(
            text(
                "SELECT status, prompt_version, result_json, input_fingerprint "
                "FROM llm_tasks WHERE id = :id"
            ),
            {"id": task["task_id"]},
        ).one() == before
    engine.dispose()


def test_synthesis_provenance_audit_rejects_invalid_task_from_deployed_0051(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0050_synthesis_source_count")
    engine = create_engine(postgres_url)
    task = add_valid_v2_synthesis_task_at_0050(engine)
    engine.dispose()
    command.upgrade(config, "0051_fingerprint_reuse")

    payload = json.loads(str(task["result_json"]))
    payload["request"].pop("input")
    invalid_result = json.dumps(payload, ensure_ascii=False)
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE llm_tasks SET result_json = :result WHERE id = :id"),
            {"id": task["task_id"], "result": invalid_result},
        )
    with engine.connect() as connection:
        before = connection.execute(
            text(
                "SELECT status, prompt_version, result_json, input_fingerprint "
                "FROM llm_tasks WHERE id = :id"
            ),
            {"id": task["task_id"]},
        ).one()
    engine.dispose()

    with pytest.raises(
        RuntimeError, match="active_synthesis_task_provenance_invalid"
    ):
        command.upgrade(config, "head")
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0051_fingerprint_reuse"
        )
        assert connection.execute(
            text(
                "SELECT status, prompt_version, result_json, input_fingerprint "
                "FROM llm_tasks WHERE id = :id"
            ),
            {"id": task["task_id"]},
        ).one() == before
    engine.dispose()


def test_event_synthesis_artifacts_are_transactionally_complete_and_immutable(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        items: list[ContentItem] = []
        for index in range(2):
            source = Source(
                name=f"Synthesis source {index}",
                url=f"https://example.com/synthesis-{index}.xml",
                status="active",
                media_type="article",
            )
            session.add(source)
            session.flush()
            raw = make_raw_entry(
                source=source,
                external_id=f"synthesis-{index}",
                title=f"Synthesis evidence {index}",
                url=f"https://example.com/synthesis-{index}",
                raw_content=f"Evidence body {index}",
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                document_type="normal_article",
                title=raw.title,
                content_text=raw.raw_content,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=raw.title,
                content_text=raw.raw_content,
                url=raw.url,
                canonical_url=raw.url,
                content_hash=raw.content_hash,
                embedding_vector=POSTGRES_TEST_VECTOR,
                embedding_model="synthesis-test-model",
            )
            session.add(item)
            session.flush()
            items.append(item)
        with clustering_run(
            session,
            scope_type="event-synthesis-postgres",
            item_ids=[item.id for item in items],
            rule_version="event-synthesis-postgres-v1",
        ):
            for item in items:
                assign_cluster(session, item)
        cluster_id = session.scalar(select(Cluster.id))
        event_row = session.scalar(select(ReaderEvent))
        assert event_row is not None
        assert cluster_id is not None
        revision = session.get(EventRevision, event_row.current_revision_id)
        assert revision is not None
        snapshot, evidence, source_count, generation_fingerprint = (
            create_evidence_snapshot(session, event_row, revision)
        )
        assert source_count == 2
        item_ids = [item.id for item in items]
        event_id = event_row.id
        revision_id = revision.id
        snapshot_id = snapshot.id
        session.commit()

    with SessionLocal() as session:
        event_row = session.get(ReaderEvent, event_id)
        revision = session.get(EventRevision, revision_id)
        snapshot = session.get(EvidenceSnapshot, snapshot_id)
        assert event_row is not None and revision is not None and snapshot is not None
        input_data = synthesis_input_data(
            event_row,
            revision,
            snapshot,
            evidence,
            generation_fingerprint,
        )
        request = synthesis_model_request(input_data)
        validated_snapshot, _uids, validated_source_count, validated_fingerprint = (
            synthesis_task_snapshot_from_request(session, event_row, request)
        )
        assert validated_snapshot.id == snapshot.id
        assert validated_source_count == source_count
        assert validated_fingerprint == generation_fingerprint
        request_json = json.dumps({"request": request}, ensure_ascii=False)
        active = LLMTask(
            task_type="event-synthesis",
            provider="local",
            object_type="event",
            object_id=event_id,
            status="running",
            input_fingerprint=generation_fingerprint,
            result_json=request_json,
        )
        duplicate_failed = LLMTask(
            task_type="event-synthesis",
            provider="legacy",
            object_type="event",
            object_id=event_id,
            status="failed",
            input_fingerprint=generation_fingerprint,
            result_json=request_json,
        )
        ordinary_failed = LLMTask(
            task_type="item-summary",
            provider="legacy",
            object_type="item",
            object_id=item_ids[0],
            status="failed",
            result_json='{"request":{"input":"summarize"},"error":"retry"}',
        )
        session.add_all([active, duplicate_failed, ordinary_failed])
        session.commit()

        session.refresh(active)
        session.refresh(duplicate_failed)
        session.refresh(ordinary_failed)
        assert active.status == "running"
        assert duplicate_failed.status == "failed"
        assert ordinary_failed.status == "failed"

    # This is the Legacy async boundary: Snapshot/Members committed in the
    # enqueue transaction, Version/Blocks/Citations commit together later.
    with SessionLocal() as session:
        event_row = session.get(ReaderEvent, event_id)
        snapshot = session.get(EvidenceSnapshot, snapshot_id)
        assert event_row is not None and snapshot is not None
        version_uids = [str(row["evidence_version_uid"]) for row in evidence]
        version = save_synthesis_version(
            session,
            event=event_row,
            snapshot=snapshot,
            source_count=source_count,
            provider="legacy",
            model="legacy-test",
            generation_fingerprint=generation_fingerprint,
            blocks=[
                {
                    "kind": "fact",
                    "body": "A cited fact",
                    "attribution": "",
                    "citations": [
                        {
                            "evidence_version_uid": version_uids[0],
                            "side": "support",
                        }
                    ],
                }
            ],
        )
        session.commit()
        version_id = version.id
        block_id = session.scalar(
            select(SynthesisBlock.id).where(
                SynthesisBlock.synthesis_version_id == version.id
            )
        )
        citation_id = session.scalar(
            select(SynthesisCitation.id).where(
                SynthesisCitation.synthesis_version_id == version.id
            )
        )
        members = list(
            session.scalars(
                select(EvidenceSnapshotMember)
                .where(EvidenceSnapshotMember.snapshot_id == snapshot_id)
                .order_by(EvidenceSnapshotMember.position)
            )
        )
        assert block_id is not None and citation_id is not None
        assert len(members) == 2
        member_rows = [
            {
                "evidence_version_id": member.evidence_version_id,
                "evidence_type": member.evidence_type,
                "role": member.role,
            }
            for member in members
        ]

    with engine.begin() as connection:
        forced_snapshot_id, snapshot_transaction_was_forced = connection.execute(
            text(
                "INSERT INTO evidence_snapshots "
                "(uid, event_id, target_revision_id, source_coverage_fingerprint, "
                " content_fingerprint, policy_version, created_transaction_id) "
                "VALUES (:uid, :event_id, :revision_id, :source_fingerprint, "
                " :content_fingerprint, 'test-v1', '1'::xid8) "
                "RETURNING id, created_transaction_id = pg_current_xact_id()"
            ),
            {
                "uid": str(uuid4()),
                "event_id": event_id,
                "revision_id": revision_id,
                "source_fingerprint": "c" * 64,
                "content_fingerprint": "d" * 64,
            },
        ).one()
        assert snapshot_transaction_was_forced is True
        for position, member in enumerate(member_rows, 1):
            connection.execute(
                text(
                    "INSERT INTO evidence_snapshot_members "
                    "(snapshot_id, target_revision_id, evidence_version_id, "
                    " evidence_type, role, position) "
                    "VALUES (:snapshot_id, :revision_id, :evidence_version_id, "
                    " :evidence_type, :role, :position)"
                ),
                {
                    "snapshot_id": forced_snapshot_id,
                    "revision_id": revision_id,
                    "position": position,
                    **member,
                },
            )

    with engine.begin() as connection:
        forced_version_id, version_transaction_was_forced = connection.execute(
            text(
                "INSERT INTO synthesis_versions "
                "(uid, event_id, target_revision_id, snapshot_id, source_count, "
                " provider, model, prompt_version, schema_version, "
                " generation_fingerprint, created_transaction_id) "
                "VALUES (:uid, :event_id, :revision_id, :snapshot_id, 2, "
                " 'test', 'test', 'test-v1', 'test-v1', :fingerprint, '1'::xid8) "
                "RETURNING id, created_transaction_id = pg_current_xact_id()"
            ),
            {
                "uid": str(uuid4()),
                "event_id": event_id,
                "revision_id": revision_id,
                "snapshot_id": forced_snapshot_id,
                "fingerprint": hashlib.sha256(str(uuid4()).encode()).hexdigest(),
            },
        ).one()
        assert version_transaction_was_forced is True
        forced_block_id = connection.scalar(
            text(
                "INSERT INTO synthesis_blocks "
                "(uid, synthesis_version_id, position, kind, body, attribution) "
                "VALUES (:uid, :version_id, 1, 'fact', 'test block', '') "
                "RETURNING id"
            ),
            {"uid": str(uuid4()), "version_id": forced_version_id},
        )
        assert forced_block_id is not None
        connection.execute(
            text(
                "INSERT INTO synthesis_citations "
                "(block_id, synthesis_version_id, snapshot_id, evidence_version_id, "
                " evidence_type, role, side, position) "
                "VALUES (:block_id, :version_id, :snapshot_id, :evidence_version_id, "
                " :evidence_type, :role, 'support', 1)"
            ),
            {
                "block_id": forced_block_id,
                "version_id": forced_version_id,
                "snapshot_id": forced_snapshot_id,
                **member_rows[0],
            },
        )

    with SessionLocal() as session:
        event_row = session.get(ReaderEvent, event_id)
        revision = session.get(EventRevision, revision_id)
        assert event_row is not None and revision is not None
        second_snapshot, _evidence, second_source_count, _fingerprint = (
            create_evidence_snapshot(session, event_row, revision)
        )
        assert second_source_count == 2
        assert second_snapshot.id == snapshot_id
        session.commit()

    with pytest.raises(DBAPIError, match="immutable_evidence_snapshot_members"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO evidence_snapshot_members "
                    "(snapshot_id, target_revision_id, evidence_version_id, "
                    " evidence_type, role, position) "
                    "VALUES (:snapshot_id, :revision_id, :version_id, "
                    " :evidence_type, :role, 99)"
                ),
                {
                    "snapshot_id": forced_snapshot_id,
                    "revision_id": revision_id,
                    "version_id": member_rows[0]["evidence_version_id"],
                    "evidence_type": member_rows[0]["evidence_type"],
                    "role": member_rows[0]["role"],
                },
            )
    with pytest.raises(DBAPIError, match="immutable_synthesis_blocks"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO synthesis_blocks "
                    "(uid, synthesis_version_id, position, kind, body, attribution) "
                    "VALUES (:uid, :version_id, 99, 'fact', 'late', '')"
                ),
                {"uid": str(uuid4()), "version_id": forced_version_id},
            )
    with pytest.raises(DBAPIError, match="immutable_synthesis_citations"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO synthesis_citations "
                    "(block_id, synthesis_version_id, snapshot_id, evidence_version_id, "
                    " evidence_type, role, side, position) "
                    "VALUES (:block_id, :version_id, :snapshot_id, :evidence_version_id, "
                    " :evidence_type, :role, 'late', 99)"
                ),
                {
                    "block_id": forced_block_id,
                    "version_id": forced_version_id,
                    "snapshot_id": forced_snapshot_id,
                    **member_rows[1],
                },
            )

    artifact_rows = (
        ("evidence_snapshots", snapshot_id, "policy_version = policy_version"),
        ("evidence_snapshot_members", members[0].id, "position = position"),
        ("synthesis_versions", version_id, "model = model"),
        ("synthesis_blocks", block_id, "body = body"),
        ("synthesis_citations", citation_id, "side = side"),
    )
    for table_name, row_id, assignment in artifact_rows:
        with pytest.raises(DBAPIError, match="immutable_synthesis_artifact"):
            with engine.begin() as connection:
                connection.execute(
                    text(f"UPDATE {table_name} SET {assignment} WHERE id = :id"),
                    {"id": row_id},
                )
        with pytest.raises(DBAPIError, match="immutable_synthesis_artifact"):
            with engine.begin() as connection:
                connection.execute(
                    text(f"DELETE FROM {table_name} WHERE id = :id"),
                    {"id": row_id},
                )

    def insert_version_and_block(
        connection: Connection,
        *,
        snapshot: int,
        source_total: int,
        kind: str = "fact",
    ) -> tuple[int, int]:
        version = connection.scalar(
            text(
                "INSERT INTO synthesis_versions "
                "(uid, event_id, target_revision_id, snapshot_id, source_count, "
                " provider, model, prompt_version, schema_version, generation_fingerprint) "
                "VALUES (:uid, :event_id, :revision_id, :snapshot_id, :source_count, "
                " 'test', 'test', 'test-v1', 'test-v1', :fingerprint) RETURNING id"
            ),
            {
                "uid": str(uuid4()),
                "event_id": event_id,
                "revision_id": revision_id,
                "snapshot_id": snapshot,
                "source_count": source_total,
                "fingerprint": hashlib.sha256(str(uuid4()).encode()).hexdigest(),
            },
        )
        assert version is not None
        block = connection.scalar(
            text(
                "INSERT INTO synthesis_blocks "
                "(uid, synthesis_version_id, position, kind, body, attribution) "
                "VALUES (:uid, :version_id, 1, :kind, 'test block', '') RETURNING id"
            ),
            {"uid": str(uuid4()), "version_id": version, "kind": kind},
        )
        assert block is not None
        return version, block

    with engine.begin() as connection:
        single_source_snapshot_id = connection.scalar(
            text(
                "INSERT INTO evidence_snapshots "
                "(uid, event_id, target_revision_id, source_coverage_fingerprint, "
                " content_fingerprint, policy_version) "
                "VALUES (:uid, :event_id, :revision_id, :source_fingerprint, "
                " :content_fingerprint, 'test-v1') RETURNING id"
            ),
            {
                "uid": str(uuid4()),
                "event_id": event_id,
                "revision_id": revision_id,
                "source_fingerprint": "e" * 64,
                "content_fingerprint": "f" * 64,
            },
        )
        assert single_source_snapshot_id is not None
        connection.execute(
            text(
                "INSERT INTO evidence_snapshot_members "
                "(snapshot_id, target_revision_id, evidence_version_id, "
                " evidence_type, role, position) "
                "VALUES (:snapshot_id, :revision_id, :evidence_version_id, "
                " :evidence_type, :role, 1)"
            ),
            {
                "snapshot_id": single_source_snapshot_id,
                "revision_id": revision_id,
                **member_rows[0],
            },
        )

    with pytest.raises(DBAPIError, match="ck_synthesis_version_source_count"):
        with engine.begin() as connection:
            single_source_version, single_source_block = insert_version_and_block(
                connection,
                snapshot=single_source_snapshot_id,
                source_total=1,
            )
            connection.execute(
                text(
                    "INSERT INTO synthesis_citations "
                    "(block_id, synthesis_version_id, snapshot_id, "
                    " evidence_version_id, evidence_type, role, side, position) "
                    "VALUES (:block_id, :version_id, :snapshot_id, "
                    " :evidence_version_id, :evidence_type, :role, 'support', 1)"
                ),
                {
                    "block_id": single_source_block,
                    "version_id": single_source_version,
                    "snapshot_id": single_source_snapshot_id,
                    **member_rows[0],
                },
            )
            connection.execute(
                text(
                    "UPDATE events SET current_synthesis_version_id = :version_id "
                    "WHERE id = :event_id"
                ),
                {"version_id": single_source_version, "event_id": event_id},
            )

    with pytest.raises(DBAPIError, match="synthesis_block_without_citation"):
        with engine.begin() as connection:
            insert_version_and_block(connection, snapshot=snapshot_id, source_total=2)

    with pytest.raises(DBAPIError, match="fk_synthesis_citation_version_snapshot"):
        with engine.begin() as connection:
            cross_version, cross_block = insert_version_and_block(
                connection, snapshot=forced_snapshot_id, source_total=2
            )
            connection.execute(
                text(
                    "INSERT INTO synthesis_citations "
                    "(block_id, synthesis_version_id, snapshot_id, evidence_version_id, "
                    " evidence_type, role, side, position) "
                    "VALUES (:block_id, :version_id, :snapshot_id, :evidence_version_id, "
                    " :evidence_type, :role, 'cross_snapshot', 1)"
                ),
                {
                    "block_id": cross_block,
                    "version_id": cross_version,
                    "snapshot_id": snapshot_id,
                    **member_rows[0],
                },
            )

    with pytest.raises(DBAPIError, match="synthesis_source_count_mismatch"):
        with engine.begin() as connection:
            bad_version, bad_block = insert_version_and_block(
                connection, snapshot=snapshot_id, source_total=3
            )
            connection.execute(
                text(
                    "INSERT INTO synthesis_citations "
                    "(block_id, synthesis_version_id, snapshot_id, evidence_version_id, "
                    " evidence_type, role, side, position) "
                    "VALUES (:block_id, :version_id, :snapshot_id, :evidence_version_id, "
                    " :evidence_type, :role, 'support', 1)"
                ),
                {
                    "block_id": bad_block,
                    "version_id": bad_version,
                    "snapshot_id": snapshot_id,
                    **member_rows[0],
                },
            )

    with pytest.raises(DBAPIError, match="synthesis_disagreement_requires_two_sides"):
        with engine.begin() as connection:
            bad_version, bad_block = insert_version_and_block(
                connection,
                snapshot=snapshot_id,
                source_total=2,
                kind="disagreement",
            )
            for position, member in enumerate(member_rows, 1):
                connection.execute(
                    text(
                        "INSERT INTO synthesis_citations "
                        "(block_id, synthesis_version_id, snapshot_id, evidence_version_id, "
                        " evidence_type, role, side, position) "
                        "VALUES (:block_id, :version_id, :snapshot_id, :evidence_version_id, "
                        " :evidence_type, :role, 'same_side', :position)"
                    ),
                    {
                        "block_id": bad_block,
                        "version_id": bad_version,
                        "snapshot_id": snapshot_id,
                        "position": position,
                        **member,
                    },
                )

    # A member must belong to the exact target Revision, not merely to the Event.
    with SessionLocal() as session:
        artifact_counts_before_reconciliation = (
            session.query(EvidenceSnapshot).count(),
            session.query(SynthesisVersion).count(),
            session.query(LLMTask).count(),
        )
        event_row = session.get(ReaderEvent, event_id)
        item = session.get(ContentItem, item_ids[0])
        assert event_row is not None and item is not None
        with clustering_run(
            session,
            scope_type="event-synthesis-postgres-next-revision",
            item_ids=[item.id],
            rule_version="event-synthesis-postgres-v1",
        ):
            item.content_text = "Changed evidence body"
            item.content_hash = hashlib.sha256(item.content_text.encode()).hexdigest()
        assert event_row.current_revision_id != revision_id
        next_link = session.scalar(
            select(EventRevisionEvidence)
            .where(
                EventRevisionEvidence.revision_id == event_row.current_revision_id,
                EventRevisionEvidence.evidence_version_id.not_in(
                    [row["evidence_version_id"] for row in member_rows]
                ),
            )
            .limit(1)
        )
        assert next_link is not None
        next_version_id = next_link.evidence_version_id
        next_evidence_type = next_link.evidence_type
        next_role = next_link.role
        session.commit()

    with SessionLocal() as session:
        event_row = session.get(ReaderEvent, event_id)
        assert event_row is not None
        next_revision = session.get(EventRevision, event_row.current_revision_id)
        assert next_revision is not None
        state = event_synthesis_state(session, event_row, next_revision)
        listed_freshness = event_synthesis_freshness_for(
            session, {event_row.uid: next_revision.uid}
        )

        assert state.status == listed_freshness[event_row.uid].status == "unreviewed"
        assert state.target_revision_uid == next_revision.uid
        assert state.source_count == 2
        assert state.current is not None
        current_version = session.get(SynthesisVersion, version_id)
        assert current_version is not None
        assert state.current.version_uid == current_version.uid
        assert state.current.target_revision_uid != state.target_revision_uid
        assert (
            state.current.blocks[0].citations[0].evidence_version_uid
            == version_uids[0]
        )
        current_evidence = list(
            session.scalars(
                select(EventRevisionEvidence).where(
                    EventRevisionEvidence.revision_id
                    == event_row.current_revision_id
                )
            )
        )
        assert len(current_evidence) == 2
        assert (
            session.query(EvidenceSnapshot).count(),
            session.query(SynthesisVersion).count(),
            session.query(LLMTask).count(),
        ) == artifact_counts_before_reconciliation

        target_snapshot, _target_evidence, target_source_count, _ = (
            create_evidence_snapshot(session, event_row, next_revision)
        )
        synthesis_count_before_review = session.query(SynthesisVersion).count()
        baseline_snapshot = session.get(EvidenceSnapshot, snapshot_id)
        assert baseline_snapshot is not None and target_source_count == 2
        review_input, new_source_count = evidence_review_input_data(
            session, event_row, baseline_snapshot, target_snapshot
        )
        assert new_source_count == 0
        new_version_uid = next(
            str(row["evidence_version_uid"])
            for row in review_input["new_evidence"]
            if isinstance(row, dict)
        )
        review = save_evidence_review(
            session,
            event=event_row,
            baseline_snapshot=baseline_snapshot,
            target_snapshot=target_snapshot,
            comparison_fingerprint=str(
                review_input["comparison_fingerprint"]
            ),
            result="ordinary",
            reason="The replacement only corroborates the existing claim.",
            cited_version_uids=[new_version_uid],
            provider="legacy",
            model="legacy-postgres-test",
        )
        session.add(
            EventUserState(
                event_id=event_row.id,
                seen_revision_id=next_revision.id,
                read_status="summary_seen",
                read_later=False,
                starred=False,
            )
        )
        session.commit()
        review_id = review.id
        target_snapshot_id = target_snapshot.id
        target_revision_id = next_revision.id
        comparison_fingerprint = review.comparison_fingerprint
        review_citation = session.scalar(
            select(EvidenceReviewCitation).where(
                EvidenceReviewCitation.review_id == review.id
            )
        )
        assert review_citation is not None
        review_citation_id = review_citation.id
        target_members = list(
            session.scalars(
                select(EvidenceSnapshotMember)
                .where(
                    EvidenceSnapshotMember.snapshot_id == target_snapshot.id
                )
                .order_by(EvidenceSnapshotMember.position)
            )
        )
        target_member_ids = {
            member.evidence_version_id for member in target_members
        }
        obsolete_member = next(
            member
            for member in members
            if member.evidence_version_id not in target_member_ids
        )
        unused_target_member = next(
            member
            for member in target_members
            if member.evidence_version_id
            != review_citation.evidence_version_id
        )

        state = event_synthesis_state(session, event_row, next_revision)
        listed_freshness = event_synthesis_freshness_for(
            session, {event_row.uid: next_revision.uid}
        )[event_row.uid]
        assert state.status == listed_freshness.status == "current"
        assert state.covered_revision_uid == revision.uid
        assert state.reviewed_revision_uid == next_revision.uid
        assert state.current is not None
        assert state.current.version_uid == current_version.uid
        assert event_row.current_synthesis_version_id == version_id
        assert event_row.reviewed_evidence_review_id == review.id
        assert session.query(EvidenceReview).count() == 1
        assert session.query(EvidenceReviewCitation).count() == 1
        assert session.query(SynthesisVersion).count() == synthesis_count_before_review
        assert event_material_updates_for(session, [event_id]) == {event_id: None}

    with SessionLocal() as session:
        event_row = session.get(ReaderEvent, event_id)
        item = session.get(ContentItem, item_ids[1])
        assert event_row is not None and item is not None
        with clustering_run(
            session,
            scope_type="event-synthesis-postgres-material-revision",
            item_ids=[item.id],
            rule_version="event-synthesis-postgres-material-v1",
        ):
            item.content_text = "Materially changed second evidence body"
            item.content_hash = hashlib.sha256(item.content_text.encode()).hexdigest()
        material_revision_id = event_row.current_revision_id
        session.commit()

    def publish_material(session: Session) -> tuple[int, int]:
        event_row = session.get(ReaderEvent, event_id)
        baseline_snapshot = session.get(EvidenceSnapshot, target_snapshot_id)
        material_revision = session.get(EventRevision, material_revision_id)
        assert (
            event_row is not None
            and baseline_snapshot is not None
            and material_revision is not None
        )
        material_snapshot, evidence, source_count, generation_fingerprint = (
            create_evidence_snapshot(session, event_row, material_revision)
        )
        review_input, _ = evidence_review_input_data(
            session, event_row, baseline_snapshot, material_snapshot
        )
        cited_uid = next(
            str(row["evidence_version_uid"])
            for row in review_input["new_evidence"]
            if isinstance(row, dict)
        )
        material_review = save_evidence_review(
            session,
            event=event_row,
            baseline_snapshot=baseline_snapshot,
            target_snapshot=material_snapshot,
            comparison_fingerprint=str(review_input["comparison_fingerprint"]),
            result="material",
            reason="The replacement changes a core fact.",
            cited_version_uids=[cited_uid],
            provider="legacy",
            model="legacy-postgres-test",
        )
        material_version = save_synthesis_version(
            session,
            event=event_row,
            snapshot=material_snapshot,
            source_count=source_count,
            provider="legacy",
            model="legacy-postgres-test",
            generation_fingerprint=generation_fingerprint,
            blocks=[
                {
                    "kind": "fact",
                    "body": "The corrected fact.",
                    "attribution": "",
                    "citations": [
                        {
                            "evidence_version_uid": cited_uid,
                            "side": "support",
                        }
                    ],
                }
            ],
        )
        assert event_row.reviewed_evidence_review_id == material_review.id
        assert event_row.current_synthesis_version_id == material_version.id
        return material_review.id, material_version.id

    with SessionLocal() as session:
        publish_material(session)
        session.rollback()

    with SessionLocal() as session:
        event_row = session.get(ReaderEvent, event_id)
        assert event_row is not None
        assert event_row.reviewed_evidence_review_id == review_id
        assert event_row.current_synthesis_version_id == version_id
        assert session.query(EvidenceReview).count() == 1
        assert session.query(SynthesisVersion).count() == synthesis_count_before_review

    with SessionLocal() as session:
        material_review_id, material_version_id = publish_material(session)
        session.commit()
        event_row = session.get(ReaderEvent, event_id)
        material_revision = session.get(EventRevision, material_revision_id)
        assert event_row is not None and material_revision is not None
        assert event_row.reviewed_evidence_review_id == material_review_id
        assert event_row.current_synthesis_version_id == material_version_id
        assert event_synthesis_state(
            session, event_row, material_revision
        ).status == "current"
        assert material_revision.revision_no > next_revision.revision_no
        assert event_material_updates_for(session, [event_id]) == {
            event_id: material_revision.uid
        }
        assert session.query(EvidenceReview).count() == 2
        assert (
            session.query(SynthesisVersion).count()
            == synthesis_count_before_review + 1
        )

    def read_material_update() -> str | None:
        with SessionLocal() as session:
            return event_material_updates_for(session, [event_id])[event_id]

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert list(executor.map(lambda _: read_material_update(), range(2))) == [
            material_revision.uid,
            material_revision.uid,
        ]

    with SessionLocal() as session:
        state = session.scalar(
            select(EventUserState).where(EventUserState.event_id == event_id)
        )
        assert state is not None
        assert state.seen_revision_id == target_revision_id
        assert state.read_status == "summary_seen"
        state.seen_revision_id = material_revision_id
        session.commit()
        assert event_material_updates_for(session, [event_id]) == {event_id: None}
        assert state.read_status == "summary_seen"

    for table_name, row_id, assignment in (
        ("evidence_reviews", review_id, "reason = reason"),
        (
            "evidence_review_citations",
            review_citation_id,
            "position = position",
        ),
    ):
        with pytest.raises(DBAPIError, match="immutable_synthesis_artifact"):
            with engine.begin() as connection:
                connection.execute(
                    text(f"UPDATE {table_name} SET {assignment} WHERE id = :id"),
                    {"id": row_id},
                )
        with pytest.raises(DBAPIError, match="immutable_synthesis_artifact"):
            with engine.begin() as connection:
                connection.execute(
                    text(f"DELETE FROM {table_name} WHERE id = :id"),
                    {"id": row_id},
                )

    with pytest.raises(DBAPIError, match="immutable_evidence_review_citations"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO evidence_review_citations "
                    "(review_id, target_snapshot_id, evidence_version_id, "
                    " evidence_type, role, position) VALUES "
                    "(:review_id, :snapshot_id, :version_id, :evidence_type, "
                    " :role, 2)"
                ),
                {
                    "review_id": review_id,
                    "snapshot_id": target_snapshot_id,
                    "version_id": unused_target_member.evidence_version_id,
                    "evidence_type": unused_target_member.evidence_type,
                    "role": unused_target_member.role,
                },
            )

    review_values = {
        "uid": str(uuid4()),
        "event_id": event_id,
        "baseline_revision_id": revision_id,
        "baseline_snapshot_id": snapshot_id,
        "target_revision_id": target_revision_id,
        "target_snapshot_id": target_snapshot_id,
        "fingerprint": hashlib.sha256(str(uuid4()).encode()).hexdigest(),
        "policy_version": EVIDENCE_REVIEW_POLICY_VERSION,
    }
    with pytest.raises(DBAPIError, match="evidence_review_without_citations"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO evidence_reviews "
                    "(uid, event_id, baseline_revision_id, baseline_snapshot_id, "
                    " target_revision_id, target_snapshot_id, "
                    " comparison_fingerprint, result, reason, provider, model, "
                    " policy_version) VALUES "
                    "(:uid, :event_id, :baseline_revision_id, "
                    " :baseline_snapshot_id, :target_revision_id, "
                    " :target_snapshot_id, :fingerprint, 'ordinary', "
                    " 'missing citations', 'test', 'test', :policy_version)"
                ),
                review_values,
            )

    with pytest.raises(
        DBAPIError, match="fk_evidence_review_citation_snapshot_member"
    ):
        with engine.begin() as connection:
            invalid_review_id = connection.scalar(
                text(
                    "INSERT INTO evidence_reviews "
                    "(uid, event_id, baseline_revision_id, baseline_snapshot_id, "
                    " target_revision_id, target_snapshot_id, "
                    " comparison_fingerprint, result, reason, provider, model, "
                    " policy_version) VALUES "
                    "(:uid, :event_id, :baseline_revision_id, "
                    " :baseline_snapshot_id, :target_revision_id, "
                    " :target_snapshot_id, :fingerprint, 'ordinary', "
                    " 'cross snapshot', 'test', 'test', :policy_version) "
                    "RETURNING id"
                ),
                {**review_values, "uid": str(uuid4())},
            )
            connection.execute(
                text(
                    "INSERT INTO evidence_review_citations "
                    "(review_id, target_snapshot_id, evidence_version_id, "
                    " evidence_type, role, position) VALUES "
                    "(:review_id, :snapshot_id, :version_id, :evidence_type, "
                    " :role, 1)"
                ),
                {
                    "review_id": invalid_review_id,
                    "snapshot_id": target_snapshot_id,
                    "version_id": obsolete_member.evidence_version_id,
                    "evidence_type": obsolete_member.evidence_type,
                    "role": obsolete_member.role,
                },
            )

    with pytest.raises(
        IntegrityError, match="uq_evidence_review_comparison_fingerprint"
    ):
        with SessionLocal() as session:
            session.add(
                EvidenceReview(
                    uid=str(uuid4()),
                    event_id=event_id,
                    baseline_revision_id=revision_id,
                    baseline_snapshot_id=snapshot_id,
                    target_revision_id=target_revision_id,
                    target_snapshot_id=target_snapshot_id,
                    comparison_fingerprint=comparison_fingerprint,
                    result="ordinary",
                    reason="duplicate comparison",
                    provider="local",
                    model="test",
                    policy_version=EVIDENCE_REVIEW_POLICY_VERSION,
                )
            )
            session.commit()

    connection = engine.connect()
    transaction = connection.begin()
    try:
        forced_review_id, transaction_was_forced = connection.execute(
            text(
                "INSERT INTO evidence_reviews "
                "(uid, event_id, baseline_revision_id, baseline_snapshot_id, "
                " target_revision_id, target_snapshot_id, "
                " comparison_fingerprint, result, reason, provider, model, "
                " policy_version, created_transaction_id) VALUES "
                "(:uid, :event_id, :baseline_revision_id, "
                " :baseline_snapshot_id, :target_revision_id, "
                " :target_snapshot_id, :fingerprint, 'ordinary', "
                " 'forced transaction', 'test', 'test', :policy_version, "
                " '1'::xid8) RETURNING id, "
                "created_transaction_id = pg_current_xact_id()"
            ),
            {**review_values, "uid": str(uuid4())},
        ).one()
        assert transaction_was_forced is True
        connection.execute(
            text(
                "INSERT INTO evidence_review_citations "
                "(review_id, target_snapshot_id, evidence_version_id, "
                " evidence_type, role, position) VALUES "
                "(:review_id, :snapshot_id, :version_id, :evidence_type, "
                " :role, 1)"
            ),
            {
                "review_id": forced_review_id,
                "snapshot_id": target_snapshot_id,
                "version_id": unused_target_member.evidence_version_id,
                "evidence_type": unused_target_member.evidence_type,
                "role": unused_target_member.role,
            },
        )
        connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    finally:
        transaction.rollback()
        connection.close()

    with pytest.raises(
        DBAPIError, match="fk_evidence_snapshot_member_revision_evidence"
    ):
        with engine.begin() as connection:
            cross_snapshot_id = connection.scalar(
                text(
                    "INSERT INTO evidence_snapshots "
                    "(uid, event_id, target_revision_id, source_coverage_fingerprint, "
                    " content_fingerprint, policy_version) "
                    "VALUES (:uid, :event_id, :revision_id, :source_fingerprint, "
                    " :content_fingerprint, 'test-v1') RETURNING id"
                ),
                {
                    "uid": str(uuid4()),
                    "event_id": event_id,
                    "revision_id": revision_id,
                    "source_fingerprint": "a" * 64,
                    "content_fingerprint": "b" * 64,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO evidence_snapshot_members "
                    "(snapshot_id, target_revision_id, evidence_version_id, "
                    " evidence_type, role, position) "
                    "VALUES (:snapshot_id, :revision_id, :version_id, "
                    " :evidence_type, :role, 1)"
                ),
                {
                    "snapshot_id": cross_snapshot_id,
                    "revision_id": revision_id,
                    "version_id": next_version_id,
                    "evidence_type": next_evidence_type,
                    "role": next_role,
                },
            )

    with engine.begin() as connection:
        connection.execute(
            delete(ClusterItem).where(ClusterItem.cluster_id == cluster_id)
        )
        connection.execute(delete(Cluster).where(Cluster.id == cluster_id))

    with SessionLocal() as session:
        preserved_event = session.get(ReaderEvent, event_id)
        assert preserved_event is not None
        assert preserved_event.current_synthesis_version_id == material_version_id
        assert preserved_event.reviewed_evidence_review_id == material_review_id
        assert session.get(EvidenceSnapshot, snapshot_id) is not None
        assert session.get(SynthesisVersion, version_id) is not None
        assert session.get(SynthesisVersion, material_version_id) is not None
        assert session.get(SynthesisBlock, block_id) is not None
        assert session.get(SynthesisCitation, citation_id) is not None
        assert session.get(EvidenceReview, review_id) is not None
        assert session.get(EvidenceReview, material_review_id) is not None
        assert session.get(EvidenceReviewCitation, review_citation_id) is not None
        assert set(
            session.scalars(
                select(ClusterEventProjection.cluster_id).where(
                    ClusterEventProjection.event_id == event_id
                )
            )
        ) == {None}

    engine.dispose()


def test_postgres_async_synthesis_callbacks_are_idempotent_and_monotonic(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from reader_api import main as main_module

    model_calls = 0

    def unexpected_model_call(*_args, **_kwargs) -> None:
        nonlocal model_calls
        model_calls += 1
        raise AssertionError("async callback persistence must not call a model")

    monkeypatch.setattr(
        main_module.LocalChatProvider, "chat", unexpected_model_call
    )
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    def add_item(session: Session, index: int) -> ContentItem:
        source = Source(
            name=f"Async synthesis source {index}",
            url=f"https://example.com/async-synthesis-{index}.xml",
            status="active",
            media_type="article",
            privacy_class="public",
            external_generation_allowed=True,
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source=source,
            external_id=f"async-synthesis-{index}",
            title=f"Async synthesis evidence {index}",
            url=f"https://example.com/async-synthesis-{index}",
            raw_content=f"Async evidence body {index}",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            document_type="normal_article",
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_text=raw.raw_content,
            url=raw.url,
            canonical_url=raw.url,
            content_hash=raw.content_hash,
            embedding_vector=POSTGRES_TEST_VECTOR,
            embedding_model="async-synthesis-test-model",
        )
        session.add(item)
        session.flush()
        return item

    with SessionLocal() as session:
        initial_items = [add_item(session, index) for index in (1, 2)]
        with clustering_run(
            session,
            scope_type="async-synthesis-initial",
            item_ids=[item.id for item in initial_items],
            rule_version="async-synthesis-v1",
        ):
            for item in initial_items:
                assign_cluster(session, item)
        event_row = session.scalar(select(ReaderEvent))
        assert event_row is not None
        initial_revision = session.get(EventRevision, event_row.current_revision_id)
        assert initial_revision is not None
        snapshot, evidence, source_count, fingerprint = create_evidence_snapshot(
            session, event_row, initial_revision
        )
        save_synthesis_version(
            session,
            event=event_row,
            snapshot=snapshot,
            source_count=source_count,
            provider="local",
            model="initial-model",
            generation_fingerprint=fingerprint,
            blocks=[
                {
                    "kind": "fact",
                    "body": "Initial fact.",
                    "attribution": "",
                    "citations": [
                        {
                            "evidence_version_uid": evidence[0][
                                "evidence_version_uid"
                            ],
                            "side": "support",
                        }
                    ],
                }
            ],
        )
        event_uid = event_row.uid
        third = add_item(session, 3)
        with clustering_run(
            session,
            scope_type="async-synthesis-older-target",
            item_ids=[third.id],
            rule_version="async-synthesis-v1",
        ):
            assign_cluster(session, third)
        session.refresh(event_row)
        older_revision = session.get(EventRevision, event_row.current_revision_id)
        assert older_revision is not None
        session.commit()

    click_barrier = Barrier(2)

    def click_update() -> str:
        with SessionLocal() as session:
            click_barrier.wait(timeout=10)
            return generate_event_synthesis(
                event_uid,
                SynthesisGenerateIn(provider="legacy"),
                session=session,
            ).task_status

    with ThreadPoolExecutor(max_workers=2) as executor:
        click_results = list(executor.map(lambda _index: click_update(), (1, 2)))
    assert click_results == ["pending", "pending"]

    with SessionLocal() as session:
        event_row = session.scalar(select(ReaderEvent).where(ReaderEvent.uid == event_uid))
        requests = list(
            session.scalars(
                select(GenerationRequest).where(
                    GenerationRequest.task_type == "evidence-review"
                )
            )
        )
        assert event_row is not None
        assert len(requests) == 1
        assert session.query(GenerationRequestPayload).count() == 1
        assert session.query(LLMTask).filter_by(task_type="evidence-review").count() == 0
        older_request_id = requests[0].id
        fourth = add_item(session, 4)
        with clustering_run(
            session,
            scope_type="async-synthesis-newer-target",
            item_ids=[fourth.id],
            rule_version="async-synthesis-v1",
        ):
            assign_cluster(session, fourth)
        session.refresh(event_row)
        newer_revision = session.get(EventRevision, event_row.current_revision_id)
        assert newer_revision is not None
        session.commit()

    with SessionLocal() as session:
        state = generate_event_synthesis(
            event_uid,
            SynthesisGenerateIn(provider="legacy"),
            session=session,
        )
        assert state.task_status == "pending"
        requests = list(
            session.scalars(
                select(GenerationRequest)
                .where(GenerationRequest.task_type == "evidence-review")
                .order_by(GenerationRequest.id)
            )
        )
        assert len(requests) == 2
        assert requests[0].id == older_request_id
        assert requests[0].input_fingerprint != requests[1].input_fingerprint
        assert session.query(GenerationRequestPayload).count() == 2
        assert session.query(LLMTask).filter_by(task_type="evidence-review").count() == 0
        assert model_calls == 0

    engine.dispose()


def test_p02_deployment_manifest_reconciles_event_authority_cutover(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0014_identity_key_kind_unique")
    engine = create_engine(postgres_url)
    with Session(engine) as session:
        add_legacy_event_backfill_seed(session)
    engine.dispose()

    target = database_target(postgres_url)
    before = collect_database_manifest(target)
    assert before["snapshot"]["legacy_user_state_evidence"]["storage"] == (
        "user_states"
    )

    upgrade_database(postgres_url)
    after = collect_database_manifest(target)
    assert after["snapshot"]["legacy_user_state_evidence"]["storage"] == (
        "migration_baselines"
    )
    assert after["snapshot"]["counts"]["user_states"] == 1
    assert after["snapshot"]["p02_projection_evidence"]["interaction_events"][
        "row_count"
    ] == 0
    assert compare_database_manifests(before, after) == {
        "ok": True,
        "mismatches": [],
    }

    upgrade_database(postgres_url)
    repeated = collect_database_manifest(target)
    assert repeated["snapshot"] == after["snapshot"]


def test_event_read_indexes_upgrade_existing_0044_database(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0044_ambiguous_upgrade_audit")
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        before_indexes = {
            index["name"]
            for index in inspect(connection).get_indexes(
                "cluster_event_projections"
            )
        }
        assert (
            "ix_cluster_event_projections_cluster_snapshot_id"
            not in before_indexes
        )
        assert "ix_cluster_event_projections_event_id" not in before_indexes
    engine.dispose()

    command.upgrade(config, "head")
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspect(connection).get_indexes(
                "cluster_event_projections"
            )
        }
        assert indexes["ix_cluster_event_projections_cluster_snapshot_id"] == (
            "cluster_id_snapshot",
            "id",
        )
        assert indexes["ix_cluster_event_projections_event_id"] == (
            "event_id",
            "id",
        )
    engine.dispose()


def test_event_authority_contract_migrates_late_cluster_state_and_forbids_reintroduction(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0046_event_read_indexes")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Event authority source",
            url="https://example.com/event-authority.xml",
        )
        raw = make_raw_entry(
            source_id=source_id,
            external_id="event-authority-entry",
            title="Event authority entry",
            url="https://example.com/event-authority",
            raw_content="Event authority evidence",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            document_type="normal_article",
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source_id,
            title=raw.title,
            content_text=raw.raw_content,
            url=raw.url,
            canonical_url=raw.url,
            published_at=raw.published_at,
            content_hash=raw.content_hash,
        )
        session.add(item)
        session.flush()
        with clustering_run(
            session,
            scope_type="event-authority-contract-test",
            item_ids=[item.id],
            rule_version="event-authority-contract-test-v1",
        ):
            cluster = assign_cluster(session, item)
        projection = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.cluster_id == cluster.id
            )
        )
        assert projection is not None
        legacy_state_id = insert_legacy_user_state(
            session,
            object_type="cluster",
            object_id=cluster.id,
            read_status="summary_seen",
            read_later=True,
            starred=True,
            updated_at=datetime(2026, 7, 15, 2, 0, tzinfo=timezone.utc),
        )
        session.commit()
        event_id = projection.event_id
        revision_id = projection.event_revision_id
        cluster_id = cluster.id
        item_id = item.id
    engine.dispose()

    command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with SessionLocal(bind=engine) as session:
        assert session.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        assert session.scalar(
            select(func.count(UserState.id)).where(
                UserState.object_type == "cluster"
            )
        ) == 0
        baseline = session.scalar(
            select(MigrationBaseline).where(
                MigrationBaseline.legacy_user_state_id == legacy_state_id
            )
        )
        assert baseline is not None
        assert baseline.legacy_object_id == cluster_id
        assert baseline.resolved_event_id == event_id
        assert baseline.resolved_revision_id == revision_id
        state = session.scalar(
            select(EventUserState).where(EventUserState.event_id == event_id)
        )
        assert state is not None
        assert state.baseline_id == baseline.id
        assert state.seen_revision_id == revision_id
        assert (state.read_status, state.read_later, state.starred) == (
            "summary_seen",
            True,
            True,
        )

        session.add(
            UserState(
                object_type="cluster",
                object_id=cluster_id,
                read_status="unread",
            )
        )
        with pytest.raises(IntegrityError, match="ck_user_state_event_authority"):
            session.commit()
        session.rollback()

        session.add(
            UserState(
                object_type="item",
                object_id=item_id,
                read_status="unread",
            )
        )
        session.commit()
    engine.dispose()


def test_event_authority_contract_rolls_back_when_old_and_event_state_disagree(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        seeded = add_user_state_baseline_seed(session)
    engine.dispose()
    command.upgrade(config, "0046_event_read_indexes")

    engine = create_engine(postgres_url)
    with Session(engine) as session:
        changed = session.execute(
            text(
                "UPDATE user_states SET starred = NOT starred "
                "WHERE object_type = 'cluster' AND object_id = :object_id"
            ),
            {"object_id": seeded["normal_cluster_id"]},
        )
        assert changed.rowcount == 1
        session.commit()
    engine.dispose()

    with pytest.raises(RuntimeError, match="event_authority_state_mismatch"):
        command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0046_event_read_indexes"
        )
        assert connection.scalar(
            text("SELECT count(*) FROM user_states WHERE object_type = 'cluster'")
        ) == 2
        assert connection.scalar(
            text(
                "SELECT count(*) FROM pg_constraint "
                "WHERE conname = 'ck_user_state_event_authority'"
            )
        ) == 0
    engine.dispose()


def test_event_authority_contract_rolls_back_when_cluster_states_share_event(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        seeded = add_user_state_baseline_seed(session)
    engine.dispose()
    command.upgrade(config, "0046_event_read_indexes")

    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        source_projection = connection.execute(
            text(
                "SELECT event_id, event_revision_id "
                "FROM cluster_event_projections WHERE cluster_id = :cluster_id"
            ),
            {"cluster_id": int(seeded["normal_cluster_id"])},
        ).mappings().one()
        connection.execute(
            text("ALTER TABLE cluster_event_projections DISABLE TRIGGER USER")
        )
        connection.execute(
            text(
                "UPDATE cluster_event_projections "
                "SET event_id = :event_id, event_revision_id = :revision_id "
                "WHERE cluster_id = :cluster_id"
            ),
            {
                "event_id": int(source_projection["event_id"]),
                "revision_id": int(source_projection["event_revision_id"]),
                "cluster_id": int(seeded["digest_cluster_id"]),
            },
        )
        connection.execute(
            text("ALTER TABLE cluster_event_projections ENABLE TRIGGER USER")
        )
    engine.dispose()

    with pytest.raises(RuntimeError, match="event_authority_state_ambiguous"):
        command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0046_event_read_indexes"
        )
        assert connection.scalar(
            text("SELECT count(*) FROM user_states WHERE object_type = 'cluster'")
        ) == 2
        assert connection.scalar(
            text(
                "SELECT count(*) FROM pg_constraint "
                "WHERE conname = 'ck_user_state_event_authority'"
            )
        ) == 0
    engine.dispose()


def test_feed_metric_upgrade_merges_duplicates_and_enforces_one_row_per_source(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0044_ambiguous_upgrade_audit")
    engine = create_engine(postgres_url)
    with Session(engine) as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Metric source",
            url="https://example.com/metric.xml",
        )
        session.commit()
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO feed_metrics "
                "(source_id, fetched_count, read_count, opened_count, starred_count, "
                " read_later_count, cluster_count, duplicate_count, updated_at) VALUES "
                "(:source_id, 3, 1, 0, 1, 0, 2, 1, CURRENT_TIMESTAMP), "
                "(:source_id, 4, 0, 1, 0, 1, 3, 2, CURRENT_TIMESTAMP)"
            ),
            {"source_id": source_id},
        )

    command.upgrade(config, "head")

    with engine.begin() as connection:
        rows = connection.execute(
            text(
                "SELECT fetched_count, read_count, opened_count, starred_count, "
                "read_later_count, cluster_count, duplicate_count "
                "FROM feed_metrics WHERE source_id = :source_id"
            ),
            {"source_id": source_id},
        ).mappings().all()
        assert rows == [
            {
                "fetched_count": 7,
                "read_count": 1,
                "opened_count": 1,
                "starred_count": 1,
                "read_later_count": 1,
                "cluster_count": 5,
                "duplicate_count": 3,
            }
        ]
        with pytest.raises(IntegrityError):
            connection.execute(
                text(
                    "INSERT INTO feed_metrics "
                    "(source_id, fetched_count, read_count, opened_count, starred_count, "
                    " read_later_count, cluster_count, duplicate_count, updated_at) "
                    "VALUES (:source_id, 0, 0, 0, 0, 0, 0, 0, CURRENT_TIMESTAMP)"
                ),
                {"source_id": source_id},
            )

    with Session(engine) as session:
        concurrent_source = Source(
            name="Concurrent metric source",
            url="https://example.com/concurrent-metric.xml",
        )
        session.add(concurrent_source)
        session.commit()
        concurrent_source_id = concurrent_source.id
    barrier = Barrier(2)

    def insert_metric() -> str:
        with Session(engine) as session:
            session.add(FeedMetric(source_id=concurrent_source_id))
            barrier.wait(timeout=10)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                return "duplicate"
            return "inserted"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _index: insert_metric(), range(2)))
    assert sorted(outcomes) == ["duplicate", "inserted"]
    with Session(engine) as session:
        assert session.scalar(
            select(func.count(FeedMetric.id)).where(
                FeedMetric.source_id == concurrent_source_id
            )
        ) == 1
    engine.dispose()


def test_projection_rebuild_empty_database_is_a_stable_noop(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    with Session(engine) as session:
        report = inspect_projection_rebuild(session)
        assert report["matches"] is True
        assert report["safe_to_apply"] is True
        assert report["unsupported_user_states"] == []
        assert report["expected_counts"] == {
            "event_user_states": 0,
            "user_states": 0,
            "feed_metrics": 0,
        }
        rebuilt = rebuild_projections(session)
        assert rebuilt["matches"] is True
        assert rebuilt["applied"] is False
        session.commit()
    engine.dispose()


def test_projection_rebuild_fails_closed_for_user_state_without_replay_fact(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        source = Source(
            name="Unsupported state source",
            url="https://example.com/unsupported-state.xml",
            feed_trust_score=42.0,
        )
        session.add(source)
        session.flush()
        session.add_all(
            [
                FeedMetric(
                    source_id=source.id,
                    fetched_count=11,
                    read_count=4,
                    opened_count=3,
                    starred_count=2,
                    read_later_count=1,
                    cluster_count=7,
                    duplicate_count=5,
                ),
                UserState(
                    object_type="preference",
                    object_id=123,
                    read_status="summary_seen",
                    read_later=False,
                    starred=False,
                ),
            ]
        )
        session.commit()

    def projection_snapshot() -> dict[str, object]:
        tables = (
            "sources",
            "event_user_states",
            "user_states",
            "feed_metrics",
            "migration_baselines",
            "interaction_events",
        )
        with engine.connect() as connection:
            return {
                table: list(
                    connection.scalars(
                        text(
                            f"SELECT to_jsonb(snapshot_row)::text FROM "
                            f"(SELECT * FROM {table} ORDER BY id) snapshot_row"
                        )
                    ).all()
                )
                for table in tables
            }

    before = projection_snapshot()
    for mode in ("verify", "dry-run"):
        with SessionLocal() as session:
            report = inspect_projection_rebuild(session, mode=mode)
            assert report["matches"] is False
            assert report["safe_to_apply"] is False
            assert report["unsupported_user_states"] == [
                {"object_type": "preference", "object_id": 123}
            ]
            session.rollback()
        assert projection_snapshot() == before

    result = run_explicit_maintenance(
        REBUILD_PROJECTIONS,
        session_factory=SessionLocal,
        prepare_database=lambda: None,
    )

    assert result.end_status == "failed"
    assert "无事实支撑" in result.failure_info
    assert "preference:123" in result.failure_info
    assert projection_snapshot() == before
    engine.dispose()


def insert_event_evidence_version(
    connection: Connection,
    fixture: EventProjectionFixture,
    *,
    uid: str,
    fingerprint: str,
    source_id: int | None = None,
    fragment: str | None = None,
) -> int:
    return connection.execute(
        text(
            "INSERT INTO event_evidence_versions "
            "(uid, evidence_id, version_fingerprint, raw_entry_id, "
            " source_entry_id, source_id, raw_revision_no, "
            " legacy_content_item_id, legacy_content_item_id_snapshot, "
            " fragment_fingerprint) "
            "VALUES (:uid, :evidence_id, :fingerprint, :raw_id, "
            " :source_entry_id, :source_id, 1, :item_id, :item_id, "
            " :fragment) RETURNING id"
        ),
        {
            "uid": uid,
            "evidence_id": fixture.evidence_id,
            "fingerprint": fingerprint,
            "raw_id": fixture.raw_id,
            "source_entry_id": fixture.source_entry_id,
            "source_id": fixture.source_id if source_id is None else source_id,
            "item_id": fixture.item_id,
            "fragment": (
                fixture.fragment_fingerprint if fragment is None else fragment
            ),
        },
    ).scalar_one()


def add_migration_source_entry_seed(
    session: Session,
    *,
    source_name: str,
    source_url: str,
    external_id: str,
    title: str,
    legacy_key_fingerprint: str,
) -> SourceEntryIdentity:
    source_id = insert_pre_privacy_source(
        session,
        name=source_name,
        url=source_url,
    )
    identity = SourceEntryIdentity(source_id=source_id, current_revision_no=1)
    session.add(identity)
    session.flush()
    session.add(
        SourceEntryKey(
            source_entry_id=identity.id,
            source_id=source_id,
            identity_kind="legacy",
            identity_key=f"legacy:{legacy_key_fingerprint}",
        )
    )
    revision = make_revision_input(external_id=external_id, title=title)
    session.add(
        RawEntry(
            **raw_entry_revision_values(
                source_id=source_id,
                source_entry_id=identity.id,
                revision_no=1,
                revision=revision,
            )
        )
    )
    return identity


def legacy_event_backfill_snapshot(connection: Connection) -> dict[str, list[str]]:
    tables = (
        "raw_entries",
        "source_entry_identities",
        "documents",
        "content_items",
        "clusters",
        "cluster_items",
        "user_states",
    )
    snapshots: dict[str, list[str]] = {}
    for table_name in tables:
        row_json = "to_jsonb(snapshot_row)"
        if table_name == "documents":
            row_json += (
                " - 'reading_html' - 'body_source' - 'web_fetch_status'"
            )
        snapshots[table_name] = list(
            connection.scalars(
                text(
                    f"SELECT ({row_json})::text "
                    f"FROM (SELECT * FROM {table_name} ORDER BY id) snapshot_row"
                )
            ).all()
        )
    return snapshots


def insert_legacy_user_state(
    session: Session,
    *,
    object_type: str,
    object_id: int,
    read_status: str,
    read_later: bool,
    starred: bool,
    updated_at: datetime | None = None,
) -> int:
    return int(
        session.scalar(
            text(
                "INSERT INTO user_states "
                "(object_type, object_id, read_status, read_later, starred, updated_at) "
                "VALUES (:object_type, :object_id, :read_status, :read_later, :starred, "
                ":updated_at) RETURNING id"
            ),
            {
                "object_type": object_type,
                "object_id": object_id,
                "read_status": read_status,
                "read_later": read_later,
                "starred": starred,
                "updated_at": updated_at or datetime.now(timezone.utc),
            },
        )
    )


def add_legacy_event_backfill_seed(session: Session) -> dict[str, object]:
    source_id = insert_pre_privacy_source(
        session,
        name="Legacy Event migration source",
        url="https://Example.com/feed/?utm_source=migration",
        created_at=datetime(2026, 7, 13, 6, 0, tzinfo=timezone.utc),
    )

    normal_raw = make_raw_entry(
        source_id=source_id,
        external_id="legacy-normal-article",
        title="Normal article raw title",
        url="https://example.com/normal?utm_campaign=reader",
        author="Normal Author",
        published_at=datetime(2026, 7, 13, 7, 0, tzinfo=timezone.utc),
        raw_content="Normal article evidence body.",
    )
    session.add(normal_raw)
    session.flush()
    session.add(
        SourceEntryKey(
            source_entry_id=normal_raw.source_entry_id,
            source_id=source_id,
            identity_kind="guid",
            identity_key=f"guid:{hashlib.sha256(b'legacy-normal-guid').hexdigest()}",
        )
    )
    normal_document = Document(
        raw_entry_id=normal_raw.id,
        document_type="normal_article",
        title=normal_raw.title,
        content_text=normal_raw.raw_content,
    )
    session.add(normal_document)
    session.flush()
    normal_item = ContentItem(
        document_id=normal_document.id,
        source_id=source_id,
        title="Normal article projected title",
        summary="Normal projected summary",
        content_text="Normal projected full body.",
        url="https://example.com/normal?utm_campaign=reader",
        canonical_url="https://example.com/normal",
        published_at=datetime(2026, 7, 13, 7, 15, tzinfo=timezone.utc),
        content_hash=hashlib.sha256(b"normal-projected-content").hexdigest(),
    )
    session.add(normal_item)

    digest_raw = make_raw_entry(
        source_id=source_id,
        external_id="legacy-digest-entry",
        title="Legacy morning digest",
        url="https://example.com/digest",
        author="Digest Author",
        published_at=datetime(2026, 7, 13, 8, 0, tzinfo=timezone.utc),
        raw_content="A digest containing two independently projected fragments.",
    )
    session.add(digest_raw)
    session.flush()
    digest_document = Document(
        raw_entry_id=digest_raw.id,
        document_type="mixed",
        title=digest_raw.title,
        content_text=digest_raw.raw_content,
    )
    session.add(digest_document)
    session.flush()
    digest_items = [
        ContentItem(
            document_id=digest_document.id,
            source_id=source_id,
            title=f"Digest fragment {index}",
            summary=f"Digest summary {index}",
            content_text=f"Frozen digest fragment body {index}.",
            url=f"https://example.com/digest/{index}",
            canonical_url=f"https://example.com/digest/{index}",
            published_at=datetime(2026, 7, 13, 8, index, tzinfo=timezone.utc),
            content_hash=hashlib.sha256(
                f"digest-fragment-{index}".encode()
            ).hexdigest(),
        )
        for index in (1, 2)
    ]
    session.add_all(digest_items)
    session.flush()

    newer_digest = make_revision_input(
        external_id=digest_raw.external_id,
        title="Updated digest that must not replace the frozen projection",
        url=digest_raw.url,
        author=digest_raw.author,
        published_at=datetime(2026, 7, 13, 9, 0, tzinfo=timezone.utc),
        raw_summary="New summary",
        raw_content="New raw revision waiting for an explicit projection rebuild.",
        content_hash=hashlib.sha256(b"newer-digest-revision").hexdigest(),
        fetched_at=datetime(2026, 7, 13, 9, 5, tzinfo=timezone.utc),
    )
    allocated = allocate_raw_entry_revision(
        session, digest_raw.source_entry_id, newer_digest
    )
    assert allocated.outcome is RawEntryRevisionOutcome.CREATED
    digest_raw.source_entry.projection_pending = True

    normal_cluster = Cluster(
        cluster_key="legacy-normal-cluster",
        title="Legacy normal cluster title",
        generated_title="Generated migration title",
        first_seen_at=datetime(2026, 7, 13, 7, 30, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 7, 13, 7, 45, tzinfo=timezone.utc),
    )
    digest_cluster = Cluster(
        cluster_key="legacy-digest-cluster",
        title="Legacy digest cluster title",
        generated_title="",
        first_seen_at=datetime(2026, 7, 13, 8, 30, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 7, 13, 8, 45, tzinfo=timezone.utc),
    )
    session.add_all([normal_cluster, digest_cluster])
    session.flush()
    session.add(
        ClusterItem(cluster_id=normal_cluster.id, content_item_id=normal_item.id)
    )
    session.add_all(
        ClusterItem(cluster_id=digest_cluster.id, content_item_id=item.id)
        for item in digest_items
    )
    insert_legacy_user_state(
        session,
        object_type="cluster",
        object_id=normal_cluster.id,
        read_status="read",
        read_later=True,
        starred=True,
    )
    insert_legacy_user_state(
        session,
        object_type="item",
        object_id=digest_items[0].id,
        read_status="seen",
        read_later=False,
        starred=False,
    )
    session.commit()
    return {
        "source_id": source_id,
        "normal_cluster_id": normal_cluster.id,
        "digest_cluster_id": digest_cluster.id,
        "normal_item_id": normal_item.id,
        "digest_item_ids": tuple(item.id for item in digest_items),
        "digest_raw_id": digest_raw.id,
        "digest_source_entry_id": digest_raw.source_entry_id,
    }


def test_legacy_clusters_backfill_one_event_and_frozen_revision_each(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        seeded = add_legacy_event_backfill_seed(session)

    with engine.connect() as connection:
        legacy_before = legacy_event_backfill_snapshot(connection)

    command.upgrade(config, "head")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        legacy_after = legacy_event_backfill_snapshot(connection)
        assert {
            table: rows
            for table, rows in legacy_after.items()
            if table != "user_states"
        } == {
            table: rows
            for table, rows in legacy_before.items()
            if table != "user_states"
        }
        assert connection.scalar(
            text("SELECT count(*) FROM user_states WHERE object_type = 'cluster'")
        ) == 0
        assert connection.scalar(
            text("SELECT count(*) FROM user_states WHERE object_type = 'item'")
        ) == 1
        assert connection.scalar(text("SELECT count(*) FROM clusters")) == 2
        assert connection.scalar(text("SELECT count(*) FROM events")) == 2
        assert connection.scalar(text("SELECT count(*) FROM event_revisions")) == 2
        assert connection.scalar(text("SELECT count(*) FROM event_evidence")) == 3
        assert connection.scalar(text("SELECT count(*) FROM event_evidence_versions")) == 3
        assert connection.scalar(text("SELECT count(*) FROM event_revision_evidence")) == 3
        assert connection.scalar(text("SELECT count(*) FROM cluster_event_projections")) == 2
        assert connection.scalar(
            text("SELECT count(*) FROM cluster_current_event_projections")
        ) == 2
        assert connection.scalar(
            text(
                "SELECT count(*) "
                "FROM cluster_current_event_projections AS current "
                "WHERE current.projection_id <> ("
                "  SELECT projection.id "
                "  FROM cluster_event_projections AS projection "
                "  WHERE projection.cluster_id = current.cluster_id "
                "  ORDER BY projection.id DESC LIMIT 1"
                ")"
            )
        ) == 0
        assert connection.scalar(
            text("SELECT count(*) FROM clustering_run_projection_predecessors")
        ) == 0
        assert connection.execute(
            text(
                "SELECT projection.reconciliation_kind, "
                "       projection.predecessor_projection_id, "
                "       projection.reconciliation_rule_version, "
                "       projection.before_evidence_fingerprint, "
                "       projection.after_evidence_fingerprint = "
                "           revision.evidence_fingerprint "
                "FROM cluster_event_projections projection "
                "JOIN event_revisions revision "
                "  ON revision.id = projection.event_revision_id "
                "ORDER BY projection.id"
            )
        ).all() == [
            ("initial", None, None, None, True),
            ("initial", None, None, None, True),
        ]
        assert connection.scalar(
            text(
                "SELECT count(*) FROM clustering_runs "
                "WHERE scope_type = 'legacy-cluster-backfill' "
                "AND status = 'completed' AND after_snapshot_finalized"
            )
        ) == 1
        assert connection.scalar(
            text(
                "SELECT count(*) FROM clustering_run_snapshot_seals seal "
                "JOIN clustering_runs run ON run.id = seal.run_id "
                "WHERE run.scope_type = 'legacy-cluster-backfill'"
            )
        ) == 2
        assert connection.scalar(
            text(
                "SELECT count(*) FROM clustering_run_memberships membership "
                "JOIN clustering_runs run ON run.id = membership.run_id "
                "WHERE run.scope_type = 'legacy-cluster-backfill'"
            )
        ) == 6

        mappings = connection.execute(
            text(
                "SELECT projection.cluster_id_snapshot, event.current_revision_id, "
                "       projection.event_revision_id, revision.title_snapshot, "
                "       revision.event_time_snapshot "
                "FROM cluster_event_projections projection "
                "JOIN events event ON event.id = projection.event_id "
                "JOIN event_revisions revision ON revision.id = projection.event_revision_id "
                "ORDER BY projection.cluster_id_snapshot"
            )
        ).mappings().all()
        assert {row["cluster_id_snapshot"] for row in mappings} == {
            seeded["normal_cluster_id"],
            seeded["digest_cluster_id"],
        }
        assert all(
            row["current_revision_id"] == row["event_revision_id"] for row in mappings
        )
        by_cluster = {row["cluster_id_snapshot"]: row for row in mappings}
        assert by_cluster[seeded["normal_cluster_id"]]["title_snapshot"] == (
            "Generated migration title"
        )
        assert by_cluster[seeded["normal_cluster_id"]]["event_time_snapshot"] == (
            datetime(2026, 7, 13, 7, 30, tzinfo=timezone.utc)
        )
        assert by_cluster[seeded["digest_cluster_id"]]["title_snapshot"] == (
            "Legacy digest cluster title"
        )
        assert connection.execute(
            text(
                "SELECT DISTINCT evidence_type, role "
                "FROM event_revision_evidence"
            )
        ).all() == [("article", "material")]
        assert connection.execute(
            text(
                "SELECT raw_entry_id, raw_revision_no "
                "FROM event_evidence_versions "
                "WHERE source_entry_id = :source_entry_id "
                "ORDER BY legacy_content_item_id_snapshot"
            ),
            {"source_entry_id": seeded["digest_source_entry_id"]},
        ).all() == [
            (seeded["digest_raw_id"], 1),
            (seeded["digest_raw_id"], 1),
        ]

        graph_before_second_upgrade = {
            table_name: list(
                connection.scalars(
                    text(
                        f"SELECT to_jsonb(snapshot_row)::text FROM "
                        f"(SELECT * FROM {table_name} ORDER BY id) snapshot_row"
                    )
                ).all()
            )
            for table_name in (
                "events",
                "event_revisions",
                "event_evidence",
                "event_evidence_versions",
                "event_revision_evidence",
                "cluster_event_projections",
            )
        }

    command.upgrade(config, "head")
    with engine.connect() as connection:
        for table_name, expected in graph_before_second_upgrade.items():
            actual = list(
                connection.scalars(
                    text(
                        f"SELECT to_jsonb(snapshot_row)::text FROM "
                        f"(SELECT * FROM {table_name} ORDER BY id) snapshot_row"
                    )
                ).all()
            )
            assert actual == expected

    with SessionLocal() as session:
        counts_before_runtime_projection = (
            session.scalar(select(func.count(ReaderEvent.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(EventEvidence.id))),
            session.scalar(select(func.count(EventEvidenceVersion.id))),
        )
        item_ids = [
            int(seeded["normal_item_id"]),
            *(int(item_id) for item_id in seeded["digest_item_ids"]),
        ]
        with clustering_run(
            session,
            scope_type="historical-backfill-idempotency",
            item_ids=item_ids,
            rule_version="historical-backfill-idempotency-v1",
        ):
            pass
        assert (
            session.scalar(select(func.count(ReaderEvent.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(EventEvidence.id))),
            session.scalar(select(func.count(EventEvidenceVersion.id))),
        ) == counts_before_runtime_projection
        assert session.get(SourceEntryIdentity, seeded["digest_source_entry_id"]).projection_pending

    engine.dispose()


def test_cluster_current_projection_backfills_latest_historical_mapping(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        seeded = add_legacy_event_backfill_seed(session)

    command.upgrade(config, "0067_source_deletion_tombstone")
    with SessionLocal() as session:
        item_ids = [
            int(seeded["normal_item_id"]),
            *(int(item_id) for item_id in seeded["digest_item_ids"]),
        ]
        with clustering_run(
            session,
            scope_type="current-projection-backfill-history",
            item_ids=item_ids,
            rule_version="current-projection-backfill-v1",
        ):
            pass

    with engine.connect() as connection:
        expected = dict(
            connection.execute(
                text(
                    "SELECT cluster_id, max(id) "
                    "FROM cluster_event_projections "
                    "WHERE cluster_id IS NOT NULL "
                    "GROUP BY cluster_id ORDER BY cluster_id"
                )
            ).all()
        )
        assert len(expected) == 2
        assert connection.execute(
            text(
                "SELECT cluster_id, count(*) "
                "FROM cluster_event_projections "
                "WHERE cluster_id IS NOT NULL "
                "GROUP BY cluster_id ORDER BY cluster_id"
            )
        ).all() == [
            (cluster_id, 2) for cluster_id in sorted(expected)
        ]

    command.upgrade(config, "head")
    with engine.connect() as connection:
        actual = dict(
            connection.execute(
                text(
                    "SELECT cluster_id, projection_id "
                    "FROM cluster_current_event_projections "
                    "ORDER BY cluster_id"
                )
            ).all()
        )
        assert actual == expected
    engine.dispose()


def test_legacy_cluster_backfill_fails_atomically_when_evidence_is_unlocatable(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        add_legacy_event_backfill_seed(session)
        session.add(
            Cluster(
                cluster_key="legacy-empty-cluster",
                title="This cluster has no frozen evidence",
            )
        )
        session.commit()
    with engine.connect() as connection:
        legacy_before = legacy_event_backfill_snapshot(connection)

    with pytest.raises(RuntimeError, match="legacy_cluster_evidence_unlocatable"):
        command.upgrade(config, "head")

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0025_event_evidence_identity"
        )
        assert legacy_event_backfill_snapshot(connection) == legacy_before
        for table_name in (
            "events",
            "event_revisions",
            "event_evidence",
            "event_evidence_versions",
            "event_revision_evidence",
            "cluster_event_projections",
        ):
            assert connection.scalar(text(f"SELECT count(*) FROM {table_name}")) == 0
        assert connection.scalar(
            text(
                "SELECT count(*) FROM clustering_runs "
                "WHERE scope_type = 'legacy-cluster-backfill'"
            )
        ) == 0
    engine.dispose()


def add_user_state_baseline_seed(session: Session) -> dict[str, object]:
    seeded = add_legacy_event_backfill_seed(session)
    normal_updated_at = datetime(2026, 7, 13, 10, 0, tzinfo=timezone.utc)
    default_updated_at = datetime(2026, 7, 13, 10, 5, tzinfo=timezone.utc)
    normal_state_id = session.scalar(
        text(
            "UPDATE user_states SET read_status = 'summary_seen', "
            "read_later = true, starred = true, updated_at = :updated_at "
            "WHERE object_type = 'cluster' AND object_id = :object_id RETURNING id"
        ),
        {
            "object_id": seeded["normal_cluster_id"],
            "updated_at": normal_updated_at,
        },
    )
    assert normal_state_id is not None
    item_state_id = session.scalar(
        text(
            "UPDATE user_states SET read_status = 'unread', "
            "read_later = false, starred = false, updated_at = :updated_at "
            "WHERE object_type = 'item' AND object_id = :object_id RETURNING id"
        ),
        {
            "object_id": seeded["digest_item_ids"][0],
            "updated_at": default_updated_at,
        },
    )
    assert item_state_id is not None
    digest_state_id = insert_legacy_user_state(
        session,
        object_type="cluster",
        object_id=int(seeded["digest_cluster_id"]),
        read_status="unread",
        read_later=True,
        starred=False,
        updated_at=datetime(2026, 7, 13, 10, 10, tzinfo=timezone.utc),
    )
    report_state_id = insert_legacy_user_state(
        session,
        object_type="report",
        object_id=700001,
        read_status="original_opened",
        read_later=False,
        starred=True,
        updated_at=datetime(2026, 7, 13, 10, 15, tzinfo=timezone.utc),
    )
    topic_state_id = insert_legacy_user_state(
        session,
        object_type="topic",
        object_id=800001,
        read_status="summary_seen",
        read_later=True,
        starred=False,
        updated_at=datetime(2026, 7, 13, 10, 20, tzinfo=timezone.utc),
    )
    no_state_cluster = Cluster(
        cluster_key="legacy-baseline-no-state",
        title="Legacy cluster without UserState",
        first_seen_at=datetime(2026, 7, 13, 10, 25, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 7, 13, 10, 30, tzinfo=timezone.utc),
    )
    session.add(no_state_cluster)
    session.flush()
    session.add(
        ClusterItem(
            cluster_id=no_state_cluster.id,
            content_item_id=int(seeded["normal_item_id"]),
        )
    )
    session.commit()
    return {
        **seeded,
        "no_state_cluster_id": no_state_cluster.id,
        "legacy_user_state_ids": {
            "normal_cluster": int(normal_state_id),
            "digest_cluster": digest_state_id,
            "item": int(item_state_id),
            "report": report_state_id,
            "topic": topic_state_id,
        },
    }


def test_user_state_baseline_backfill_is_lossless_and_event_projection_is_scoped(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        seeded = add_user_state_baseline_seed(session)

    command.upgrade(config, "0026_legacy_cluster_events")
    with engine.connect() as connection:
        legacy_states = {
            int(row["id"]): row
            for row in connection.execute(
                text(
                    "SELECT id, object_type, object_id, read_status, read_later, "
                    "       starred, updated_at FROM user_states ORDER BY id"
                )
            ).mappings()
        }
        mappings = {
            int(row["cluster_id_snapshot"]): (
                int(row["event_id"]),
                int(row["event_revision_id"]),
            )
            for row in connection.execute(
                text(
                    "SELECT cluster_id_snapshot, event_id, event_revision_id "
                    "FROM cluster_event_projections"
                )
            ).mappings()
        }

    command.upgrade(config, "head")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        remaining_user_state_ids = set(
            connection.scalars(text("SELECT id FROM user_states ORDER BY id")).all()
        )
        assert remaining_user_state_ids == {
            user_state_id
            for user_state_id, state in legacy_states.items()
            if state["object_type"] != "cluster"
        }

        baselines = connection.execute(
            text(
                "SELECT id, idempotency_key, migration_version, "
                "       legacy_user_state_id, legacy_object_type, legacy_object_id, "
                "       resolved_event_id, resolved_revision_id, read_status, "
                "       read_later, starred, source_updated_at "
                "FROM migration_baselines ORDER BY legacy_user_state_id"
            )
        ).mappings().all()
        assert len(baselines) == len(legacy_states) == 5
        baseline_by_state = {
            int(row["legacy_user_state_id"]): row for row in baselines
        }
        for user_state_id, state in legacy_states.items():
            baseline = baseline_by_state[user_state_id]
            assert baseline["idempotency_key"] == hashlib.sha256(
                f"legacy-user-state-baseline-v1:{user_state_id}".encode()
            ).hexdigest()
            assert baseline["migration_version"] == "legacy-user-state-baseline-v1"
            assert baseline["legacy_object_type"] == state["object_type"]
            assert baseline["legacy_object_id"] == state["object_id"]
            assert baseline["read_status"] == state["read_status"]
            assert baseline["read_later"] == state["read_later"]
            assert baseline["starred"] == state["starred"]
            assert baseline["source_updated_at"] == state["updated_at"]
            if state["object_type"] == "cluster":
                expected_event, expected_revision = mappings[int(state["object_id"])]
                assert baseline["resolved_event_id"] == expected_event
                assert baseline["resolved_revision_id"] == expected_revision
            else:
                assert baseline["resolved_event_id"] is None
                assert baseline["resolved_revision_id"] is None

        event_states = connection.execute(
            text(
                "SELECT baseline_id, event_id, seen_revision_id, read_status, "
                "       read_later, starred, updated_at "
                "FROM event_user_states ORDER BY event_id"
            )
        ).mappings().all()
        assert len(event_states) == 2
        event_state_by_event = {int(row["event_id"]): row for row in event_states}
        normal_event, normal_revision = mappings[int(seeded["normal_cluster_id"])]
        digest_event, _digest_revision = mappings[int(seeded["digest_cluster_id"])]
        no_state_event, _no_state_revision = mappings[
            int(seeded["no_state_cluster_id"])
        ]
        normal_event_state = event_state_by_event[normal_event]
        normal_legacy_state = legacy_states[
            int(seeded["legacy_user_state_ids"]["normal_cluster"])
        ]
        assert normal_event_state["seen_revision_id"] == normal_revision
        assert normal_event_state["read_status"] == normal_legacy_state["read_status"]
        assert normal_event_state["read_later"] == normal_legacy_state["read_later"]
        assert normal_event_state["starred"] == normal_legacy_state["starred"]
        assert normal_event_state["updated_at"] == normal_legacy_state["updated_at"]
        assert normal_event_state["baseline_id"] == baseline_by_state[
            int(seeded["legacy_user_state_ids"]["normal_cluster"])
        ]["id"]
        assert event_state_by_event[digest_event]["seen_revision_id"] is None
        assert no_state_event not in event_state_by_event
        assert connection.scalar(text("SELECT count(*) FROM interaction_events")) == 0

        baseline_snapshot = list(
            connection.scalars(
                text(
                    "SELECT to_jsonb(snapshot_row)::text FROM "
                    "(SELECT * FROM migration_baselines ORDER BY id) snapshot_row"
                )
            ).all()
        )
        event_state_snapshot = list(
            connection.scalars(
                text(
                    "SELECT to_jsonb(snapshot_row)::text FROM "
                    "(SELECT * FROM event_user_states ORDER BY id) snapshot_row"
                )
            ).all()
        )

    command.upgrade(config, "head")
    with engine.connect() as connection:
        assert list(
            connection.scalars(
                text(
                    "SELECT to_jsonb(snapshot_row)::text FROM "
                    "(SELECT * FROM migration_baselines ORDER BY id) snapshot_row"
                )
            ).all()
        ) == baseline_snapshot
        assert list(
            connection.scalars(
                text(
                    "SELECT to_jsonb(snapshot_row)::text FROM "
                    "(SELECT * FROM event_user_states ORDER BY id) snapshot_row"
                )
            ).all()
        ) == event_state_snapshot

        with pytest.raises(IntegrityError):
            connection.execute(
                text(
                    "INSERT INTO event_user_states "
                    "(event_id, seen_revision_id, read_status, read_later, starred, "
                    " updated_at) VALUES (:event_id, :seen_revision_id, 'summary_seen', "
                    " false, false, CURRENT_TIMESTAMP)"
                ),
                {
                    "event_id": no_state_event,
                    "seen_revision_id": normal_revision,
                },
            )
    engine.dispose()


def test_postgres_concurrent_event_operation_is_idempotent_for_new_and_baseline_state(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        seeded = add_user_state_baseline_seed(session)
    command.upgrade(config, "head")

    with SessionLocal() as session:
        projections = {
            projection.cluster_id_snapshot: projection
            for projection in session.scalars(select(ClusterEventProjection)).all()
        }

        def identity_for(cluster_id: int) -> tuple[str, str, int]:
            projection = projections[cluster_id]
            event = session.get(ReaderEvent, projection.event_id)
            revision = session.get(EventRevision, projection.event_revision_id)
            assert event is not None
            assert revision is not None
            return event.uid, revision.uid, event.id

        no_state_identity = identity_for(int(seeded["no_state_cluster_id"]))
        baseline_identity = identity_for(int(seeded["normal_cluster_id"]))

    def apply_twice(mutation: EventUserStateMutationIn) -> list[dict[str, object]]:
        barrier = Barrier(2)

        def apply_once() -> dict[str, object]:
            with SessionLocal() as session:
                barrier.wait(timeout=10)
                result = apply_event_user_state_mutation(session, mutation)
                session.commit()
                return result.model_dump(mode="json")

        with ThreadPoolExecutor(max_workers=2) as executor:
            return list(executor.map(lambda _index: apply_once(), range(2)))

    no_state_results = apply_twice(
        EventUserStateMutationIn(
            event_uid=no_state_identity[0],
            observed_revision_uid=no_state_identity[1],
            operation_id="77777777-7777-4777-8777-777777777777",
            action="starred_set",
            value=True,
        )
    )
    assert no_state_results[0] == no_state_results[1]

    baseline_results = apply_twice(
        EventUserStateMutationIn(
            event_uid=baseline_identity[0],
            observed_revision_uid=baseline_identity[1],
            operation_id="88888888-8888-4888-8888-888888888888",
            action="read_later_set",
            value=False,
        )
    )
    assert baseline_results[0] == baseline_results[1]

    read_results = apply_twice(
        EventUserStateMutationIn(
            event_uid=no_state_identity[0],
            observed_revision_uid=no_state_identity[1],
            operation_id="99999999-9999-4999-8999-999999999999",
            action="read_status_set",
            value="summary_seen",
        )
    )
    assert read_results[0] == read_results[1]
    assert read_results[0]["read_status"] == "summary_seen"
    assert read_results[0]["seen_revision_uid"] == no_state_identity[1]

    with SessionLocal() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 3
        new_state = session.scalar(
            select(EventUserState).where(
                EventUserState.event_id == no_state_identity[2]
            )
        )
        baseline_state = session.scalar(
            select(EventUserState).where(
                EventUserState.event_id == baseline_identity[2]
            )
        )
        assert new_state is not None
        assert new_state.baseline_id is None
        assert new_state.starred is True
        assert new_state.read_status == "summary_seen"
        assert new_state.seen_revision_id is not None
        assert baseline_state is not None
        assert baseline_state.baseline_id is not None
        assert baseline_state.read_later is False
        assert session.scalar(
            select(func.count(UserState.id)).where(
                UserState.object_type == "cluster"
            )
        ) == 0
        metrics = session.scalars(select(FeedMetric)).all()
        assert sum(metric.starred_count or 0 for metric in metrics) == 1
        assert sum(metric.read_later_count or 0 for metric in metrics) == 0
        assert sum(metric.read_count or 0 for metric in metrics) == 1
    engine.dispose()


def test_projection_rebuild_restores_event_object_state_and_metrics(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        seeded = add_user_state_baseline_seed(session)
    command.upgrade(config, "head")

    with SessionLocal() as session:
        projection = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.cluster_id_snapshot
                == seeded["no_state_cluster_id"]
            )
        )
        assert projection is not None
        event_row = session.get(ReaderEvent, projection.event_id)
        revision = session.get(EventRevision, projection.event_revision_id)
        assert event_row is not None
        assert revision is not None
        source_id = int(seeded["source_id"])
        event_mutations = [
            EventUserStateMutationIn(
                event_uid=event_row.uid,
                observed_revision_uid=revision.uid,
                operation_id="32320000-0000-4000-8000-000000000001",
                action="starred_set",
                value=True,
            ),
            EventUserStateMutationIn(
                event_uid=event_row.uid,
                observed_revision_uid=revision.uid,
                operation_id="32320000-0000-4000-8000-000000000002",
                action="read_later_set",
                value=True,
            ),
            EventUserStateMutationIn(
                event_uid=event_row.uid,
                observed_revision_uid=revision.uid,
                operation_id="32320000-0000-4000-8000-000000000003",
                action="read_status_set",
                value="summary_seen",
            ),
            EventUserStateMutationIn(
                event_uid=event_row.uid,
                observed_revision_uid=revision.uid,
                operation_id="32320000-0000-4000-8000-000000000004",
                action="read_status_set",
                value="original_opened",
                source_id=source_id,
            ),
        ]
        for mutation in event_mutations:
            apply_event_user_state_mutation(session, mutation)
        assert (
            apply_event_user_state_mutation(session, event_mutations[0]).starred
            is True
        )
        item_id = int(seeded["digest_item_ids"][0])
        for mutation in (
            UserStatePatch(
                operation_id="32320000-0000-4000-8000-000000000005",
                read_status="summary_seen",
            ),
            UserStatePatch(
                operation_id="32320000-0000-4000-8000-000000000006",
                read_later=True,
            ),
            UserStatePatch(
                operation_id="32320000-0000-4000-8000-000000000007",
                starred=True,
            ),
        ):
            apply_object_user_state_mutation(session, "item", item_id, mutation)
        session.commit()

    def projection_snapshot(session: Session) -> dict[str, list[tuple[object, ...]]]:
        return {
            "event": list(
                session.execute(
                    select(
                        EventUserState.baseline_id,
                        EventUserState.event_id,
                        EventUserState.seen_revision_id,
                        EventUserState.read_status,
                        EventUserState.read_later,
                        EventUserState.starred,
                        EventUserState.updated_at,
                    ).order_by(EventUserState.event_id)
                ).all()
            ),
            "object": list(
                session.execute(
                    select(
                        UserState.object_type,
                        UserState.object_id,
                        UserState.read_status,
                        UserState.read_later,
                        UserState.starred,
                        UserState.updated_at,
                    ).order_by(UserState.object_type, UserState.object_id)
                ).all()
            ),
            "metric": list(
                session.execute(
                    select(
                        FeedMetric.source_id,
                        FeedMetric.fetched_count,
                        FeedMetric.read_count,
                        FeedMetric.opened_count,
                        FeedMetric.starred_count,
                        FeedMetric.read_later_count,
                        FeedMetric.cluster_count,
                        FeedMetric.duplicate_count,
                    ).order_by(FeedMetric.source_id)
                ).all()
            ),
        }

    with SessionLocal() as session:
        expected = projection_snapshot(session)
        fact_snapshot = list(
            session.execute(
                select(
                    InteractionEvent.id,
                    InteractionEvent.operation_id,
                    InteractionEvent.payload,
                ).order_by(InteractionEvent.recorded_at, InteractionEvent.id)
            ).all()
        )
        session.execute(delete(EventUserState))
        changed_object_state = session.scalar(
            select(UserState).order_by(UserState.id).limit(1)
        )
        assert changed_object_state is not None
        changed_object_state.starred = not changed_object_state.starred
        for metric in session.scalars(select(FeedMetric)).all():
            metric.read_count = 99
            metric.opened_count = 98
            metric.starred_count = 97
            metric.read_later_count = 96
        session.commit()

    with SessionLocal() as session:
        damaged = projection_snapshot(session)
        verify = inspect_projection_rebuild(session, mode="verify")
        assert verify["matches"] is False
        assert verify["safe_to_apply"] is True
        assert verify["differences"]["event_user_states"]["missing"] > 0
        assert verify["differences"]["user_states"]["changed"] > 0
        assert projection_snapshot(session) == damaged
        session.rollback()
    with SessionLocal() as session:
        dry_run = inspect_projection_rebuild(session, mode="dry-run")
        assert dry_run["matches"] is False
        assert projection_snapshot(session) == damaged
        session.rollback()

    with SessionLocal() as session:
        rebuilt = rebuild_projections(session)
        assert rebuilt["matches"] is True
        session.commit()
    with SessionLocal() as session:
        assert projection_snapshot(session) == expected
        assert fact_snapshot == list(
            session.execute(
                select(
                    InteractionEvent.id,
                    InteractionEvent.operation_id,
                    InteractionEvent.payload,
                ).order_by(InteractionEvent.recorded_at, InteractionEvent.id)
            ).all()
        )
        repeated = rebuild_projections(session)
        assert repeated["matches"] is True
        session.commit()
    with SessionLocal() as session:
        assert projection_snapshot(session) == expected
        assert session.scalar(select(func.count(InteractionEvent.id))) == 7
    engine.dispose()


def test_projection_rebuild_uses_recorded_order_and_stable_id_tie_breaker(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        seeded = add_user_state_baseline_seed(session)
    command.upgrade(config, "head")

    recorded_at = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
    with SessionLocal() as session:
        projection = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.cluster_id_snapshot
                == seeded["no_state_cluster_id"]
            )
        )
        assert projection is not None
        source_id = int(seeded["source_id"])
        events = [
            InteractionEvent(
                id="32000000-0000-4000-8000-000000000001",
                operation_id="projection-order-star-on",
                target_kind="event",
                event_id=projection.event_id,
                observed_revision_id=projection.event_revision_id,
                action="starred_set",
                set_value=True,
                payload={"metric_source_ids": [source_id]},
                occurred_at=datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc),
                recorded_at=recorded_at,
            ),
            InteractionEvent(
                id="32000000-0000-4000-8000-000000000002",
                operation_id="projection-order-star-off",
                target_kind="event",
                event_id=projection.event_id,
                observed_revision_id=projection.event_revision_id,
                action="starred_set",
                set_value=False,
                payload={"metric_source_ids": []},
                occurred_at=datetime(2026, 7, 15, 11, 0, tzinfo=timezone.utc),
                recorded_at=recorded_at,
            ),
            InteractionEvent(
                id="32000000-0000-4000-8000-000000000003",
                operation_id="projection-order-seen",
                target_kind="event",
                event_id=projection.event_id,
                observed_revision_id=projection.event_revision_id,
                action="read_status_set",
                set_value="summary_seen",
                payload={
                    "read_metric_source_ids": [source_id],
                    "opened_metric_source_ids": [],
                },
                occurred_at=datetime(2026, 7, 15, 10, 0, tzinfo=timezone.utc),
                recorded_at=recorded_at,
            ),
            InteractionEvent(
                id="32000000-0000-4000-8000-000000000004",
                operation_id="projection-order-unread",
                target_kind="event",
                event_id=projection.event_id,
                observed_revision_id=projection.event_revision_id,
                action="read_status_set",
                set_value="unread",
                payload={
                    "read_metric_source_ids": [],
                    "opened_metric_source_ids": [],
                },
                occurred_at=datetime(2026, 7, 15, 9, 0, tzinfo=timezone.utc),
                recorded_at=recorded_at,
            ),
            InteractionEvent(
                id="32000000-0000-4000-8000-000000000005",
                operation_id="projection-order-read-later",
                target_kind="event",
                event_id=projection.event_id,
                observed_revision_id=projection.event_revision_id,
                action="read_later_set",
                set_value=True,
                payload={"metric_source_ids": [source_id]},
                occurred_at=datetime(2026, 7, 15, 8, 30, tzinfo=timezone.utc),
                recorded_at=recorded_at,
            ),
        ]
        session.add_all(reversed(events))
        session.commit()

    with SessionLocal() as session:
        result = rebuild_projections(session)
        assert result["matches"] is True
        session.commit()
    with SessionLocal() as session:
        state = session.scalar(
            select(EventUserState).where(
                EventUserState.event_id == projection.event_id
            )
        )
        assert state is not None
        assert state.read_status == "unread"
        assert state.seen_revision_id == projection.event_revision_id
        assert state.starred is False
        assert state.read_later is True
        assert state.updated_at == datetime(
            2026, 7, 15, 8, 30, tzinfo=timezone.utc
        )
        metric = session.scalar(
            select(FeedMetric).where(FeedMetric.source_id == source_id)
        )
        assert metric is not None
        assert metric.read_count == 0
        assert metric.opened_count == 0
        assert metric.starred_count == 0
        assert metric.read_later_count == 1
    engine.dispose()


def test_projection_rebuild_failure_preserves_previous_projection_and_facts(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        seeded = add_user_state_baseline_seed(session)
    command.upgrade(config, "head")

    with SessionLocal() as session:
        projection = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.cluster_id_snapshot
                == seeded["no_state_cluster_id"]
            )
        )
        assert projection is not None
        event_row = session.get(ReaderEvent, projection.event_id)
        revision = session.get(EventRevision, projection.event_revision_id)
        assert event_row is not None
        assert revision is not None
        apply_event_user_state_mutation(
            session,
            EventUserStateMutationIn(
                event_uid=event_row.uid,
                observed_revision_uid=revision.uid,
                operation_id="32320000-0000-4000-8000-000000000099",
                action="starred_set",
                value=True,
            ),
        )
        session.commit()
        state = session.scalar(
            select(EventUserState).where(EventUserState.event_id == event_row.id)
        )
        assert state is not None
        state.starred = False
        session.commit()

    def snapshot(table: str) -> list[str]:
        with engine.connect() as connection:
            return list(
                connection.scalars(
                    text(
                        f"SELECT to_jsonb(snapshot_row)::text FROM "
                        f"(SELECT * FROM {table} ORDER BY id) snapshot_row"
                    )
                ).all()
            )

    old_event_states = snapshot("event_user_states")
    old_user_states = snapshot("user_states")
    old_metrics = snapshot("feed_metrics")
    baselines = snapshot("migration_baselines")
    interactions = snapshot("interaction_events")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE FUNCTION fail_projection_rebuild() RETURNS trigger "
                "LANGUAGE plpgsql AS $$ BEGIN "
                "RAISE EXCEPTION 'forced_projection_rebuild_failure'; END $$"
            )
        )
        connection.execute(
            text(
                "CREATE TRIGGER trg_test_projection_rebuild_failure "
                "BEFORE INSERT ON event_user_states FOR EACH ROW "
                "EXECUTE FUNCTION fail_projection_rebuild()"
            )
        )

    with SessionLocal() as session:
        with pytest.raises(DBAPIError, match="forced_projection_rebuild_failure"):
            rebuild_projections(session)
        session.rollback()

    assert snapshot("event_user_states") == old_event_states
    assert snapshot("user_states") == old_user_states
    assert snapshot("feed_metrics") == old_metrics
    assert snapshot("migration_baselines") == baselines
    assert snapshot("interaction_events") == interactions
    engine.dispose()


def test_projection_rebuild_does_not_regress_baseline_seen_revision(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        source = Source(
            name="Projection revision waterline",
            url="https://example.com/projection-revision-waterline.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        item = add_event_split_item(
            session,
            source_id=source.id,
            prefix="projection-revision-waterline",
            suffix="one",
        )
        with clustering_run(
            session,
            scope_type="projection-revision-waterline-initial",
            item_ids=[item.id],
            rule_version="projection-revision-waterline-v1",
        ):
            cluster = assign_cluster(session, item)
        event_row = session.scalar(select(ReaderEvent))
        first_revision = session.scalar(select(EventRevision))
        assert event_row is not None
        assert first_revision is not None
        with clustering_run(
            session,
            scope_type="projection-revision-waterline-change",
            item_ids=[item.id],
            rule_version="projection-revision-waterline-v1",
        ):
            item.content_text = "Materially changed evidence for revision two"
        session.refresh(event_row)
        second_revision = session.get(EventRevision, event_row.current_revision_id)
        assert second_revision is not None
        assert second_revision.revision_no == 2

        occurred_at = datetime(2026, 7, 15, 9, 0, tzinfo=timezone.utc)
        historical_state_id = 900001
        baseline = MigrationBaseline(
            idempotency_key=hashlib.sha256(
                f"legacy-user-state-baseline-v1:{historical_state_id}".encode()
            ).hexdigest(),
            migration_version="legacy-user-state-baseline-v1",
            legacy_user_state_id=historical_state_id,
            legacy_object_type="cluster",
            legacy_object_id=cluster.id,
            resolved_event_id=event_row.id,
            resolved_revision_id=second_revision.id,
            read_status="summary_seen",
            read_later=False,
            starred=False,
            source_updated_at=occurred_at,
            recorded_at=occurred_at,
        )
        session.add(baseline)
        session.flush()
        session.add_all(
            [
                EventUserState(
                    baseline_id=baseline.id,
                    event_id=event_row.id,
                    seen_revision_id=second_revision.id,
                    read_status="summary_seen",
                    read_later=False,
                    starred=False,
                    updated_at=occurred_at,
                ),
                InteractionEvent(
                    id="32000000-0000-4000-8000-000000000099",
                    operation_id="projection-old-tab-read",
                    target_kind="event",
                    event_id=event_row.id,
                    observed_revision_id=first_revision.id,
                    action="read_status_set",
                    set_value="summary_seen",
                    payload={
                        "read_metric_source_ids": [],
                        "opened_metric_source_ids": [],
                    },
                    occurred_at=datetime(
                        2026, 7, 15, 10, 0, tzinfo=timezone.utc
                    ),
                    recorded_at=datetime(
                        2026, 7, 15, 10, 0, tzinfo=timezone.utc
                    ),
                ),
            ]
        )
        session.commit()

    with SessionLocal() as session:
        rebuild_projections(session)
        session.commit()
    with SessionLocal() as session:
        rebuilt = session.scalar(
            select(EventUserState).where(EventUserState.event_id == event_row.id)
        )
        assert rebuilt is not None
        assert rebuilt.seen_revision_id == second_revision.id
    engine.dispose()


def test_projection_rebuild_does_not_inherit_state_into_lineage_children(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        source = Source(
            name="Projection split isolation",
            url="https://example.com/projection-split-isolation.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        first = add_event_split_item(
            session,
            source_id=source.id,
            prefix="projection-split-isolation",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source.id,
            prefix="projection-split-isolation",
            suffix="two",
        )
        with clustering_run(
            session,
            scope_type="projection-split-isolation-initial",
            item_ids=[first.id, second.id],
            rule_version="projection-split-isolation-v1",
        ):
            original_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=original_cluster.id,
                    content_item_id=second.id,
                )
            )
        parent_projection = session.scalar(select(ClusterEventProjection))
        assert parent_projection is not None
        parent = session.get(ReaderEvent, parent_projection.event_id)
        parent_revision = session.get(
            EventRevision, parent_projection.event_revision_id
        )
        assert parent is not None
        assert parent_revision is not None
        apply_event_user_state_mutation(
            session,
            EventUserStateMutationIn(
                event_uid=parent.uid,
                observed_revision_uid=parent_revision.uid,
                operation_id="32320000-0000-4000-8000-000000000201",
                action="starred_set",
                value=True,
            ),
        )
        session.commit()

        with clustering_run(
            session,
            scope_type="projection-split-isolation-change",
            item_ids=[first.id, second.id],
            rule_version="projection-split-isolation-v1",
        ) as split_run_id:
            link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == original_cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            assert link is not None
            session.delete(link)
            second_cluster = Cluster(
                cluster_key="projection-split-isolation-second",
                title="Projection split isolation second",
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=second.id,
                )
            )
        child_projections = list(
            session.scalars(
                select(ClusterEventProjection).where(
                    ClusterEventProjection.clustering_run_id == split_run_id
                )
            )
        )
        assert len(child_projections) == 2
        assert {row.reconciliation_kind for row in child_projections} == {"split"}
        session.add_all(
            EventUserState(
                event_id=row.event_id,
                seen_revision_id=row.event_revision_id,
                read_status="summary_seen",
                read_later=True,
                starred=True,
            )
            for row in child_projections
        )
        session.commit()

    child_event_ids = {row.event_id for row in child_projections}
    with SessionLocal() as session:
        report = inspect_projection_rebuild(session)
        assert report["matches"] is False
        assert report["safe_to_apply"] is True
        assert report["unsupported_user_states"] == []
        rebuild_projections(session)
        session.commit()
    with SessionLocal() as session:
        parent_state = session.scalar(
            select(EventUserState).where(EventUserState.event_id == parent.id)
        )
        assert parent_state is not None
        assert parent_state.starred is True
        assert session.scalar(
            select(func.count(EventUserState.id)).where(
                EventUserState.event_id.in_(child_event_ids)
            )
        ) == 0
        assert session.scalar(
            select(func.count(UserState.id)).where(
                UserState.object_type == "cluster",
                UserState.object_id.in_([original_cluster.id, second_cluster.id]),
            )
        ) == 0
    engine.dispose()


def test_postgres_concurrent_bulk_read_reuses_the_fixed_operation_manifest(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        seeded = add_user_state_baseline_seed(session)
    command.upgrade(config, "head")

    cluster_ids = [
        int(seeded["digest_cluster_id"]),
        int(seeded["no_state_cluster_id"]),
    ]
    with SessionLocal() as session:
        projections = {
            projection.cluster_id_snapshot: projection
            for projection in session.scalars(
                select(ClusterEventProjection).where(
                    ClusterEventProjection.cluster_id_snapshot.in_(cluster_ids)
                )
            ).all()
        }
        targets: list[BulkReadTarget] = []
        expected: dict[int, int] = {}
        for index, cluster_id in enumerate(cluster_ids, start=1):
            projection = projections[cluster_id]
            event = session.get(ReaderEvent, projection.event_id)
            revision = session.get(EventRevision, projection.event_revision_id)
            assert event is not None
            assert revision is not None
            expected[event.id] = revision.id
            targets.append(
                BulkReadTarget(
                    target_kind="event",
                    event_uid=event.uid,
                    observed_revision_uid=revision.uid,
                    operation_id=(
                        f"26262626-2626-4262-8262-2626262626{index:02d}"
                    ),
                )
            )
    manifest = BulkReadManifest(targets=targets)
    barrier = Barrier(2)

    def confirm_once() -> int:
        with SessionLocal() as session:
            barrier.wait(timeout=10)
            lock_operation_id(
                session,
                "bulk-read-batch:26262626-2626-4262-8262-262626262626",
            )
            updated = confirm_bulk_read_manifest(session, manifest)
            session.commit()
            return updated

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: confirm_once(), range(2)))

    assert results == [2, 2]
    with SessionLocal() as session:
        interactions = session.scalars(
            select(InteractionEvent).where(
                InteractionEvent.operation_id.in_(
                    [target.operation_id for target in targets]
                )
            )
        ).all()
        assert len(interactions) == 2
        states = session.scalars(
            select(EventUserState).where(EventUserState.event_id.in_(expected))
        ).all()
        assert {state.event_id: state.seen_revision_id for state in states} == expected
        assert all(state.read_status == "summary_seen" for state in states)
    engine.dispose()


def test_postgres_crossing_bulk_event_batches_lock_targets_in_one_order(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "head")
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    def create_pair(suffix: str) -> tuple[str, str, int]:
        with SessionLocal() as session:
            source = Source(
                name=f"Bulk crossing source {suffix}",
                url=f"https://example.com/bulk-crossing-{suffix}.xml",
                status="active",
                media_type="article",
            )
            session.add(source)
            session.flush()
            raw = make_raw_entry(
                source=source,
                external_id=f"bulk-crossing-{suffix}",
                title=f"Bulk crossing {suffix}",
                raw_content=f"Bulk crossing evidence {suffix}",
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                document_type="normal_article",
                title=raw.title,
                content_text=raw.raw_content,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=raw.title,
                content_text=raw.raw_content,
                content_hash=raw.content_hash,
            )
            session.add(item)
            session.flush()
            with clustering_run(
                session,
                scope_type=f"bulk-crossing-{suffix}",
                item_ids=[item.id],
                rule_version="bulk-crossing-v1",
            ):
                cluster = assign_cluster(session, item)
            projection = session.scalar(
                select(ClusterEventProjection).where(
                    ClusterEventProjection.cluster_id == cluster.id
                )
            )
            assert projection is not None
            event_row = session.get(ReaderEvent, projection.event_id)
            revision = session.get(EventRevision, projection.event_revision_id)
            assert event_row is not None
            assert revision is not None
            session.commit()
            return event_row.uid, revision.uid, item.id

    first_event_uid, first_revision_uid, _first_item_id = create_pair("first")
    second_event_uid, second_revision_uid, _second_item_id = create_pair("second")
    first_manifest = BulkReadManifest(
        targets=[
            BulkReadTarget(
                target_kind="event",
                event_uid=first_event_uid,
                observed_revision_uid=first_revision_uid,
                operation_id="51515151-5151-4151-8151-515151515151",
            ),
            BulkReadTarget(
                target_kind="event",
                event_uid=second_event_uid,
                observed_revision_uid=second_revision_uid,
                operation_id="52525252-5252-4252-8252-525252525252",
            ),
        ]
    )
    second_manifest = BulkReadManifest(
        targets=[
            BulkReadTarget(
                target_kind="event",
                event_uid=second_event_uid,
                observed_revision_uid=second_revision_uid,
                operation_id="53535353-5353-4353-8353-535353535353",
            ),
            BulkReadTarget(
                target_kind="event",
                event_uid=first_event_uid,
                observed_revision_uid=first_revision_uid,
                operation_id="54545454-5454-4454-8454-545454545454",
            ),
        ]
    )
    barrier = Barrier(2)

    def confirm(manifest: BulkReadManifest) -> int:
        with SessionLocal() as session:
            barrier.wait(timeout=10)
            updated = confirm_bulk_read_manifest(session, manifest)
            session.commit()
            return updated

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(confirm, first_manifest),
            executor.submit(confirm, second_manifest),
        ]
        assert [future.result(timeout=15) for future in futures] == [2, 2]

    with SessionLocal() as session:
        operation_ids = [
            target.operation_id
            for manifest in (first_manifest, second_manifest)
            for target in manifest.targets
        ]
        assert session.scalar(
            select(func.count(InteractionEvent.id)).where(
                InteractionEvent.operation_id.in_(operation_ids)
            )
        ) == 4
    engine.dispose()


def test_postgres_concurrent_object_operation_is_one_event_after_baseline(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        seeded = add_user_state_baseline_seed(session)
    command.upgrade(config, "head")
    item_id = int(seeded["digest_item_ids"][0])
    mutation = UserStatePatch(
        operation_id="89898989-8989-4989-8989-898989898989",
        starred=True,
    )
    barrier = Barrier(2)

    def apply_once() -> dict[str, object]:
        with SessionLocal() as session:
            barrier.wait(timeout=10)
            result = apply_object_user_state_mutation(
                session,
                "item",
                item_id,
                mutation,
            )
            session.commit()
            return result.model_dump(mode="json")

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: apply_once(), range(2)))

    assert results[0] == results[1]
    assert results[0]["starred"] is True
    with SessionLocal() as session:
        interactions = session.scalars(select(InteractionEvent)).all()
        assert len(interactions) == 1
        assert interactions[0].target_kind == "legacy"
        assert interactions[0].object_type == "item"
        assert interactions[0].object_id == item_id
        assert interactions[0].observed_revision_id is None
        assert session.scalar(select(func.count()).select_from(MigrationBaseline)) > 0
        state = session.scalar(
            select(UserState).where(
                UserState.object_type == "item",
                UserState.object_id == item_id,
            )
        )
        assert state is not None
        assert state.starred is True
        metric = session.scalar(select(FeedMetric))
        assert metric is not None
        assert metric.starred_count == 1
    engine.dispose()


def test_source_archive_does_not_restore_deleted_cluster_state_after_event_waits(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        seeded = add_user_state_baseline_seed(session)
    command.upgrade(config, "head")

    cluster_id = int(seeded["no_state_cluster_id"])
    source_id = int(seeded["source_id"])
    with SessionLocal() as session:
        projection = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.cluster_id == cluster_id
            )
        )
        assert projection is not None
        event_row = session.get(ReaderEvent, projection.event_id)
        revision = session.get(EventRevision, projection.event_revision_id)
        assert event_row is not None
        assert revision is not None
        event_id = event_row.id
        mutation = EventUserStateMutationIn(
            event_uid=event_row.uid,
            observed_revision_uid=revision.uid,
            operation_id="aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa",
            action="read_status_set",
            value="summary_seen",
        )

    mutation_started = Event()
    mutation_pid: list[int] = []

    def mark_event_seen() -> dict[str, object]:
        with SessionLocal() as session:
            mutation_pid.append(int(session.scalar(text("SELECT pg_backend_pid()"))))
            mutation_started.set()
            result = apply_event_user_state_mutation(session, mutation)
            session.commit()
            return result.model_dump(mode="json")

    with SessionLocal() as archive_session:
        source = archive_session.scalar(
            select(Source).where(Source.id == source_id).with_for_update()
        )
        assert source is not None
        with ThreadPoolExecutor(max_workers=1) as executor:
            mutation_future = executor.submit(mark_event_seen)
            assert mutation_started.wait(timeout=10)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                with engine.connect() as observer:
                    wait_type = observer.scalar(
                        text(
                            "SELECT wait_event_type FROM pg_stat_activity "
                            "WHERE pid = :pid"
                        ),
                        {"pid": mutation_pid[0]},
                    )
                if wait_type == "Lock":
                    break
                time.sleep(0.02)
            else:
                pytest.fail("Event mutation did not wait for the locked Source")

            apply_source_update(
                archive_session,
                source,
                SourcePatch(status="archived", enabled=False),
            )
            archive_session.commit()
            result = mutation_future.result(timeout=10)

    assert result["read_status"] == "summary_seen"
    with SessionLocal() as session:
        assert session.get(Cluster, cluster_id) is None
        assert session.scalar(
            select(UserState).where(
                UserState.object_type == "cluster",
                UserState.object_id == cluster_id,
            )
        ) is None
        assert session.scalar(select(func.count(InteractionEvent.id))) == 1
        assert session.scalar(
            select(EventUserState).where(EventUserState.event_id == event_id)
        ) is not None
    engine.dispose()


def test_user_state_baseline_backfill_fails_atomically_for_unmapped_cluster_state(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0025_event_evidence_identity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        add_legacy_event_backfill_seed(session)
    command.upgrade(config, "0026_legacy_cluster_events")
    with SessionLocal() as session:
        insert_legacy_user_state(
            session,
            object_type="cluster",
            object_id=987654321,
            read_status="summary_seen",
            read_later=True,
            starred=True,
            updated_at=datetime(2026, 7, 13, 11, 0, tzinfo=timezone.utc),
        )
        session.commit()
    with engine.connect() as connection:
        legacy_before = list(
            connection.scalars(
                text(
                    "SELECT to_jsonb(snapshot_row)::text FROM "
                    "(SELECT * FROM user_states ORDER BY id) snapshot_row"
                )
            ).all()
        )

    with pytest.raises(RuntimeError, match="legacy_user_state_baseline_unresolvable"):
        command.upgrade(config, "head")

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0026_legacy_cluster_events"
        )
        assert list(
            connection.scalars(
                text(
                    "SELECT to_jsonb(snapshot_row)::text FROM "
                    "(SELECT * FROM user_states ORDER BY id) snapshot_row"
                )
            ).all()
        ) == legacy_before
        assert not inspect(connection).has_table("migration_baselines")
        assert not inspect(connection).has_table("event_user_states")
        assert not inspect(connection).has_table("interaction_events")
    engine.dispose()


def assert_upgrade_refused_at_revision(
    *,
    config: Config,
    engine: Engine,
    error: str,
    revision: str,
) -> None:
    with pytest.raises(DBAPIError, match=error):
        command.upgrade(config, "head")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == revision


def test_event_projection_constraints_and_cluster_deletion_preserve_history(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        source = Source(
            name="Event projection PG",
            url="https://example.com/event-projection-pg.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="event-projection-pg",
            title="Event projection PG",
            url="https://example.com/event-projection-pg",
            author="PG Author",
            published_at=datetime(2026, 7, 13, 9, 0, tzinfo=timezone.utc),
            raw_content="Stable event evidence",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            document_type="normal_article",
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_text=raw.raw_content,
            url=raw.url,
            canonical_url=raw.url,
            published_at=raw.published_at,
            content_hash=raw.content_hash,
        )
        session.add(item)
        session.flush()
        with clustering_run(
            session,
            scope_type="event-projection-postgres-test",
            item_ids=[item.id],
            rule_version="event-projection-postgres-test-v1",
        ) as run_id:
            cluster = assign_cluster(session, item)

        event_row = session.scalar(select(ReaderEvent))
        revision = session.scalar(select(EventRevision))
        evidence = session.scalar(select(EventEvidence))
        version = session.scalar(select(EventEvidenceVersion))
        link = session.scalar(select(EventRevisionEvidence))
        mapping = session.scalar(select(ClusterEventProjection))
        assert event_row is not None
        assert revision is not None
        assert evidence is not None
        assert version is not None
        assert link is not None
        assert mapping is not None
        assert event_row.current_revision_id == revision.id
        assert link.evidence_type == "article"
        assert link.role == "material"
        assert version.raw_entry_id == raw.id
        assert version.source_entry_id == raw.source_entry_id
        assert version.fragment_fingerprint == evidence.fragment_fingerprint
        assert version.raw_revision_no == raw.revision_no
        assert version.legacy_content_item_id_snapshot == item.id
        assert mapping.clustering_run_id == run_id
        fixture = EventProjectionFixture(
            cluster_id=cluster.id,
            event_id=event_row.id,
            revision_id=revision.id,
            evidence_id=evidence.id,
            version_id=version.id,
            raw_id=raw.id,
            source_entry_id=raw.source_entry_id,
            source_id=source.id,
            item_id=item.id,
            mapping_id=mapping.id,
            fragment_fingerprint=evidence.fragment_fingerprint,
        )

    with engine.begin() as connection:
        with pytest.raises(DBAPIError, match="uq_event_evidence_source_fragment"):
            connection.execute(
                text(
                    "INSERT INTO event_evidence "
                    "(uid, identity_fingerprint, source_entry_id, "
                    " fragment_fingerprint) "
                    "VALUES (:uid, :identity, :source_entry_id, :fragment)"
                ),
                {
                    "uid": "09090909-0909-4909-8909-090909090909",
                    "identity": "9" * 64,
                    "source_entry_id": fixture.source_entry_id,
                    "fragment": fixture.fragment_fingerprint,
                },
            )

    with engine.begin() as connection:
        with pytest.raises(DBAPIError, match="ck_event_uid_uuid"):
            connection.execute(
                text(
                    "INSERT INTO events (uid, status) "
                    "VALUES ('not-a-uuid', 'active')"
                )
            )

    with engine.begin() as connection:
        with pytest.raises(DBAPIError, match="event_revision_sequence"):
            connection.execute(
                text(
                    "INSERT INTO event_revisions "
                    "(uid, event_id, revision_no, evidence_fingerprint, title_snapshot) "
                    "VALUES (:uid, :event_id, 3, :fingerprint, 'gap')"
                ),
                {
                    "uid": "11111111-1111-4111-8111-111111111111",
                    "event_id": fixture.event_id,
                    "fingerprint": "a" * 64,
                },
            )

    with engine.begin() as connection:
        with pytest.raises(DBAPIError, match="ck_event_revision_fingerprint_sha256"):
            connection.execute(
                text(
                    "INSERT INTO event_revisions "
                    "(uid, event_id, revision_no, evidence_fingerprint, title_snapshot) "
                    "VALUES (:uid, :event_id, 2, 'bad', 'bad fingerprint')"
                ),
                {
                    "uid": "22222222-2222-4222-8222-222222222222",
                    "event_id": fixture.event_id,
                },
            )

    with engine.begin() as connection:
        with pytest.raises(DBAPIError, match="event_history_immutable"):
            connection.execute(
                text(
                    "UPDATE event_revisions SET title_snapshot = 'rewritten' "
                    "WHERE id = :revision_id"
                ),
                {"revision_id": fixture.revision_id},
            )

    with engine.begin() as connection:
        with pytest.raises(DBAPIError, match="fk_event_evidence_version_raw_revision"):
            insert_event_evidence_version(
                connection,
                fixture,
                uid="33333333-3333-4333-8333-333333333333",
                fingerprint="b" * 64,
                source_id=999999,
            )

    with engine.begin() as connection:
        with pytest.raises(
            DBAPIError,
            match="fk_event_evidence_version_parent",
        ):
            insert_event_evidence_version(
                connection,
                fixture,
                uid="34343434-3434-4434-8434-343434343434",
                fingerprint="d" * 64,
                fragment="e" * 64,
            )

    with engine.begin() as connection:
        inserted_version_id = insert_event_evidence_version(
            connection,
            fixture,
            uid="44444444-4444-4444-8444-444444444444",
            fingerprint="c" * 64,
        )

    with engine.begin() as connection:
        with pytest.raises(DBAPIError, match="ck_event_revision_evidence_type"):
            connection.execute(
                text(
                    "INSERT INTO event_revision_evidence "
                    "(revision_id, evidence_version_id, evidence_type, role) "
                    "VALUES (:revision_id, :version_id, 'video', 'material')"
                ),
                {
                    "revision_id": fixture.revision_id,
                    "version_id": inserted_version_id,
                },
            )

    with SessionLocal() as session:
        started = ClusteringRun(
            scope_type="unfinished-event-projection",
            scope_key="d" * 64,
            rule_version="unfinished-v1",
        )
        session.add(started)
        session.flush()
        session.add(
            ClusterEventProjection(
                cluster_id=fixture.cluster_id,
                cluster_id_snapshot=fixture.cluster_id,
                clustering_run_id=started.id,
                cluster_anchor="e" * 64,
                cluster_occurrence=1,
                event_id=fixture.event_id,
                event_revision_id=fixture.revision_id,
                reconciliation_kind="initial",
                after_evidence_fingerprint="a" * 64,
            )
        )
        with pytest.raises(DBAPIError, match="event_projection_requires_completed_run"):
            session.flush()
        session.rollback()

    with engine.begin() as connection:
        connection.execute(
            delete(ClusterItem).where(
                ClusterItem.cluster_id == fixture.cluster_id
            )
        )
        connection.execute(delete(Cluster).where(Cluster.id == fixture.cluster_id))

    with SessionLocal() as session:
        preserved_mapping = session.get(
            ClusterEventProjection, fixture.mapping_id
        )
        assert preserved_mapping is not None
        assert preserved_mapping.cluster_id is None
        assert preserved_mapping.cluster_id_snapshot == fixture.cluster_id
        assert session.get(ReaderEvent, fixture.event_id) is not None
        assert session.get(EventRevision, fixture.revision_id) is not None
        assert session.get(EventEvidenceVersion, fixture.version_id) is not None
        assert session.get(
            ClusterCurrentEventProjection, fixture.cluster_id
        ) is None

    engine.dispose()


def test_postgres_event_continuation_is_revisioned_and_concurrent_noops_are_idempotent(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Event continuation PG",
            url="https://example.com/event-continuation-pg.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="event-continuation-pg",
            title="Event continuation PG",
            url="https://example.com/event-continuation-pg",
            raw_content="Initial evidence",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            document_type="normal_article",
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_text=raw.raw_content,
            url=raw.url,
            canonical_url=raw.url,
            content_hash=raw.content_hash,
        )
        session.add(item)
        session.flush()
        with clustering_run(
            session,
            scope_type="event-continuation-pg-initial",
            item_ids=[item.id],
            rule_version="event-continuation-pg-v1",
        ):
            assign_cluster(session, item)

        event_row = session.scalar(select(ReaderEvent))
        initial_revision = session.scalar(select(EventRevision))
        assert event_row is not None
        assert initial_revision is not None
        with clustering_run(
            session,
            scope_type="event-continuation-pg-version",
            item_ids=[item.id],
            rule_version="event-continuation-pg-v1",
        ):
            item.content_text = "Enriched evidence with stable membership identity"
        item_id = item.id
        event_id = event_row.id

    barrier = Barrier(2)

    def reconcile_noop(index: int) -> str:
        with SessionLocal() as session:
            item = session.get(ContentItem, item_id)
            assert item is not None
            barrier.wait(timeout=10)
            with clustering_run(
                session,
                scope_type=f"event-continuation-pg-concurrent-{index}",
                item_ids=[item.id],
                rule_version="event-continuation-pg-v1",
            ) as run_id:
                pass
            return run_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        run_ids = list(executor.map(reconcile_noop, (1, 2)))

    with SessionLocal() as session:
        event_row = session.get(ReaderEvent, event_id)
        assert event_row is not None
        current_revision = session.get(EventRevision, event_row.current_revision_id)
        assert current_revision is not None
        assert current_revision.revision_no == 2
        assert session.scalar(select(func.count(ReaderEvent.id))) == 1
        assert session.scalar(select(func.count(EventRevision.id))) == 2
        mappings = list(
            session.scalars(
                select(ClusterEventProjection).order_by(ClusterEventProjection.id)
            )
        )
        predecessors = list(
            session.scalars(
                select(ClusteringRunProjectionPredecessor).order_by(
                    ClusteringRunProjectionPredecessor.id
                )
            )
        )
        assert len(mappings) == 4
        assert len(predecessors) == 3
        assert [mapping.reconciliation_kind for mapping in mappings] == [
            "initial",
            "continued",
            "continued",
            "continued",
        ]
        assert mappings[1].event_revision_id == current_revision.id
        assert mappings[2].event_revision_id == current_revision.id
        assert mappings[3].event_revision_id == current_revision.id
        assert {mappings[2].clustering_run_id, mappings[3].clustering_run_id} == set(
            run_ids
        )
        assert mappings[2].predecessor_projection_id == mappings[1].id
        assert mappings[3].predecessor_projection_id == mappings[2].id
        assert [row.predecessor_projection_id for row in predecessors] == [
            mappings[0].id,
            mappings[1].id,
            mappings[2].id,
        ]
        current_projection = session.get(
            ClusterCurrentEventProjection, mappings[-1].cluster_id
        )
        assert current_projection is not None
        assert current_projection.projection_id == mappings[-1].id
        predecessor_id = predecessors[0].id
        completed_run_id = predecessors[0].run_id
        cluster_id = mappings[-1].cluster_id

    with engine.begin() as connection:
        with pytest.raises(
            DBAPIError,
            match="cluster_current_event_projection_not_latest",
        ):
            connection.execute(
                text(
                    "UPDATE cluster_current_event_projections "
                    "SET projection_id = :projection_id "
                    "WHERE cluster_id = :cluster_id"
                ),
                {
                    "projection_id": mappings[0].id,
                    "cluster_id": cluster_id,
                },
            )

    with engine.begin() as connection:
        with pytest.raises(
            DBAPIError,
            match="clustering_run_projection_predecessor_immutable",
        ):
            connection.execute(
                text(
                    "UPDATE clustering_run_projection_predecessors "
                    "SET cluster_occurrence = 2 WHERE id = :id"
                ),
                {"id": predecessor_id},
            )

    with engine.begin() as connection:
        with pytest.raises(
            DBAPIError,
            match="clustering_run_projection_predecessor_closed",
        ):
            connection.execute(
                text(
                    "INSERT INTO clustering_run_projection_predecessors "
                    "(run_id, cluster_anchor, cluster_occurrence, "
                    " predecessor_projection_id) "
                    "VALUES (:run_id, :anchor, 99, :projection_id)"
                ),
                {
                    "run_id": completed_run_id,
                    "anchor": "f" * 64,
                    "projection_id": mappings[0].id,
                },
            )
    engine.dispose()


def add_event_split_item(
    session: Session,
    *,
    source_id: int,
    prefix: str,
    suffix: str,
) -> ContentItem:
    raw = make_raw_entry(
        source_id=source_id,
        external_id=f"{prefix}-{suffix}",
        title=f"{prefix} {suffix}",
        url=f"https://example.com/{prefix}-{suffix}",
        raw_content=f"{prefix} evidence {suffix}",
    )
    session.add(raw)
    session.flush()
    document = Document(
        raw_entry_id=raw.id,
        document_type="normal_article",
        title=raw.title,
        content_text=raw.raw_content,
    )
    session.add(document)
    session.flush()
    item = ContentItem(
        document_id=document.id,
        source_id=source_id,
        title=raw.title,
        content_text=raw.raw_content,
        url=raw.url,
        canonical_url=raw.url,
        content_hash=raw.content_hash,
    )
    session.add(item)
    session.flush()
    return item


def seed_partial_overlap_clusters(
    session: Session,
    *,
    prefix: str,
) -> tuple[ContentItem, ContentItem, ContentItem, Cluster]:
    source_id = insert_pre_privacy_source(
        session,
        name=f"{prefix} source",
        url=f"https://example.com/{prefix}.xml",
    )
    first = add_event_split_item(
        session,
        source_id=source_id,
        prefix=prefix,
        suffix="one",
    )
    second = add_event_split_item(
        session,
        source_id=source_id,
        prefix=prefix,
        suffix="two",
    )
    third = add_event_split_item(
        session,
        source_id=source_id,
        prefix=prefix,
        suffix="three",
    )
    with clustering_run(
        session,
        scope_type=f"{prefix}-initial",
        item_ids=[first.id, second.id, third.id],
        rule_version=f"{prefix}-v1",
    ):
        first_cluster = assign_cluster(session, first)
        session.add(
            ClusterItem(
                cluster_id=first_cluster.id,
                content_item_id=second.id,
            )
        )
        second_cluster = Cluster(
            cluster_key=f"{prefix}-second",
            title=f"{prefix} second",
        )
        session.add(second_cluster)
        session.flush()
        session.add(
            ClusterItem(
                cluster_id=second_cluster.id,
                content_item_id=third.id,
            )
        )
    return first, second, third, second_cluster


def test_postgres_event_split_is_stateless_idempotent_and_preserves_history(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Event split PG",
            url="https://example.com/event-split-pg.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()

        def add_item(suffix: str) -> ContentItem:
            raw = make_raw_entry(
                source_id=source.id,
                external_id=f"event-split-pg-{suffix}",
                title=f"Event split PG {suffix}",
                url=f"https://example.com/event-split-pg-{suffix}",
                raw_content=f"Event split evidence {suffix}",
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                document_type="normal_article",
                title=raw.title,
                content_text=raw.raw_content,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=raw.title,
                content_text=raw.raw_content,
                url=raw.url,
                canonical_url=raw.url,
                content_hash=raw.content_hash,
            )
            session.add(item)
            session.flush()
            return item

        first = add_item("one")
        second = add_item("two")
        with clustering_run(
            session,
            scope_type="event-split-pg-initial",
            item_ids=[first.id, second.id],
            rule_version="event-split-pg-v1",
        ):
            original_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=original_cluster.id,
                    content_item_id=second.id,
                )
            )

        original_mapping = session.scalar(select(ClusterEventProjection))
        original_event = session.scalar(select(ReaderEvent))
        original_revision = session.scalar(select(EventRevision))
        assert original_mapping is not None
        assert original_event is not None
        assert original_revision is not None
        occurred_at = datetime(2026, 7, 14, 9, 30, tzinfo=timezone.utc)
        historical_state_id = 900002
        baseline = MigrationBaseline(
            idempotency_key=hashlib.sha256(
                f"legacy-user-state-baseline-v1:{historical_state_id}".encode()
            ).hexdigest(),
            migration_version="legacy-user-state-baseline-v1",
            legacy_user_state_id=historical_state_id,
            legacy_object_type="cluster",
            legacy_object_id=original_cluster.id,
            resolved_event_id=original_event.id,
            resolved_revision_id=original_revision.id,
            read_status="summary_seen",
            read_later=True,
            starred=True,
            source_updated_at=occurred_at,
            recorded_at=occurred_at,
        )
        session.add(baseline)
        session.flush()
        state = EventUserState(
            baseline_id=baseline.id,
            event_id=original_event.id,
            seen_revision_id=original_revision.id,
            read_status="summary_seen",
            read_later=True,
            starred=True,
            updated_at=occurred_at,
        )
        interaction = InteractionEvent(
            operation_id="event-split-pg-starred",
            target_kind="event",
            event_id=original_event.id,
            observed_revision_id=original_revision.id,
            action="starred_set",
            set_value=True,
            payload={},
            occurred_at=occurred_at,
            recorded_at=occurred_at,
        )
        session.add_all([state, interaction])
        session.commit()

        with session.begin_nested():
            with clustering_run(
                session,
                scope_type="event-split-pg-change",
                item_ids=[first.id, second.id],
                rule_version="event-split-pg-v1",
                commit_on_success=False,
            ) as split_run_id:
                second_link = session.scalar(
                    select(ClusterItem).where(
                        ClusterItem.cluster_id == original_cluster.id,
                        ClusterItem.content_item_id == second.id,
                    )
                )
                assert second_link is not None
                session.delete(second_link)
                second_cluster = Cluster(
                    cluster_key="event-split-pg-second",
                    title="Event split PG second",
                )
                session.add(second_cluster)
                session.flush()
                session.add(
                    ClusterItem(
                        cluster_id=second_cluster.id,
                        content_item_id=second.id,
                    )
                )
        session.commit()

        split_cluster_ids = [original_cluster.id, second_cluster.id]
        split_mappings = list(
            session.scalars(
                select(ClusterEventProjection)
                .where(ClusterEventProjection.clustering_run_id == split_run_id)
                .order_by(ClusterEventProjection.id)
            )
        )
        lineages = list(
            session.scalars(
                select(EventLineage)
                .where(EventLineage.clustering_run_id == split_run_id)
                .order_by(EventLineage.id)
            )
        )
        assert len(split_mappings) == 2
        assert len(lineages) == 2
        assert {mapping.reconciliation_kind for mapping in split_mappings} == {
            "split"
        }
        assert {mapping.predecessor_projection_id for mapping in split_mappings} == {
            original_mapping.id
        }
        assert {lineage.source_event_id for lineage in lineages} == {
            original_event.id
        }
        assert {lineage.target_event_id for lineage in lineages} == {
            mapping.event_id for mapping in split_mappings
        }
        transaction_ids = list(
            session.scalars(
                text(
                    "SELECT projection.created_transaction_id::text "
                    "FROM cluster_event_projections projection "
                    "WHERE projection.clustering_run_id = :run_id "
                    "UNION ALL "
                    "SELECT event.created_transaction_id::text "
                    "FROM events event JOIN cluster_event_projections projection "
                    "ON projection.event_id = event.id "
                    "WHERE projection.clustering_run_id = :run_id "
                    "UNION ALL "
                    "SELECT revision.created_transaction_id::text "
                    "FROM event_revisions revision "
                    "JOIN cluster_event_projections projection "
                    "ON projection.event_revision_id = revision.id "
                    "WHERE projection.clustering_run_id = :run_id "
                    "UNION ALL "
                    "SELECT lineage.created_transaction_id::text "
                    "FROM event_lineages lineage "
                    "WHERE lineage.clustering_run_id = :run_id "
                    "UNION ALL "
                    "SELECT event.superseded_transaction_id::text "
                    "FROM events event WHERE event.id = :parent_event_id"
                ),
                {
                    "run_id": split_run_id,
                    "parent_event_id": original_event.id,
                },
            )
        )
        assert len(transaction_ids) == 9
        assert len(set(transaction_ids)) == 1
        assert session.scalar(
            text(
                "SELECT pg_typeof(created_transaction_id)::text "
                "FROM events LIMIT 1"
            )
        ) == "xid8"
        assert original_event.status == "superseded"
        assert session.scalar(select(func.count(EventUserState.id))) == 1
        assert session.scalar(select(func.count(InteractionEvent.id))) == 1
        original_mapping_id = original_mapping.id
        original_event_id = original_event.id
        original_revision_id = original_revision.id
        expected_mapping_ids = [mapping.id for mapping in split_mappings]
        lineage_id = lineages[0].id

        reused_cluster_child = next(
            mapping
            for mapping in split_mappings
            if mapping.cluster_id == original_cluster.id
        )
        with clustering_run(
            session,
            scope_type="event-split-pg-post-split-continuation",
            item_ids=[first.id, second.id],
            rule_version="event-split-pg-v1",
        ) as post_split_run_id:
            pass
        continued_child = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id
                    == post_split_run_id,
                ClusterEventProjection.event_id
                    == reused_cluster_child.event_id,
            )
        )
        assert continued_child is not None
        assert continued_child.reconciliation_kind == "continued"
        child_identity = cluster_event_identities_for(
            session, [original_cluster.id]
        )[original_cluster.id]
        assert child_identity.read_status == "unread"
        assert child_identity.read_later is False
        assert child_identity.starred is False
        child_event = session.get(ReaderEvent, reused_cluster_child.event_id)
        child_revision = session.get(
            EventRevision,
            reused_cluster_child.event_revision_id,
        )
        assert child_event is not None
        assert child_revision is not None
        child_result = apply_event_user_state_mutation(
            session,
            EventUserStateMutationIn(
                event_uid=child_event.uid,
                observed_revision_uid=child_revision.uid,
                operation_id="56565656-5656-4565-8565-565656565656",
                action="starred_set",
                value=False,
            ),
        )
        session.commit()
        child_state = session.scalar(
            select(EventUserState).where(
                EventUserState.event_id == child_event.id
            )
        )
        assert child_state is not None
        assert child_result.read_later is False
        assert child_state.read_status == "unread"
        assert child_state.read_later is False
        assert child_state.starred is False
        historical_baseline = session.get(MigrationBaseline, baseline.id)
        assert historical_baseline is not None
        assert historical_baseline.resolved_event_id == original_event.id
        assert historical_baseline.read_status == "summary_seen"

        unrelated_target = ReaderEvent(
            uid="98989898-9898-4989-8989-989898989898",
            status="active",
        )
        session.add(unrelated_target)
        session.flush()
        unrelated_revision = EventRevision(
            uid="97979797-9797-4979-8979-979797979797",
            event_id=unrelated_target.id,
            revision_no=1,
            evidence_fingerprint=split_mappings[0].after_evidence_fingerprint,
            title_snapshot="Unrelated split target",
        )
        session.add(unrelated_revision)
        session.flush()
        unrelated_target.current_revision_id = unrelated_revision.id
        session.add(
            ClusterEventProjection(
                cluster_id=original_cluster.id,
                cluster_id_snapshot=original_cluster.id,
                clustering_run_id=split_run_id,
                cluster_anchor="f" * 64,
                cluster_occurrence=99,
                event_id=unrelated_target.id,
                event_revision_id=unrelated_revision.id,
                predecessor_projection_id=split_mappings[0].id,
                reconciliation_kind="split",
                reconciliation_rule_version="event-split-pg-v1",
                before_evidence_fingerprint=(
                    split_mappings[0].after_evidence_fingerprint
                ),
                after_evidence_fingerprint=(
                    unrelated_revision.evidence_fingerprint
                ),
            )
        )
        with pytest.raises(
            DBAPIError,
            match="event_projection_split_frozen_predecessor_mismatch",
        ):
            session.flush()
        session.rollback()

    with SessionLocal() as session:
        existing_child = session.get(
            ClusterEventProjection,
            expected_mapping_ids[0],
        )
        assert existing_child is not None
        session.add(
            EventUserState(
                event_id=existing_child.event_id,
                seen_revision_id=existing_child.event_revision_id,
                read_status="summary_seen",
                read_later=True,
                starred=True,
            )
        )
        session.flush()
        session.add(
            ClusterEventProjection(
                cluster_id=existing_child.cluster_id,
                cluster_id_snapshot=existing_child.cluster_id_snapshot,
                clustering_run_id=existing_child.clustering_run_id,
                cluster_anchor=existing_child.cluster_anchor,
                cluster_occurrence=existing_child.cluster_occurrence,
                event_id=existing_child.event_id,
                event_revision_id=existing_child.event_revision_id,
                predecessor_projection_id=(
                    existing_child.predecessor_projection_id
                ),
                reconciliation_kind="split",
                reconciliation_rule_version=(
                    existing_child.reconciliation_rule_version
                ),
                before_evidence_fingerprint=(
                    existing_child.before_evidence_fingerprint
                ),
                after_evidence_fingerprint=(
                    existing_child.after_evidence_fingerprint
                ),
            )
        )
        with pytest.raises(
            DBAPIError,
            match="event_projection_split_target_not_newborn",
        ):
            session.flush()
        session.rollback()

    barrier = Barrier(2)

    def replay_split() -> list[int]:
        with SessionLocal() as session:
            barrier.wait(timeout=10)
            mappings = project_completed_clustering_run(
                session,
                split_run_id,
                split_cluster_ids,
            )
            session.commit()
            return [mapping.id for mapping in mappings]

    with ThreadPoolExecutor(max_workers=2) as executor:
        replayed = list(executor.map(lambda _index: replay_split(), (1, 2)))
    assert replayed == [expected_mapping_ids, expected_mapping_ids]

    with engine.begin() as connection:
        with pytest.raises(DBAPIError, match="event_lifecycle_terminal"):
            connection.execute(
                text(
                    "UPDATE events SET superseded_at = superseded_at "
                    "WHERE id = :event_id"
                ),
                {"event_id": original_event_id},
            )

    with engine.begin() as connection:
        with pytest.raises(DBAPIError, match="event_revision_event_terminal"):
            connection.execute(
                text(
                    "INSERT INTO event_revisions "
                    "(uid, event_id, revision_no, evidence_fingerprint, "
                    " title_snapshot) "
                    "VALUES "
                    "('67676767-6767-4676-8676-676767676767', "
                    " :event_id, 2, :fingerprint, 'late revision')"
                ),
                {
                    "event_id": original_event_id,
                    "fingerprint": "0" * 64,
                },
            )

    with engine.begin() as connection:
        with pytest.raises(DBAPIError, match="event_projection_immutable"):
            connection.execute(
                text(
                    "UPDATE cluster_event_projections "
                    "SET cluster_id = NULL, "
                    "    created_transaction_id = pg_current_xact_id() "
                    "WHERE id = :projection_id"
                ),
                {"projection_id": original_mapping_id},
            )

    with engine.begin() as connection:
        with pytest.raises(DBAPIError, match="event_lineage_immutable"):
            connection.execute(
                text(
                    "UPDATE event_lineages SET decision_reason = 'rewritten' "
                    "WHERE id = :lineage_id"
                ),
                {"lineage_id": lineage_id},
            )

    with engine.begin() as connection:
        connection.execute(
            delete(ClusterItem).where(
                ClusterItem.cluster_id.in_(split_cluster_ids)
            )
        )
        connection.execute(
            delete(Cluster).where(Cluster.id.in_(split_cluster_ids))
        )

    with SessionLocal() as session:
        assert session.scalar(select(func.count(ReaderEvent.id))) == 3
        assert session.scalar(select(func.count(EventRevision.id))) == 3
        assert session.scalar(select(func.count(EventLineage.id))) == 2
        assert session.scalar(select(func.count(EventUserState.id))) == 2
        assert session.scalar(select(func.count(InteractionEvent.id))) == 2
        assert session.scalar(select(func.count(MigrationBaseline.id))) == 1
        assert session.scalar(select(func.count(UserState.id))) == 0
        assert session.scalar(
            select(func.count(EventRevisionEvidence.id)).where(
                EventRevisionEvidence.revision_id == original_revision_id
            )
        ) == 2
        preserved_mappings = list(
            session.scalars(
                select(ClusterEventProjection).where(
                    ClusterEventProjection.id.in_(
                        [original_mapping_id, *expected_mapping_ids]
                    )
                )
            )
        )
        assert len(preserved_mappings) == 3
        assert {mapping.cluster_id for mapping in preserved_mappings} == {None}
    engine.dispose()


def test_postgres_event_merge_is_stateless_idempotent_and_supersedes_all_parents(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Event merge PG",
            url="https://example.com/event-merge-pg.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        first = add_event_split_item(
            session,
            source_id=source.id,
            prefix="event-merge-pg",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source.id,
            prefix="event-merge-pg",
            suffix="two",
        )
        with clustering_run(
            session,
            scope_type="event-merge-pg-initial",
            item_ids=[first.id, second.id],
            rule_version="event-merge-pg-v1",
        ):
            first_cluster = assign_cluster(session, first)
            second_cluster = Cluster(
                cluster_key="event-merge-pg-second",
                title="Event merge PG second",
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=second.id,
                )
            )

        parents = list(
            session.scalars(
                select(ClusterEventProjection).order_by(
                    ClusterEventProjection.id
                )
            )
        )
        assert len(parents) == 2
        parent_event_ids = {parent.event_id for parent in parents}
        session.add_all(
            [
                EventUserState(
                    event_id=parents[0].event_id,
                    seen_revision_id=parents[0].event_revision_id,
                    read_status="summary_seen",
                    read_later=True,
                    starred=False,
                ),
                EventUserState(
                    event_id=parents[1].event_id,
                    seen_revision_id=parents[1].event_revision_id,
                    read_status="original_opened",
                    read_later=False,
                    starred=True,
                ),
            ]
        )
        session.commit()

        with clustering_run(
            session,
            scope_type="event-merge-pg-change",
            item_ids=[first.id, second.id],
            rule_version="event-merge-pg-v1",
        ) as merge_run_id:
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == second_cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            assert second_link is not None
            session.delete(second_link)
            session.add(
                ClusterItem(
                    cluster_id=first_cluster.id,
                    content_item_id=second.id,
                )
            )

        merged = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == merge_run_id
            )
        )
        assert merged is not None
        assert merged.reconciliation_kind == "merged"
        assert merged.predecessor_projection_id is None
        assert merged.event_id not in parent_event_ids
        assert session.scalar(
            select(func.count(EventLineage.id)).where(
                EventLineage.clustering_run_id == merge_run_id,
                EventLineage.relation_type == "merged_from",
                EventLineage.target_event_id == merged.event_id,
            )
        ) == 2
        assert set(
            session.scalars(
                select(EventLineage.source_event_id).where(
                    EventLineage.clustering_run_id == merge_run_id
                )
            )
        ) == parent_event_ids
        assert set(
            session.scalars(
                select(ReaderEvent.status).where(
                    ReaderEvent.id.in_(parent_event_ids)
                )
            )
        ) == {"superseded"}
        assert session.scalar(
            select(func.count(EventUserState.id)).where(
                EventUserState.event_id == merged.event_id
            )
        ) == 0
        assert session.scalar(
            select(func.count(EventRevisionEvidence.id)).where(
                EventRevisionEvidence.revision_id
                    == merged.event_revision_id
            )
        ) == 2

        counts = (
            session.scalar(select(func.count(ReaderEvent.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
            session.scalar(select(func.count(EventLineage.id))),
        )
        replayed = project_completed_clustering_run(
            session,
            merge_run_id,
            [first_cluster.id],
        )
        session.commit()
        assert [projection.id for projection in replayed] == [merged.id]
        assert (
            session.scalar(select(func.count(ReaderEvent.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
            session.scalar(select(func.count(EventLineage.id))),
        ) == counts

        merged_id = merged.id
        merged_revision_id = merged.event_revision_id
        first_cluster_id = first_cluster.id
        second_cluster_id = second_cluster.id

    barrier = Barrier(2)

    def replay_merge() -> list[int]:
        with SessionLocal() as session:
            barrier.wait(timeout=10)
            mappings = project_completed_clustering_run(
                session,
                merge_run_id,
                [first_cluster_id],
            )
            session.commit()
            return [mapping.id for mapping in mappings]

    with ThreadPoolExecutor(max_workers=2) as executor:
        replayed_concurrently = list(
            executor.map(lambda _index: replay_merge(), (1, 2))
        )
    assert replayed_concurrently == [[merged_id], [merged_id]]

    with SessionLocal() as session:
        assert (
            session.scalar(select(func.count(ReaderEvent.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
            session.scalar(select(func.count(EventLineage.id))),
        ) == counts

    with engine.begin() as connection:
        cluster_ids = [first_cluster_id, second_cluster_id]
        connection.execute(
            delete(ClusterItem).where(ClusterItem.cluster_id.in_(cluster_ids))
        )
        connection.execute(delete(Cluster).where(Cluster.id.in_(cluster_ids)))

    with SessionLocal() as session:
        preserved_mappings = list(
            session.scalars(select(ClusterEventProjection))
        )
        assert len(preserved_mappings) == 3
        assert {mapping.cluster_id for mapping in preserved_mappings} == {None}
        assert session.scalar(select(func.count(ReaderEvent.id))) == 3
        assert session.scalar(select(func.count(EventRevision.id))) == 3
        assert session.scalar(select(func.count(EventLineage.id))) == 2
        assert session.scalar(select(func.count(EventUserState.id))) == 2
        assert session.scalar(
            select(func.count(EventRevisionEvidence.id)).where(
                EventRevisionEvidence.revision_id == merged_revision_id
            )
        ) == 2

    engine.dispose()


def test_postgres_shared_parent_subset_is_not_misclassified_as_merge(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        assert session.scalar(
            text(
                "SELECT to_regclass("
                "'public.ix_cluster_event_projection_merge_created_transaction'"
                ") IS NOT NULL"
            )
        ) is True
        source = Source(
            name="Rejected shrinking merge",
            url="https://example.com/rejected-shrinking-merge.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        first = add_event_split_item(
            session,
            source_id=source.id,
            prefix="rejected-shrinking-merge",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source.id,
            prefix="rejected-shrinking-merge",
            suffix="two",
        )
        third = add_event_split_item(
            session,
            source_id=source.id,
            prefix="rejected-shrinking-merge",
            suffix="three",
        )
        with clustering_run(
            session,
            scope_type="rejected-shrinking-merge-initial",
            item_ids=[first.id, second.id, third.id],
            rule_version="rejected-shrinking-merge-v1",
        ):
            target_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=target_cluster.id,
                    content_item_id=second.id,
                )
            )
            other_cluster = Cluster(
                cluster_key="rejected-shrinking-merge-other",
                title="Rejected shrinking merge other",
            )
            session.add(other_cluster)
            session.flush()
            session.add_all(
                [
                    ClusterItem(
                        cluster_id=other_cluster.id,
                        content_item_id=first.id,
                    ),
                    ClusterItem(
                        cluster_id=other_cluster.id,
                        content_item_id=third.id,
                    ),
                ]
            )

        parent_event_ids = set(session.scalars(select(ReaderEvent.id)))
        assert len(parent_event_ids) == 2
        with clustering_run(
            session,
            scope_type="rejected-shrinking-merge-change",
            item_ids=[first.id, second.id, third.id],
            rule_version="rejected-shrinking-merge-v1",
        ) as run_id:
            for link in session.scalars(
                select(ClusterItem).where(
                    (ClusterItem.cluster_id == other_cluster.id)
                    | (
                        (ClusterItem.cluster_id == target_cluster.id)
                        & (ClusterItem.content_item_id == second.id)
                    )
                )
            ):
                session.delete(link)

        run = session.get(ClusteringRun, run_id)
        assert run is not None
        assert run.status == "completed"
        projection = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == run_id
            )
        )
        assert projection is not None
        assert projection.reconciliation_kind == "ambiguous"
        assert projection.event_id not in parent_event_ids
        assert set(
            session.scalars(
                select(ReaderEvent.status).where(
                    ReaderEvent.id.in_(parent_event_ids)
                )
            )
        ) == {"superseded"}
        lineages = list(
            session.scalars(
                select(EventLineage).where(
                    EventLineage.clustering_run_id == run_id
                )
            )
        )
        assert len(lineages) == 2
        assert {lineage.relation_type for lineage in lineages} == {
            "ambiguous_from"
        }
        assert {lineage.source_event_id for lineage in lineages} == (
            parent_event_ids
        )

    engine.dispose()


def test_postgres_event_ambiguous_many_to_many_is_stateless_and_idempotent(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Event ambiguous PG",
            url="https://example.com/event-ambiguous-pg.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        items = [
            add_event_split_item(
                session,
                source_id=source.id,
                prefix="event-ambiguous-pg",
                suffix=suffix,
            )
            for suffix in ("one", "two", "three", "four")
        ]
        with clustering_run(
            session,
            scope_type="event-ambiguous-pg-initial",
            item_ids=[item.id for item in items],
            rule_version="event-ambiguous-pg-v1",
        ):
            first_cluster = assign_cluster(session, items[0])
            session.add(
                ClusterItem(
                    cluster_id=first_cluster.id,
                    content_item_id=items[1].id,
                )
            )
            second_cluster = Cluster(
                cluster_key="event-ambiguous-pg-second",
                title="Event ambiguous PG second",
            )
            session.add(second_cluster)
            session.flush()
            session.add_all(
                [
                    ClusterItem(
                        cluster_id=second_cluster.id,
                        content_item_id=items[2].id,
                    ),
                    ClusterItem(
                        cluster_id=second_cluster.id,
                        content_item_id=items[3].id,
                    ),
                ]
            )

        parents = list(
            session.scalars(
                select(ClusterEventProjection).order_by(
                    ClusterEventProjection.id
                )
            )
        )
        assert len(parents) == 2
        parent_event_ids = {parent.event_id for parent in parents}
        session.add_all(
            EventUserState(
                event_id=parent.event_id,
                seen_revision_id=parent.event_revision_id,
                read_status="summary_seen",
                read_later=True,
                starred=True,
            )
            for parent in parents
        )
        session.commit()

        with clustering_run(
            session,
            scope_type="event-ambiguous-pg-change",
            item_ids=[item.id for item in reversed(items)],
            rule_version="event-ambiguous-pg-v1",
        ) as run_id:
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == first_cluster.id,
                    ClusterItem.content_item_id == items[1].id,
                )
            )
            third_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == second_cluster.id,
                    ClusterItem.content_item_id == items[2].id,
                )
            )
            assert second_link is not None
            assert third_link is not None
            session.delete(second_link)
            session.delete(third_link)
            session.add_all(
                [
                    ClusterItem(
                        cluster_id=second_cluster.id,
                        content_item_id=items[1].id,
                    ),
                    ClusterItem(
                        cluster_id=first_cluster.id,
                        content_item_id=items[2].id,
                    ),
                ]
            )

        mappings = list(
            session.scalars(
                select(ClusterEventProjection)
                .where(ClusterEventProjection.clustering_run_id == run_id)
                .order_by(
                    ClusterEventProjection.cluster_anchor,
                    ClusterEventProjection.cluster_occurrence,
                )
            )
        )
        assert len(mappings) == 2
        assert {mapping.reconciliation_kind for mapping in mappings} == {
            "ambiguous"
        }
        assert not ({mapping.event_id for mapping in mappings} & parent_event_ids)
        assert set(
            session.scalars(
                select(ReaderEvent.status).where(
                    ReaderEvent.id.in_(parent_event_ids)
                )
            )
        ) == {"superseded"}
        lineages = list(
            session.scalars(
                select(EventLineage).where(
                    EventLineage.clustering_run_id == run_id
                )
            )
        )
        assert len(lineages) == 4
        assert {lineage.relation_type for lineage in lineages} == {
            "ambiguous_from"
        }
        assert set(session.scalars(select(EventUserState.event_id))) == (
            parent_event_ids
        )
        assert session.scalar(
            select(func.count(EventRevisionEvidence.id)).where(
                EventRevisionEvidence.revision_id.in_(
                    mapping.event_revision_id for mapping in mappings
                )
            )
        ) == 4

        counts = (
            session.scalar(select(func.count(ReaderEvent.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
            session.scalar(select(func.count(EventLineage.id))),
        )
        replayed = project_completed_clustering_run(
            session,
            run_id,
            [second_cluster.id, first_cluster.id],
        )
        session.commit()
        assert [mapping.id for mapping in replayed] == [
            mapping.id for mapping in mappings
        ]
        assert (
            session.scalar(select(func.count(ReaderEvent.id))),
            session.scalar(select(func.count(EventRevision.id))),
            session.scalar(select(func.count(ClusterEventProjection.id))),
            session.scalar(select(func.count(EventLineage.id))),
        ) == counts

        mapping_ids = [mapping.id for mapping in mappings]
        cluster_ids = [first_cluster.id, second_cluster.id]

    with engine.begin() as connection:
        connection.execute(
            delete(ClusterItem).where(ClusterItem.cluster_id.in_(cluster_ids))
        )
        connection.execute(delete(Cluster).where(Cluster.id.in_(cluster_ids)))

    with SessionLocal() as session:
        preserved = list(
            session.scalars(
                select(ClusterEventProjection).where(
                    ClusterEventProjection.id.in_(mapping_ids)
                )
            )
        )
        assert len(preserved) == 2
        assert {mapping.cluster_id for mapping in preserved} == {None}
        assert session.scalar(
            select(func.count(EventLineage.id)).where(
                EventLineage.clustering_run_id == run_id
            )
        ) == 4
        assert session.scalar(select(func.count(ReaderEvent.id))) == 4
        assert session.scalar(select(func.count(EventRevision.id))) == 4
        assert session.scalar(select(func.count(EventEvidence.id))) == 4
        assert session.scalar(select(func.count(EventEvidenceVersion.id))) == 4
        assert session.scalar(select(func.count(EventRevisionEvidence.id))) == 8
        assert session.scalar(select(func.count(EventUserState.id))) == 2

    engine.dispose()


def test_postgres_partial_overlap_blocks_unique_inclusion_continuations(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Partial overlap PG",
            url="https://example.com/partial-overlap-pg.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        first = add_event_split_item(
            session,
            source_id=source.id,
            prefix="partial-overlap-pg",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source.id,
            prefix="partial-overlap-pg",
            suffix="two",
        )
        third = add_event_split_item(
            session,
            source_id=source.id,
            prefix="partial-overlap-pg",
            suffix="three",
        )
        with clustering_run(
            session,
            scope_type="partial-overlap-pg-initial",
            item_ids=[first.id, second.id, third.id],
            rule_version="partial-overlap-pg-v1",
        ):
            first_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=first_cluster.id,
                    content_item_id=second.id,
                )
            )
            second_cluster = Cluster(
                cluster_key="partial-overlap-pg-second",
                title="Partial overlap PG second",
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=third.id,
                )
            )

        parent_event_ids = set(session.scalars(select(ReaderEvent.id)))
        assert len(parent_event_ids) == 2
        with clustering_run(
            session,
            scope_type="partial-overlap-pg-change",
            item_ids=[first.id, second.id, third.id],
            rule_version="partial-overlap-pg-v1",
        ) as run_id:
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=second.id,
                )
            )

        mappings = list(
            session.scalars(
                select(ClusterEventProjection).where(
                    ClusterEventProjection.clustering_run_id == run_id
                )
            )
        )
        assert len(mappings) == 2
        assert {mapping.reconciliation_kind for mapping in mappings} == {
            "ambiguous"
        }
        assert not ({mapping.event_id for mapping in mappings} & parent_event_ids)
        lineages = list(
            session.scalars(
                select(EventLineage).where(
                    EventLineage.clustering_run_id == run_id
                )
            )
        )
        assert len(lineages) == 3
        assert {lineage.source_event_id for lineage in lineages} == (
            parent_event_ids
        )
        assert {lineage.target_event_id for lineage in lineages} == {
            mapping.event_id for mapping in mappings
        }

    engine.dispose()


def test_postgres_no_anchor_ambiguous_replacement_records_no_false_lineage(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Event no-anchor PG",
            url="https://example.com/event-no-anchor-pg.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        first = add_event_split_item(
            session,
            source_id=source.id,
            prefix="event-no-anchor-pg",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source.id,
            prefix="event-no-anchor-pg",
            suffix="two",
        )
        with clustering_run(
            session,
            scope_type="event-no-anchor-pg-initial",
            item_ids=[first.id],
            rule_version="event-no-anchor-pg-v1",
        ):
            cluster = assign_cluster(session, first)

        parent = session.scalar(select(ClusterEventProjection))
        assert parent is not None
        with clustering_run(
            session,
            scope_type="event-no-anchor-pg-change",
            item_ids=[first.id, second.id],
            rule_version="event-no-anchor-pg-v1",
        ) as run_id:
            link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == cluster.id,
                    ClusterItem.content_item_id == first.id,
                )
            )
            assert link is not None
            session.delete(link)
            session.add(
                ClusterItem(
                    cluster_id=cluster.id,
                    content_item_id=second.id,
                )
            )

        mapping = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == run_id
            )
        )
        assert mapping is not None
        assert mapping.reconciliation_kind == "ambiguous"
        assert mapping.event_id != parent.event_id
        assert session.scalar(
            select(ReaderEvent.status).where(ReaderEvent.id == parent.event_id)
        ) == "superseded"
        assert session.scalar(
            select(func.count(EventLineage.id)).where(
                EventLineage.clustering_run_id == run_id
            )
        ) == 0

    engine.dispose()


def test_postgres_ambiguous_commit_rejects_missing_overlap_lineage(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Incomplete ambiguous PG",
            url="https://example.com/incomplete-ambiguous-pg.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        first = add_event_split_item(
            session,
            source_id=source.id,
            prefix="incomplete-ambiguous-pg",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source.id,
            prefix="incomplete-ambiguous-pg",
            suffix="two",
        )
        third = add_event_split_item(
            session,
            source_id=source.id,
            prefix="incomplete-ambiguous-pg",
            suffix="three",
        )
        with clustering_run(
            session,
            scope_type="incomplete-ambiguous-pg-initial",
            item_ids=[first.id, second.id],
            rule_version="incomplete-ambiguous-pg-v1",
        ):
            cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=cluster.id,
                    content_item_id=second.id,
                )
            )

        parent = session.scalar(select(ClusterEventProjection))
        assert parent is not None
        original_add_all = session.add_all

        def omit_ambiguous_lineage(instances: object) -> None:
            values = list(instances)  # type: ignore[arg-type]
            values = [
                value
                for value in values
                if not (
                    isinstance(value, EventLineage)
                    and value.relation_type == "ambiguous_from"
                )
            ]
            original_add_all(values)

        monkeypatch.setattr(session, "add_all", omit_ambiguous_lineage)
        rejected_run_id = ""
        with pytest.raises(
            DBAPIError,
            match="event_ambiguous_terminal_graph_incomplete",
        ):
            with clustering_run(
                session,
                scope_type="incomplete-ambiguous-pg-change",
                item_ids=[first.id, second.id, third.id],
                rule_version="incomplete-ambiguous-pg-v1",
            ) as rejected_run_id:
                second_link = session.scalar(
                    select(ClusterItem).where(
                        ClusterItem.cluster_id == cluster.id,
                        ClusterItem.content_item_id == second.id,
                    )
                )
                assert second_link is not None
                session.delete(second_link)
                session.add(
                    ClusterItem(
                        cluster_id=cluster.id,
                        content_item_id=third.id,
                    )
                )

        rejected_run = session.get(ClusteringRun, rejected_run_id)
        assert rejected_run is not None
        assert rejected_run.status == "failed"
        assert session.scalar(
            select(ReaderEvent.status).where(ReaderEvent.id == parent.event_id)
        ) == "active"
        assert session.scalar(select(func.count(ReaderEvent.id))) == 1
        assert session.scalar(select(func.count(EventRevision.id))) == 1
        assert session.scalar(
            select(func.count(ClusterEventProjection.id))
        ) == 1
        assert session.scalar(select(func.count(EventLineage.id))) == 0

    engine.dispose()


def test_postgres_definite_continuation_cannot_be_labeled_ambiguous(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Definite continuation PG",
            url="https://example.com/definite-continuation-pg.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        item = add_event_split_item(
            session,
            source_id=source.id,
            prefix="definite-continuation-pg",
            suffix="one",
        )
        with clustering_run(
            session,
            scope_type="definite-continuation-pg-initial",
            item_ids=[item.id],
            rule_version="definite-continuation-pg-v1",
        ):
            assign_cluster(session, item)

        parent = session.scalar(select(ClusterEventProjection))
        assert parent is not None
        original_create = event_projection_module._create_ambiguous_projection

        def force_ambiguous_projection(
            current_session: Session,
            *,
            run: ClusteringRun,
            cluster: Cluster,
            cluster_anchor: str,
            cluster_occurrence: int,
            predecessor: ClusterEventProjection,
        ) -> ClusterEventProjection:
            assert predecessor.id == parent.id
            return original_create(
                current_session,
                run=run,
                cluster=cluster,
                cluster_anchor=cluster_anchor,
                cluster_occurrence=cluster_occurrence,
            )

        monkeypatch.setattr(
            event_projection_module,
            "_continue_projection",
            force_ambiguous_projection,
        )
        rejected_run_id = ""
        with pytest.raises(
            DBAPIError,
            match="event_projection_ambiguous_target_invalid",
        ):
            with clustering_run(
                session,
                scope_type="definite-continuation-pg-change",
                item_ids=[item.id],
                rule_version="definite-continuation-pg-v1",
            ) as rejected_run_id:
                pass

        rejected_run = session.get(ClusteringRun, rejected_run_id)
        assert rejected_run is not None
        assert rejected_run.status == "failed"
        assert session.scalar(select(func.count(ReaderEvent.id))) == 1
        assert session.scalar(select(func.count(EventRevision.id))) == 1
        assert session.scalar(
            select(func.count(ClusterEventProjection.id))
        ) == 1
        assert session.scalar(select(func.count(EventLineage.id))) == 0
        assert session.scalar(
            select(ReaderEvent.status).where(ReaderEvent.id == parent.event_id)
        ) == "active"

    engine.dispose()


def test_postgres_completed_continuation_rejects_late_ambiguous_projection(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Late ambiguous projection PG",
            url="https://example.com/late-ambiguous-projection-pg.xml",
        )
        item = add_event_split_item(
            session,
            source_id=source_id,
            prefix="late-ambiguous-projection-pg",
            suffix="one",
        )
        with clustering_run(
            session,
            scope_type="late-ambiguous-projection-pg-initial",
            item_ids=[item.id],
            rule_version="late-ambiguous-projection-pg-v1",
        ):
            cluster = assign_cluster(session, item)

        initial_projection = session.scalar(select(ClusterEventProjection))
        assert initial_projection is not None
        monkeypatch.setattr(
            event_projection_module,
            "project_completed_clustering_run",
            lambda *_args, **_kwargs: [],
        )
        with clustering_run(
            session,
            scope_type="late-ambiguous-projection-pg-change",
            item_ids=[item.id],
            rule_version="late-ambiguous-projection-pg-v1",
        ) as completed_run_id:
            pass

        completed_run = session.get(ClusteringRun, completed_run_id)
        assert completed_run is not None
        assert session.scalar(
            text(
                "SELECT completed_transaction_id IS NOT NULL "
                "FROM clustering_runs WHERE id = :run_id"
            ),
            {"run_id": completed_run_id},
        ) is True
        after_member = session.scalar(
            select(ClusteringRunMembership).where(
                ClusteringRunMembership.run_id == completed_run_id,
                ClusteringRunMembership.snapshot_phase == "after",
            )
        )
        assert after_member is not None

        with pytest.raises(
            DBAPIError,
            match="event_projection_ambiguous_target_invalid",
        ):
            event_projection_module._create_ambiguous_projection(
                session,
                run=completed_run,
                cluster=cluster,
                cluster_anchor=after_member.cluster_anchor,
                cluster_occurrence=after_member.cluster_occurrence,
            )
            session.commit()

        session.rollback()
        assert session.scalar(select(func.count(ReaderEvent.id))) == 1
        assert session.scalar(select(func.count(EventRevision.id))) == 1
        assert session.scalar(
            select(func.count(ClusterEventProjection.id))
        ) == 1
        assert session.scalar(select(func.count(EventLineage.id))) == 0
        assert session.scalar(
            select(ReaderEvent.status).where(
                ReaderEvent.id == initial_projection.event_id
            )
        ) == "active"

    engine.dispose()


def test_postgres_ambiguous_commit_rejects_omitted_second_target(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Omitted ambiguous target PG",
            url="https://example.com/omitted-ambiguous-target-pg.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        first = add_event_split_item(
            session,
            source_id=source.id,
            prefix="omitted-ambiguous-target-pg",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source.id,
            prefix="omitted-ambiguous-target-pg",
            suffix="two",
        )
        third = add_event_split_item(
            session,
            source_id=source.id,
            prefix="omitted-ambiguous-target-pg",
            suffix="three",
        )
        with clustering_run(
            session,
            scope_type="omitted-ambiguous-target-pg-initial",
            item_ids=[first.id, second.id, third.id],
            rule_version="omitted-ambiguous-target-pg-v1",
        ):
            first_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=first_cluster.id,
                    content_item_id=second.id,
                )
            )
            second_cluster = Cluster(
                cluster_key="omitted-ambiguous-target-pg-second",
                title="Omitted ambiguous target PG second",
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=third.id,
                )
            )

        parent_event_ids = set(session.scalars(select(ReaderEvent.id)))
        assert len(parent_event_ids) == 2
        original_add = session.add
        original_add_all = session.add_all
        ambiguous_projection_count = 0
        omitted_event_ids: set[int] = set()

        def omit_second_projection(instance: object, *args: object, **kwargs: object) -> None:
            nonlocal ambiguous_projection_count
            if (
                isinstance(instance, ClusterEventProjection)
                and instance.reconciliation_kind == "ambiguous"
            ):
                ambiguous_projection_count += 1
                if ambiguous_projection_count == 2:
                    assert instance.event_id is not None
                    omitted_event_ids.add(instance.event_id)
                    return
            original_add(instance, *args, **kwargs)  # type: ignore[arg-type]

        def omit_lineage_for_missing_target(instances: object) -> None:
            values = list(instances)  # type: ignore[arg-type]
            original_add_all(
                value
                for value in values
                if not (
                    isinstance(value, EventLineage)
                    and value.target_event_id in omitted_event_ids
                )
            )

        monkeypatch.setattr(session, "add", omit_second_projection)
        monkeypatch.setattr(session, "add_all", omit_lineage_for_missing_target)
        rejected_run_id = ""
        with pytest.raises(
            DBAPIError,
            match="event_ambiguous_terminal_graph_incomplete",
        ):
            with clustering_run(
                session,
                scope_type="omitted-ambiguous-target-pg-change",
                item_ids=[first.id, second.id, third.id],
                rule_version="omitted-ambiguous-target-pg-v1",
            ) as rejected_run_id:
                session.add(
                    ClusterItem(
                        cluster_id=second_cluster.id,
                        content_item_id=second.id,
                    )
                )

        assert ambiguous_projection_count == 2
        assert len(omitted_event_ids) == 1
        rejected_run = session.get(ClusteringRun, rejected_run_id)
        assert rejected_run is not None
        assert rejected_run.status == "failed"
        assert session.scalar(select(func.count(ReaderEvent.id))) == 2
        assert session.scalar(select(func.count(EventRevision.id))) == 2
        assert session.scalar(
            select(func.count(ClusterEventProjection.id))
        ) == 2
        assert session.scalar(select(func.count(EventLineage.id))) == 0
        assert set(
            session.scalars(
                select(ReaderEvent.status).where(
                    ReaderEvent.id.in_(parent_event_ids)
                )
            )
        ) == {"active"}

    engine.dispose()


def test_postgres_ambiguous_lookup_scales_with_projectionless_cluster(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Ambiguous scale source",
            url="https://example.com/ambiguous-scale.xml",
        )
        item = add_event_split_item(
            session,
            source_id=source_id,
            prefix="ambiguous-scale",
            suffix="seed",
        )
        with clustering_run(
            session,
            scope_type="ambiguous-scale-seed",
            item_ids=[item.id],
            rule_version="ambiguous-scale-v1",
        ):
            assign_cluster(session, item)

        predecessor = session.scalar(select(ClusterEventProjection))
        assert predecessor is not None
        run = ClusteringRun(
            scope_type="ambiguous-scale-probe",
            scope_key="a" * 64,
            rule_version="ambiguous-scale-v1",
            status="started",
        )
        session.add(run)
        session.flush()

        anchors = [
            hashlib.sha256(f"ambiguous-scale-{index}".encode()).hexdigest()
            for index in range(877)
        ]
        session.add_all(
            ClusteringRunMembership(
                run_id=run.id,
                snapshot_phase=phase,
                cluster_anchor=anchor,
                cluster_occurrence=1,
                evidence_anchor=f"ambiguous-scale-evidence-{index}",
                evidence_occurrence=1,
            )
            for phase in ("before", "after")
            for index, anchor in enumerate(anchors)
        )
        session.flush()
        session.add(
            ClusteringRunProjectionPredecessor(
                run_id=run.id,
                cluster_anchor=anchors[0],
                cluster_occurrence=1,
                predecessor_projection_id=predecessor.id,
            )
        )
        session.flush()
        session.execute(text("SET LOCAL statement_timeout = '1s'"))

        assert session.scalar(
            text(
                "SELECT count(*) = 2 FROM pg_proc "
                "WHERE oid IN ("
                "'reader_expected_ambiguous_clusters(character varying)'"
                "::regprocedure, "
                "'reader_ambiguous_run_terminal_graph_status(character varying)'"
                "::regprocedure) "
                "AND 'enable_nestloop=off' = ANY(proconfig)"
            )
        ) is True
        assert session.scalar(
            text(
                "SELECT reader_expected_ambiguous_after("
                ":run_id, :cluster_anchor, 1)"
            ),
            {"run_id": run.id, "cluster_anchor": anchors[-1]},
        ) is False

        session.rollback()

    engine.dispose()


def test_postgres_caller_owned_clustering_run_bounds_commit_statement(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Caller-owned commit timeout source",
            url="https://example.com/caller-owned-commit-timeout.xml",
        )
        item = add_event_split_item(
            session,
            source_id=source_id,
            prefix="caller-owned-commit-timeout",
            suffix="seed",
        )

        with clustering_run(
            session,
            scope_type="caller-owned-commit-timeout",
            item_ids=[item.id],
            rule_version="caller-owned-commit-timeout-v1",
            commit_on_success=False,
        ):
            assign_cluster(session, item)

        assert session.scalar(text("SHOW statement_timeout")) == "5min"
        session.rollback()

    engine.dispose()


def test_postgres_ambiguous_commit_scales_for_complete_graph(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    cluster_count = 48

    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Complete ambiguous scale source",
            url="https://example.com/complete-ambiguous-scale.xml",
        )
        items = [
            add_event_split_item(
                session,
                source_id=source_id,
                prefix="complete-ambiguous-scale",
                suffix=str(index),
            )
            for index in range(cluster_count * 2)
        ]
        with clustering_run(
            session,
            scope_type="complete-ambiguous-scale-seed",
            item_ids=[item.id for item in items],
            rule_version="complete-ambiguous-scale-v1",
        ):
            initial_clusters = [
                Cluster(
                    cluster_key=f"complete-ambiguous-before-{index}",
                    title=f"Complete ambiguous before {index}",
                )
                for index in range(cluster_count)
            ]
            session.add_all(initial_clusters)
            session.flush()
            session.add_all(
                ClusterItem(
                    cluster_id=cluster.id,
                    content_item_id=items[(index * 2) + offset].id,
                )
                for index, cluster in enumerate(initial_clusters)
                for offset in (0, 1)
            )

        with clustering_run(
            session,
            scope_type="complete-ambiguous-scale-change",
            item_ids=[item.id for item in items],
            rule_version="complete-ambiguous-scale-v1",
            commit_on_success=False,
        ) as run_id:
            session.execute(
                delete(ClusterItem).where(
                    ClusterItem.cluster_id.in_(
                        cluster.id for cluster in initial_clusters
                    )
                )
            )
            target_clusters = [
                Cluster(
                    cluster_key=f"complete-ambiguous-after-{index}",
                    title=f"Complete ambiguous after {index}",
                )
                for index in range(cluster_count)
            ]
            session.add_all(target_clusters)
            session.flush()
            session.add_all(
                ClusterItem(
                    cluster_id=cluster.id,
                    content_item_id=item_id,
                )
                for index, cluster in enumerate(target_clusters)
                for item_id in (
                    items[index * 2].id,
                    items[((index + 1) % cluster_count) * 2 + 1].id,
                )
            )

        session.execute(text("SET LOCAL statement_timeout = '1s'"))
        session.commit()
        run_guard_definition = session.scalar(
            text(
                "SELECT pg_get_functiondef("
                "'reader_require_complete_ambiguous_run(character varying)'"
                "::regprocedure)"
            )
        )
        status_definition = session.scalar(
            text(
                "SELECT pg_get_functiondef("
                "'reader_ambiguous_run_terminal_graph_status(character varying)'"
                "::regprocedure)"
            )
        )
        assert run_guard_definition is not None
        assert status_definition is not None
        assert run_guard_definition.count(
            "reader_ambiguous_run_terminal_graph_status"
        ) == 1
        assert "reader_expected_ambiguous_clusters" not in run_guard_definition
        assert status_definition.count("reader_expected_ambiguous_clusters") == 1
        assert session.scalar(
            select(func.count(ClusterEventProjection.id)).where(
                ClusterEventProjection.clustering_run_id == run_id,
                ClusterEventProjection.reconciliation_kind == "ambiguous",
            )
        ) == cluster_count
    engine.dispose()


@pytest.mark.parametrize("corruption", ["missing_all", "initial"])
def test_postgres_ambiguous_run_rejects_all_targets_missing_or_mislabeled(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        first, second, third, second_cluster = seed_partial_overlap_clusters(
            session,
            prefix=f"ambiguous-run-guard-{corruption}",
        )

        parent_event_ids = set(session.scalars(select(ReaderEvent.id)))
        assert len(parent_event_ids) == 2

        def corrupt_projection(
            target_session: Session,
            run_id: str,
            cluster_ids: list[int] | tuple[int, ...],
        ) -> list[ClusterEventProjection]:
            if corruption == "missing_all":
                return []
            run = target_session.get(ClusteringRun, run_id)
            assert run is not None
            _before, after = event_projection_module._sealed_snapshot_clusters(
                target_session,
                run_id,
            )
            live_after = event_projection_module._live_after_clusters(
                target_session,
                cluster_ids,
            )
            return [
                event_projection_module._create_initial_projection(
                    target_session,
                    run=run,
                    cluster=live_after[after_key],
                    cluster_anchor=after_key[0],
                    cluster_occurrence=after_key[1],
                )
                for after_key in sorted(after)
            ]

        monkeypatch.setattr(
            event_projection_module,
            "project_completed_clustering_run",
            corrupt_projection,
        )
        rejected_run_id = ""
        with pytest.raises(
            DBAPIError,
            match="event_ambiguous_terminal_graph_incomplete",
        ):
            with clustering_run(
                session,
                scope_type=f"ambiguous-run-guard-{corruption}-change",
                item_ids=[first.id, second.id, third.id],
                rule_version=f"ambiguous-run-guard-{corruption}-v1",
            ) as rejected_run_id:
                session.add(
                    ClusterItem(
                        cluster_id=second_cluster.id,
                        content_item_id=second.id,
                    )
                )

        rejected_run = session.get(ClusteringRun, rejected_run_id)
        assert rejected_run is not None
        assert rejected_run.status == "failed"
        assert session.scalar(select(func.count(ReaderEvent.id))) == 2
        assert session.scalar(select(func.count(EventRevision.id))) == 2
        assert session.scalar(
            select(func.count(ClusterEventProjection.id))
        ) == 2
        assert session.scalar(select(func.count(EventLineage.id))) == 0
        assert set(
            session.scalars(
                select(ReaderEvent.status).where(
                    ReaderEvent.id.in_(parent_event_ids)
                )
            )
        ) == {"active"}

    engine.dispose()


def test_ambiguous_run_upgrade_rejects_preexisting_incomplete_graph(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0042_ambiguous_topology_guard")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        first, second, third, second_cluster = seed_partial_overlap_clusters(
            session,
            prefix="ambiguous-upgrade-audit",
        )
        parent_event_ids = set(session.scalars(select(ReaderEvent.id)))
        assert len(parent_event_ids) == 2
        monkeypatch.setattr(
            event_projection_module,
            "project_completed_clustering_run",
            lambda *_args, **_kwargs: [],
        )
        with clustering_run(
            session,
            scope_type="ambiguous-upgrade-audit-change",
            item_ids=[first.id, second.id, third.id],
            rule_version="ambiguous-upgrade-audit-v1",
        ) as invalid_run_id:
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=second.id,
                )
            )

        invalid_run = session.get(ClusteringRun, invalid_run_id)
        assert invalid_run is not None
        assert invalid_run.status == "completed"
        assert session.scalar(
            select(func.count(ClusterEventProjection.id)).where(
                ClusterEventProjection.clustering_run_id == invalid_run_id
            )
        ) == 0
        assert set(
            session.scalars(
                select(ReaderEvent.status).where(
                    ReaderEvent.id.in_(parent_event_ids)
                )
            )
        ) == {"active"}

    engine.dispose()
    with pytest.raises(
        DBAPIError,
        match="event_ambiguous_run_upgrade_requires_complete_graph",
    ):
        command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0042_ambiguous_topology_guard"
        )
        assert connection.scalar(
            text(
                "SELECT count(*) FROM cluster_event_projections "
                "WHERE clustering_run_id = :run_id"
            ),
            {"run_id": invalid_run_id},
        ) == 0
    engine.dispose()


def test_ambiguous_topology_upgrade_rejects_unproven_existing_graph(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0041_event_ambiguous_lineage")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Ambiguous topology upgrade PG",
            url="https://example.com/ambiguous-topology-upgrade-pg.xml",
        )
        first = add_event_split_item(
            session,
            source_id=source_id,
            prefix="ambiguous-topology-upgrade-pg",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source_id,
            prefix="ambiguous-topology-upgrade-pg",
            suffix="two",
        )
        with clustering_run(
            session,
            scope_type="ambiguous-topology-upgrade-pg-initial",
            item_ids=[first.id],
            rule_version="ambiguous-topology-upgrade-pg-v1",
        ):
            cluster = assign_cluster(session, first)

        with clustering_run(
            session,
            scope_type="ambiguous-topology-upgrade-pg-change",
            item_ids=[first.id, second.id],
            rule_version="ambiguous-topology-upgrade-pg-v1",
        ):
            first_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == cluster.id,
                    ClusterItem.content_item_id == first.id,
                )
            )
            assert first_link is not None
            session.delete(first_link)
            session.add(
                ClusterItem(
                    cluster_id=cluster.id,
                    content_item_id=second.id,
                )
            )

        assert session.scalar(
            select(func.count(ClusterEventProjection.id)).where(
                ClusterEventProjection.reconciliation_kind == "ambiguous"
            )
        ) == 1
        assert session.scalar(select(func.count(EventLineage.id))) == 0

    engine.dispose()
    with pytest.raises(
        DBAPIError,
        match="event_ambiguous_topology_upgrade_requires_empty_graph",
    ):
        command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0041_event_ambiguous_lineage"
        )
        assert connection.scalar(
            text(
                "SELECT count(*) FROM cluster_event_projections "
                "WHERE reconciliation_kind = 'ambiguous'"
            )
        ) == 1
    engine.dispose()


@pytest.mark.parametrize(
    "corruption",
    ["missing_lineage", "state", "baseline", "interaction"],
)
def test_postgres_merge_rejects_incomplete_or_stateful_terminal_graph(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name=f"Rejected merge {corruption}",
            url=f"https://example.com/rejected-merge-{corruption}.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        first = add_event_split_item(
            session,
            source_id=source.id,
            prefix=f"rejected-merge-{corruption}",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source.id,
            prefix=f"rejected-merge-{corruption}",
            suffix="two",
        )
        with clustering_run(
            session,
            scope_type=f"rejected-merge-{corruption}-initial",
            item_ids=[first.id, second.id],
            rule_version="rejected-merge-v1",
        ):
            first_cluster = assign_cluster(session, first)
            second_cluster = Cluster(
                cluster_key=f"rejected-merge-{corruption}-second",
                title=f"Rejected merge {corruption} second",
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=second.id,
                )
            )

        parent_event_ids = set(
            session.scalars(select(ClusterEventProjection.event_id))
        )
        assert len(parent_event_ids) == 2
        session.commit()

        if corruption == "missing_lineage":
            original_add_all = session.add_all

            def omit_one_merge_lineage(instances: object) -> None:
                values = list(instances)  # type: ignore[arg-type]
                if values and all(
                    isinstance(value, EventLineage) for value in values
                ):
                    values = values[:1]
                original_add_all(values)

            monkeypatch.setattr(session, "add_all", omit_one_merge_lineage)
        else:
            original_create_merge = (
                event_projection_module._create_merge_projection
            )

            def create_stateful_merge(*args: object, **kwargs: object):
                projection = original_create_merge(*args, **kwargs)
                if corruption == "state":
                    session.add(
                        EventUserState(
                            event_id=projection.event_id,
                            seen_revision_id=projection.event_revision_id,
                            read_status="summary_seen",
                            read_later=True,
                            starred=True,
                        )
                    )
                elif corruption == "baseline":
                    session.add(
                        MigrationBaseline(
                            idempotency_key="9" * 64,
                            migration_version=(
                                "legacy-user-state-baseline-v1"
                            ),
                            legacy_user_state_id=900003,
                            legacy_object_type="cluster",
                            legacy_object_id=first_cluster.id,
                            resolved_event_id=projection.event_id,
                            resolved_revision_id=(
                                projection.event_revision_id
                            ),
                            read_status="summary_seen",
                            read_later=True,
                            starred=True,
                            source_updated_at=datetime.now(timezone.utc),
                        )
                    )
                else:
                    session.add(
                        InteractionEvent(
                            operation_id=(
                                "rejected-merge-terminal-interaction"
                            ),
                            target_kind="event",
                            event_id=projection.event_id,
                            observed_revision_id=(
                                projection.event_revision_id
                            ),
                            action="starred_set",
                            set_value=True,
                            payload={},
                            occurred_at=datetime.now(timezone.utc),
                        )
                    )
                return projection

            monkeypatch.setattr(
                event_projection_module,
                "_create_merge_projection",
                create_stateful_merge,
            )

        with pytest.raises(
            DBAPIError,
            match="event_merge_terminal_graph_incomplete",
        ):
            with clustering_run(
                session,
                scope_type=f"rejected-merge-{corruption}-change",
                item_ids=[first.id, second.id],
                rule_version="rejected-merge-v1",
            ) as rejected_run_id:
                second_link = session.scalar(
                    select(ClusterItem).where(
                        ClusterItem.cluster_id == second_cluster.id,
                        ClusterItem.content_item_id == second.id,
                    )
                )
                assert second_link is not None
                session.delete(second_link)
                session.add(
                    ClusterItem(
                        cluster_id=first_cluster.id,
                        content_item_id=second.id,
                    )
                )

        rejected_run = session.get(ClusteringRun, rejected_run_id)
        assert rejected_run is not None
        assert rejected_run.status == "failed"
        assert set(
            session.scalars(
                select(ReaderEvent.status).where(
                    ReaderEvent.id.in_(parent_event_ids)
                )
            )
        ) == {"active"}
        assert session.scalar(select(func.count(ReaderEvent.id))) == 2
        assert session.scalar(
            select(func.count(ClusterEventProjection.id))
        ) == 2
        assert session.scalar(select(func.count(EventLineage.id))) == 0
        assert session.scalar(select(func.count(EventUserState.id))) == 0
        assert session.scalar(select(func.count(MigrationBaseline.id))) == 0
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0

    engine.dispose()


def test_split_commit_rejects_missing_lineage_and_active_parent(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Incomplete split graph",
            url="https://example.com/incomplete-split-graph.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        first = add_event_split_item(
            session,
            source_id=source.id,
            prefix="incomplete-split",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source.id,
            prefix="incomplete-split",
            suffix="two",
        )
        with clustering_run(
            session,
            scope_type="incomplete-split-initial",
            item_ids=[first.id, second.id],
            rule_version="incomplete-split-v1",
        ):
            original_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=original_cluster.id,
                    content_item_id=second.id,
                )
            )

        predecessor = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.cluster_id_snapshot == original_cluster.id
            )
        )
        assert predecessor is not None
        monkeypatch.setattr(
            event_projection_module,
            "project_completed_clustering_run",
            lambda *_args, **_kwargs: [],
        )
        with clustering_run(
            session,
            scope_type="incomplete-split-change",
            item_ids=[first.id, second.id],
            rule_version="incomplete-split-v1",
        ) as split_run_id:
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == original_cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            assert second_link is not None
            session.delete(second_link)
            second_cluster = Cluster(
                cluster_key="incomplete-split-second",
                title="Incomplete split second",
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=second.id,
                )
            )

        live_after = event_projection_module._live_after_clusters(
            session,
            [original_cluster.id, second_cluster.id],
        )
        original_key = next(
            key for key, cluster in live_after.items() if cluster.id == original_cluster.id
        )
        child_event, child_revision, child_fingerprint = (
            event_projection_module._create_event_with_initial_revision(
                session,
                cluster=original_cluster,
            )
        )
        session.add(
            ClusterEventProjection(
                cluster_id=original_cluster.id,
                cluster_id_snapshot=original_cluster.id,
                clustering_run_id=split_run_id,
                cluster_anchor=original_key[0],
                cluster_occurrence=original_key[1],
                event_id=child_event.id,
                event_revision_id=child_revision.id,
                predecessor_projection_id=predecessor.id,
                reconciliation_kind="split",
                reconciliation_rule_version="incomplete-split-v1",
                before_evidence_fingerprint=predecessor.after_evidence_fingerprint,
                after_evidence_fingerprint=child_fingerprint,
            )
        )
        with pytest.raises(
            DBAPIError,
            match="event_split_terminal_graph_incomplete",
        ):
            session.commit()
        session.rollback()
    engine.dispose()


@pytest.mark.parametrize("late_write", ["state", "continuation"])
def test_split_commit_rejects_late_write_after_complete_lineage(
    postgres_url: str, late_write: str
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Late-state split graph",
            url="https://example.com/late-state-split-graph.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        first = add_event_split_item(
            session,
            source_id=source.id,
            prefix="late-state-split",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source.id,
            prefix="late-state-split",
            suffix="two",
        )
        with clustering_run(
            session,
            scope_type="late-state-split-initial",
            item_ids=[first.id, second.id],
            rule_version="late-state-split-v1",
        ):
            original_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=original_cluster.id,
                    content_item_id=second.id,
                )
            )

        with clustering_run(
            session,
            scope_type="late-state-split-change",
            item_ids=[first.id, second.id],
            rule_version="late-state-split-v1",
            commit_on_success=False,
        ) as split_run_id:
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == original_cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            assert second_link is not None
            session.delete(second_link)
            second_cluster = Cluster(
                cluster_key="late-state-split-second",
                title="Late-state split second",
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=second.id,
                )
            )

        child = session.scalar(
            select(ClusterEventProjection)
            .where(
                ClusterEventProjection.clustering_run_id == split_run_id,
                ClusterEventProjection.reconciliation_kind == "split",
            )
            .order_by(ClusterEventProjection.id)
        )
        assert child is not None
        session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
        if late_write == "continuation":
            with pytest.raises(DBAPIError) as error:
                with clustering_run(
                    session,
                    scope_type="late-state-split-continuation",
                    item_ids=[first.id, second.id],
                    rule_version="late-state-split-v1",
                    commit_on_success=False,
                ):
                    pass
                session.commit()
            error_chain: list[str] = []
            current_error: BaseException | None = error.value
            while current_error is not None:
                error_chain.append(str(current_error))
                current_error = current_error.__context__
            assert any("event_split_" in message for message in error_chain)
            session.rollback()
            return
        session.add(
            EventUserState(
                event_id=child.event_id,
                seen_revision_id=child.event_revision_id,
                read_status="summary_seen",
                read_later=True,
                starred=True,
            )
        )
        with pytest.raises(
            DBAPIError,
            match="event_split_terminal_graph_incomplete",
        ):
            session.flush()
        session.rollback()
    engine.dispose()


def test_split_projection_guard_upgrade_rejects_existing_same_transaction_extra_projection(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0035_split_transaction_integrity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Pre-0036 split projection hole",
            url="https://example.com/pre-0036-split-projection-hole.xml",
        )
        first = add_event_split_item(
            session,
            source_id=source_id,
            prefix="pre-0036-split-projection-hole",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source_id,
            prefix="pre-0036-split-projection-hole",
            suffix="two",
        )
        with clustering_run(
            session,
            scope_type="pre-0036-split-initial",
            item_ids=[first.id, second.id],
            rule_version="pre-0036-split-v1",
        ):
            original_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=original_cluster.id,
                    content_item_id=second.id,
                )
            )

        with clustering_run(
            session,
            scope_type="pre-0036-split-change",
            item_ids=[first.id, second.id],
            rule_version="pre-0036-split-v1",
            commit_on_success=False,
        ) as split_run_id:
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == original_cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            assert second_link is not None
            session.delete(second_link)
            second_cluster = Cluster(
                cluster_key="pre-0036-split-second",
                title="Pre-0036 split second",
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=second.id,
                )
            )

        child = session.scalar(
            select(ClusterEventProjection)
            .where(
                ClusterEventProjection.clustering_run_id == split_run_id,
                ClusterEventProjection.reconciliation_kind == "split",
            )
            .order_by(ClusterEventProjection.id)
        )
        assert child is not None
        session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
        with clustering_run(
            session,
            scope_type="pre-0036-split-continuation",
            item_ids=[first.id, second.id],
            rule_version="pre-0036-split-v1",
            commit_on_success=False,
        ):
            pass
        session.commit()
        assert session.scalar(
            select(func.count(ClusterEventProjection.id)).where(
                ClusterEventProjection.event_id == child.event_id
            )
        ) == 2

    engine.dispose()
    with pytest.raises(
        DBAPIError,
        match="event_split_projection_upgrade_requires_terminal_graph",
    ):
        command.upgrade(config, "head")


def test_split_projection_audit_upgrade_rejects_forged_created_transaction_id(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0035_split_transaction_integrity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Forged split projection transaction",
            url="https://example.com/forged-split-projection-transaction.xml",
        )
        first = add_event_split_item(
            session,
            source_id=source_id,
            prefix="forged-split-projection-transaction",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source_id,
            prefix="forged-split-projection-transaction",
            suffix="two",
        )
        with clustering_run(
            session,
            scope_type="forged-split-initial",
            item_ids=[first.id, second.id],
            rule_version="forged-split-v1",
        ):
            original_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=original_cluster.id,
                    content_item_id=second.id,
                )
            )

        with clustering_run(
            session,
            scope_type="forged-split-change",
            item_ids=[first.id, second.id],
            rule_version="forged-split-v1",
            commit_on_success=False,
        ) as split_run_id:
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == original_cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            assert second_link is not None
            session.delete(second_link)
            second_cluster = Cluster(
                cluster_key="forged-split-second",
                title="Forged split second",
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=second.id,
                )
            )

        child = session.scalar(
            select(ClusterEventProjection)
            .where(
                ClusterEventProjection.clustering_run_id == split_run_id,
                ClusterEventProjection.reconciliation_kind == "split",
            )
            .order_by(ClusterEventProjection.id)
        )
        assert child is not None
        session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))

        import reader_api.event_projection as event_projection

        def insert_continuation_with_forged_transaction(
            target_session: Session,
            *,
            run: ClusteringRun,
            cluster: Cluster,
            cluster_anchor: str,
            cluster_occurrence: int,
            predecessor: ClusterEventProjection,
        ) -> ClusterEventProjection:
            projection_id = target_session.execute(
                text(
                    "INSERT INTO cluster_event_projections "
                    "(cluster_id, cluster_id_snapshot, clustering_run_id, "
                    " cluster_anchor, cluster_occurrence, event_id, "
                    " event_revision_id, predecessor_projection_id, "
                    " reconciliation_kind, reconciliation_rule_version, "
                    " before_evidence_fingerprint, "
                    " after_evidence_fingerprint, created_transaction_id) "
                    "VALUES (:cluster_id, :cluster_id, :run_id, :anchor, "
                    " :occurrence, :event_id, :revision_id, "
                    " :predecessor_id, 'continued', :rule_version, "
                    " :fingerprint, :fingerprint, '1'::xid8) "
                    "RETURNING id"
                ),
                {
                    "cluster_id": cluster.id,
                    "run_id": run.id,
                    "anchor": cluster_anchor,
                    "occurrence": cluster_occurrence,
                    "event_id": predecessor.event_id,
                    "revision_id": predecessor.event_revision_id,
                    "predecessor_id": predecessor.id,
                    "rule_version": run.rule_version,
                    "fingerprint": predecessor.after_evidence_fingerprint,
                },
            ).scalar_one()
            projection = target_session.get(
                ClusterEventProjection,
                projection_id,
            )
            assert projection is not None
            return projection

        monkeypatch.setattr(
            event_projection,
            "_continue_projection",
            insert_continuation_with_forged_transaction,
        )
        with clustering_run(
            session,
            scope_type="forged-split-continuation",
            item_ids=[first.id, second.id],
            rule_version="forged-split-v1",
            commit_on_success=False,
        ):
            pass
        session.commit()
        assert session.scalar(
            select(func.count(ClusterEventProjection.id)).where(
                ClusterEventProjection.event_id == child.event_id
            )
        ) == 2

    engine.dispose()
    with pytest.raises(
        DBAPIError,
        match="event_split_xid_provenance_upgrade_requires_empty_graph",
    ):
        command.upgrade(config, "head")


def test_split_xid_provenance_forces_transaction_and_installs_all_guards(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        target = ReaderEvent(uid="00380000-0000-4000-8000-000000000001")
        session.add(target)
        session.flush()
        transaction_was_forced = session.execute(
            text(
                "INSERT INTO event_revisions "
                "(uid, event_id, revision_no, evidence_fingerprint, "
                " title_snapshot, created_at, created_transaction_id) "
                "VALUES ('00380000-0000-4000-8000-000000000002', "
                " :event_id, 1, :fingerprint, '', CURRENT_TIMESTAMP, "
                " '1'::xid8) "
                "RETURNING created_transaction_id = pg_current_xact_id()"
            ),
            {
                "event_id": target.id,
                "fingerprint": "8" * 64,
            },
        ).scalar_one()
        assert transaction_was_forced is True
        installed_guards = set(
            session.execute(
                text(
                    "SELECT table_name.relname, trigger.tgname "
                    "FROM pg_trigger trigger "
                    "JOIN pg_class table_name "
                    "  ON table_name.oid = trigger.tgrelid "
                    "WHERE NOT trigger.tgisinternal "
                    "  AND trigger.tgname LIKE "
                    "      'trg_00_%_created_transaction'"
                )
            ).tuples()
        )
        assert installed_guards == {
            (
                "event_revisions",
                "trg_00_event_revision_created_transaction",
            ),
            (
                "cluster_event_projections",
                "trg_00_projection_created_transaction",
            ),
            (
                "event_lineages",
                "trg_00_lineage_created_transaction",
            ),
            (
                "evidence_snapshots",
                "trg_00_evidence_snapshot_created_transaction",
            ),
                (
                    "synthesis_versions",
                    "trg_00_synthesis_version_created_transaction",
                ),
                (
                    "evidence_reviews",
                    "trg_00_evidence_review_created_transaction",
                ),
            }
        session.rollback()

    engine.dispose()


def test_split_projection_rejects_parent_with_any_ambiguous_child(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0042_ambiguous_topology_guard")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Mixed split topology",
            url="https://example.com/mixed-split-topology.xml",
        )
        items = [
            add_event_split_item(
                session,
                source_id=source_id,
                prefix="mixed-split",
                suffix=suffix,
            )
            for suffix in ("a-one", "a-two", "a-three", "b-one")
        ]
        with clustering_run(
            session,
            scope_type="mixed-split-initial",
            item_ids=[item.id for item in items],
            rule_version="mixed-split-v1",
        ):
            cluster_a = assign_cluster(session, items[0])
            session.add_all(
                ClusterItem(cluster_id=cluster_a.id, content_item_id=item.id)
                for item in items[1:3]
            )
            cluster_b = assign_cluster(session, items[3])

        predecessor = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.cluster_id_snapshot == cluster_a.id
            )
        )
        assert predecessor is not None
        monkeypatch.setattr(
            event_projection_module,
            "project_completed_clustering_run",
            lambda *_args, **_kwargs: [],
        )
        with clustering_run(
            session,
            scope_type="mixed-split-change",
            item_ids=[item.id for item in items],
            rule_version="mixed-split-v1",
        ) as mixed_run_id:
            for item in items[1:3]:
                link = session.scalar(
                    select(ClusterItem).where(
                        ClusterItem.cluster_id == cluster_a.id,
                        ClusterItem.content_item_id == item.id,
                    )
                )
                assert link is not None
                session.delete(link)
            b_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == cluster_b.id,
                    ClusterItem.content_item_id == items[3].id,
                )
            )
            assert b_link is not None
            session.delete(b_link)
            unique_child = Cluster(
                cluster_key="mixed-split-unique-child",
                title="Mixed split unique child",
            )
            ambiguous_child = Cluster(
                cluster_key="mixed-split-ambiguous-child",
                title="Mixed split ambiguous child",
            )
            session.add_all([unique_child, ambiguous_child])
            session.flush()
            session.add_all(
                [
                    ClusterItem(
                        cluster_id=unique_child.id,
                        content_item_id=items[1].id,
                    ),
                    ClusterItem(
                        cluster_id=ambiguous_child.id,
                        content_item_id=items[2].id,
                    ),
                    ClusterItem(
                        cluster_id=ambiguous_child.id,
                        content_item_id=items[3].id,
                    ),
                ]
            )

        command.upgrade(config, "0043_ambiguous_run_guard")
        live_after = event_projection_module._live_after_clusters(
            session,
            [cluster_a.id, unique_child.id, ambiguous_child.id],
        )
        cluster_a_key = next(
            key for key, cluster in live_after.items() if cluster.id == cluster_a.id
        )
        child_event, child_revision, child_fingerprint = (
            event_projection_module._create_event_with_initial_revision(
                session,
                cluster=cluster_a,
            )
        )
        session.add(
            ClusterEventProjection(
                cluster_id=cluster_a.id,
                cluster_id_snapshot=cluster_a.id,
                clustering_run_id=mixed_run_id,
                cluster_anchor=cluster_a_key[0],
                cluster_occurrence=cluster_a_key[1],
                event_id=child_event.id,
                event_revision_id=child_revision.id,
                predecessor_projection_id=predecessor.id,
                reconciliation_kind="split",
                reconciliation_rule_version="mixed-split-v1",
                before_evidence_fingerprint=predecessor.after_evidence_fingerprint,
                after_evidence_fingerprint=child_fingerprint,
            )
        )
        with pytest.raises(
            DBAPIError,
            match="event_projection_split_frozen_predecessor_mismatch",
        ):
            session.flush()
        session.rollback()
    engine.dispose()


def test_split_newborn_upgrade_refuses_an_unprovable_existing_split_graph(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0032_split_lineage_integrity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Pre-newborn split",
            url="https://example.com/pre-newborn-split.xml",
        )

        items: list[ContentItem] = []
        for suffix in ("one", "two"):
            raw = make_raw_entry(
                source_id=source_id,
                external_id=f"pre-newborn-split-{suffix}",
                title=f"Pre-newborn split {suffix}",
                url=f"https://example.com/pre-newborn-split-{suffix}",
                raw_content=f"Pre-newborn split evidence {suffix}",
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                document_type="normal_article",
                title=raw.title,
                content_text=raw.raw_content,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source_id,
                title=raw.title,
                content_text=raw.raw_content,
                url=raw.url,
                canonical_url=raw.url,
                content_hash=raw.content_hash,
            )
            session.add(item)
            session.flush()
            items.append(item)

        with clustering_run(
            session,
            scope_type="pre-newborn-split-initial",
            item_ids=[item.id for item in items],
            rule_version="pre-newborn-split-v1",
        ):
            original_cluster = assign_cluster(session, items[0])
            session.add(
                ClusterItem(
                    cluster_id=original_cluster.id,
                    content_item_id=items[1].id,
                )
            )

        with clustering_run(
            session,
            scope_type="pre-newborn-split-change",
            item_ids=[item.id for item in items],
            rule_version="pre-newborn-split-v1",
        ):
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == original_cluster.id,
                    ClusterItem.content_item_id == items[1].id,
                )
            )
            assert second_link is not None
            session.delete(second_link)
            second_cluster = Cluster(
                cluster_key="pre-newborn-split-second",
                title="Pre-newborn split second",
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=items[1].id,
                )
            )

        assert session.scalar(
            select(func.count(ClusterEventProjection.id)).where(
                ClusterEventProjection.reconciliation_kind == "split"
            )
        ) == 2

    assert_upgrade_refused_at_revision(
        config=config,
        engine=engine,
        error="event_split_newborn_upgrade_requires_empty_graph",
        revision="0032_split_lineage_integrity",
    )
    engine.dispose()


@pytest.mark.parametrize(
    ("source_revision", "expected_error"),
    [
        (
            "0033_split_target_newborn",
            "event_split_terminal_upgrade_requires_empty_graph",
        ),
        (
            "0034_split_terminal_integrity",
            "event_split_transaction_upgrade_requires_empty_graph",
        ),
    ],
)
def test_split_integrity_upgrade_refuses_an_unverifiable_existing_split_graph(
    postgres_url: str,
    source_revision: str,
    expected_error: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, source_revision)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Pre-terminal split",
            url="https://example.com/pre-terminal-split.xml",
        )
        first = add_event_split_item(
            session,
            source_id=source_id,
            prefix="pre-terminal-split",
            suffix="one",
        )
        second = add_event_split_item(
            session,
            source_id=source_id,
            prefix="pre-terminal-split",
            suffix="two",
        )
        with clustering_run(
            session,
            scope_type="pre-terminal-split-initial",
            item_ids=[first.id, second.id],
            rule_version="pre-terminal-split-v1",
        ):
            original_cluster = assign_cluster(session, first)
            session.add(
                ClusterItem(
                    cluster_id=original_cluster.id,
                    content_item_id=second.id,
                )
            )

        with clustering_run(
            session,
            scope_type="pre-terminal-split-change",
            item_ids=[first.id, second.id],
            rule_version="pre-terminal-split-v1",
        ):
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == original_cluster.id,
                    ClusterItem.content_item_id == second.id,
                )
            )
            assert second_link is not None
            session.delete(second_link)
            second_cluster = Cluster(
                cluster_key="pre-terminal-split-second",
                title="Pre-terminal split second",
            )
            session.add(second_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=second_cluster.id,
                    content_item_id=second.id,
                )
            )

        assert session.scalar(
            select(func.count(ClusterEventProjection.id)).where(
                ClusterEventProjection.reconciliation_kind == "split"
            )
        ) == 2

    assert_upgrade_refused_at_revision(
        config=config,
        engine=engine,
        error=expected_error,
        revision=source_revision,
    )
    engine.dispose()


def test_event_evidence_integrity_upgrade_refuses_nonempty_frozen_graph(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0023_event_projection")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        identity = add_migration_source_entry_seed(
            session,
            source_name="Frozen 0023 evidence",
            source_url="https://example.com/frozen-0023.xml",
            external_id="frozen-0023",
            title="Frozen 0023 evidence",
            legacy_key_fingerprint="a" * 64,
        )
        session.add(
            EventEvidence(
                uid="99999999-9999-4999-8999-999999999999",
                identity_fingerprint="b" * 64,
                source_entry_id=identity.id,
                fragment_fingerprint=None,  # type: ignore[arg-type]
            )
        )
        session.commit()

    assert_upgrade_refused_at_revision(
        config=config,
        engine=engine,
        error="event_projection_integrity_requires_empty_graph",
        revision="0023_event_projection",
    )
    engine.dispose()


def test_event_evidence_identity_upgrade_refuses_duplicate_source_fragment(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0024_event_evidence_integrity")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        identity = add_migration_source_entry_seed(
            session,
            source_name="Duplicate Event Evidence source",
            source_url="https://example.com/duplicate-event-evidence.xml",
            external_id="duplicate-event-evidence",
            title="Duplicate Event Evidence",
            legacy_key_fingerprint="d" * 64,
        )
        for index, fingerprint in enumerate(("e" * 64, "f" * 64), start=1):
            session.add(
                EventEvidence(
                    uid=f"{index:08d}-0000-4000-8000-000000000000",
                    identity_fingerprint=fingerprint,
                    source_entry_id=identity.id,
                    fragment_fingerprint="a" * 64,
                )
            )
        session.commit()

    assert_upgrade_refused_at_revision(
        config=config,
        engine=engine,
        error="event_evidence_source_fragment_duplicate",
        revision="0024_event_evidence_integrity",
    )
    engine.dispose()


def run_migration_cli(
    postgres_url: str,
    migration_command: str,
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "apps/api")
    environment["READER_MIGRATION_DATABASE_URL"] = postgres_url
    return subprocess.run(
        [sys.executable, "-m", "reader_api.migrations", migration_command],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )


def probe_runtime_startup(
    postgres_url: str,
    target: str,
    *,
    expected_head: str = "",
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["DATABASE_URL"] = postgres_url
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "apps/api")
    if expected_head:
        environment["READER_TEST_EXPECTED_HEAD"] = expected_head
    else:
        environment.pop("READER_TEST_EXPECTED_HEAD", None)
    return subprocess.run(
        [sys.executable, "-c", RUNTIME_STARTUP_PROBE, target],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )


def assert_runtime_targets_fail(
    postgres_url: str,
    expected_message: str,
    *,
    expected_head: str = "",
) -> None:
    for target in ("api", "fetch", "llm"):
        result = probe_runtime_startup(
            postgres_url,
            target,
            expected_head=expected_head,
        )
        output = result.stdout + result.stderr
        assert result.returncode != 0, f"{target} unexpectedly started: {output}"
        assert expected_message in output, output


def install_frozen_legacy_schema(connection) -> None:
    sql = LEGACY_SCHEMA_SQL.read_text(encoding="utf-8")
    for statement in sql.split(";\n"):
        if statement.strip():
            connection.exec_driver_sql(statement)


def install_legacy_schema_with_not_null_columns(
    connection,
    not_null_columns: tuple[tuple[str, str], ...],
) -> None:
    install_frozen_legacy_schema(connection)
    for table, column in not_null_columns:
        connection.exec_driver_sql(
            f'ALTER TABLE "{table}" ALTER COLUMN "{column}" SET NOT NULL'
        )


def install_strict_legacy_schema(connection) -> None:
    install_legacy_schema_with_not_null_columns(
        connection,
        STRICT_LEGACY_NULLABILITY_COLUMNS,
    )


def install_production_legacy_schema(connection) -> None:
    install_legacy_schema_with_not_null_columns(
        connection,
        PRODUCTION_LEGACY_NOT_NULL_COLUMNS,
    )


def add_frozen_legacy_manifest_seed(connection: Connection) -> None:
    source_id = connection.scalar(
        text(
            """
            INSERT INTO sources (
                folder_id, name, url, site_url, media_type, status, enabled,
                fetch_full_content, feed_trust_score, last_fetched_at,
                last_error, created_at, status_changed_at
            ) VALUES (
                NULL, '旧库来源', 'https://example.com/feed.xml',
                'https://example.com', 'article', 'active', true, false,
                0.75, NULL, '', CURRENT_TIMESTAMP, NULL
            ) RETURNING id
            """
        )
    )
    raw_id = connection.scalar(
        text(
            """
            INSERT INTO raw_entries (
                source_id, external_id, title, url, author, published_at,
                fetched_at, raw_summary, raw_content, content_hash
            ) VALUES (
                :source_id, 'legacy-guid', '旧库文章',
                'https://example.com/article', '作者', CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP, '旧摘要', '旧正文', :content_hash
            ) RETURNING id
            """
        ),
        {"source_id": source_id, "content_hash": "a" * 64},
    )
    document_id = connection.scalar(
        text(
            """
            INSERT INTO documents (
                raw_entry_id, document_type, title, summary, content_text,
                digest_score, created_at
            ) VALUES (
                :raw_id, 'normal_article', '旧库文章', '旧摘要', '旧正文',
                0.1, CURRENT_TIMESTAMP
            ) RETURNING id
            """
        ),
        {"raw_id": raw_id},
    )
    content_item_id = connection.scalar(
        text(
            """
            INSERT INTO content_items (
                document_id, source_id, title, summary, content_text, url,
                published_at, content_hash, canonical_url, normalized_title,
                lsh_signature, media_url, media_kind, media_duration,
                embedding_vector, embedding_model, cluster_score, created_at
            ) VALUES (
                :document_id, :source_id, '旧库文章', '旧摘要', '旧正文',
                'https://example.com/article', CURRENT_TIMESTAMP,
                :content_hash, 'https://example.com/article', '旧库文章',
                'legacy-signature', NULL, NULL, 0, NULL, NULL, 1.0,
                CURRENT_TIMESTAMP
            ) RETURNING id
            """
        ),
        {
            "document_id": document_id,
            "source_id": source_id,
            "content_hash": "b" * 64,
        },
    )
    cluster_id = connection.scalar(
        text(
            """
            INSERT INTO clusters (
                cluster_key, title, generated_title, generated_summary,
                generated_content, citations, model_version, prompt_version,
                first_seen_at, last_seen_at, created_at
            ) VALUES (
                'legacy-cluster', '旧库事件', '', '', NULL, '[]', '', '',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            ) RETURNING id
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO cluster_items (
                cluster_id, content_item_id, duplicate_score, created_at
            ) VALUES (
                :cluster_id, :content_item_id, 1.0, CURRENT_TIMESTAMP
            )
            """
        ),
        {"cluster_id": cluster_id, "content_item_id": content_item_id},
    )
    connection.execute(
        text(
            """
            INSERT INTO user_states (
                object_type, object_id, read_status, read_later, starred,
                updated_at
            ) VALUES (
                'cluster', :cluster_id, 'summary_seen', true, true,
                CURRENT_TIMESTAMP
            )
            """
        ),
        {"cluster_id": cluster_id},
    )


def test_clustering_run_lifecycle_and_snapshots_are_database_immutable(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    run_id = "00000000-0000-4000-8000-000000000019"

    with pytest.raises(DBAPIError, match="clustering_run_insert_started"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO clustering_runs (
                        id, scope_type, scope_key, rule_version, status,
                        failure_info, started_at, completed_at,
                        after_snapshot_finalized
                    ) VALUES (
                        '00000000-0000-4000-8000-000000000118',
                        'direct-terminal', :scope_key, 'test-rule-v1',
                        'completed', '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                        true
                    )
                    """
                ),
                {"scope_key": "e" * 64},
            )

    with pytest.raises(DBAPIError, match="clustering_run_snapshot_unsealed"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO clustering_runs (
                        id, scope_type, scope_key, rule_version, status,
                        failure_info, started_at
                    ) VALUES (
                        '00000000-0000-4000-8000-000000000119',
                        'direct-failed', :scope_key, 'test-rule-v1',
                        'started', '', CURRENT_TIMESTAMP
                    )
                    """
                ),
                {"scope_key": "f" * 64},
            )
            connection.execute(
                text(
                    """
                    UPDATE clustering_runs
                    SET status = 'failed', failed_at = CURRENT_TIMESTAMP,
                        failure_info = 'direct failure'
                    WHERE id = '00000000-0000-4000-8000-000000000119'
                    """
                )
            )

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO clustering_runs (
                    id, scope_type, scope_key, rule_version, status,
                    failure_info, started_at
                ) VALUES (
                    :run_id, 'postgres-test', :scope_key, 'test-rule-v1',
                    'started', '', CURRENT_TIMESTAMP
                )
                """
            ),
            {"run_id": run_id, "scope_key": "a" * 64},
        )
        connection.execute(
            text(
                """
                INSERT INTO clustering_run_scope_evidence (
                    run_id, evidence_anchor, evidence_occurrence
                ) VALUES (
                    :run_id, 'source-entry:1:revision:1:item:test', 1
                ), (
                    :run_id, 'source-entry:1:revision:1:item:test', 2
                )
                """
            ),
            {"run_id": run_id},
        )
        assert connection.scalar(
            text(
                "SELECT count(*) FROM clustering_run_scope_evidence "
                "WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        ) == 2
        connection.execute(
            text(
                """
                INSERT INTO clustering_run_memberships (
                    run_id, snapshot_phase, cluster_anchor,
                    cluster_occurrence, evidence_anchor, evidence_occurrence
                ) VALUES (
                    :run_id, 'before', :cluster_anchor, 1,
                    'source-entry:1:revision:1:item:test', 1
                ), (
                    :run_id, 'before', :cluster_anchor, 1,
                    'source-entry:1:revision:1:item:test', 2
                ), (
                    :run_id, 'before', :cluster_anchor, 2,
                    'source-entry:1:revision:1:item:test', 1
                ), (
                    :run_id, 'before', :cluster_anchor, 2,
                    'source-entry:1:revision:1:item:test', 2
                )
                """
            ),
            {
                "run_id": run_id,
                "cluster_anchor": "b" * 64,
            },
        )
        assert connection.scalar(
            text(
                "SELECT count(*) FROM clustering_run_memberships "
                "WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        ) == 4
        connection.execute(
            text(
                """
                INSERT INTO clustering_run_snapshot_seals (
                    run_id, snapshot_phase, snapshot_row_count,
                    snapshot_fingerprint
                ) VALUES (:run_id, 'before', 0, :fingerprint)
                """
            ),
            {"run_id": run_id, "fingerprint": "0" * 64},
        )

    with pytest.raises(DBAPIError, match="clustering_snapshot_immutable"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE clustering_run_memberships
                    SET cluster_anchor = :cluster_anchor
                    WHERE run_id = :run_id
                    """
                ),
                {"run_id": run_id, "cluster_anchor": "c" * 64},
            )

    with pytest.raises(DBAPIError, match="clustering_run_snapshot_incomplete"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE clustering_runs
                    SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                    WHERE id = :run_id
                    """
                ),
                {"run_id": run_id},
            )

    with pytest.raises(DBAPIError, match="clustering_run_snapshot_unsealed"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE clustering_runs
                    SET status = 'completed', completed_at = CURRENT_TIMESTAMP,
                        after_snapshot_finalized = true
                    WHERE id = :run_id
                    """
                ),
                {"run_id": run_id},
            )

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO clustering_run_snapshot_seals (
                    run_id, snapshot_phase, snapshot_row_count,
                    snapshot_fingerprint
                ) VALUES (:run_id, 'after', 999, :fingerprint)
                """
            ),
            {"run_id": run_id, "fingerprint": "f" * 64},
        )
        seal = connection.execute(
            text(
                """
                SELECT snapshot_row_count, snapshot_fingerprint
                FROM clustering_run_snapshot_seals
                WHERE run_id = :run_id AND snapshot_phase = 'after'
                """
            ),
            {"run_id": run_id},
        ).one()
        assert seal == (
            0,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )

    with pytest.raises(DBAPIError, match="clustering_snapshot_sealed"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO clustering_run_memberships (
                        run_id, snapshot_phase, cluster_anchor,
                        cluster_occurrence, evidence_anchor,
                        evidence_occurrence
                    ) VALUES (
                        :run_id, 'after', :cluster_anchor, 1,
                        'late-after-evidence', 1
                    )
                    """
                ),
                {"run_id": run_id, "cluster_anchor": "d" * 64},
            )

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE clustering_runs
                SET status = 'completed', completed_at = CURRENT_TIMESTAMP,
                    after_snapshot_finalized = true
                WHERE id = :run_id
                """
            ),
            {"run_id": run_id},
        )

    with pytest.raises(DBAPIError, match="clustering_snapshot_terminal"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO clustering_run_scope_evidence (
                        run_id, evidence_anchor
                    ) VALUES (:run_id, 'late-evidence')
                    """
                ),
                {"run_id": run_id},
            )

    with pytest.raises(DBAPIError, match="clustering_run_terminal"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE clustering_runs
                    SET failure_info = 'rewrite'
                    WHERE id = :run_id
                    """
                ),
                {"run_id": run_id},
            )
    engine.dispose()


def test_snapshot_seal_migration_backfills_existing_completed_runs(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0018_run_snapshot_finalized")
    engine = create_engine(postgres_url)
    run_id = "00000000-0000-4000-8000-000000000119"
    cluster_anchor = "a" * 64
    evidence_anchor = "source-entry:1:revision:1:item:backfill"
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO clustering_runs (
                    id, scope_type, scope_key, rule_version, status,
                    failure_info, started_at
                ) VALUES (
                    :run_id, 'seal-backfill', :scope_key, 'test-rule-v1',
                    'started', '', CURRENT_TIMESTAMP
                )
                """
            ),
            {"run_id": run_id, "scope_key": "b" * 64},
        )
        connection.execute(
            text(
                """
                INSERT INTO clustering_run_memberships (
                    run_id, snapshot_phase, cluster_anchor,
                    cluster_occurrence, evidence_anchor, evidence_occurrence
                ) VALUES (:run_id, 'after', :cluster_anchor, 1, :evidence, 1)
                """
            ),
            {
                "run_id": run_id,
                "cluster_anchor": cluster_anchor,
                "evidence": evidence_anchor,
            },
        )
        connection.execute(
            text(
                """
                UPDATE clustering_runs
                SET status = 'completed', completed_at = CURRENT_TIMESTAMP,
                    after_snapshot_finalized = true
                WHERE id = :run_id
                """
            ),
            {"run_id": run_id},
        )

    command.upgrade(config, "head")
    expected = hashlib.sha256(
        f"{cluster_anchor}|1|{evidence_anchor}|1".encode()
    ).hexdigest()
    with engine.connect() as connection:
        seal = connection.execute(
            text(
                """
                SELECT snapshot_row_count, snapshot_fingerprint
                FROM clustering_run_snapshot_seals
                WHERE run_id = :run_id AND snapshot_phase = 'after'
                """
            ),
            {"run_id": run_id},
        ).one()
        assert seal == (1, expected)
    engine.dispose()


def test_clustering_snapshot_insert_and_terminal_transition_are_serialized(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=2)
    run_id = "00000000-0000-4000-8000-000000000117"
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO clustering_runs (
                    id, scope_type, scope_key, rule_version, status,
                    failure_info, started_at
                ) VALUES (
                    :run_id, 'snapshot-race', :scope_key, 'test-rule-v1',
                    'started', '', CURRENT_TIMESTAMP
                )
                """
            ),
            {"run_id": run_id, "scope_key": "f" * 64},
        )
        connection.execute(
            text(
                """
                INSERT INTO clustering_run_snapshot_seals (
                    run_id, snapshot_phase, snapshot_row_count,
                    snapshot_fingerprint
                ) VALUES
                    (:run_id, 'before', 0, :fingerprint),
                    (:run_id, 'after', 0, :fingerprint)
                """
            ),
            {"run_id": run_id, "fingerprint": "0" * 64},
        )

    insert_ready = Event()
    release_insert = Event()
    terminal_done = Event()

    def insert_snapshot() -> None:
        with engine.connect() as connection:
            transaction = connection.begin()
            connection.execute(
                text(
                    """
                    INSERT INTO clustering_run_scope_evidence (
                        run_id, evidence_anchor, evidence_occurrence
                    ) VALUES (:run_id, 'race-evidence', 1)
                    """
                ),
                {"run_id": run_id},
            )
            insert_ready.set()
            assert release_insert.wait(timeout=5)
            transaction.commit()

    def finish_run() -> None:
        assert insert_ready.wait(timeout=5)
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE clustering_runs
                    SET status = 'completed', completed_at = CURRENT_TIMESTAMP,
                        after_snapshot_finalized = true
                    WHERE id = :run_id
                    """
                ),
                {"run_id": run_id},
            )
        terminal_done.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        insert_future = executor.submit(insert_snapshot)
        terminal_future = executor.submit(finish_run)
        try:
            assert insert_ready.wait(timeout=5)
            assert terminal_done.wait(timeout=0.2) is False
        finally:
            release_insert.set()
        insert_future.result(timeout=5)
        terminal_future.result(timeout=5)

    with engine.connect() as connection:
        assert connection.scalar(
            text("SELECT status FROM clustering_runs WHERE id = :run_id"),
            {"run_id": run_id},
        ) == "completed"
        assert connection.scalar(
            text(
                "SELECT count(*) FROM clustering_run_scope_evidence "
                "WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        ) == 1
    engine.dispose()


@pytest.mark.parametrize("caller_owned_transaction", [False, True])
def test_clustering_runs_are_serialized_until_the_owning_transaction_ends(
    postgres_url: str,
    caller_owned_transaction: bool,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Concurrent run source",
            url="https://example.com/concurrent-run.xml",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source=source,
            external_id="concurrent-run-entry",
            title="Concurrent run item",
            url="https://example.com/concurrent-run-item",
            raw_content="Concurrent body",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_text=raw.raw_content,
            url=raw.url,
            canonical_url=raw.url,
            content_hash=content_hash(raw.title, raw.raw_content, raw.url),
        )
        first_cluster = Cluster(cluster_key="concurrent-a", title="Concurrent A")
        second_cluster = Cluster(cluster_key="concurrent-b", title="Concurrent B")
        session.add_all([item, first_cluster, second_cluster])
        session.flush()
        session.add(
            ClusterItem(
                cluster_id=first_cluster.id,
                content_item_id=item.id,
            )
        )
        session.commit()
        item_id = item.id
        first_cluster_id = first_cluster.id
        second_cluster_id = second_cluster.id

    first_ready = Event()
    release_first = Event()
    second_entered = Event()

    def first_run() -> str:
        with SessionLocal() as session:
            with clustering_run(
                session,
                scope_type="postgres-concurrency",
                item_ids=[item_id],
                rule_version="test-rule-v1",
                commit_on_success=not caller_owned_transaction,
            ) as run_id:
                session.execute(
                    text(
                        "DELETE FROM cluster_items "
                        "WHERE content_item_id = :item_id"
                    ),
                    {"item_id": item_id},
                )
                session.add(
                    ClusterItem(
                        cluster_id=second_cluster_id,
                        content_item_id=item_id,
                    )
                )
                if not caller_owned_transaction:
                    session.commit()
                    first_ready.set()
                    assert release_first.wait(timeout=10)
            if caller_owned_transaction:
                first_ready.set()
                assert release_first.wait(timeout=10)
                session.commit()
            return run_id

    def second_run() -> str:
        assert first_ready.wait(timeout=10)
        with SessionLocal() as session:
            with clustering_run(
                session,
                scope_type="postgres-concurrency",
                item_ids=[item_id],
                rule_version="test-rule-v1",
            ) as run_id:
                second_entered.set()
            return run_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(first_run)
        second_future = executor.submit(second_run)
        try:
            assert first_ready.wait(timeout=10)
            assert second_entered.wait(timeout=0.5) is False
        finally:
            release_first.set()
        first_run_id = first_future.result(timeout=10)
        second_run_id = second_future.result(timeout=10)

    with SessionLocal() as session:
        runs = session.scalars(
            select(ClusteringRun)
            .where(ClusteringRun.id.in_([first_run_id, second_run_id]))
            .order_by(ClusteringRun.started_at)
        ).all()
        assert [run.status for run in runs] == ["completed", "completed"]
        first_after = set(
            session.execute(
                select(
                    ClusteringRunMembership.cluster_anchor,
                    ClusteringRunMembership.evidence_anchor,
                ).where(
                    ClusteringRunMembership.run_id == first_run_id,
                    ClusteringRunMembership.snapshot_phase == "after",
                )
            ).all()
        )
        second_before = set(
            session.execute(
                select(
                    ClusteringRunMembership.cluster_anchor,
                    ClusteringRunMembership.evidence_anchor,
                ).where(
                    ClusteringRunMembership.run_id == second_run_id,
                    ClusteringRunMembership.snapshot_phase == "before",
                )
            ).all()
        )
        assert first_after == second_before
        assert session.scalar(
            select(ClusterItem.cluster_id).where(
                ClusterItem.content_item_id == item_id
            )
        ) == second_cluster_id
        assert first_cluster_id != second_cluster_id

    engine.dispose()


def test_source_cluster_rechecks_status_after_waiting_for_execution_lock(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Source status race",
            url="https://example.com/source-status-race.xml",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source=source,
            external_id="source-status-race-entry",
            title="Source status race item",
            raw_content="Source status race body",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_text=raw.raw_content,
            content_hash=content_hash(raw.title, raw.raw_content),
        )
        cluster = Cluster(
            cluster_key="source-status-race",
            title="Source status race",
        )
        session.add_all([item, cluster])
        session.flush()
        session.add(
            ClusterItem(cluster_id=cluster.id, content_item_id=item.id)
        )
        session.commit()
        source_id = source.id
        item_id = item.id

    lock_held = Event()
    stale_source_loaded = Event()

    def mute_and_decluster() -> None:
        with SessionLocal() as session:
            with clustering_run_execution_lock(session):
                lock_held.set()
                assert stale_source_loaded.wait(timeout=10)
                source = session.get(Source, source_id)
                assert source is not None
                source.status = "muted"
                decluster_source_items(session, source_id)
                session.commit()

    def stale_cluster_request() -> None:
        assert lock_held.wait(timeout=10)
        with SessionLocal() as session:
            source = session.get(Source, source_id)
            assert source is not None and source.status == "active"
            stale_source_loaded.set()
            cluster_source_items(session, source_id)
            session.commit()

    with ThreadPoolExecutor(max_workers=2) as executor:
        mute_future = executor.submit(mute_and_decluster)
        cluster_future = executor.submit(stale_cluster_request)
        mute_future.result(timeout=10)
        cluster_future.result(timeout=10)

    with SessionLocal() as session:
        source = session.get(Source, source_id)
        assert source is not None and source.status == "muted"
        assert session.scalar(
            select(ClusterItem.id).where(
                ClusterItem.content_item_id == item_id
            )
        ) is None

    engine.dispose()


def test_source_noop_holds_postgres_execution_lock_until_caller_commit(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="No-op source lock",
            url="https://example.com/noop-lock.xml",
            status="muted",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source=source,
            external_id="noop-lock-entry",
            title="No-op lock item",
            raw_content="No-op lock body",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_text=raw.raw_content,
            content_hash=content_hash(raw.title, raw.raw_content),
        )
        session.add(item)
        session.commit()
        source_id = source.id
        item_id = item.id

    first_returned = Event()
    release_first = Event()
    second_entered = Event()

    def no_op_decluster() -> None:
        with SessionLocal() as session:
            decluster_source_items(session, source_id)
            first_returned.set()
            assert release_first.wait(timeout=10)
            session.commit()

    def competing_run() -> None:
        assert first_returned.wait(timeout=10)
        with SessionLocal() as session:
            with clustering_run(
                session,
                scope_type="postgres-source-noop-lock-test",
                item_ids=[item_id],
                rule_version="test-rule-v1",
            ):
                second_entered.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(no_op_decluster)
        second_future = executor.submit(competing_run)
        try:
            assert first_returned.wait(timeout=10)
            assert second_entered.wait(timeout=0.2) is False
        finally:
            release_first.set()
        first_future.result(timeout=10)
        second_future.result(timeout=10)

    engine.dispose()


@pytest.mark.parametrize(
    ("status", "terminal_column", "snapshot_phases"),
    [
        ("completed", "completed_at", ("before", "after")),
        ("failed", "failed_at", ("before",)),
    ],
)
def test_clustering_run_rejects_terminal_time_before_start(
    postgres_url: str,
    status: str,
    terminal_column: str,
    snapshot_phases: tuple[str, ...],
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    run_id = f"terminal-time-order-{status}"
    empty_fingerprint = (
        "e3b0c44298fc1c149afbf4c8996fb924"
        "27ae41e4649b934ca495991b7852b855"
    )

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO clustering_runs (
                    id, scope_type, scope_key, rule_version, status,
                    failure_info, started_at, after_snapshot_finalized
                ) VALUES (
                    :run_id, 'terminal-time-order', :scope_key, 'test-rule-v1',
                    'started', '', CURRENT_TIMESTAMP, false
                )
                """
            ),
            {"run_id": run_id, "scope_key": "a" * 64},
        )
        for phase in snapshot_phases:
            connection.execute(
                text(
                    """
                    INSERT INTO clustering_run_snapshot_seals (
                        run_id, snapshot_phase, snapshot_row_count,
                        snapshot_fingerprint
                    ) VALUES (:run_id, :phase, 0, :fingerprint)
                    """
                ),
                {
                    "run_id": run_id,
                    "phase": phase,
                    "fingerprint": empty_fingerprint,
                },
            )

    with pytest.raises(DBAPIError, match="clustering_run_terminal_time_order"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"""
                    UPDATE clustering_runs
                    SET status = :status,
                        {terminal_column} = started_at - INTERVAL '1 second',
                        after_snapshot_finalized = :after_finalized
                    WHERE id = :run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "status": status,
                    "after_finalized": status == "completed",
                },
            )

    engine.dispose()


def test_rss_response_is_discarded_when_source_url_changes_during_fetch(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Changing RSS endpoint",
            url="https://old.example/feed.xml",
        )
        session.add(source)
        session.commit()
        source_id = source.id

    request_started = Event()
    url_updated = Event()
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Old endpoint</title><item>
      <guid>old-entry</guid><title>Old entry</title>
      <description>Old body</description>
    </item></channel></rss>
    """

    class FeedResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def read(self) -> bytes:
            return feed

    def delayed_old_response(*_args, **_kwargs) -> FeedResponse:
        request_started.set()
        assert url_updated.wait(timeout=10)
        return FeedResponse()

    monkeypatch.setattr("reader_api.rss.urlopen", delayed_old_response)

    def fetch_old_url() -> int:
        with SessionLocal() as session:
            source = session.get(Source, source_id)
            assert source is not None
            return fetch_source(session, source)

    with ThreadPoolExecutor(max_workers=1) as executor:
        fetch_future = executor.submit(fetch_old_url)
        assert request_started.wait(timeout=10)
        with SessionLocal() as session:
            source = session.get(Source, source_id)
            assert source is not None
            source.url = "https://new.example/feed.xml"
            session.commit()
        url_updated.set()
        assert fetch_future.result(timeout=10) == 0

    with SessionLocal() as session:
        source = session.get(Source, source_id)
        assert source is not None
        assert source.url == "https://new.example/feed.xml"
        assert source.last_error == ""
        assert source.last_fetched_at is None
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 0

    engine.dispose()


def test_rss_revision_audit_rows_follow_affected_cluster_on_real_postgres(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    history_size = 25
    target_bodies = iter(("Original target body", "Updated target body"))

    def feed(target_body: str) -> bytes:
        items = [
            "<item><guid>target-entry</guid><title>Target entry</title>"
            "<link>https://example.com/target-entry</link>"
            f"<description>{target_body}</description></item>"
        ]
        items.extend(
            "<item>"
            f"<guid>history-{index}</guid><title>History {index}</title>"
            f"<link>https://example.com/history-{index}</link>"
            f"<description>Stable history body {index}</description>"
            "</item>"
            for index in range(1, history_size)
        )
        return (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<rss version="2.0"><channel><title>PostgreSQL scoped RSS</title>'
            + "".join(items)
            + "</channel></rss>"
        ).encode()

    responses = iter(feed(body) for body in target_bodies)

    class FeedResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def read(self) -> bytes:
            return next(responses)

    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(),
    )

    with SessionLocal() as session:
        source = Source(
            name="PostgreSQL scoped RSS",
            url="https://example.com/postgres-scoped.xml",
        )
        session.add(source)
        session.commit()
        assert fetch_source(session, source) == history_size

        items = session.scalars(
            select(ContentItem).order_by(ContentItem.id)
        ).all()
        assert len(items) == history_size
        for item in items:
            with clustering_run(
                session,
                scope_type="postgres-rss-history-fixture",
                item_ids=[item.id],
                rule_version="postgres-rss-history-fixture-v1",
            ):
                assign_cluster(session, item)

        target = items[0]
        target_cluster_id = session.scalar(
            select(ClusterItem.cluster_id).where(
                ClusterItem.content_item_id == target.id
            )
        )

        assert fetch_source(session, source) == 0
        rss_run = session.scalar(
            select(ClusteringRun).where(
                ClusteringRun.scope_type == "rss-source-ingest"
            )
        )
        assert rss_run is not None
        assert session.scalar(
            select(func.count())
            .select_from(ClusteringRunMembership)
            .where(ClusteringRunMembership.run_id == rss_run.id)
        ) == 2
        assert session.scalar(
            select(func.count())
            .select_from(ClusteringRunScopeEvidence)
            .where(ClusteringRunScopeEvidence.run_id == rss_run.id)
        ) == 1
        assert session.scalar(
            select(func.count())
            .select_from(ClusteringRunProjectionPredecessor)
            .where(ClusteringRunProjectionPredecessor.run_id == rss_run.id)
        ) == 1
        assert session.scalars(
            select(ClusterEventProjection.cluster_id_snapshot).where(
                ClusterEventProjection.clustering_run_id == rss_run.id
            )
        ).all() == [target_cluster_id]

    engine.dispose()


def test_source_patch_and_fetch_use_global_then_source_lock_order(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Lock order source",
            url="https://example.com/lock-order-old.xml",
            media_type="social",
        )
        session.add(source)
        session.commit()
        source_id = source.id

    global_lock_held = Event()
    patch_started = Event()

    def fetch_side() -> None:
        with SessionLocal() as session:
            with clustering_run_execution_lock(session):
                global_lock_held.set()
                assert patch_started.wait(timeout=10)
                time.sleep(0.2)
                source = session.get(Source, source_id, with_for_update=True)
                assert source is not None
                session.commit()

    def patch_side() -> None:
        assert global_lock_held.wait(timeout=10)
        patch_started.set()
        with SessionLocal() as session:
            source = session.get(Source, source_id)
            assert source is not None
            apply_source_update(
                session,
                source,
                SourcePatch(
                    url="https://example.com/lock-order-new.xml",
                    media_type="article",
                ),
            )
            session.commit()

    with ThreadPoolExecutor(max_workers=2) as executor:
        fetch_future = executor.submit(fetch_side)
        patch_future = executor.submit(patch_side)
        fetch_future.result(timeout=10)
        patch_future.result(timeout=10)

    with SessionLocal() as session:
        source = session.get(Source, source_id)
        assert source is not None
        assert source.url == "https://example.com/lock-order-new.xml"
        assert source.media_type == "article"

    engine.dispose()


def test_source_pause_does_not_wait_for_clustering_execution_lock(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Pause without topology lock",
            url="https://example.com/pause-without-topology-lock.xml",
        )
        session.add(source)
        session.commit()
        source_id = source.id

    lock_held = Event()
    release_lock = Event()
    pause_finished = Event()

    def clustering_side() -> None:
        with SessionLocal() as session:
            with clustering_run_execution_lock(session):
                lock_held.set()
                assert release_lock.wait(timeout=10)

    def pause_side() -> None:
        assert lock_held.wait(timeout=10)
        with SessionLocal() as session:
            source = session.get(Source, source_id)
            assert source is not None
            apply_source_update(session, source, SourcePatch(enabled=False))
            session.commit()
            pause_finished.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        clustering_future = executor.submit(clustering_side)
        pause_future = executor.submit(pause_side)
        finished_while_lock_held = pause_finished.wait(timeout=2)
        release_lock.set()
        clustering_future.result(timeout=10)
        pause_future.result(timeout=10)

    assert finished_while_lock_held is True
    with SessionLocal() as session:
        source = session.get(Source, source_id)
        assert source is not None and source.enabled is False

    engine.dispose()


def test_source_patch_refreshes_stale_status_after_waiting_for_execution_lock(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Stale source patch",
            url="https://example.com/stale-source-patch.xml",
            media_type="social",
        )
        session.add(source)
        session.commit()
        source_id = source.id

    global_lock_held = Event()
    stale_source_loaded = Event()
    restore_started = Event()

    def archive_side() -> None:
        with SessionLocal() as session:
            with clustering_run_execution_lock(session):
                source = session.get(Source, source_id, with_for_update=True)
                assert source is not None
                global_lock_held.set()
                assert stale_source_loaded.wait(timeout=10)
                source.status = "archived"
                source.enabled = False
                session.commit()
                assert restore_started.wait(timeout=10)
                time.sleep(0.2)

    def restore_side() -> None:
        assert global_lock_held.wait(timeout=10)
        with SessionLocal() as session:
            source = session.get(Source, source_id)
            assert source is not None
            assert source.status == "active"
            assert source.enabled is True
            stale_source_loaded.set()
            restore_started.set()
            apply_source_update(session, source, SourcePatch(status="active"))
            session.commit()

    with ThreadPoolExecutor(max_workers=2) as executor:
        archive_future = executor.submit(archive_side)
        restore_future = executor.submit(restore_side)
        archive_future.result(timeout=10)
        restore_future.result(timeout=10)

    with SessionLocal() as session:
        source = session.get(Source, source_id)
        assert source is not None
        assert source.status == "active"
        assert source.enabled is True

    engine.dispose()


def test_source_delete_refreshes_stale_status_after_waiting_for_execution_lock(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Stale source delete",
            url="https://example.com/stale-source-delete.xml",
            media_type="social",
            status="archived",
            enabled=False,
        )
        session.add(source)
        session.commit()
        source_id = source.id

    global_lock_held = Event()
    stale_source_loaded = Event()
    restore_committed = Event()
    delete_started = Event()

    def restore_side() -> None:
        with SessionLocal() as session:
            with clustering_run_execution_lock(session):
                global_lock_held.set()
                assert stale_source_loaded.wait(timeout=10)
                source = session.get(Source, source_id, with_for_update=True)
                assert source is not None
                source.status = "active"
                source.enabled = True
                session.commit()
                restore_committed.set()
                assert delete_started.wait(timeout=10)
                time.sleep(0.2)

    def delete_side() -> None:
        assert global_lock_held.wait(timeout=10)
        with SessionLocal() as session:
            source = session.get(Source, source_id)
            assert source is not None
            assert source.status == "archived"
            assert source.enabled is False
            stale_source_loaded.set()
            assert restore_committed.wait(timeout=10)
            delete_started.set()
            response = delete_source(source_id, session)
            assert response.status_code == 204

    with ThreadPoolExecutor(max_workers=2) as executor:
        restore_future = executor.submit(restore_side)
        delete_future = executor.submit(delete_side)
        restore_future.result(timeout=10)
        delete_future.result(timeout=10)

    with SessionLocal() as session:
        source = session.get(Source, source_id)
        assert source is not None
        assert source.status == "deleted"
        assert source.enabled is False

    engine.dispose()


def test_source_bulk_holds_execution_lock_across_all_source_updates(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        sources = [
            Source(
                name=f"Bulk lock {index}",
                url=f"https://example.com/bulk-lock-{index}.xml",
                media_type="social",
            )
            for index in (1, 2)
        ]
        session.add_all(sources)
        session.commit()
        source_ids = [source.id for source in sources]

    first_updated = Event()
    allow_bulk_to_continue = Event()
    competing_lock_entered = Event()
    original_apply = apply_source_update

    def pausing_apply(
        session: Session,
        source: Source,
        payload: object,
    ) -> None:
        original_apply(session, source, payload)
        if source.id == source_ids[0]:
            first_updated.set()
            assert allow_bulk_to_continue.wait(timeout=10)

    monkeypatch.setattr("reader_api.main.apply_source_update", pausing_apply)

    def bulk_side() -> None:
        with SessionLocal() as session:
            result = bulk_update_sources(
                SourceBulkPatch.model_validate(
                    {"ids": source_ids, "set": {"status": "muted"}}
                ),
                session,
            )
            assert result.updated == 2

    def competing_side() -> None:
        assert first_updated.wait(timeout=10)
        with SessionLocal() as session:
            with clustering_run_execution_lock(session):
                competing_lock_entered.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        bulk_future = executor.submit(bulk_side)
        competing_future = executor.submit(competing_side)
        assert first_updated.wait(timeout=10)
        assert competing_lock_entered.wait(timeout=0.2) is False
        allow_bulk_to_continue.set()
        bulk_future.result(timeout=10)
        competing_future.result(timeout=10)

    with SessionLocal() as session:
        assert all(
            source.status == "muted"
            for source in session.scalars(
                select(Source).where(Source.id.in_(source_ids))
            )
        )

    engine.dispose()


@pytest.mark.parametrize("entrypoint", ("post", "opml"))
def test_source_restore_resolves_url_after_waiting_for_execution_lock(
    postgres_url: str,
    entrypoint: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    old_url = f"https://example.com/{entrypoint}-restore-old.xml"
    new_url = f"https://example.com/{entrypoint}-restore-new.xml"

    with SessionLocal() as session:
        source = Source(
            name=f"{entrypoint} restore stale source",
            url=old_url,
            media_type="social",
            status="archived",
            enabled=False,
        )
        session.add(source)
        session.commit()
        original_source_id = source.id

    global_lock_held = Event()
    restore_started = Event()

    def move_existing_side() -> None:
        with SessionLocal() as session:
            with clustering_run_execution_lock(session):
                source = session.get(
                    Source,
                    original_source_id,
                    with_for_update=True,
                )
                assert source is not None
                global_lock_held.set()
                assert restore_started.wait(timeout=10)
                source.url = new_url
                source.status = "active"
                source.enabled = True
                session.commit()
                time.sleep(0.2)

    def restore_old_url_side() -> None:
        assert global_lock_held.wait(timeout=10)
        restore_started.set()
        with SessionLocal() as session:
            if entrypoint == "post":
                restored = create_source(
                    SourceCreate(
                        name="Restored old URL",
                        url=old_url,
                        media_type="social",
                    ),
                    session,
                )
                assert restored.url == old_url
            else:
                imported = import_opml(
                    session,
                    '<opml version="2.0"><body><outline text="Restored old URL" '
                    f'xmlUrl="{old_url}" /></body></opml>',
                )
                assert imported == 1

    with ThreadPoolExecutor(max_workers=2) as executor:
        move_future = executor.submit(move_existing_side)
        restore_future = executor.submit(restore_old_url_side)
        move_future.result(timeout=10)
        restore_future.result(timeout=10)

    with SessionLocal() as session:
        original = session.get(Source, original_source_id)
        restored = session.scalar(select(Source).where(Source.url == old_url))
        assert original is not None
        assert original.url == new_url
        assert original.status == "active"
        assert restored is not None
        assert restored.id != original_source_id

    engine.dispose()


def test_rss_response_is_discarded_when_source_is_archived_during_fetch(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url, pool_size=4)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Archiving RSS source",
            url="https://example.com/archive-during-fetch.xml",
        )
        session.add(source)
        session.commit()
        source_id = source.id

    request_started = Event()
    source_archived = Event()
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Archived endpoint</title><item>
      <guid>archived-entry</guid><title>Archived entry</title>
      <description>Archived body</description>
    </item></channel></rss>
    """

    class FeedResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def read(self) -> bytes:
            return feed

    def delayed_response(*_args, **_kwargs) -> FeedResponse:
        request_started.set()
        assert source_archived.wait(timeout=10)
        return FeedResponse()

    monkeypatch.setattr("reader_api.rss.urlopen", delayed_response)

    def fetch_before_archive() -> int:
        with SessionLocal() as session:
            source = session.get(Source, source_id)
            assert source is not None
            return fetch_source(session, source)

    with ThreadPoolExecutor(max_workers=1) as executor:
        fetch_future = executor.submit(fetch_before_archive)
        assert request_started.wait(timeout=10)
        with SessionLocal() as session:
            source = session.get(Source, source_id)
            assert source is not None
            source.status = "archived"
            source.enabled = False
            session.commit()
        source_archived.set()
        assert fetch_future.result(timeout=10) == 0

    with SessionLocal() as session:
        source = session.get(Source, source_id)
        assert source is not None
        assert source.status == "archived"
        assert source.enabled is False
        assert source.last_error == ""
        assert source.last_fetched_at is None
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 0

    engine.dispose()


def insert_backfill_source(connection: Connection) -> None:
    connection.execute(
        text(
            """
            INSERT INTO sources (
                id, name, url, site_url, media_type, status, enabled,
                fetch_full_content, feed_trust_score, last_error, created_at
            ) VALUES (
                1, 'Backfill source', 'https://example.com/backfill.xml', '',
                'article', 'active', true, false, 0, '', CURRENT_TIMESTAMP
            )
            """
        )
    )


def insert_backfill_raw(
    connection: Connection,
    *,
    raw_id: int,
    external_id: str,
    source_entry_id: int | None = None,
    revision_no: int | None = None,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO raw_entries (
                id, source_id, source_entry_id, revision_no, external_id, title,
                url, author, fetched_at, raw_summary, raw_content, content_hash
            ) VALUES (
                :raw_id, 1, :source_entry_id, :revision_no, :external_id,
                :title, :url, 'Backfill author', CURRENT_TIMESTAMP,
                '<p>Summary</p>', '<p>Body</p>', :content_hash
            )
            """
        ),
        {
            "raw_id": raw_id,
            "source_entry_id": source_entry_id,
            "revision_no": revision_no,
            "external_id": external_id,
            "title": f"Backfill title {raw_id}",
            "url": f"https://example.com/backfill/{raw_id}",
            "content_hash": f"{raw_id:064x}",
        },
    )


def insert_backfill_identity(
    connection: Connection,
    *,
    identity_id: int,
    external_id: str,
    current_revision_no: int = 1,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO source_entry_identities (
                id, source_id, current_revision_no, projection_pending, created_at
            ) VALUES (
                :identity_id, 1, :current_revision_no, false, CURRENT_TIMESTAMP
            )
            """
        ),
        {
            "identity_id": identity_id,
            "current_revision_no": current_revision_no,
        },
    )
    connection.execute(
        text(
            """
            INSERT INTO source_entry_keys (
                source_entry_id, source_id, identity_kind, identity_key, created_at
            ) VALUES (
                :identity_id, 1, 'legacy', :identity_key, CURRENT_TIMESTAMP
            )
            """
        ),
        {
            "identity_id": identity_id,
            "identity_key": legacy_source_entry_key(external_id),
        },
    )


def snapshot_derived_associations(
    connection: Connection,
) -> dict[str, list[dict[str, object]]]:
    queries = {
        "documents": "SELECT id, raw_entry_id FROM documents ORDER BY id",
        "content_items": (
            "SELECT id, document_id, source_id FROM content_items ORDER BY id"
        ),
        "cluster_items": (
            "SELECT id, cluster_id, content_item_id FROM cluster_items ORDER BY id"
        ),
        "user_states": (
            "SELECT id, object_type, object_id FROM user_states ORDER BY id"
        ),
    }
    return {
        table: [dict(row) for row in connection.execute(text(query)).mappings()]
        for table, query in queries.items()
    }


def raw_mapping_payload_fingerprint(row: Mapping[str, object]) -> str:
    return calculate_payload_fingerprint(
        RawEntryRevisionInput(
            external_id=str(row["external_id"]),
            title=str(row["title"]) if row.get("title") is not None else None,
            url=str(row["url"]) if row.get("url") is not None else None,
            author=str(row["author"]) if row.get("author") is not None else None,
            published_at=(
                row["published_at"]
                if isinstance(row.get("published_at"), datetime)
                else None
            ),
            raw_summary=(
                str(row["raw_summary"])
                if row.get("raw_summary") is not None
                else None
            ),
            raw_content=(
                str(row["raw_content"])
                if row.get("raw_content") is not None
                else None
            ),
        )
    )


def add_keyless_revision_seed(
    session: Session,
    *,
    suffix: str,
) -> tuple[SourceEntryIdentity, RawEntry]:
    source_id = insert_pre_privacy_source(
        session,
        name=f"Keyless source {suffix}",
        url=f"https://example.com/keyless-{suffix}.xml",
    )
    identity = SourceEntryIdentity(
        source_id=source_id,
        current_revision_no=1,
    )
    session.add(identity)
    session.flush()
    revision = make_revision_input(
        external_id=f"keyless-{suffix}",
        title=f"Keyless {suffix}",
    )
    raw = RawEntry(
        **raw_entry_revision_values(
            source_id=source_id,
            source_entry_id=identity.id,
            revision_no=1,
            revision=revision,
        )
    )
    session.add(raw)
    session.flush()
    return identity, raw


@contextmanager
def frozen_legacy_schema(postgres_url: str) -> Iterator[Connection]:
    engine = create_engine(postgres_url)
    try:
        with engine.begin() as connection:
            install_frozen_legacy_schema(connection)
            yield connection
    finally:
        engine.dispose()


def assert_preflight_rejects_schema_drift(
    postgres_url: str,
    mutation: str | tuple[MutationStep, ...],
    *expected_error_fragments: tuple[str, ...],
) -> None:
    steps = (mutation,) if isinstance(mutation, str) else mutation
    with frozen_legacy_schema(postgres_url) as connection:
        for step in steps:
            if isinstance(step, str):
                connection.execute(text(step))
            else:
                statement, parameters = step
                connection.execute(text(statement), parameters)

    report = preflight_legacy_database(postgres_url)

    assert report.ok is False
    for fragments in expected_error_fragments:
        assert any(
            all(fragment in error for fragment in fragments)
            for error in report.errors
        ), report.errors


def assert_preflight_rejects_alembic_version_drift(
    postgres_url: str,
    mutation_sql: str | tuple[str, ...],
    *expected_error_fragments: tuple[str, ...],
) -> None:
    statements = (mutation_sql,) if isinstance(mutation_sql, str) else mutation_sql
    assert_preflight_rejects_schema_drift(
        postgres_url,
        (
            *statements,
            (
                "INSERT INTO alembic_version (version_num) VALUES (:revision)",
                {"revision": BASELINE_REVISION},
            ),
        ),
        *expected_error_fragments,
    )


def test_empty_postgres_upgrades_to_complete_schema_head(postgres_url: str) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0003_maintenance_runs")
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        legacy_snapshot = read_postgres_schema(connection)
        assert compare_legacy_schema(
            legacy_snapshot,
            allowed_auxiliary_tables=frozenset(
                {"alembic_version", "maintenance_runs"}
            ),
        ) == []
    engine.dispose()

    command.upgrade(config, "head")
    command.upgrade(config, "head")
    check_result = run_migration_cli(postgres_url, "check-head")
    assert check_result.returncode == 0, check_result.stdout + check_result.stderr
    engine = create_engine(postgres_url)

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        snapshot = read_postgres_schema(connection)
        assert snapshot.column_nullability["content_items"]["media_url"] is True
        assert snapshot.column_nullability["content_items"]["media_kind"] is True
        assert snapshot.column_nullability["raw_entries"]["source_entry_id"] is False
        assert snapshot.column_nullability["raw_entries"]["revision_no"] is False
        assert snapshot.column_nullability["raw_entries"]["payload_fingerprint"] is False
        raw_unique_constraints = {
            constraint["name"]
            for constraint in inspect(connection).get_unique_constraints("raw_entries")
        }
        assert raw_unique_constraints >= {
            "uq_raw_source_entry_fingerprint",
            "uq_raw_source_entry_revision",
        }
        assert "uq_raw_source_external" not in raw_unique_constraints
        event_revision_unique_constraints = {
            constraint["name"]
            for constraint in inspect(connection).get_unique_constraints(
                "event_revisions"
            )
        }
        assert "uq_event_revision_number" in event_revision_unique_constraints
        assert (
            "uq_event_revision_fingerprint"
            not in event_revision_unique_constraints
        )
        raw_check_constraints = {
            constraint["name"]
            for constraint in inspect(connection).get_check_constraints("raw_entries")
        }
        assert "ck_raw_payload_fingerprint_sha256" in raw_check_constraints
        assert set(snapshot.tables["maintenance_runs"]) == {
            "id",
            "operation_type",
            "start_status",
            "end_status",
            "scanned_count",
            "processed_count",
            "failure_info",
            "started_at",
            "finished_at",
        }
        assert set(snapshot.tables["source_entry_identities"]) >= {
            "id",
            "source_id",
            "current_revision_no",
            "projection_pending",
            "created_at",
        }
        assert set(snapshot.tables["source_entry_keys"]) >= {
            "id",
            "source_entry_id",
            "source_id",
            "identity_kind",
            "identity_key",
            "created_at",
        }
        assert set(snapshot.tables["source_entry_relations"]) >= {
            "id",
            "source_entry_id",
            "canonical_source_entry_id",
            "relation_type",
            "reason",
            "detected_at",
            "rule_version",
            "active",
            "revoked_at",
        }
        assert set(inspect(connection).get_table_names()) >= set(snapshot.tables) | {"alembic_version"}

    engine.dispose()


@pytest.mark.parametrize(
    "invalid_fingerprint",
    ["", "z" * 64, "A" * 64, "d" * 64],
)
def test_postgres_head_rejects_invalid_payload_fingerprint(
    postgres_url: str,
    invalid_fingerprint: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        source = Source(
            name="Malformed fingerprint source",
            url="https://example.com/malformed-fingerprint.xml",
        )
        session.add(source)
        session.flush()
        identity = SourceEntryIdentity(
            source_id=source.id,
            current_revision_no=1,
        )
        session.add(identity)
        session.flush()
        session.add(
            SourceEntryKey(
                source_entry_id=identity.id,
                source_id=source.id,
                identity_kind="legacy",
                identity_key=legacy_source_entry_key("malformed-fingerprint"),
            )
        )
        session.add(
            RawEntry(
                source_id=source.id,
                source_entry_id=identity.id,
                revision_no=1,
                payload_fingerprint=invalid_fingerprint,
                external_id="malformed-fingerprint",
                title="Malformed fingerprint",
                content_hash="c" * 64,
            )
        )

        with pytest.raises(
            IntegrityError,
            match="ck_raw_payload_fingerprint_canonical",
        ):
            session.commit()

    engine.dispose()


def test_raw_fingerprint_contracts_upgrade_existing_0008_database(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0008_raw_revision_contract")
    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0008_raw_revision_contract"
        )
        assert "ck_raw_payload_fingerprint_sha256" not in {
            constraint["name"]
            for constraint in inspect(connection).get_check_constraints("raw_entries")
        }
    engine.dispose()

    command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        check_constraints = {
            constraint["name"]
            for constraint in inspect(connection).get_check_constraints("raw_entries")
        }
        assert "ck_raw_payload_fingerprint_sha256" in check_constraints
        assert "ck_raw_payload_fingerprint_canonical" in check_constraints
    engine.dispose()


def test_raw_fingerprint_canonical_migration_rejects_existing_mismatch(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0009_raw_fingerprint_format")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Canonical mismatch source",
            url="https://example.com/canonical-mismatch.xml",
        )
        identity = SourceEntryIdentity(
            source_id=source_id,
            current_revision_no=1,
        )
        session.add(identity)
        session.flush()
        session.add(
            RawEntry(
                source_id=source_id,
                source_entry_id=identity.id,
                revision_no=1,
                payload_fingerprint="d" * 64,
                external_id="canonical-mismatch",
                title="Canonical mismatch",
                content_hash="c" * 64,
            )
        )
        session.commit()
    engine.dispose()

    with pytest.raises(DBAPIError, match="raw_fingerprint_canonical_mismatch"):
        command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0009_raw_fingerprint_format"
        )
        assert "ck_raw_payload_fingerprint_canonical" not in {
            constraint["name"]
            for constraint in inspect(connection).get_check_constraints("raw_entries")
        }
    engine.dispose()


def test_revision_write_protocol_migration_rejects_invalid_existing_key(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0010_raw_fingerprint_canonical")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    initial = make_revision_input(external_id="invalid-key-migration")
    with SessionLocal() as session:
        identity, raw = add_pre_privacy_raw_revision_seed(
            session,
            revision=initial,
            payload_fingerprint=calculate_payload_fingerprint(initial),
        )
        session.flush()
        session.add(
            SourceEntryKey(
                source_entry_id=identity.id,
                source_id=raw.source_id,
                identity_kind="other",
                identity_key="other:" + "a" * 64,
            )
        )
        session.commit()
    engine.dispose()

    with pytest.raises(DBAPIError, match="source_entry_key_contract_invalid"):
        command.upgrade(config, "head")


def test_revision_write_protocol_migration_rejects_existing_revision_gap(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0010_raw_fingerprint_canonical")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        source_id = insert_pre_privacy_source(
            session,
            name="Revision gap source",
            url="https://example.com/revision-gap.xml",
        )
        identity = SourceEntryIdentity(
            source_id=source_id,
            current_revision_no=3,
        )
        session.add(identity)
        session.flush()
        for revision_no in (1, 3):
            revision = make_revision_input(
                external_id=f"revision-gap-{revision_no}",
                title=f"Revision gap {revision_no}",
            )
            session.add(
                RawEntry(
                    **raw_entry_revision_values(
                        source_id=source_id,
                        source_entry_id=identity.id,
                        revision_no=revision_no,
                        revision=revision,
                    )
                )
            )
        session.commit()
    engine.dispose()

    with pytest.raises(DBAPIError, match="source_entry_revision_contract_invalid"):
        command.upgrade(config, "head")


def test_identity_key_presence_migration_rejects_existing_keyless_identity(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0011_revision_write_protocol")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        add_keyless_revision_seed(session, suffix="migration")
        session.commit()
    engine.dispose()

    with pytest.raises(DBAPIError, match="source_entry_identity_key_missing"):
        command.upgrade(config, "head")


def test_identity_key_kind_unique_migration_rejects_existing_conflict(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0013_identity_key_mutation_lock")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    initial = make_revision_input(external_id="duplicate-identity-kind")
    with SessionLocal() as session:
        identity, raw = add_pre_privacy_raw_revision_seed(
            session,
            revision=initial,
            payload_fingerprint=calculate_payload_fingerprint(initial),
        )
        session.add(
            SourceEntryKey(
                source_entry_id=identity.id,
                source_id=raw.source_id,
                identity_kind="legacy",
                identity_key="legacy:" + "b" * 64,
            )
        )
        session.commit()
    engine.dispose()

    with pytest.raises(DBAPIError, match="source_entry_identity_kind_conflict"):
        command.upgrade(config, "head")


def test_postgres_head_rejects_keyless_identity_commit(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        add_keyless_revision_seed(session, suffix="runtime")
        with pytest.raises(DBAPIError):
            session.commit()
    engine.dispose()


def test_postgres_head_rejects_deleting_last_identity_key(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        source = Source(
            name="Last key source",
            url="https://example.com/last-key.xml",
        )
        session.add(source)
        session.flush()
        entry = IngestEntry(
            source_guid="last-key-guid",
            external_id="last-key-guid",
            title="Last key",
            url="https://example.com/last-key",
            raw_content="Last key body",
            content_text="Last key body",
        )
        assert ingest_source_entries(session, source, [entry]) == 1
        session.commit()
        key = session.scalars(select(SourceEntryKey)).one()
        session.delete(key)
        with pytest.raises(DBAPIError):
            session.commit()
    engine.dispose()


def delete_identity_key_in_transaction(
    postgres_url: str,
    key_id: int,
    *,
    wait_before_delete: Event | None = None,
    delete_started: Event | None = None,
    delete_returned: Event | None = None,
    wait_before_commit: Event | None = None,
) -> str:
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    try:
        with SessionLocal() as session:
            if wait_before_delete is not None:
                assert wait_before_delete.wait(timeout=10)
            try:
                if delete_started is not None:
                    delete_started.set()
                session.execute(
                    text("DELETE FROM source_entry_keys WHERE id = :key_id"),
                    {"key_id": key_id},
                )
                if delete_returned is not None:
                    delete_returned.set()
                if wait_before_commit is not None:
                    assert wait_before_commit.wait(timeout=10)
                session.commit()
                return "deleted"
            except DBAPIError as exc:
                session.rollback()
                return str(exc)
    finally:
        engine.dispose()


def test_postgres_serializes_concurrent_identity_key_deletes(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    initial = make_revision_input(external_id="concurrent-key-delete")
    with SessionLocal() as session:
        identity, raw = add_raw_revision_seed(
            session,
            revision=initial,
            payload_fingerprint=calculate_payload_fingerprint(initial),
        )
        session.add(
            SourceEntryKey(
                source_entry_id=identity.id,
                source_id=raw.source_id,
                identity_kind="url",
                identity_key="url:" + "b" * 64,
            )
        )
        session.commit()
        identity_id = identity.id
        key_ids = tuple(
            session.scalars(
                select(SourceEntryKey.id)
                .where(SourceEntryKey.source_entry_id == identity_id)
                .order_by(SourceEntryKey.id)
            )
        )
    engine.dispose()
    assert len(key_ids) == 2

    first_delete_returned = Event()
    release_first_commit = Event()
    second_delete_started = Event()
    second_delete_returned = Event()
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            delete_identity_key_in_transaction,
            postgres_url,
            key_ids[0],
            delete_returned=first_delete_returned,
            wait_before_commit=release_first_commit,
        )
        assert first_delete_returned.wait(timeout=10)
        second = executor.submit(
            delete_identity_key_in_transaction,
            postgres_url,
            key_ids[1],
            wait_before_delete=first_delete_returned,
            delete_started=second_delete_started,
            delete_returned=second_delete_returned,
        )
        assert second_delete_started.wait(timeout=10)
        assert second_delete_returned.wait(timeout=0.5) is False
        release_first_commit.set()
        results = [first.result(timeout=20), second.result(timeout=20)]

    assert results.count("deleted") == 1
    assert sum("source_entry_identity_key_missing" in result for result in results) == 1

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(
            text(
                "SELECT count(*) FROM source_entry_keys "
                "WHERE source_entry_id = :identity_id"
            ),
            {"identity_id": identity_id},
        ) == 1
    engine.dispose()


def resolve_legacy_guid_generation(
    postgres_url: str,
    source_id: int,
    *,
    url: str,
    conflict_check_barrier: Barrier,
) -> tuple[SourceEntryResolutionOutcome, int | None]:
    engine = create_engine(postgres_url)
    synchronized = False

    @event.listens_for(engine, "before_cursor_execute")
    def synchronize_legacy_conflict_check(
        _connection,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        nonlocal synchronized
        if (
            not synchronized
            and "source_entry_keys.identity_key !=" in statement
        ):
            synchronized = True
            conflict_check_barrier.wait(timeout=10)

    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        with SessionLocal() as session:
            resolution = resolve_source_entry_identity(
                session,
                source_id,
                SourceEntryIdentityInput(
                    source_guid="concurrent-generation-guid",
                    url=url,
                ),
            )
            source_entry_id = (
                resolution.source_entry.id
                if resolution.source_entry is not None
                else None
            )
            session.commit()
            return resolution.outcome, source_entry_id
    finally:
        engine.dispose()


def test_postgres_serializes_legacy_guid_generation_claims(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as session:
        source = Source(
            name="Concurrent legacy generation source",
            url="https://example.com/concurrent-legacy-generation.xml",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source=source,
            external_id="concurrent-generation-guid",
            title="Concurrent legacy generation",
            url="",
        )
        session.add(raw)
        session.commit()
        source_id = source.id
        legacy_identity_id = raw.source_entry_id
    engine.dispose()

    barrier = Barrier(2)
    urls = (
        "https://example.com/concurrent-generation-a",
        "https://example.com/concurrent-generation-b",
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                resolve_legacy_guid_generation,
                postgres_url,
                source_id,
                url=url,
                conflict_check_barrier=barrier,
            )
            for url in urls
        ]
        results = [future.result(timeout=20) for future in futures]

    assert {result[0] for result in results} == {
        SourceEntryResolutionOutcome.CLAIMED_LEGACY,
        SourceEntryResolutionOutcome.CREATE_NEW,
    }
    assert [result[1] for result in results].count(legacy_identity_id) == 1
    assert [result[1] for result in results].count(None) == 1

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(
            text(
                "SELECT count(*) FROM source_entry_keys "
                "WHERE source_entry_id = :identity_id AND identity_kind = 'guid'"
            ),
            {"identity_id": legacy_identity_id},
        ) == 1
    engine.dispose()


@pytest.mark.parametrize(
    ("identity_kind", "identity_key"),
    INVALID_SOURCE_ENTRY_KEYS,
)
def test_postgres_head_rejects_invalid_source_entry_key(
    postgres_url: str,
    identity_kind: str,
    identity_key: str,
) -> None:
    identity_id, _initial = create_postgres_revision_seed(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        identity = session.get(SourceEntryIdentity, identity_id)
        assert identity is not None
        session.add(
            SourceEntryKey(
                source_entry_id=identity.id,
                source_id=identity.source_id,
                identity_kind=identity_kind,
                identity_key=identity_key,
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()
    engine.dispose()


def test_postgres_head_rejects_second_identity_key_of_same_kind(
    postgres_url: str,
) -> None:
    identity_id, _initial = create_postgres_revision_seed(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        identity = session.get(SourceEntryIdentity, identity_id)
        assert identity is not None
        session.add(
            SourceEntryKey(
                source_entry_id=identity.id,
                source_id=identity.source_id,
                identity_kind="legacy",
                identity_key="legacy:" + "c" * 64,
            )
        )
        with pytest.raises(
            IntegrityError,
            match="uq_source_entry_key_identity_kind",
        ):
            session.commit()
    engine.dispose()


@pytest.mark.parametrize(
    "bypass_operation",
    [
        "update",
        "delete",
        "gap",
        "missing-current-pointer",
        "stale-current-pointer",
    ],
)
def test_postgres_head_rejects_revision_protocol_bypass(
    postgres_url: str,
    bypass_operation: str,
) -> None:
    identity_id, initial = create_postgres_revision_seed(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        raw = session.scalars(
            select(RawEntry).where(RawEntry.source_entry_id == identity_id)
        ).one()
        if bypass_operation == "stale-current-pointer":
            identity = session.get(SourceEntryIdentity, identity_id)
            assert identity is not None
            identity.current_revision_no = 99
            with pytest.raises(DBAPIError):
                session.commit()
        elif bypass_operation == "update":
            changed = replace(initial, title="Mutated in place")
            with pytest.raises(DBAPIError):
                session.execute(
                    text(
                        """
                        UPDATE raw_entries
                        SET title = :title,
                            payload_fingerprint = :payload_fingerprint
                        WHERE id = :raw_id
                        """
                    ),
                    {
                        "title": changed.title,
                        "payload_fingerprint": calculate_payload_fingerprint(changed),
                        "raw_id": raw.id,
                    },
                )
                session.commit()
        elif bypass_operation == "delete":
            with pytest.raises(DBAPIError):
                session.execute(
                    text("DELETE FROM raw_entries WHERE id = :raw_id"),
                    {"raw_id": raw.id},
                )
                session.commit()
        else:
            revision_no = 99 if bypass_operation == "gap" else 2
            changed = replace(
                initial,
                external_id=f"direct-{bypass_operation}",
                title=f"Direct {bypass_operation}",
            )
            session.add(
                RawEntry(
                    **raw_entry_revision_values(
                        source_id=raw.source_id,
                        source_entry_id=identity_id,
                        revision_no=revision_no,
                        revision=changed,
                    )
                )
            )
            with pytest.raises(DBAPIError):
                session.commit()
    engine.dispose()


def test_normal_article_ingest_migration_drops_renamed_legacy_constraint(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0006_raw_revision_allocator")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE raw_entries RENAME CONSTRAINT "
                "uq_raw_source_external TO legacy_source_external_unique"
            )
        )
    engine.dispose()

    command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        legacy_constraints = [
            constraint
            for constraint in inspect(connection).get_unique_constraints(
                "raw_entries"
            )
            if constraint["column_names"] == ["source_id", "external_id"]
        ]
        assert legacy_constraints == []
    engine.dispose()


def test_source_entry_backfill_preserves_legacy_rows_and_user_data(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0003_maintenance_runs")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO sources (
                    id, name, url, site_url, media_type, status, enabled,
                    fetch_full_content, feed_trust_score, last_error, created_at
                ) VALUES (
                    1, 'Legacy source', 'https://example.com/legacy.xml', '',
                    'article', 'active', true, false, 0, '', CURRENT_TIMESTAMP
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO raw_entries (
                    id, source_id, external_id, title, url, author,
                    published_at, fetched_at, raw_summary, raw_content, content_hash
                ) VALUES (
                    1, 1, 'legacy-guid', 'Legacy title',
                    'https://example.com/legacy', 'Legacy author', CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP, '<p>Legacy summary</p>', '<p>Legacy body</p>',
                    '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO raw_entries (
                    id, source_id, external_id, title, url, author,
                    published_at, fetched_at, raw_summary, raw_content, content_hash
                ) VALUES (
                    2, 1, 'legacy-guid:abcdef123456', 'Legacy title corrected',
                    'https://example.com/legacy-corrected', 'Legacy editor',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '<p>Corrected summary</p>',
                    '<p>Corrected body</p>',
                    '1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO documents (
                    id, raw_entry_id, document_type, title, summary,
                    content_text, digest_score, created_at
                ) VALUES (
                    1, 1, 'normal_article', 'Legacy title', 'Legacy summary',
                    'Legacy body', 0, CURRENT_TIMESTAMP
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO content_items (
                    id, document_id, source_id, title, summary, content_text, url,
                    published_at, content_hash, canonical_url, normalized_title,
                    lsh_signature, media_url, media_kind, media_duration,
                    embedding_vector, embedding_model, cluster_score, created_at
                ) VALUES (
                    1, 1, 1, 'Legacy title', 'Legacy summary', 'Legacy body',
                    'https://example.com/legacy', CURRENT_TIMESTAMP,
                    'abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789',
                    'https://example.com/legacy', 'legacy title', '', '', '', 0,
                    NULL, '', 0, CURRENT_TIMESTAMP
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO clusters (
                    id, cluster_key, title, generated_title, generated_summary,
                    generated_content, citations, model_version, prompt_version,
                    first_seen_at, last_seen_at, created_at
                ) VALUES (
                    1, 'legacy-cluster', 'Legacy title', '', '', '', '[]', '', '',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO cluster_items (
                    id, cluster_id, content_item_id, duplicate_score, created_at
                ) VALUES (1, 1, 1, 1, CURRENT_TIMESTAMP)
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO user_states (
                    id, object_type, object_id, read_status,
                    read_later, starred, updated_at
                ) VALUES (1, 'item', 1, 'summary_seen', false, true, CURRENT_TIMESTAMP)
                """
            )
        )
        before_raw = [
            dict(row)
            for row in connection.execute(
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
        ]
        before_counts = {
            table: connection.scalar(text(f"SELECT count(*) FROM {table}"))
            for table in (
                "sources",
                "raw_entries",
                "documents",
                "content_items",
                "clusters",
                "cluster_items",
                "user_states",
            )
        }
        before_associations = snapshot_derived_associations(connection)
    engine.dispose()

    command.upgrade(config, "head")
    command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        after_raw = list(
            connection.execute(
                text(
                    """
                    SELECT id, source_id, external_id, title, url, author,
                           published_at, fetched_at, raw_summary, raw_content,
                           content_hash, source_entry_id, revision_no,
                           payload_fingerprint
                    FROM raw_entries
                    ORDER BY id
                    """
                )
            ).mappings()
        )
        assert [
            {key: row[key] for key in before_raw[index]}
            for index, row in enumerate(after_raw)
        ] == before_raw
        assert all(row["source_entry_id"] is not None for row in after_raw)
        assert all(row["revision_no"] == 1 for row in after_raw)
        assert all(row["payload_fingerprint"] is not None for row in after_raw)
        for row in after_raw:
            assert row["payload_fingerprint"] == raw_mapping_payload_fingerprint(row)
        assert len({row["source_entry_id"] for row in after_raw}) == len(after_raw)

        identity_rows = connection.execute(
            text(
                """
                SELECT id, source_id, current_revision_no, projection_pending
                FROM source_entry_identities
                ORDER BY id
                """
            )
        ).mappings().all()
        assert len(identity_rows) == len(after_raw)
        assert all(row["current_revision_no"] == 1 for row in identity_rows)
        assert all(row["projection_pending"] is False for row in identity_rows)

        key_rows = connection.execute(
            text(
                """
                SELECT source_entry_id, source_id, identity_kind, identity_key
                FROM source_entry_keys
                ORDER BY source_entry_id
                """
            )
        ).mappings().all()
        assert len(key_rows) == len(after_raw)
        assert {row["identity_kind"] for row in key_rows} == {"legacy"}
        keys_by_identity = {row["source_entry_id"]: row for row in key_rows}
        for row in after_raw:
            key = keys_by_identity[row["source_entry_id"]]
            assert key["source_id"] == row["source_id"]
            assert key["identity_key"] == legacy_source_entry_key(row["external_id"])

        assert connection.scalar(text("SELECT count(*) FROM source_entry_relations")) == 0
        for table, expected_count in before_counts.items():
            assert connection.scalar(text(f"SELECT count(*) FROM {table}")) == expected_count
        assert snapshot_derived_associations(connection) == before_associations
        snapshot = read_postgres_schema(connection)
        assert snapshot.column_nullability["raw_entries"]["source_entry_id"] is False
        assert snapshot.column_nullability["raw_entries"]["revision_no"] is False
        assert snapshot.column_nullability["raw_entries"]["payload_fingerprint"] is False
        raw_unique_constraints = {
            constraint["name"]
            for constraint in inspect(connection).get_unique_constraints("raw_entries")
        }
        assert raw_unique_constraints >= {
            "uq_raw_source_entry_fingerprint",
            "uq_raw_source_entry_revision",
        }
        assert "uq_raw_source_external" not in raw_unique_constraints
    engine.dispose()


def test_source_entry_backfill_preserves_valid_preexisting_expanded_rows(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0004_source_entry_expand")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        insert_backfill_source(connection)
        insert_backfill_identity(
            connection,
            identity_id=10,
            external_id="preexisting-entry",
        )
        insert_backfill_raw(
            connection,
            raw_id=1,
            external_id="preexisting-entry",
            source_entry_id=10,
            revision_no=1,
        )
        insert_backfill_raw(
            connection,
            raw_id=2,
            external_id="legacy-entry",
        )
    engine.dispose()

    command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT id, source_entry_id, revision_no
                FROM raw_entries
                ORDER BY id
                """
            )
        ).mappings().all()
        assert rows[0]["source_entry_id"] == 10
        assert rows[0]["revision_no"] == 1
        assert rows[1]["source_entry_id"] not in {None, 10}
        assert rows[1]["revision_no"] == 1
        assert connection.scalar(text("SELECT count(*) FROM source_entry_identities")) == 2
        assert connection.scalar(text("SELECT count(*) FROM source_entry_keys")) == 2
    engine.dispose()


def test_source_entry_backfill_rejects_extra_legacy_key(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0004_source_entry_expand")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        insert_backfill_source(connection)
        insert_backfill_identity(
            connection,
            identity_id=10,
            external_id="extra-key-entry",
        )
        connection.execute(
            text(
                """
                INSERT INTO source_entry_keys (
                    source_entry_id, source_id, identity_kind, identity_key, created_at
                ) VALUES (
                    10, 1, 'legacy', :identity_key, CURRENT_TIMESTAMP
                )
                """
            ),
            {"identity_key": f"legacy:{'f' * 64}"},
        )
        insert_backfill_raw(
            connection,
            raw_id=1,
            external_id="extra-key-entry",
            source_entry_id=10,
            revision_no=1,
        )
    engine.dispose()

    with pytest.raises(DBAPIError, match="source_entry_backfill_legacy_key_count"):
        command.upgrade(config, "head")


def test_source_entry_backfill_rejects_partial_identity_revision_state(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0004_source_entry_expand")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        insert_backfill_source(connection)
        insert_backfill_raw(
            connection,
            raw_id=1,
            external_id="partial-entry",
            revision_no=1,
        )
    engine.dispose()

    with pytest.raises(DBAPIError, match="source_entry_backfill_partial_revision"):
        command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0004_source_entry_expand"
        )
        assert connection.scalar(text("SELECT count(*) FROM source_entry_identities")) == 0
    engine.dispose()


def test_source_entry_backfill_rejects_duplicate_revision_number(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0004_source_entry_expand")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        insert_backfill_source(connection)
        insert_backfill_identity(
            connection,
            identity_id=10,
            external_id="duplicate-one",
        )
        insert_backfill_raw(
            connection,
            raw_id=1,
            external_id="duplicate-one",
            source_entry_id=10,
            revision_no=1,
        )
        insert_backfill_raw(
            connection,
            raw_id=2,
            external_id="duplicate-two",
            source_entry_id=10,
            revision_no=1,
        )
    engine.dispose()

    with pytest.raises(DBAPIError, match="source_entry_backfill_duplicate_revision"):
        command.upgrade(config, "head")


def test_source_entry_backfill_rejects_inconsistent_current_revision(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0004_source_entry_expand")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        insert_backfill_source(connection)
        insert_backfill_identity(
            connection,
            identity_id=10,
            external_id="current-mismatch",
            current_revision_no=2,
        )
        insert_backfill_raw(
            connection,
            raw_id=1,
            external_id="current-mismatch",
            source_entry_id=10,
            revision_no=1,
        )
    engine.dispose()

    with pytest.raises(DBAPIError, match="source_entry_backfill_current_mismatch"):
        command.upgrade(config, "head")


def test_raw_revision_allocator_migration_rejects_duplicate_fingerprints(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0005_source_entry_backfill")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        insert_backfill_source(connection)
        insert_backfill_identity(
            connection,
            identity_id=10,
            external_id="duplicate-fingerprint-one",
            current_revision_no=2,
        )
        insert_backfill_raw(
            connection,
            raw_id=1,
            external_id="duplicate-fingerprint-one",
            source_entry_id=10,
            revision_no=1,
        )
        insert_backfill_raw(
            connection,
            raw_id=2,
            external_id="duplicate-fingerprint-two",
            source_entry_id=10,
            revision_no=2,
        )
        connection.execute(
            text(
                """
                UPDATE raw_entries
                SET payload_fingerprint = 'duplicate-fingerprint'
                WHERE source_entry_id = 10
                """
            )
        )
    engine.dispose()

    with pytest.raises(DBAPIError, match="raw_revision_duplicate_fingerprint"):
        command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0005_source_entry_backfill"
        )
        constraint_names = {
            constraint["name"]
            for constraint in inspect(connection).get_unique_constraints("raw_entries")
        }
        assert "uq_raw_source_entry_fingerprint" not in constraint_names
    engine.dispose()


def test_raw_revision_contract_rejects_duplicate_computed_fingerprints(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0007_normal_article_ingest")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        insert_backfill_source(connection)
        insert_backfill_identity(
            connection,
            identity_id=10,
            external_id="duplicate-computed-one",
            current_revision_no=2,
        )
        insert_backfill_raw(
            connection,
            raw_id=1,
            external_id="duplicate-computed-one",
            source_entry_id=10,
            revision_no=1,
        )
        insert_backfill_raw(
            connection,
            raw_id=2,
            external_id="duplicate-computed-two",
            source_entry_id=10,
            revision_no=2,
        )
        connection.execute(
            text(
                """
                UPDATE raw_entries
                SET title = 'Same evidence',
                    url = 'https://example.com/same-evidence'
                WHERE source_entry_id = 10
                """
            )
        )
    engine.dispose()

    with pytest.raises(
        RuntimeError,
        match="raw_revision_duplicate_canonical_fingerprint",
    ):
        command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0007_normal_article_ingest"
        )
        assert connection.scalar(
            text(
                """
                SELECT count(*)
                FROM raw_entries
                WHERE payload_fingerprint IS NULL
                """
            )
        ) == 2
        snapshot = read_postgres_schema(connection)
        assert snapshot.column_nullability["raw_entries"]["payload_fingerprint"] is True
    engine.dispose()


def test_raw_revision_contract_rejects_empty_persisted_fingerprint(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0007_normal_article_ingest")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        insert_backfill_source(connection)
        insert_backfill_identity(
            connection,
            identity_id=10,
            external_id="empty-persisted-fingerprint",
        )
        insert_backfill_raw(
            connection,
            raw_id=1,
            external_id="empty-persisted-fingerprint",
            source_entry_id=10,
            revision_no=1,
        )
        connection.execute(
            text(
                """
                UPDATE raw_entries
                SET payload_fingerprint = ''
                WHERE id = 1
                """
            )
        )
    engine.dispose()

    with pytest.raises(DBAPIError, match="raw_fingerprint_format_invalid"):
        command.upgrade(config, "head")

    engine = create_engine(postgres_url)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0007_normal_article_ingest"
        )
        assert connection.scalar(
            text("SELECT payload_fingerprint FROM raw_entries WHERE id = 1")
        ) == ""
        snapshot = read_postgres_schema(connection)
        assert snapshot.column_nullability["raw_entries"]["payload_fingerprint"] is True
    engine.dispose()


def test_postgres_ingest_creates_natural_identity_and_fingerprinted_revision(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Postgres expand",
            url="https://example.com/postgres-expand.xml",
            status="trial",
        )
        session.add(source)
        session.commit()
        entry = IngestEntry(
            source_guid="postgres-expand-guid",
            external_id="postgres-expand-guid",
            title="Postgres expand title",
            url="https://example.com/postgres-expand",
            raw_content="<p>Postgres expand body</p>",
            content_text="Postgres expand body",
        )

        assert ingest_source_entries(session, source, [entry]) == 1
        session.commit()

        raw = session.scalars(select(RawEntry)).one()
        identity = session.scalars(select(SourceEntryIdentity)).one()
        key = session.scalars(select(SourceEntryKey)).one()
        assert raw.source_entry_id == identity.id
        assert raw.revision_no == 1
        assert raw.payload_fingerprint == calculate_payload_fingerprint(
            RawEntryRevisionInput.from_raw_entry(raw)
        )
        assert identity.current_revision_no == 1
        assert identity.projection_pending is False
        assert key.source_entry_id == identity.id
        assert key.source_id == source.id
        assert key.identity_kind == "guid"

        conflicting_identity = SourceEntryIdentity(
            source_id=source.id,
            current_revision_no=1,
        )
        session.add(conflicting_identity)
        session.flush()
        session.add(
            SourceEntryKey(
                source_entry_id=conflicting_identity.id,
                source_id=source.id,
                identity_kind=key.identity_kind,
                identity_key=key.identity_key,
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()

    engine.dispose()


def test_postgres_guid_url_fallback_and_legacy_resolution_matrix(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        guid_source = Source(
            name="Postgres legacy GUID",
            url="https://example.com/postgres-legacy-guid.xml",
        )
        url_source = Source(
            name="Postgres legacy URL",
            url="https://example.com/postgres-legacy-url.xml",
        )
        fallback_source = Source(
            name="Postgres legacy fallback",
            url="https://example.com/postgres-legacy-fallback.xml",
        )
        session.add_all([guid_source, url_source, fallback_source])
        session.flush()
        guid_raw = make_raw_entry(
            source=guid_source,
            external_id="legacy-guid:0123456789ab",
            title="Legacy GUID",
        )
        url_raw = make_raw_entry(
            source=url_source,
            external_id="legacy-url",
            title="Legacy URL",
            url="HTTPS://EXAMPLE.COM/legacy-url/?utm_source=rss#fragment",
        )
        fallback_raw = make_raw_entry(
            source=fallback_source,
            external_id="legacy-fallback",
            title="Legacy fallback",
            author="Reader Author",
            published_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
        session.add_all([guid_raw, url_raw, fallback_raw])
        session.commit()

        cases = (
            (
                guid_source,
                guid_raw,
                SourceEntryIdentityInput(
                    source_guid="legacy-guid",
                    url="https://example.com/legacy-guid",
                ),
                "guid",
            ),
            (
                url_source,
                url_raw,
                SourceEntryIdentityInput(url="https://example.com/legacy-url"),
                "url",
            ),
            (
                fallback_source,
                fallback_raw,
                SourceEntryIdentityInput(
                    title=fallback_raw.title,
                    author=fallback_raw.author,
                    published_at=fallback_raw.published_at,
                    payload_fingerprint=fallback_raw.payload_fingerprint,
                ),
                "fallback",
            ),
        )
        for source, raw, identity_input, expected_kind in cases:
            resolution = resolve_source_entry_identity(
                session,
                source.id,
                identity_input,
            )
            assert resolution.outcome is SourceEntryResolutionOutcome.CLAIMED_LEGACY
            assert resolution.source_entry is not None
            assert resolution.source_entry.id == raw.source_entry_id
            assert session.scalar(
                select(func.count())
                .select_from(SourceEntryKey)
                .where(
                    SourceEntryKey.source_entry_id == raw.source_entry_id,
                    SourceEntryKey.identity_kind == expected_kind,
                )
            ) == 1
        session.commit()

    engine.dispose()


def test_postgres_digest_update_appends_pending_revision_without_reprojecting(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    title = "AI 早报：OpenAI / Nvidia / Anthropic / Microsoft / Google / Apple"
    original_body = "\n".join(f"{index}. 原始新闻 {index}" for index in range(1, 7))
    updated_body = original_body + "\n7. 新增新闻"

    with SessionLocal() as session:
        source = Source(
            name="Postgres digest",
            url="https://example.com/postgres-digest.xml",
            status="trial",
        )
        session.add(source)
        session.commit()
        first = IngestEntry(
            source_guid="postgres-digest-guid",
            external_id="postgres-digest-guid",
            title=title,
            url="https://example.com/postgres-digest",
            raw_content=original_body,
            content_text=original_body,
        )
        assert ingest_source_entries(session, source, [first]) == 1
        session.commit()
        document = session.scalars(select(Document)).one()
        document.document_type = "digest"
        session.commit()
        original_raw_id = document.raw_entry_id

        changed = IngestEntry(
            source_guid="postgres-digest-guid",
            external_id="postgres-digest-guid",
            title=title,
            url="https://example.com/postgres-digest",
            raw_content=updated_body,
            content_text=updated_body,
        )
        assert ingest_source_entries(session, source, [changed]) == 0
        session.commit()

        identity = session.scalars(select(SourceEntryIdentity)).one()
        raws = session.scalars(
            select(RawEntry).order_by(RawEntry.revision_no)
        ).all()
        document = session.scalars(select(Document)).one()
        assert [raw.revision_no for raw in raws] == [1, 2]
        assert identity.current_revision_no == 2
        assert identity.projection_pending is True
        assert document.raw_entry_id == original_raw_id
        assert document.content_text == original_body

    engine.dispose()


def test_postgres_head_allows_reused_external_id_across_source_entries(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source = Source(
            name="Reused GUID source",
            url="https://example.com/reused-guid.xml",
            status="trial",
        )
        session.add(source)
        session.commit()
        entries = [
            IngestEntry(
                source_guid="reused-guid",
                external_id="reused-guid",
                title="First generation",
                url="https://example.com/first-generation",
                raw_content="First body",
                content_text="First body",
            ),
            IngestEntry(
                source_guid="reused-guid",
                external_id="reused-guid",
                title="Second generation",
                url="https://example.com/second-generation",
                raw_content="Second body",
                content_text="Second body",
            ),
        ]

        assert ingest_source_entries(session, source, entries) == 2
        session.commit()

        assert session.scalars(
            select(RawEntry.external_id).order_by(RawEntry.id)
        ).all() == ["reused-guid", "reused-guid"]
        assert session.scalar(
            select(func.count()).select_from(SourceEntryIdentity)
        ) == 2
        constraint_names = {
            constraint["name"]
            for constraint in inspect(session.connection()).get_unique_constraints(
                "raw_entries"
            )
        }
        assert "uq_raw_source_external" not in constraint_names

    engine.dispose()


def test_postgres_source_entry_references_reject_cross_source_identity(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        source_a = Source(name="Source A", url="https://example.com/source-a.xml")
        source_b = Source(name="Source B", url="https://example.com/source-b.xml")
        session.add_all([source_a, source_b])
        session.flush()
        identity = SourceEntryIdentity(
            source_id=source_a.id,
            current_revision_no=1,
        )
        session.add(identity)
        session.flush()
        seed_revision = make_revision_input(
            external_id="cross-source-seed",
            title="Cross-source seed",
        )
        session.add(
            RawEntry(
                **raw_entry_revision_values(
                    source_id=source_a.id,
                    source_entry_id=identity.id,
                    revision_no=1,
                    revision=seed_revision,
                )
            )
        )
        session.add(
            SourceEntryKey(
                source_entry_id=identity.id,
                source_id=source_a.id,
                identity_kind="legacy",
                identity_key=legacy_source_entry_key(seed_revision.external_id),
            )
        )
        session.commit()

        session.add(
            SourceEntryKey(
                source_entry_id=identity.id,
                source_id=source_b.id,
                identity_kind="guid",
                identity_key="guid:" + "b" * 64,
            )
        )
        with pytest.raises(
            IntegrityError,
            match="fk_source_entry_key_identity_source",
        ):
            session.commit()
        session.rollback()

        cross_source_revision = make_revision_input(
            external_id="cross-source-raw",
            title="Cross-source raw",
        )
        cross_source_values = raw_entry_revision_values(
            source_id=source_b.id,
            source_entry_id=identity.id,
            revision_no=2,
            revision=cross_source_revision,
        )
        with pytest.raises(IntegrityError, match="fk_raw_entry_identity_source"):
            session.execute(
                text(
                    """
                    WITH inserted AS (
                        INSERT INTO raw_entries (
                            source_id, source_entry_id, revision_no,
                            payload_fingerprint, external_id, title, url, author,
                            published_at, fetched_at, raw_summary, raw_content,
                            content_hash
                        ) VALUES (
                            :source_id, :source_entry_id, :revision_no,
                            :payload_fingerprint, :external_id, :title, :url, :author,
                            :published_at, :fetched_at, :raw_summary, :raw_content,
                            :content_hash
                        )
                        RETURNING source_entry_id, revision_no
                    )
                    UPDATE source_entry_identities AS identity
                    SET current_revision_no = inserted.revision_no
                    FROM inserted
                    WHERE identity.id = inserted.source_entry_id
                    """
                ),
                cross_source_values,
            )
            session.commit()
        session.rollback()

    engine.dispose()


def create_postgres_revision_seed(
    postgres_url: str,
) -> tuple[int, RawEntryRevisionInput]:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    initial = make_revision_input(
        external_id="postgres-revision-guid",
        title="Postgres revision title",
        url="https://example.com/postgres-revision",
        author="Postgres Author",
        raw_summary="Postgres summary",
        raw_content="Postgres body",
        content_hash="1" * 64,
    )
    with SessionLocal() as session:
        identity, _raw = add_raw_revision_seed(
            session,
            revision=initial,
            payload_fingerprint=calculate_payload_fingerprint(initial),
            source_name="Postgres revision source",
            source_url="https://example.com/postgres-revision.xml",
        )
        session.commit()
        identity_id = identity.id
    engine.dispose()
    return identity_id, initial


def allocate_postgres_revision(
    postgres_url: str,
    source_entry_id: int,
    revision: RawEntryRevisionInput,
    barrier: Barrier,
) -> tuple[RawEntryRevisionOutcome, int, int]:
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        with SessionLocal() as session:
            barrier.wait(timeout=10)
            allocation = allocate_raw_entry_revision(
                session,
                source_entry_id,
                revision,
            )
            result = (
                allocation.outcome,
                allocation.raw_entry.id,
                allocation.raw_entry.revision_no,
            )
            session.commit()
            return result
    finally:
        engine.dispose()


def run_concurrent_revision_allocations(
    postgres_url: str,
    source_entry_id: int,
    revisions: tuple[RawEntryRevisionInput, RawEntryRevisionInput],
) -> list[tuple[RawEntryRevisionOutcome, int, int]]:
    barrier = Barrier(len(revisions))
    with ThreadPoolExecutor(max_workers=len(revisions)) as executor:
        futures = [
            executor.submit(
                allocate_postgres_revision,
                postgres_url,
                source_entry_id,
                revision,
                barrier,
            )
            for revision in revisions
        ]
        return [future.result(timeout=20) for future in futures]


def test_postgres_same_payload_concurrency_creates_one_revision(
    postgres_url: str,
) -> None:
    identity_id, initial = create_postgres_revision_seed(postgres_url)
    incoming = replace(
        initial,
        external_id="postgres-same-concurrent",
        title="Postgres same concurrent update",
        raw_content="Postgres same concurrent body",
        content_hash="2" * 64,
    )
    results = run_concurrent_revision_allocations(
        postgres_url,
        identity_id,
        (incoming, incoming),
    )

    assert {result[0] for result in results} == {
        RawEntryRevisionOutcome.CREATED,
        RawEntryRevisionOutcome.EXISTING,
    }
    assert {result[1] for result in results} == {results[0][1]}
    assert {result[2] for result in results} == {2}

    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        identity = session.get(SourceEntryIdentity, identity_id)
        assert identity is not None
        assert identity.current_revision_no == 2
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 2
    engine.dispose()


def test_postgres_different_payload_concurrency_allocates_contiguous_revisions(
    postgres_url: str,
) -> None:
    identity_id, initial = create_postgres_revision_seed(postgres_url)
    revisions = (
        replace(
            initial,
            external_id="postgres-different-a",
            title="Postgres different update A",
            raw_content="Postgres different body A",
            content_hash="3" * 64,
        ),
        replace(
            initial,
            external_id="postgres-different-b",
            title="Postgres different update B",
            raw_content="Postgres different body B",
            content_hash="4" * 64,
        ),
    )
    results = run_concurrent_revision_allocations(
        postgres_url,
        identity_id,
        revisions,
    )

    assert {result[0] for result in results} == {RawEntryRevisionOutcome.CREATED}
    assert {result[2] for result in results} == {2, 3}

    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        identity = session.get(SourceEntryIdentity, identity_id)
        assert identity is not None
        assert identity.current_revision_no == 3
        stored = session.scalars(
            select(RawEntry)
            .where(RawEntry.source_entry_id == identity_id)
            .order_by(RawEntry.revision_no)
        ).all()
        assert [raw.revision_no for raw in stored] == [1, 2, 3]
        assert {raw.payload_fingerprint for raw in stored[1:]} == {
            calculate_payload_fingerprint(revision) for revision in revisions
        }
        revision_three_id = next(
            result[1] for result in results if result[2] == 3
        )
        assert stored[-1].id == revision_three_id
    engine.dispose()


def test_postgres_allocator_refreshes_preloaded_identity_after_lock(
    postgres_url: str,
) -> None:
    identity_id, initial = create_postgres_revision_seed(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as stale_session:
        stale_identity = stale_session.get(SourceEntryIdentity, identity_id)
        assert stale_identity is not None
        assert stale_identity.current_revision_no == 1

        with SessionLocal() as writer_session:
            first_update = allocate_raw_entry_revision(
                writer_session,
                identity_id,
                replace(
                    initial,
                    external_id="postgres-stale-first",
                    title="First committed update",
                ),
            )
            assert first_update.raw_entry.revision_no == 2
            writer_session.commit()

        second_update = allocate_raw_entry_revision(
            stale_session,
            identity_id,
            replace(
                initial,
                external_id="postgres-stale-second",
                title="Second committed update",
            ),
        )

        assert second_update.raw_entry.revision_no == 3
        assert stale_identity.current_revision_no == 3
        stale_session.commit()

    with SessionLocal() as session:
        assert session.scalars(
            select(RawEntry.revision_no)
            .where(RawEntry.source_entry_id == identity_id)
            .order_by(RawEntry.revision_no)
        ).all() == [1, 2, 3]
    engine.dispose()


def test_runtime_head_gate_is_read_only_and_rejects_wrong_revision(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    statements: list[str] = []

    def capture_statement(*args: object) -> None:
        statements.append(str(args[2]))

    event.listen(engine, "before_cursor_execute", capture_statement)
    assert_database_at_head(engine)

    assert any("SET TRANSACTION READ ONLY" in statement for statement in statements)
    assert not any(
        statement.lstrip().upper().startswith(
            ("CREATE ", "ALTER ", "DROP ", "INSERT ", "UPDATE ", "DELETE ")
        )
        for statement in statements
    )

    with engine.begin() as connection:
        connection.execute(
            text("UPDATE alembic_version SET version_num = 'unknown_revision'")
        )

    with pytest.raises(SchemaRevisionError, match="unknown_revision"):
        assert_database_at_head(engine)

    engine.dispose()


def test_deployed_privacy_head_upgrades_through_admission_merge(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0054_generation_lifecycle")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        source_id = insert_pre_privacy_source(
            connection,
            name="Preserved privacy policy",
            url="https://example.com/privacy.xml",
        )

    command.upgrade(config, "0055_source_generation_privacy")
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE sources SET privacy_class = 'public', "
                "external_generation_allowed = true WHERE id = :source_id"
            ),
            {"source_id": source_id},
        )

    command.upgrade(config, "head")

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0072_reading_body_contract"
        )
        source_columns = {
            column["name"]: column
            for column in inspect(connection).get_columns("sources")
        }
        for name in (
            "fetch_etag",
            "fetch_last_modified",
            "last_successful_payload_hash",
        ):
            assert source_columns[name]["nullable"] is True
        source_policy = connection.execute(
            text(
                "SELECT privacy_class, external_generation_allowed "
                "FROM sources WHERE id = :source_id"
            ),
            {"source_id": source_id},
        ).one()
        assert source_policy == ("public", True)
        assert connection.scalar(
            text("SELECT global_pause FROM generation_controls WHERE id = 1")
        ) is True
    engine.dispose()


def test_reading_body_contract_migration_preserves_legacy_documents_and_enforces_states(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0071_source_fetch_validators")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        identity = add_migration_source_entry_seed(
            session,
            source_name="Legacy body source",
            source_url="https://example.com/legacy-body.xml",
            external_id="legacy-body",
            title="Legacy body",
            legacy_key_fingerprint="a" * 64,
        )
        session.flush()
        raw_id = session.scalar(
            select(RawEntry.id).where(
                RawEntry.source_entry_id == identity.id,
                RawEntry.revision_no == 1,
            )
        )
        document_id = session.scalar(
            text(
                """
                INSERT INTO documents (
                    raw_entry_id, document_type, title, summary,
                    content_text, digest_score, created_at
                ) VALUES (
                    :raw_id, 'normal_article', 'Legacy body', 'Legacy summary',
                    'Legacy content', 0, CURRENT_TIMESTAMP
                ) RETURNING id
                """
            ),
            {"raw_id": raw_id},
        )
        session.commit()

    command.upgrade(config, "head")

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0072_reading_body_contract"
        )
        assert {
            column["name"]
            for column in inspect(connection).get_columns("sources")
        } >= {"article_selector", "remove_selector"}
        assert {
            column["name"]
            for column in inspect(connection).get_columns("documents")
        } >= {"reading_html", "body_source", "web_fetch_status"}
        legacy = connection.execute(
            text(
                """
                SELECT title, summary, content_text, reading_html,
                       body_source, web_fetch_status
                FROM documents
                WHERE id = :document_id
                """
            ),
            {"document_id": document_id},
        ).one()
        assert legacy == (
            "Legacy body",
            "Legacy summary",
            "Legacy content",
            None,
            None,
            None,
        )
        assert "ck_document_reading_body_state" in {
            constraint["name"]
            for constraint in inspect(connection).get_check_constraints("documents")
        }

    for body_source, web_fetch_status in (
        ("rss", "not_requested"),
        ("rss", "failed"),
        ("webpage", "succeeded"),
    ):
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE documents
                    SET reading_html = '<p>New body</p>',
                        body_source = :body_source,
                        web_fetch_status = :web_fetch_status
                    WHERE id = :document_id
                    """
                ),
                {
                    "body_source": body_source,
                    "web_fetch_status": web_fetch_status,
                    "document_id": document_id,
                },
            )

    for reading_html, body_source, web_fetch_status in (
        ("<p>Missing states</p>", None, None),
        (None, "rss", "not_requested"),
        ("<p>Invalid body</p>", "webpage", "failed"),
    ):
        with pytest.raises(IntegrityError, match="ck_document_reading_body_state"):
            with engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        UPDATE documents
                        SET reading_html = :reading_html,
                            body_source = :body_source,
                            web_fetch_status = :web_fetch_status
                        WHERE id = :document_id
                        """
                    ),
                    {
                        "reading_html": reading_html,
                        "body_source": body_source,
                        "web_fetch_status": web_fetch_status,
                        "document_id": document_id,
                    },
                )
    engine.dispose()


def test_deployed_result_apply_head_upgrades_through_w5_merge(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0061_result_apply_contract")
    engine = create_engine(postgres_url)
    assert "output_fingerprint" in {
        column["name"] for column in inspect(engine).get_columns("generation_results")
    }
    assert "retry_kind" not in {
        column["name"] for column in inspect(engine).get_columns("generation_attempts")
    }

    command.upgrade(config, "head")

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0072_reading_body_contract"
        )
    assert "retry_kind" in {
        column["name"] for column in inspect(engine).get_columns("generation_attempts")
    }
    engine.dispose()


def test_0069_folder_media_type_upgrade_preserves_effective_type_and_enforces_constraints(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0068_cluster_current_projection")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        folder_id = int(
            connection.scalar(
                text("INSERT INTO folders (name, created_at) VALUES ('Videos', CURRENT_TIMESTAMP) RETURNING id")
            )
        )
        source_id = insert_pre_privacy_source(
            connection,
            name="Legacy article in a video folder",
            url="https://example.com/legacy-video.xml",
        )
        connection.execute(
            text("UPDATE sources SET folder_id = :folder_id WHERE id = :source_id"),
            {"folder_id": folder_id, "source_id": source_id},
        )

    command.upgrade(config, "head")

    with engine.begin() as connection:
        folder = connection.execute(
            text("SELECT id, media_type FROM folders WHERE id = :folder_id"),
            {"folder_id": folder_id},
        ).one()
        source = connection.execute(
            text("SELECT folder_id, media_type FROM sources WHERE id = :source_id"),
            {"source_id": source_id},
        ).one()
        assert folder == (folder_id, "video")
        assert source == (folder_id, "video")

        folder_uniques = {
            tuple(constraint["column_names"])
            for constraint in inspect(connection).get_unique_constraints("folders")
        }
        assert {constraint.columns for constraint in EXPLICIT_FOLDER_UNIQUES} <= folder_uniques
        source_foreign_keys = inspect(connection).get_foreign_keys("sources")
        assert any(
            tuple(foreign_key["constrained_columns"])
            == EXPLICIT_SOURCE_FOLDER_FOREIGN_KEY.constrained_columns
            and tuple(foreign_key["referred_columns"])
            == EXPLICIT_SOURCE_FOLDER_FOREIGN_KEY.referred_columns
            for foreign_key in source_foreign_keys
        )
        checks = {
            row[0]: row[1]
            for row in connection.execute(
                text(
                    "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint "
                    "WHERE conrelid IN ('folders'::regclass, 'sources'::regclass)"
                )
            )
        }
        assert all(media_type in checks["ck_folder_media_type"] for media_type in EXPLICIT_FOLDER_MEDIA_TYPES)
        assert all(media_type in checks["ck_source_media_type"] for media_type in EXPLICIT_FOLDER_MEDIA_TYPES)

        article_folder_id = int(
            connection.scalar(
                text(
                    "INSERT INTO folders (name, media_type, created_at) "
                    "VALUES ('Shared', 'article', CURRENT_TIMESTAMP) RETURNING id"
                )
            )
        )
        video_folder_id = int(
            connection.scalar(
                text(
                    "INSERT INTO folders (name, media_type, created_at) "
                    "VALUES ('Shared', 'video', CURRENT_TIMESTAMP) RETURNING id"
                )
            )
        )
        assert article_folder_id != video_folder_id
        with pytest.raises(IntegrityError):
            with connection.begin_nested():
                connection.execute(
                    text(
                        "INSERT INTO sources (folder_id, name, url, site_url, media_type, status, enabled, "
                        "fetch_full_content, privacy_class, external_generation_allowed, generation_policy_version, "
                        "feed_trust_score, last_error, created_at) VALUES "
                        "(:folder_id, 'Wrong folder type', 'https://example.com/wrong-folder-type.xml', '', "
                        "'article', 'active', true, false, 'unclassified', false, 1, 0, '', CURRENT_TIMESTAMP)"
                    ),
                    {"folder_id": video_folder_id},
                )
        with pytest.raises(IntegrityError):
            with connection.begin_nested():
                connection.execute(
                    text(
                        "INSERT INTO folders (name, media_type, created_at) "
                        "VALUES ('Invalid', 'book', CURRENT_TIMESTAMP)"
                    )
                )
    engine.dispose()


def test_0069_prepare_declusters_atomically_then_has_zero_manifest_diff(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0068_cluster_current_projection")
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    historical_counts: dict[str, int] = {}

    with SessionLocal() as session:
        folder_id = int(
            session.scalar(
                text(
                    "INSERT INTO folders (name, created_at) VALUES "
                    "('Videos', CURRENT_TIMESTAMP) RETURNING id"
                )
            )
        )
        source_id = insert_pre_privacy_source(
            session,
            name="Clustered legacy article in a video folder",
            url="https://example.com/clustered-video.xml",
        )
        session.execute(
            text("UPDATE sources SET folder_id = :folder_id WHERE id = :source_id"),
            {"folder_id": folder_id, "source_id": source_id},
        )
        raw = make_raw_entry(
            source_id=source_id,
            external_id="clustered-video-entry",
            title="Clustered legacy video",
            url="https://example.com/clustered-video-entry",
            raw_content="migration evidence",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            document_type="normal_article",
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source_id,
            title=raw.title,
            content_text=raw.raw_content,
            url=raw.url,
            canonical_url=raw.url,
            content_hash=raw.content_hash,
        )
        session.add(item)
        session.flush()
        cluster = Cluster(
            cluster_key="0069-clustered-video",
            title="Clustered legacy video",
        )
        session.add(cluster)
        session.flush()
        session.add(ClusterItem(cluster_id=cluster.id, content_item_id=item.id))
        session.commit()

    with engine.connect() as connection:
        historical_counts = {
            table: int(connection.scalar(text(f"SELECT count(*) FROM {table}")))
            for table in ("raw_entries", "documents", "content_items", "user_states")
        }

    target = database_target(postgres_url)
    report = prepare_folder_media_types(
        postgres_url,
        apply=True,
        target=target.public_identity(),
    )
    assert report["action_count"] == 1
    assert report["decluster_source_count"] == 1
    assert report["actions"][0]["source_id"] == source_id
    assert report["actions"][0]["requires_decluster"] is True
    assert prepare_folder_media_types(postgres_url, target=target.public_identity())["action_count"] == 0

    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT folder_id, media_type FROM sources WHERE id = :source_id"),
            {"source_id": source_id},
        ).one() == (folder_id, "video")
        assert connection.scalar(
            text(
                "SELECT count(*) FROM cluster_items "
                "JOIN content_items ON content_items.id = cluster_items.content_item_id "
                "WHERE content_items.source_id = :source_id"
            ),
            {"source_id": source_id},
        ) == 0
        run = connection.execute(
            text(
                "SELECT scope_type, status, after_snapshot_finalized "
                "FROM clustering_runs ORDER BY started_at DESC LIMIT 1"
            )
        ).one()
        assert run == ("source-decluster", "completed", True)
        assert {
            table: int(connection.scalar(text(f"SELECT count(*) FROM {table}")))
            for table in historical_counts
        } == historical_counts

    prepared_manifest = collect_database_manifest(target)
    command.upgrade(config, "head")
    assert compare_database_manifests(
        prepared_manifest,
        collect_database_manifest(target),
    ) == {"ok": True, "mismatches": []}
    engine.dispose()


def test_retired_legacy_generation_rows_remain_read_only_in_postgres(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    original = {
        "task_type": "item-summary",
        "provider": "legacy",
        "object_type": "item",
        "object_id": 42,
        "status": "pending",
        "prompt_version": "legacy-summary-v1",
        "model_version": "legacy-model",
        "result_json": '{"request":{"input":"keep"}}',
    }
    with SessionLocal() as session:
        task = LLMTask(**original)
        session.add(task)
        session.commit()
        task_id = task.id

    def postgres_session() -> Iterator[Session]:
        with SessionLocal() as session:
            yield session

    app.dependency_overrides[get_session] = postgres_session
    client = TestClient(app)
    try:
        retired = [
            client.get("/legacy/tasks"),
            client.post("/legacy/tasks/claim"),
            client.post(f"/legacy/tasks/{task_id}/complete", json={"summary": "ignore"}),
            client.post(f"/legacy/tasks/{task_id}/fail", json={"error": "ignore"}),
            client.post(f"/legacy/tasks/{task_id}/retry"),
        ]
        assert {response.status_code for response in retired} == {410}
    finally:
        app.dependency_overrides.clear()

    class TranslationProvider:
        provider_name = "local"

        def chat(self, *_args: object) -> dict[str, object]:
            return {"text": "保留的翻译缓存"}

    with SessionLocal() as session:
        task = session.get(LLMTask, task_id)
        assert task is not None
        assert {
            field: getattr(task, field)
            for field in original
        } == original
        assert ensure_translation(
            session,
            TranslationProvider(),
            "translation-model",
            "This legacy translation cache remains supported.",
        ) == "保留的翻译缓存"
        session.commit()
        assert session.scalar(
            select(func.count()).select_from(LLMTask).where(
                LLMTask.task_type == "translation:text"
            )
        ) == 1
    engine.dispose()


def test_generation_admission_migration_preserves_and_backfills_known_usage(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0054_generation_lifecycle")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        request_id = connection.scalar(
            text(
                "INSERT INTO generation_requests "
                "(uid, request_fingerprint, input_fingerprint, task_type, reason, "
                " target_type, target_id, target_uid, provider, model, "
                " prompt_version, schema_version, created_at) VALUES "
                "(:uid, :request_fingerprint, :input_fingerprint, "
                " 'event-synthesis', 'explicit-user-request', 'event', 1, "
                " :target_uid, 'local', 'local-model', 'prompt-v1', 'schema-v1', "
                " CURRENT_TIMESTAMP) RETURNING id"
            ),
            {
                "uid": str(uuid4()),
                "target_uid": str(uuid4()),
                "request_fingerprint": "1" * 64,
                "input_fingerprint": "2" * 64,
            },
        )
        connection.execute(
            text(
                "INSERT INTO generation_request_payloads "
                "(request_id, payload_json, payload_fingerprint, created_at) "
                "VALUES (:request_id, CAST(:payload AS JSON), :fingerprint, "
                "CURRENT_TIMESTAMP)"
            ),
            {
                "request_id": request_id,
                "payload": '{"input":"known usage"}',
                "fingerprint": "3" * 64,
            },
        )
        attempt_id = connection.scalar(
            text(
                "INSERT INTO generation_attempts "
                "(uid, request_id, attempt_no, status, error, started_at, "
                " created_at, updated_at) VALUES "
                "(:uid, :request_id, 1, 'running', '', CURRENT_TIMESTAMP, "
                " CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id"
            ),
            {"uid": str(uuid4()), "request_id": request_id},
        )
        connection.execute(
            text(
                "UPDATE generation_attempts SET status = 'complete', "
                "finished_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP "
                "WHERE id = :attempt_id"
            ),
            {"attempt_id": attempt_id},
        )
        result_id = connection.scalar(
            text(
                "INSERT INTO generation_results "
                "(uid, request_id, attempt_id, payload_json, payload_fingerprint, "
                " input_tokens, output_tokens, created_at) VALUES "
                "(:uid, :request_id, :attempt_id, CAST(:payload AS JSON), "
                " :fingerprint, 123, 45, CURRENT_TIMESTAMP) RETURNING id"
            ),
            {
                "uid": str(uuid4()),
                "request_id": request_id,
                "attempt_id": attempt_id,
                "payload": '{"blocks":[]}',
                "fingerprint": "4" * 64,
            },
        )
        connection.execute(
            text(
                "INSERT INTO generation_applications "
                "(request_id, result_id, status, artifact_type, error, "
                " created_at, updated_at) VALUES "
                "(:request_id, :result_id, 'pending', '', '', "
                " CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            ),
            {"request_id": request_id, "result_id": result_id},
        )

    command.upgrade(config, "head")

    with engine.connect() as connection:
        admission = connection.execute(
            text(
                "SELECT approval_status, admission_status "
                "FROM generation_admissions WHERE request_id = :request_id"
            ),
            {"request_id": request_id},
        ).mappings().one()
        assert dict(admission) == {
            "approval_status": "awaiting",
            "admission_status": "awaiting",
        }
        attempt = connection.execute(
            text(
                "SELECT estimator_version, input_tokens_estimated, "
                "output_tokens_reserved, input_tokens_actual, output_tokens_actual "
                "FROM generation_attempts WHERE id = :attempt_id"
            ),
            {"attempt_id": attempt_id},
        ).mappings().one()
        assert dict(attempt) == {
            "estimator_version": None,
            "input_tokens_estimated": None,
            "output_tokens_reserved": None,
            "input_tokens_actual": 123,
            "output_tokens_actual": 45,
        }
        result = connection.execute(
            text(
                "SELECT input_tokens, output_tokens FROM generation_results "
                "WHERE id = :result_id"
            ),
            {"result_id": result_id},
        ).mappings().one()
        assert dict(result) == {"input_tokens": 123, "output_tokens": 45}
    engine.dispose()


def test_generation_lifecycle_schema_is_additive_and_enforces_state_contracts(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        assert {
            "generation_requests",
            "generation_request_payloads",
            "generation_attempts",
            "generation_results",
            "generation_applications",
            "generation_controls",
            "generation_admissions",
            "generation_runner_presences",
        }.issubset(inspect(connection).get_table_names())
        assert {
            "runner_id",
            "runner_environment_id",
            "lease_token_hash",
            "lease_expires_at",
            "last_heartbeat_at",
        }.issubset(
            {
                column["name"]
                for column in inspect(connection).get_columns("generation_attempts")
            }
        )
        assert connection.scalar(
            text("SELECT count(*) FROM generation_runner_presences")
        ) == 0
        assert connection.scalar(text("SELECT count(*) FROM llm_tasks")) == 0
        control = connection.execute(
            text(
                "SELECT global_pause, auto_run, daily_budget_tokens, "
                "input_estimator, output_reserve_tokens, day_timezone "
                "FROM generation_controls WHERE id = 1"
            )
        ).mappings().one()
        assert dict(control) == {
            "global_pause": True,
            "auto_run": False,
            "daily_budget_tokens": None,
            "input_estimator": "unicode-codepoints-v1",
            "output_reserve_tokens": 0,
            "day_timezone": "Asia/Shanghai",
        }
        connection.execute(
            text(
                "UPDATE generation_controls "
                "SET input_estimator = 'utf8-bytes-v1' WHERE id = 1"
            )
        )
        assert connection.scalar(
            text("SELECT input_estimator FROM generation_controls WHERE id = 1")
        ) == "utf8-bytes-v1"
        request_id = connection.scalar(
            text(
                "INSERT INTO generation_requests "
                "(uid, request_fingerprint, input_fingerprint, task_type, reason, "
                " target_type, target_id, target_uid, provider, model, "
                " prompt_version, schema_version, created_at) VALUES "
                "(:uid, :request_fingerprint, :input_fingerprint, "
                " 'event-synthesis', 'explicit-user-request', 'event', 1, "
                " :target_uid, 'local', 'local-model', 'prompt-v1', 'schema-v1', "
                " CURRENT_TIMESTAMP) RETURNING id"
            ),
            {
                "uid": str(uuid4()),
                "target_uid": str(uuid4()),
                "request_fingerprint": "a" * 64,
                "input_fingerprint": "b" * 64,
            },
        )
        assert request_id is not None
        connection.execute(
            text(
                "INSERT INTO generation_request_payloads "
                "(request_id, payload_json, payload_fingerprint, created_at) "
                "VALUES (:request_id, CAST(:payload AS JSON), :fingerprint, "
                "CURRENT_TIMESTAMP)"
            ),
            {"request_id": request_id, "payload": '{"input":"frozen"}', "fingerprint": "c" * 64},
        )
        attempt_id = connection.scalar(
            text(
                "INSERT INTO generation_attempts "
                "(uid, request_id, attempt_no, status, error, started_at, "
                " created_at, updated_at) VALUES "
                "(:uid, :request_id, 1, 'running', '', CURRENT_TIMESTAMP, "
                " CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id"
            ),
            {"uid": str(uuid4()), "request_id": request_id},
        )
        assert attempt_id is not None
        connection.execute(
            text(
                "UPDATE generation_attempts SET status = 'complete', "
                "finished_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP "
                "WHERE id = :attempt_id"
            ),
            {"attempt_id": attempt_id},
        )
        result_id = connection.scalar(
            text(
                "INSERT INTO generation_results "
                "(uid, request_id, attempt_id, payload_json, payload_fingerprint, "
                " output_fingerprint, schema_version, input_tokens, output_tokens, "
                " created_at) VALUES "
                "(:uid, :request_id, :attempt_id, CAST(:payload AS JSON), "
                " :fingerprint, :output_fingerprint, 'schema-v1', 12, 7, "
                " CURRENT_TIMESTAMP) RETURNING id"
            ),
            {
                "uid": str(uuid4()),
                "request_id": request_id,
                "attempt_id": attempt_id,
                "payload": '{"blocks":[]}',
                "fingerprint": "d" * 64,
                "output_fingerprint": "f" * 64,
            },
        )
        assert result_id is not None
        application_id = connection.scalar(
            text(
                "INSERT INTO generation_applications "
                "(request_id, result_id, status, artifact_type, error, "
                " created_at, updated_at) VALUES "
                "(:request_id, :result_id, 'pending', '', '', "
                " CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id"
            ),
            {"request_id": request_id, "result_id": result_id},
        )
        assert application_id is not None
        connection.execute(
            text(
                "UPDATE generation_applications SET status = 'applied', "
                "artifact_type = 'synthesis-version', artifact_id = 9, "
                "apply_attempt_count = 1, "
                "applied_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP "
                "WHERE id = :application_id"
            ),
            {"application_id": application_id},
        )
        admission = connection.execute(
            text(
                "SELECT approval_status, admission_status "
                "FROM generation_admissions WHERE request_id = :request_id"
            ),
            {"request_id": request_id},
        ).mappings().one_or_none()
        assert admission is None

    with pytest.raises(DBAPIError, match="incomplete_generation_result_metadata"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO generation_results "
                    "(uid, request_id, attempt_id, payload_json, payload_fingerprint, "
                    " input_tokens, output_tokens, created_at) VALUES "
                    "(:uid, :request_id, :attempt_id, CAST(:payload AS JSON), "
                    " :fingerprint, 12, 7, CURRENT_TIMESTAMP)"
                ),
                {
                    "uid": str(uuid4()),
                    "request_id": request_id,
                    "attempt_id": attempt_id,
                    "payload": '{"blocks":[]}',
                    "fingerprint": "0" * 64,
                },
            )

    with pytest.raises(DBAPIError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE generation_controls "
                    "SET input_estimator = 'word-count-v1' WHERE id = 1"
                )
            )

    with pytest.raises(DBAPIError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO generation_attempts "
                    "(uid, request_id, attempt_no, status, error, started_at, "
                    " input_tokens_actual, output_tokens_actual, created_at, "
                    " updated_at) VALUES "
                    "(:uid, :request_id, 2, 'running', '', CURRENT_TIMESTAMP, "
                    " 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                ),
                {"uid": str(uuid4()), "request_id": request_id},
            )

    for statement, params in (
        (
            "UPDATE generation_requests SET reason = 'changed' WHERE id = :id",
            {"id": request_id},
        ),
        (
            "UPDATE generation_request_payloads SET payload_fingerprint = :value "
            "WHERE request_id = :id",
            {"id": request_id, "value": "e" * 64},
        ),
        (
            "UPDATE generation_results SET input_tokens = 99 WHERE id = :id",
            {"id": result_id},
        ),
        (
            "UPDATE generation_attempts SET status = 'running', "
            "finished_at = NULL WHERE id = :id",
            {"id": attempt_id},
        ),
        (
            "UPDATE generation_applications SET status = 'failed', "
            "artifact_type = '', artifact_id = NULL, error = 'late', "
            "applied_at = NULL WHERE id = :id",
            {"id": application_id},
        ),
    ):
        with pytest.raises(DBAPIError):
            with engine.begin() as connection:
                connection.execute(text(statement), params)

    with pytest.raises(DBAPIError, match="generation_request_without_payload"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO generation_requests "
                    "(uid, request_fingerprint, input_fingerprint, task_type, reason, "
                    " target_type, target_id, target_uid, provider, model, "
                    " prompt_version, schema_version, created_at) VALUES "
                    "(:uid, :request_fingerprint, :input_fingerprint, "
                    " 'event-synthesis', 'explicit-user-request', 'event', 2, "
                    " :target_uid, 'local', 'local-model', 'prompt-v1', 'schema-v1', "
                    " CURRENT_TIMESTAMP)"
                ),
                {
                    "uid": str(uuid4()),
                    "target_uid": str(uuid4()),
                    "request_fingerprint": "f" * 64,
                    "input_fingerprint": "0" * 64,
                },
            )
    engine.dispose()


def test_p04_manifest_normalizes_generation_control_operational_timestamp(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    target = database_target(postgres_url)
    before = collect_database_manifest(target)
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE generation_controls "
                "SET updated_at = updated_at + INTERVAL '1 second' WHERE id = 1"
            )
        )
    after = collect_database_manifest(target)

    assert before["snapshot"] == after["snapshot"]
    engine.dispose()


def test_source_privacy_migration_defaults_existing_sources_and_enforces_policy(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0054_generation_lifecycle")
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        source_id = insert_pre_privacy_source(
            connection,
            name="Existing source",
            url="https://example.com/existing.xml",
        )
        before_generation_counts = {
            table: connection.scalar(text(f"SELECT count(*) FROM {table}"))
            for table in (
                "generation_requests",
                "generation_attempts",
                "generation_results",
            )
        }

    command.upgrade(config, "head")

    with engine.begin() as connection:
        assert connection.execute(
            text(
                "SELECT name, privacy_class, external_generation_allowed, "
                "generation_policy_version FROM sources WHERE id = :source_id"
            ),
            {"source_id": source_id},
        ).one() == ("Existing source", "unclassified", False, 1)
        assert {
            table: connection.scalar(text(f"SELECT count(*) FROM {table}"))
            for table in before_generation_counts
        } == before_generation_counts
        connection.execute(
            text(
                "UPDATE sources SET privacy_class = 'public', "
                "external_generation_allowed = true WHERE id = :source_id"
            ),
            {"source_id": source_id},
        )
        assert connection.scalar(
            text(
                "SELECT generation_policy_version FROM sources "
                "WHERE id = :source_id"
            ),
            {"source_id": source_id},
        ) == 2
        connection.execute(
            text(
                "UPDATE sources SET privacy_class = 'private', "
                "external_generation_allowed = false WHERE id = :source_id"
            ),
            {"source_id": source_id},
        )
        assert connection.scalar(
            text(
                "SELECT generation_policy_version FROM sources "
                "WHERE id = :source_id"
            ),
            {"source_id": source_id},
        ) == 3
        connection.execute(
            text(
                "UPDATE sources SET privacy_class = 'public', "
                "external_generation_allowed = true WHERE id = :source_id"
            ),
            {"source_id": source_id},
        )
        assert connection.scalar(
            text(
                "SELECT generation_policy_version FROM sources "
                "WHERE id = :source_id"
            ),
            {"source_id": source_id},
        ) == 4

    for statement in (
        "UPDATE sources SET privacy_class = 'private' WHERE id = :source_id",
        "UPDATE sources SET privacy_class = 'unknown' WHERE id = :source_id",
        "UPDATE sources SET generation_policy_version = 0 WHERE id = :source_id",
    ):
        with pytest.raises(DBAPIError):
            with engine.begin() as connection:
                connection.execute(text(statement), {"source_id": source_id})

    with engine.begin() as connection:
        blocked_request_id = connection.scalar(
            text(
                "INSERT INTO generation_requests "
                "(uid, request_fingerprint, input_fingerprint, task_type, reason, "
                " target_type, target_id, target_uid, provider, model, prompt_version, "
                " schema_version, privacy_status, privacy_reason, "
                " source_policy_fingerprint, created_at) VALUES "
                "(:uid, :request_fingerprint, :input_fingerprint, 'event-synthesis', "
                " 'explicit-user-request', 'event', 9, :target_uid, 'legacy', "
                " 'gpt-test', 'prompt-v1', 'schema-v1', 'blocked', "
                " '来源未允许外发', :policy_fingerprint, CURRENT_TIMESTAMP) RETURNING id"
            ),
            {
                "uid": str(uuid4()),
                "request_fingerprint": "a" * 64,
                "input_fingerprint": "b" * 64,
                "target_uid": str(uuid4()),
                "policy_fingerprint": "c" * 64,
            },
        )
        connection.execute(
            text(
                "INSERT INTO generation_request_sources "
                "(request_id, source_id, source_name, privacy_class, "
                " external_generation_allowed, source_policy_version) "
                "SELECT :request_id, id, name, privacy_class, "
                "external_generation_allowed, generation_policy_version "
                "FROM sources WHERE id = :source_id"
            ),
            {"request_id": blocked_request_id, "source_id": source_id},
        )

    with pytest.raises(DBAPIError, match="blocked_generation_request_with_payload"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO generation_request_payloads "
                    "(request_id, payload_json, payload_fingerprint) "
                    "VALUES (:request_id, CAST('{}' AS jsonb), :fingerprint)"
                ),
                {"request_id": blocked_request_id, "fingerprint": "d" * 64},
            )

    with pytest.raises(DBAPIError, match="external_generation_request_without_sources"):
        with engine.begin() as connection:
            eligible_request_id = connection.scalar(
                text(
                    "INSERT INTO generation_requests "
                    "(uid, request_fingerprint, input_fingerprint, task_type, reason, "
                    " target_type, target_id, target_uid, provider, model, prompt_version, "
                    " schema_version, privacy_status, privacy_reason, "
                    " source_policy_fingerprint, created_at) VALUES "
                    "(:uid, :request_fingerprint, :input_fingerprint, 'event-synthesis', "
                    " 'explicit-user-request', 'event', 10, :target_uid, 'legacy', "
                    " 'gpt-test', 'prompt-v1', 'schema-v1', 'eligible', '', "
                    " :policy_fingerprint, CURRENT_TIMESTAMP) RETURNING id"
                ),
                {
                    "uid": str(uuid4()),
                    "request_fingerprint": "e" * 64,
                    "input_fingerprint": "f" * 64,
                    "target_uid": str(uuid4()),
                    "policy_fingerprint": "0" * 64,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO generation_request_payloads "
                    "(request_id, payload_json, payload_fingerprint) "
                    "VALUES (:request_id, CAST('{}' AS jsonb), :fingerprint)"
                ),
                {"request_id": eligible_request_id, "fingerprint": "1" * 64},
            )

    with engine.begin() as connection:
        eligible_request_id, forced_transaction_id = connection.execute(
            text(
                "INSERT INTO generation_requests "
                "(uid, request_fingerprint, input_fingerprint, task_type, reason, "
                " target_type, target_id, target_uid, provider, model, prompt_version, "
                " schema_version, privacy_status, privacy_reason, "
                " source_policy_fingerprint, source_snapshot_transaction_id, created_at) VALUES "
                "(:uid, :request_fingerprint, :input_fingerprint, 'event-synthesis', "
                " 'explicit-user-request', 'event', 11, :target_uid, 'legacy', "
                " 'gpt-test', 'prompt-v1', 'schema-v1', 'eligible', '', "
                " :policy_fingerprint, '1'::xid8, CURRENT_TIMESTAMP) "
                "RETURNING id, source_snapshot_transaction_id = pg_current_xact_id()"
            ),
            {
                "uid": str(uuid4()),
                "request_fingerprint": "2" * 64,
                "input_fingerprint": "3" * 64,
                "target_uid": str(uuid4()),
                "policy_fingerprint": "4" * 64,
            },
        ).one()
        assert forced_transaction_id is True
        connection.execute(
            text(
                "INSERT INTO generation_request_sources "
                "(request_id, source_id, source_name, privacy_class, "
                " external_generation_allowed, source_policy_version) "
                "SELECT :request_id, id, name, privacy_class, "
                "external_generation_allowed, generation_policy_version "
                "FROM sources WHERE id = :source_id"
            ),
            {"request_id": eligible_request_id, "source_id": source_id},
        )
        connection.execute(
            text(
                "INSERT INTO generation_request_payloads "
                "(request_id, payload_json, payload_fingerprint) "
                "VALUES (:request_id, CAST('{}' AS jsonb), :fingerprint)"
            ),
            {"request_id": eligible_request_id, "fingerprint": "5" * 64},
        )

    with pytest.raises(DBAPIError, match="external_generation_request_ineligible_source"):
        with engine.begin() as connection:
            ineligible_request_id = connection.scalar(
                text(
                    "INSERT INTO generation_requests "
                    "(uid, request_fingerprint, input_fingerprint, task_type, reason, "
                    " target_type, target_id, target_uid, provider, model, prompt_version, "
                    " schema_version, privacy_status, privacy_reason, "
                    " source_policy_fingerprint, created_at) VALUES "
                    "(:uid, :request_fingerprint, :input_fingerprint, 'event-synthesis', "
                    " 'explicit-user-request', 'event', 12, :target_uid, 'legacy', "
                    " 'gpt-test', 'prompt-v1', 'schema-v1', 'eligible', '', "
                    " :policy_fingerprint, CURRENT_TIMESTAMP) RETURNING id"
                ),
                {
                    "uid": str(uuid4()),
                    "request_fingerprint": "6" * 64,
                    "input_fingerprint": "7" * 64,
                    "target_uid": str(uuid4()),
                    "policy_fingerprint": "8" * 64,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO generation_request_sources "
                    "(request_id, source_id, source_name, privacy_class, "
                    " external_generation_allowed, source_policy_version) "
                    "VALUES (:request_id, :source_id, 'forged private snapshot', "
                    "'private', false, 1)"
                ),
                {"request_id": ineligible_request_id, "source_id": source_id},
            )
            connection.execute(
                text(
                    "INSERT INTO generation_request_payloads "
                    "(request_id, payload_json, payload_fingerprint) "
                    "VALUES (:request_id, CAST('{}' AS jsonb), :fingerprint)"
                ),
                {"request_id": ineligible_request_id, "fingerprint": "9" * 64},
            )

    with pytest.raises(DBAPIError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO generation_requests "
                    "(uid, request_fingerprint, input_fingerprint, task_type, reason, "
                    " target_type, target_id, target_uid, provider, model, prompt_version, "
                    " schema_version, privacy_status, privacy_reason, created_at) VALUES "
                    "(:uid, :request_fingerprint, :input_fingerprint, 'event-synthesis', "
                    " 'explicit-user-request', 'event', 13, :target_uid, 'legacy', "
                    " 'gpt-test', 'prompt-v1', 'schema-v1', 'local', '', CURRENT_TIMESTAMP)"
                ),
                {
                    "uid": str(uuid4()),
                    "request_fingerprint": "a1" * 32,
                    "input_fingerprint": "b1" * 32,
                    "target_uid": str(uuid4()),
                },
            )

    with engine.begin() as connection:
        second_source_id = insert_pre_privacy_source(
            connection,
            name="Second public source",
            url="https://example.com/second.xml",
        )
        connection.execute(
            text(
                "UPDATE sources SET privacy_class = 'public', "
                "external_generation_allowed = true WHERE id = :source_id"
            ),
            {"source_id": second_source_id},
        )
    with pytest.raises(DBAPIError, match="immutable_generation_request_sources"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO generation_request_sources "
                    "(request_id, source_id, source_name, privacy_class, "
                    " external_generation_allowed, source_policy_version) "
                    "SELECT :request_id, id, name, privacy_class, "
                    "external_generation_allowed, generation_policy_version "
                    "FROM sources WHERE id = :source_id"
                ),
                {
                    "request_id": eligible_request_id,
                    "source_id": second_source_id,
                },
            )

    claim_session = Session(engine)
    try:
        request = claim_session.get(GenerationRequest, eligible_request_id)
        assert request is not None
        start_generation_attempt(claim_session, request)
        with pytest.raises(DBAPIError):
            with Session(engine) as competing_session:
                competing_session.execute(
                    select(Source)
                    .where(Source.id == source_id)
                    .with_for_update(nowait=True)
                ).scalar_one()
    finally:
        claim_session.rollback()
        claim_session.close()
    engine.dispose()


def test_generation_lifecycle_event_api_closes_the_loop_on_real_postgres(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from reader_api import event_synthesis as event_synthesis_module
    from reader_api import main as main_module

    upgrade_baseline(postgres_url)
    postgres_engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=postgres_engine, expire_on_commit=False)
    with SessionLocal() as session:
        items: list[ContentItem] = []
        for index in range(2):
            source = Source(
                name=f"Generation API source {index}",
                url=f"https://example.com/generation-api-{index}.xml",
                status="active",
                media_type="article",
            )
            session.add(source)
            session.flush()
            raw = make_raw_entry(
                source=source,
                external_id=f"generation-api-{index}",
                title=f"Generation API evidence {index}",
                url=f"https://example.com/generation-api-{index}",
                raw_content=f"Generation API evidence body {index}",
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                document_type="normal_article",
                title=raw.title,
                content_text=raw.raw_content,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=raw.title,
                content_text=raw.raw_content,
                url=raw.url,
                canonical_url=raw.url,
                content_hash=raw.content_hash,
                embedding_vector=POSTGRES_TEST_VECTOR,
                embedding_model="generation-api-model",
            )
            session.add(item)
            session.flush()
            items.append(item)
        with clustering_run(
            session,
            scope_type="generation-api-postgres",
            item_ids=[item.id for item in items],
            rule_version="generation-api-postgres-v1",
        ):
            for item in items:
                assign_cluster(session, item)
        event_row = session.scalar(
            select(ReaderEvent).order_by(ReaderEvent.id.desc())
        )
        assert event_row is not None
        event_uid = event_row.uid
        session.commit()

    def postgres_session() -> Iterator[Session]:
        with SessionLocal() as session:
            yield session

    def valid_blocks(input_text: str) -> list[dict[str, object]]:
        input_data = json.loads(input_text)
        uids = [
            str(row["evidence_version_uid"])
            for row in input_data["evidence"]
        ]
        return [
            {
                "kind": "summary",
                "body": "两个来源共同确认该事件。",
                "citations": [
                    {"evidence_version_uid": uid, "side": "support"}
                    for uid in uids
                ],
            },
            {
                "kind": "fact",
                "body": "已确认事实。",
                "citations": [
                    {"evidence_version_uid": uids[0], "side": "support"}
                ],
            },
            {
                "kind": "viewpoint",
                "body": "来源一给出观点。",
                "attribution": "Generation API source 0",
                "citations": [
                    {"evidence_version_uid": uids[0], "side": "attributed"}
                ],
            },
            {
                "kind": "disagreement",
                "body": "两个来源存在分歧。",
                "citations": [
                    {"evidence_version_uid": uids[0], "side": "position_a"},
                    {"evidence_version_uid": uids[1], "side": "position_b"},
                ],
            },
            {
                "kind": "uncertainty",
                "body": "后续仍不确定。",
                "citations": [
                    {"evidence_version_uid": uids[1], "side": "uncertain"}
                ],
            },
        ]

    def chat(
        _provider: object,
        _model: str,
        _system_prompt: str,
        input_text: str,
    ) -> dict[str, object]:
        with SessionLocal() as audit:
            attempt = audit.scalar(select(GenerationAttempt))
            assert attempt is not None and attempt.status == "running"
            assert audit.query(GenerationRequest).count() == 1
            assert audit.query(GenerationRequestPayload).count() == 1
            assert audit.query(GenerationResult).count() == 0
        return {
            "output": json.dumps(
                {"blocks": valid_blocks(input_text)}, ensure_ascii=False
            ),
            "usage": {"input_tokens": 144, "output_tokens": 55},
        }

    original_save = event_synthesis_module.save_synthesis_version

    def save_after_result(*args: object, **kwargs: object) -> SynthesisVersion:
        with SessionLocal() as audit:
            result = audit.scalar(select(GenerationResult))
            application = audit.scalar(select(GenerationApplication))
            assert result is not None
            assert result.input_tokens == 144 and result.output_tokens == 55
            assert application is not None and application.status == "pending"
            assert audit.query(SynthesisVersion).count() == 0
        return original_save(*args, **kwargs)

    monkeypatch.setattr(main_module.LocalChatProvider, "chat", chat)
    monkeypatch.setattr(
        event_synthesis_module,
        "save_synthesis_version",
        save_after_result,
    )
    lock_statements: list[str] = []

    def capture_lock_statement(*args: object) -> None:
        statement = str(args[2])
        if "FOR UPDATE" in statement and any(
            table in statement
            for table in (
                "generation_requests",
                "generation_controls",
                "generation_admissions",
            )
        ):
            lock_statements.append(statement)

    event.listen(postgres_engine, "before_cursor_execute", capture_lock_statement)
    app.dependency_overrides[get_session] = postgres_session
    try:
        client = TestClient(app)
        listed_sources = client.get("/sources")
        source_rows = listed_sources.json()
        single_policy = client.patch(
            f"/sources/{source_rows[0]['id']}",
            json={
                "privacy_class": "public",
                "external_generation_allowed": True,
            },
        )
        bulk_policy = client.post(
            "/sources/bulk",
            json={
                "ids": [source_rows[1]["id"]],
                "set": {
                    "privacy_class": "public",
                    "external_generation_allowed": True,
                },
            },
        )
        configured = client.patch(
            "/generation/control",
            json={
                "global_pause": False,
                "daily_budget_tokens": 100_000,
                "output_reserve_tokens": 200,
            },
        )
        assert configured.status_code == 200, configured.text
        response = client.post(
            f"/events/{event_uid}/synthesis", json={"provider": "local"}
        )
        overview = client.get("/generation/tasks")
    finally:
        app.dependency_overrides.pop(get_session, None)

    assert listed_sources.status_code == 200
    assert all(
        row["privacy_class"] == "unclassified"
        and row["external_generation_allowed"] is False
        for row in source_rows
    )
    assert single_policy.status_code == 200
    assert single_policy.json()["privacy_class"] == "public"
    assert single_policy.json()["external_generation_allowed"] is True
    assert bulk_policy.status_code == 200
    assert bulk_policy.json() == {"updated": 1}
    assert response.status_code == 200, response.text
    assert lock_statements
    assert "generation_requests" in lock_statements[0]
    assert response.json()["status"] == "current"
    assert response.json()["task"]["application_status"] == "applied"
    assert overview.status_code == 200
    assert overview.json()[0]["input_tokens"] == 144
    assert overview.json()[0]["output_tokens"] == 55
    with SessionLocal() as session:
        attempt = session.scalar(select(GenerationAttempt))
        application = session.scalar(select(GenerationApplication))
        assert attempt is not None and attempt.status == "complete"
        assert application is not None and application.status == "applied"
        assert session.query(SynthesisVersion).count() == 1
        assert session.query(LLMTask).count() == 0
        assert {
            source.generation_policy_version
            for source in session.scalars(select(Source)).all()
        } == {2}
    postgres_engine.dispose()


def test_item_and_cluster_generation_lifecycle_on_real_postgres(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from reader_api import generation_runner_synthesis as runner_synthesis
    from reader_api import main as main_module
    from reader_api.llm import OpenAICompatibleChatProvider

    upgrade_baseline(postgres_url)
    postgres_engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=postgres_engine, expire_on_commit=False)
    with SessionLocal() as session:
        items: list[ContentItem] = []
        for index in range(2):
            source = Source(
                name=f"Producer lifecycle source {index}",
                url=f"https://example.com/producer-lifecycle-{index}.xml",
                status="active",
                media_type="article",
                privacy_class="public",
                external_generation_allowed=True,
            )
            session.add(source)
            session.flush()
            raw = make_raw_entry(
                source=source,
                external_id=f"producer-lifecycle-{index}",
                title=f"Producer lifecycle evidence {index}",
                url=f"https://example.com/producer-lifecycle-{index}",
                raw_content=f"Producer lifecycle body {index}",
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                document_type="normal_article",
                title=raw.title,
                content_text=raw.raw_content,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=raw.title,
                content_text=raw.raw_content,
                url=raw.url,
                canonical_url=raw.url,
                content_hash=raw.content_hash,
                embedding_vector=POSTGRES_TEST_VECTOR,
                embedding_model="producer-lifecycle-model",
            )
            session.add(item)
            session.flush()
            items.append(item)
        with clustering_run(
            session,
            scope_type="producer-lifecycle-postgres",
            item_ids=[item.id for item in items],
            rule_version="producer-lifecycle-postgres-v1",
        ):
            for item in items:
                assign_cluster(session, item)
        cluster = session.scalar(select(Cluster).order_by(Cluster.id.desc()))
        assert cluster is not None
        item_id = items[0].id
        cluster_id = cluster.id
        original_documents = list(
            session.execute(
                select(Document.id, Document.content_text).order_by(Document.id)
            ).all()
        )
        original_raw_entries = list(
            session.execute(
                select(RawEntry.id, RawEntry.raw_content).order_by(RawEntry.id)
            ).all()
        )
        control = current_generation_control(session)
        control.global_pause = False
        control.daily_budget_tokens = 100_000
        control.output_reserve_tokens = 100
        session.commit()

    def postgres_session() -> Iterator[Session]:
        with SessionLocal() as session:
            yield session

    local_calls = 0

    def local_chat(*_args: object, **_kwargs: object) -> dict[str, object]:
        nonlocal local_calls
        local_calls += 1
        if local_calls == 1:
            raise RuntimeError("temporary local transport failure")
        return {
            "summary": "本地摘要第二次 Attempt 成功",
            "usage": {"input_tokens": 21, "output_tokens": 8},
        }

    def remote_chat(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {
            "title": "远端合成",
            "summary": "远端摘要 [1][2]",
            "content": "远端补充 [1][2]",
            "usage": {"input_tokens": 34, "output_tokens": 13},
        }

    token = "real-postgres-producer-runner-token"
    environment_id = "reader-p04-producer-lifecycle-test"
    monkeypatch.setattr(main_module.LocalChatProvider, "chat", local_chat)
    monkeypatch.setattr(OpenAICompatibleChatProvider, "chat", remote_chat)
    monkeypatch.setattr(main_module.settings, "runner_token", token)
    monkeypatch.setattr(main_module.settings, "environment_id", environment_id)
    app.dependency_overrides[get_session] = postgres_session
    try:
        client = TestClient(app)
        configured = client.patch(
            "/ai/settings",
            json={
                "synthesis_remote_base_url": "https://api.example.com",
                "synthesis_remote_model": "remote-model",
                "synthesis_remote_api_key": "secret",
            },
        )
        local_summary = client.post(
            f"/items/{item_id}/summarize", json={"provider": "local"}
        )
        remote_cluster = client.post(
            f"/clusters/{cluster_id}/synthesize",
            json={"provider": "openai_compatible"},
        )
        queued = client.post(
            f"/clusters/{cluster_id}/synthesize",
            json={"provider": "legacy"},
        )
        legacy_task = client.get("/generation/tasks").json()[0]
        assert client.post(
            f"/generation/requests/{legacy_task['request_uid']}/approve"
        ).status_code == 200
        identity = {
            "environment_id": environment_id,
            "runner_id": "postgres-producer-runner",
            "runner_version": "test-runner/1",
            "cli_version": "test-legacy/1",
        }
        headers = {"Authorization": f"Bearer {token}"}
        claim = client.post(
            "/generation/runner/claim", json=identity, headers=headers
        ).json()
        citations = claim["payload"]["input_data"]["citations"]
        complete_body = {
            "environment_id": environment_id,
            "runner_id": identity["runner_id"],
            "request_uid": claim["request_uid"],
            "lease_token": claim["lease_token"],
            "result": {
                "title": "Legacy 合成",
                "summary": "Legacy 摘要 [1][2]",
                "content": "Legacy 补充 [1][2]",
            },
            "input_tokens": 55,
            "output_tokens": 21,
            "runner_exit_code": 0,
            "runner_events": [],
        }
        original_apply = runner_synthesis.apply_producer_result

        def fail_apply(*_args: object, **_kwargs: object) -> None:
            raise SQLAlchemyError("forced producer apply failure")

        monkeypatch.setattr(runner_synthesis, "apply_producer_result", fail_apply)
        failed_apply = client.post(
            f"/generation/runner/attempts/{claim['attempt_uid']}/complete",
            json=complete_body,
            headers=headers,
        )
        monkeypatch.setattr(
            runner_synthesis, "apply_producer_result", original_apply
        )
        reapplied = client.post(
            f"/generation/requests/{legacy_task['request_uid']}/reapply"
        )
        cluster_detail = client.get(f"/clusters/{cluster_id}")
    finally:
        app.dependency_overrides.pop(get_session, None)

    assert configured.status_code == 200, configured.text
    assert local_summary.status_code == 200, local_summary.text
    assert local_summary.json()["summary"] == "本地摘要第二次 Attempt 成功"
    assert local_calls == 2
    assert remote_cluster.status_code == 200, remote_cluster.text
    assert remote_cluster.json()["generated_content"] == "远端补充 [1][2]"
    assert queued.status_code == 200, queued.text
    assert len(citations) == 2
    assert failed_apply.status_code == 503, failed_apply.text
    assert reapplied.status_code == 200, reapplied.text
    assert cluster_detail.json()["generated_content"] == "Legacy 补充 [1][2]"
    assert json.loads(cluster_detail.json()["citations"]) == [
        {
            key: citation[key]
            for key in ("source_id", "source_name", "url", "published_at")
        }
        for citation in citations
    ]
    with SessionLocal() as session:
        assert [
            (attempt.status, attempt.retry_kind)
            for attempt in session.scalars(
                select(GenerationAttempt).order_by(GenerationAttempt.id)
            ).all()
        ] == [
            ("failed", "initial"),
            ("complete", "automatic"),
            ("complete", "initial"),
            ("complete", "initial"),
        ]
        assert session.query(GenerationRequest).count() == 3
        assert session.query(GenerationResult).count() == 3
        assert session.query(GenerationApplication).count() == 3
        assert session.query(LLMTask).count() == 0
        assert session.query(EventUserState).count() == 0
        assert session.query(InteractionEvent).count() == 0
        assert list(
            session.execute(
                select(Document.id, Document.content_text).order_by(Document.id)
            ).all()
        ) == original_documents
        assert list(
            session.execute(
                select(RawEntry.id, RawEntry.raw_content).order_by(RawEntry.id)
            ).all()
        ) == original_raw_entries
    postgres_engine.dispose()


def test_generation_admission_serializes_concurrent_event_api_on_real_postgres(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from reader_api import main as main_module

    upgrade_baseline(postgres_url)
    postgres_engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=postgres_engine, expire_on_commit=False)
    group_vectors = (
        [1.0, *([0.0] * 2559)],
        [0.0, 1.0, *([0.0] * 2558)],
    )
    with SessionLocal() as session:
        grouped_items: list[list[ContentItem]] = [[], []]
        for group in range(2):
            vector_text = "[" + ",".join(str(value) for value in group_vectors[group]) + "]"
            for source_index in range(2):
                source = Source(
                    name=f"Admission source {group}-{source_index}",
                    url=f"https://example.com/admission-{group}-{source_index}.xml",
                    status="active",
                    media_type="article",
                )
                session.add(source)
                session.flush()
                raw = make_raw_entry(
                    source=source,
                    external_id=f"admission-{group}-{source_index}",
                    title=f"Admission event {group}",
                    url=f"https://example.com/admission/{group}/{source_index}",
                    raw_content=f"Admission evidence {group}-{source_index}",
                )
                session.add(raw)
                session.flush()
                document = Document(
                    raw_entry_id=raw.id,
                    document_type="normal_article",
                    title=raw.title,
                    content_text=raw.raw_content,
                )
                session.add(document)
                session.flush()
                item = ContentItem(
                    document_id=document.id,
                    source_id=source.id,
                    title=raw.title,
                    content_text=raw.raw_content,
                    url=raw.url,
                    canonical_url=f"https://example.com/admission/event-{group}",
                    content_hash=raw.content_hash,
                    embedding_vector=vector_text,
                    embedding_model="admission-concurrency-model",
                )
                session.add(item)
                session.flush()
                grouped_items[group].append(item)
        all_items = [item for group in grouped_items for item in group]
        with clustering_run(
            session,
            scope_type="generation-admission-postgres",
            item_ids=[item.id for item in all_items],
            rule_version="generation-admission-postgres-v1",
        ):
            for group, items in enumerate(grouped_items):
                for item in items:
                    assign_runtime_cluster(
                        session,
                        item,
                        target_vector=group_vectors[group],
                    )
        session.commit()
        event_uids = list(
            session.scalars(select(ReaderEvent.uid).order_by(ReaderEvent.id))
        )
        assert len(event_uids) == 2

    def postgres_session() -> Iterator[Session]:
        with SessionLocal() as session:
            yield session

    def valid_blocks(input_text: str) -> list[dict[str, object]]:
        evidence_uids = [
            str(row["evidence_version_uid"])
            for row in json.loads(input_text)["evidence"]
        ]
        return [
            {
                "kind": "summary",
                "body": "两个来源共同确认事件。",
                "citations": [
                    {"evidence_version_uid": uid, "side": "support"}
                    for uid in evidence_uids
                ],
            },
            {
                "kind": "fact",
                "body": "已确认事实。",
                "citations": [
                    {"evidence_version_uid": evidence_uids[0], "side": "support"}
                ],
            },
            {
                "kind": "viewpoint",
                "body": "来源给出观点。",
                "attribution": "Admission source",
                "citations": [
                    {
                        "evidence_version_uid": evidence_uids[0],
                        "side": "attributed",
                    }
                ],
            },
            {
                "kind": "disagreement",
                "body": "来源存在分歧。",
                "citations": [
                    {"evidence_version_uid": evidence_uids[0], "side": "position_a"},
                    {"evidence_version_uid": evidence_uids[1], "side": "position_b"},
                ],
            },
            {
                "kind": "uncertainty",
                "body": "后续仍不确定。",
                "citations": [
                    {"evidence_version_uid": evidence_uids[1], "side": "uncertain"}
                ],
            },
        ]

    provider_started = Event()
    blocked_returned = Event()
    provider_calls: list[str] = []

    def chat(
        _provider: object,
        _model: str,
        _system_prompt: str,
        input_text: str,
    ) -> dict[str, object]:
        provider_calls.append(input_text)
        provider_started.set()
        assert blocked_returned.wait(timeout=10)
        return {
            "output": json.dumps(
                {"blocks": valid_blocks(input_text)}, ensure_ascii=False
            ),
            "usage": {"input_tokens": 300, "output_tokens": 100},
        }

    monkeypatch.setattr(main_module.LocalChatProvider, "chat", chat)
    app.dependency_overrides[get_session] = postgres_session
    try:
        client = TestClient(app)
        configured = client.patch(
            "/generation/control",
            json={
                "daily_budget_tokens": 1_100_000,
                "output_reserve_tokens": 1_000_000,
            },
        )
        assert configured.status_code == 200, configured.text

        paused = client.post(
            f"/events/{event_uids[0]}/synthesis",
            json={"provider": "local"},
        )
        assert paused.status_code == 200, paused.text
        assert paused.json()["task"]["approval_status"] == "approved"
        assert paused.json()["task"]["admission_status"] == "blocked_paused"
        assert provider_calls == []
        unpaused = client.patch(
            "/generation/control", json={"global_pause": False}
        )
        assert unpaused.status_code == 200, unpaused.text

        def generate(event_uid: str) -> dict[str, object]:
            response = TestClient(app).post(
                f"/events/{event_uid}/synthesis",
                json={"provider": "local"},
            )
            assert response.status_code == 200, response.text
            body = response.json()
            if body["task"]["admission_status"] == "blocked_concurrency":
                blocked_returned.set()
            return body

        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(generate, event_uids[0])
            assert provider_started.wait(timeout=10)
            running_tasks = client.get("/generation/tasks")
            assert running_tasks.status_code == 200, running_tasks.text
            running = next(
                task for task in running_tasks.json() if task["status"] == "running"
            )
            consumed = client.post(
                f"/generation/requests/{running['request_uid']}/approve"
            )
            assert consumed.status_code == 409, consumed.text
            assert consumed.json()["detail"] == "这次执行许可已被消费"
            second_future = executor.submit(generate, event_uids[1])
            results = [first_future.result(timeout=15), second_future.result(timeout=15)]
    finally:
        app.dependency_overrides.pop(get_session, None)

    assert {result["task"]["status"] for result in results} == {
        "complete",
        "blocked",
    }
    assert len(provider_calls) == 1
    with SessionLocal() as session:
        assert session.query(GenerationRequest).count() == 2
        assert session.query(GenerationAdmission).count() == 2
        assert session.query(GenerationAttempt).count() == 1
        assert session.query(GenerationResult).count() == 1
        assert set(session.scalars(select(GenerationAdmission.admission_status))) == {
            "admitted",
            "blocked_concurrency",
        }
    postgres_engine.dispose()


def test_generation_runner_claim_is_fifo_and_single_winner_on_real_postgres(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from reader_api import main as main_module

    upgrade_baseline(postgres_url)
    postgres_engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=postgres_engine, expire_on_commit=False)
    with SessionLocal() as session:
        source = Source(
            name="Runner claim source",
            url="https://example.com/runner-claim.xml",
            privacy_class="public",
            external_generation_allowed=True,
        )
        session.add(source)
        session.flush()
        source_policies, policy_fingerprint, privacy_reason = (
            external_generation_policy(session, [source.id])
        )
        assert privacy_reason == ""
        request_uids: list[str] = []
        for index in range(2):
            request, created = get_or_create_generation_request(
                session,
                task_type="event-synthesis",
                reason="explicit-user-request",
                target_type="event",
                target_id=index + 1,
                target_uid=str(uuid4()),
                provider="legacy",
                model="runner-test-model",
                prompt_version="runner-prompt-v1",
                schema_version="runner-schema-v1",
                input_fingerprint=str(index + 1) * 64,
                payload={
                    "input": f"private payload {index}",
                    "reasoning_effort": "medium",
                },
                privacy_status="eligible",
                source_policy_fingerprint=policy_fingerprint,
                source_policies=source_policies,
            )
            assert created is True
            approve_generation_request(session, request)
            request_uids.append(request.uid)
        control = session.get(GenerationControl, 1)
        assert control is not None
        control.global_pause = False
        control.daily_budget_tokens = 1_000_000
        session.commit()

    def postgres_session() -> Iterator[Session]:
        with SessionLocal() as session:
            yield session

    token = "real-postgres-runner-test-token"
    environment_id = "reader-p04-postgres-test"
    monkeypatch.setattr(main_module.settings, "runner_token", token)
    monkeypatch.setattr(main_module.settings, "environment_id", environment_id)
    app.dependency_overrides[get_session] = postgres_session
    barrier = Barrier(3)
    runner_ids = ["runner-a", "runner-b"]

    def claim(runner_id: str):
        barrier.wait(timeout=10)
        return TestClient(app).post(
            "/generation/runner/claim",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "environment_id": environment_id,
                "runner_id": runner_id,
                "runner_version": "test-runner-v1",
                "cli_version": "fake-legacy-cli-v1",
            },
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(claim, runner_id)
                for runner_id in runner_ids
            ]
            barrier.wait(timeout=10)
            responses = [future.result(timeout=15) for future in futures]
        winner_index = next(
            index
            for index, response in enumerate(responses)
            if response.status_code == 200
        )
        claimed = responses[winner_index].json()
        lease = {
            "environment_id": environment_id,
            "runner_id": runner_ids[winner_index],
            "request_uid": claimed["request_uid"],
            "lease_token": claimed["lease_token"],
        }
        missing_auth = TestClient(app).post(
            f"/generation/runner/attempts/{claimed['attempt_uid']}/heartbeat",
            json=lease,
        )
        wrong_lease = TestClient(app).post(
            f"/generation/runner/attempts/{claimed['attempt_uid']}/heartbeat",
            headers={"Authorization": f"Bearer {token}"},
            json={**lease, "lease_token": "wrong-lease-token-with-enough-bytes"},
        )
        heartbeat = TestClient(app).post(
            f"/generation/runner/attempts/{claimed['attempt_uid']}/heartbeat",
            headers={"Authorization": f"Bearer {token}"},
            json=lease,
        )
        task_center = TestClient(app).get("/generation/control")
        failed = TestClient(app).post(
            f"/generation/runner/attempts/{claimed['attempt_uid']}/fail",
            headers={"Authorization": f"Bearer {token}"},
            json={
                **lease,
                "error": "fake Legacy failure",
                "input_tokens": 12,
                "output_tokens": 3,
                "runner_exit_code": 7,
                "runner_events": [
                    {"type": "turn.started"},
                    {
                        "type": "turn.completed",
                        "usage": {
                            "input_tokens": 12,
                            "cached_input_tokens": 2,
                            "output_tokens": 3,
                        },
                    },
                ],
            },
        )
    finally:
        app.dependency_overrides.pop(get_session, None)

    assert sorted(response.status_code for response in responses) == [200, 409]
    winner = next(response for response in responses if response.status_code == 200)
    loser = next(response for response in responses if response.status_code == 409)
    claimed = winner.json()
    assert claimed["claimed"] is True
    assert claimed["request_uid"] == request_uids[0]
    assert claimed["reasoning_effort"] == "medium"
    assert claimed["payload"] == {
        "input": "private payload 0",
        "reasoning_effort": "medium",
    }
    assert "payload" not in loser.json()
    assert "lease_token" not in loser.json()
    assert missing_auth.status_code == 401
    assert wrong_lease.status_code == 409
    assert heartbeat.status_code == 200, heartbeat.text
    assert task_center.status_code == 200, task_center.text
    assert task_center.json()["runner"] == {
        "environment_id": environment_id,
        "runner_id": runner_ids[winner_index],
        "runner_version": "test-runner-v1",
        "cli_version": "fake-legacy-cli-v1",
        "status": "running",
        "current_attempt_uid": claimed["attempt_uid"],
        "last_heartbeat_at": task_center.json()["runner"]["last_heartbeat_at"],
    }
    assert failed.status_code == 200, failed.text
    assert failed.json()["status"] == "failed"
    with SessionLocal() as session:
        attempts = list(session.scalars(select(GenerationAttempt)))
        presence = session.get(GenerationRunnerPresence, 1)
        assert len(attempts) == 1
        assert attempts[0].status == "failed"
        assert attempts[0].runner_cli_version == "fake-legacy-cli-v1"
        assert attempts[0].runner_exit_code == 7
        runner_audit = session.get(GenerationAttemptRunnerAudit, attempts[0].id)
        assert runner_audit is not None
        assert runner_audit.events_json == [
            {"type": "turn.started"},
            {
                "type": "turn.completed",
                "usage": {
                    "input_tokens": 12,
                    "cached_input_tokens": 2,
                    "output_tokens": 3,
                },
            },
        ]
        assert attempts[0].request_id == session.scalar(
            select(GenerationRequest.id).where(
                GenerationRequest.uid == request_uids[0]
            )
        )
        assert attempts[0].lease_token_hash != claimed["lease_token"]
        assert len(attempts[0].lease_token_hash or "") == 64
        assert presence is not None
        assert presence.status == "idle"
        assert presence.current_attempt_id is None
        assert session.scalar(
            select(GenerationAdmission.approval_status).where(
                GenerationAdmission.request_id == attempts[0].request_id
            )
        ) == "consumed"
        with pytest.raises(DBAPIError, match="immutable_generation_attempt_identity"):
            session.execute(
                text(
                    "UPDATE generation_attempts "
                    "SET runner_cli_version = 'rewritten' WHERE id = :attempt_id"
                ),
                {"attempt_id": attempts[0].id},
            )
            session.commit()
        session.rollback()
        with pytest.raises(
            DBAPIError, match="invalid_generation_attempt_transition"
        ):
            session.execute(
                text(
                    "UPDATE generation_attempts "
                    "SET runner_exit_code = 0 WHERE id = :attempt_id"
                ),
                {"attempt_id": attempts[0].id},
            )
            session.commit()
        session.rollback()
    postgres_engine.dispose()


def test_generation_result_apply_contract_on_real_postgres(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from reader_api import generation_runner as runner_module
    from reader_api import generation_runner_synthesis as runner_synthesis
    from reader_api import main as main_module
    from tests.test_event_synthesis import valid_blocks

    upgrade_baseline(postgres_url)
    postgres_engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=postgres_engine, expire_on_commit=False)
    vectors = (
        [1.0, *([0.0] * 2559)],
        [0.0, 1.0, *([0.0] * 2558)],
        [0.0, 0.0, 1.0, *([0.0] * 2557)],
    )
    with SessionLocal() as session:
        grouped_items: list[list[ContentItem]] = []
        for group, vector in enumerate(vectors):
            items: list[ContentItem] = []
            vector_text = "[" + ",".join(str(value) for value in vector) + "]"
            for source_index in range(2):
                source = Source(
                    name=f"Result apply source {group}-{source_index}",
                    url=f"https://example.com/result-apply-{group}-{source_index}.xml",
                    status="active",
                    media_type="article",
                    privacy_class="public",
                    external_generation_allowed=True,
                )
                session.add(source)
                session.flush()
                raw = make_raw_entry(
                    source=source,
                    external_id=f"result-apply-{group}-{source_index}",
                    title=f"Result apply event {group}",
                    url=f"https://example.com/result-apply/{group}/{source_index}",
                    raw_content=f"Result apply evidence {group}-{source_index}",
                )
                session.add(raw)
                session.flush()
                document = Document(
                    raw_entry_id=raw.id,
                    document_type="normal_article",
                    title=raw.title,
                    content_text=raw.raw_content,
                )
                session.add(document)
                session.flush()
                item = ContentItem(
                    document_id=document.id,
                    source_id=source.id,
                    title=raw.title,
                    content_text=raw.raw_content,
                    url=raw.url,
                    canonical_url=f"https://example.com/result-apply/event-{group}",
                    content_hash=raw.content_hash,
                    embedding_vector=vector_text,
                    embedding_model="result-apply-model",
                )
                session.add(item)
                session.flush()
                items.append(item)
            grouped_items.append(items)
        all_items = [item for items in grouped_items for item in items]
        with clustering_run(
            session,
            scope_type="result-apply-postgres",
            item_ids=[item.id for item in all_items],
            rule_version="result-apply-postgres-v1",
        ):
            for group, items in enumerate(grouped_items):
                for item in items:
                    assign_runtime_cluster(
                        session,
                        item,
                        target_vector=vectors[group],
                    )
        event_uids = list(
            session.scalars(select(ReaderEvent.uid).order_by(ReaderEvent.id))
        )
        assert len(event_uids) == 3
        control = session.get(GenerationControl, 1)
        assert control is not None
        control.global_pause = False
        control.daily_budget_tokens = 1_000_000
        session.commit()

    def postgres_session() -> Iterator[Session]:
        with SessionLocal() as session:
            yield session

    token = "result-apply-postgres-runner-token"
    environment_id = "reader-p04-result-apply-test"
    runner_id = "result-apply-runner"
    identity = {
        "environment_id": environment_id,
        "runner_id": runner_id,
        "runner_version": "result-apply-runner-v1",
        "cli_version": "fake-legacy-cli-v1",
    }
    headers = {"Authorization": f"Bearer {token}"}
    monkeypatch.setattr(main_module.settings, "runner_token", token)
    monkeypatch.setattr(main_module.settings, "environment_id", environment_id)
    app.dependency_overrides[get_session] = postgres_session
    client = TestClient(app)

    def queue(event_uid: str) -> str:
        queued = client.post(
            f"/events/{event_uid}/synthesis", json={"provider": "legacy"}
        )
        assert queued.status_code == 200, queued.text
        request_uid = queued.json()["task"]["request_uid"]
        approved = client.post(
            f"/generation/requests/{request_uid}/approve"
        )
        assert approved.status_code == 200, approved.text
        return request_uid

    def claim() -> dict[str, object]:
        response = client.post(
            "/generation/runner/claim", json=identity, headers=headers
        )
        assert response.status_code == 200, response.text
        claimed = response.json()
        assert claimed["claimed"] is True
        return claimed

    def result_for(payload: dict[str, object]) -> dict[str, object]:
        frozen = payload["payload"]
        assert isinstance(frozen, dict)
        input_data = frozen["input_data"]
        assert isinstance(input_data, dict)
        evidence = input_data["evidence"]
        assert isinstance(evidence, list)
        return {
            "blocks": valid_blocks(
                [str(row["evidence_version_uid"]) for row in evidence]
            )
        }

    def complete_body(
        claimed: dict[str, object], *, input_tokens: int = 40, output_tokens: int = 10
    ) -> dict[str, object]:
        return {
            "environment_id": environment_id,
            "runner_id": runner_id,
            "request_uid": claimed["request_uid"],
            "lease_token": claimed["lease_token"],
            "result": result_for(claimed),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "runner_exit_code": 0,
            "runner_events": [],
        }

    try:
        queue(event_uids[0])
        first_claim = claim()
        first_endpoint = (
            f"/generation/runner/attempts/{first_claim['attempt_uid']}/complete"
        )
        first_body = complete_body(first_claim)
        barrier = Barrier(3)

        def concurrent_complete():
            barrier.wait(timeout=10)
            return TestClient(app).post(
                first_endpoint, json=first_body, headers=headers
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(concurrent_complete) for _ in range(2)]
            barrier.wait(timeout=10)
            complete_responses = [future.result(timeout=20) for future in futures]
        conflict = client.post(
            first_endpoint,
            json={**first_body, "output_tokens": 11},
            headers=headers,
        )

        second_request_uid = queue(event_uids[1])
        second_claim = claim()
        assert second_claim["request_uid"] == second_request_uid
        late_token = str(second_claim["lease_token"])
        late_attempt_uid = str(second_claim["attempt_uid"])
        late_payload = second_claim
        future = datetime.now(timezone.utc) + timedelta(minutes=2)
        with monkeypatch.context() as late_clock:
            late_clock.setattr(runner_module, "now_utc", lambda: future)
            late = client.post(
                f"/generation/runner/attempts/{late_attempt_uid}/complete",
                json={
                    "environment_id": environment_id,
                    "runner_id": runner_id,
                    "request_uid": second_request_uid,
                    "lease_token": late_token,
                    "result": result_for(late_payload),
                    "input_tokens": 20,
                    "output_tokens": 5,
                    "runner_exit_code": 0,
                    "runner_events": [],
                },
                headers=headers,
            )
        with SessionLocal() as session:
            late_attempt = session.scalar(
                select(GenerationAttempt).where(
                    GenerationAttempt.uid == late_attempt_uid
                )
            )
            presence = session.get(GenerationRunnerPresence, 1)
            assert late_attempt is not None and presence is not None
            fail_generation_attempt(session, late_attempt, "lease expired")
            presence.status = "idle"
            presence.current_attempt_id = None
            session.commit()

        third_request_uid = queue(event_uids[2])
        third_claim = claim()
        assert third_claim["request_uid"] == third_request_uid
        third_endpoint = (
            f"/generation/runner/attempts/{third_claim['attempt_uid']}/complete"
        )
        original_apply = runner_synthesis.apply_generation_synthesis_result
        failed_apply_calls: list[bool] = []

        def fail_apply(*_args: object, **_kwargs: object) -> None:
            failed_apply_calls.append(True)
            raise SQLAlchemyError("forced PostgreSQL apply failure")

        monkeypatch.setattr(
            runner_synthesis, "apply_generation_synthesis_result", fail_apply
        )
        third_body = complete_body(third_claim, input_tokens=50, output_tokens=15)
        failure_barrier = Barrier(3)

        def concurrent_failed_apply():
            failure_barrier.wait(timeout=10)
            return TestClient(app).post(
                third_endpoint, json=third_body, headers=headers
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(concurrent_failed_apply) for _ in range(2)]
            failure_barrier.wait(timeout=10)
            apply_responses = [future.result(timeout=20) for future in futures]
        monkeypatch.setattr(
            runner_synthesis,
            "apply_generation_synthesis_result",
            original_apply,
        )
        with SessionLocal() as session:
            counts_before_reapply = (
                session.query(GenerationRequest).count(),
                session.query(GenerationAttempt).count(),
                session.query(GenerationResult).count(),
            )
        provider_calls: list[bool] = []

        def forbidden_provider(*_args: object, **_kwargs: object) -> None:
            provider_calls.append(True)
            raise AssertionError("reapply must not call a provider")

        monkeypatch.setattr(main_module, "local_llm", forbidden_provider)
        reapplied = client.post(
            f"/generation/requests/{third_request_uid}/reapply"
        )
    finally:
        app.dependency_overrides.pop(get_session, None)

    assert [response.status_code for response in complete_responses] == [200, 200], [
        (response.status_code, response.text) for response in complete_responses
    ]
    assert conflict.status_code == 409
    assert conflict.json()["detail"] == "生成结果重放内容或用量冲突"
    assert late.status_code == 409
    assert sorted(response.status_code for response in apply_responses) == [200, 503]
    assert failed_apply_calls == [True]
    assert reapplied.status_code == 200, reapplied.text
    assert reapplied.json()["status"] == "complete"
    assert reapplied.json()["apply_attempt_count"] == 2
    assert provider_calls == []
    with SessionLocal() as session:
        assert (
            session.query(GenerationRequest).count(),
            session.query(GenerationAttempt).count(),
            session.query(GenerationResult).count(),
        ) == counts_before_reapply
        assert session.query(GenerationAttempt).count() == 3
        assert session.query(GenerationResult).count() == 2
        assert session.query(GenerationApplication).count() == 2
        assert session.query(SynthesisVersion).count() == 2
        assert session.query(EventUserState).count() == 0
        assert session.query(InteractionEvent).count() == 0
        results = list(session.scalars(select(GenerationResult)))
        assert all(result.output_fingerprint for result in results)
        assert all(result.schema_version for result in results)
        application = session.scalar(
            select(GenerationApplication)
            .join(
                GenerationResult,
                GenerationResult.id == GenerationApplication.result_id,
            )
            .join(
                GenerationRequest,
                GenerationRequest.id == GenerationApplication.request_id,
            )
            .where(GenerationRequest.uid == third_request_uid)
        )
        assert application is not None
        assert application.status == "applied"
        assert application.apply_attempt_count == 2
        assert application.last_error == "生成结果保存失败，请重试"
    postgres_engine.dispose()


def test_evidence_review_generation_lifecycle_on_real_postgres(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from reader_api import event_synthesis as event_synthesis_module
    from reader_api import main as main_module
    from tests.test_event_synthesis import valid_blocks

    upgrade_baseline(postgres_url)
    postgres_engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=postgres_engine, expire_on_commit=False)
    event_uid = ""
    cluster_id = 0

    def add_evidence(index: int) -> str:
        nonlocal cluster_id, event_uid
        with SessionLocal() as session:
            source = Source(
                name=f"Review lifecycle source {index}",
                url=f"https://example.com/review-lifecycle-{index}.xml",
                status="active",
                media_type="article",
                privacy_class="public",
                external_generation_allowed=True,
            )
            session.add(source)
            session.flush()
            raw = make_raw_entry(
                source=source,
                external_id=f"review-lifecycle-{index}",
                title="Review lifecycle event",
                url=f"https://example.com/review-lifecycle/{index}",
                raw_content=f"Review lifecycle evidence {index}",
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                document_type="normal_article",
                title=raw.title,
                content_text=raw.raw_content,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=raw.title,
                content_text=raw.raw_content,
                url=raw.url,
                canonical_url=raw.url,
                content_hash=raw.content_hash,
                embedding_vector=POSTGRES_TEST_VECTOR,
                embedding_model="review-lifecycle-model",
            )
            session.add(item)
            session.flush()
            with clustering_run(
                session,
                scope_type=f"review-lifecycle-{index}",
                item_ids=[item.id],
                rule_version="review-lifecycle-v1",
            ):
                assigned = assign_cluster(session, item)
            if cluster_id:
                assert assigned.id == cluster_id
            else:
                cluster_id = assigned.id
            event = session.scalar(
                select(ReaderEvent).where(ReaderEvent.uid == event_uid)
                if event_uid
                else select(ReaderEvent).order_by(ReaderEvent.id.desc()).limit(1)
            )
            if event is None:
                session.commit()
                return ""
            if not event_uid:
                event_uid = event.uid
            revision = session.get(EventRevision, event.current_revision_id)
            assert revision is not None
            session.commit()
            return revision.uid

    first_revision = add_evidence(1)
    second_revision = add_evidence(2)
    assert event_uid and cluster_id and second_revision
    if first_revision:
        assert second_revision != first_revision
    with SessionLocal() as session:
        control = session.get(GenerationControl, 1)
        assert control is not None
        control.global_pause = False
        control.daily_budget_tokens = 1_000_000
        session.commit()

    def postgres_session() -> Iterator[Session]:
        with SessionLocal() as session:
            yield session

    review_mode = "ordinary"
    provider_calls: list[str] = []

    def chat(
        _provider: object,
        _model: str,
        _system_prompt: str,
        input_text: str,
    ) -> dict[str, object]:
        data = json.loads(input_text)
        if "evidence" in data:
            provider_calls.append("synthesis")
            return {
                "blocks": valid_blocks(
                    [str(row["evidence_version_uid"]) for row in data["evidence"]]
                )
            }
        provider_calls.append(review_mode)
        citation_uid = str(data["new_evidence"][0]["evidence_version_uid"])
        if review_mode == "citation":
            citation_uid = "outside-frozen-snapshot"
        result: dict[str, object] = {
            "result": review_mode if review_mode != "citation" else "ordinary",
            "reason": f"PostgreSQL {review_mode} review",
            "citations": [{"evidence_version_uid": citation_uid}],
        }
        if review_mode == "material":
            result["synthesis"] = {
                "blocks": valid_blocks(
                    [
                        str(row["evidence_version_uid"])
                        for row in data["target_evidence"]
                    ]
                )
            }
        return result

    runner_token = "evidence-review-postgres-runner-token"
    environment_id = "reader-p04-evidence-review-test"
    runner_identity = {
        "environment_id": environment_id,
        "runner_id": "evidence-review-postgres-runner",
        "runner_version": "evidence-review-runner-v1",
        "cli_version": "fake-legacy-cli-v1",
    }
    runner_headers = {"Authorization": f"Bearer {runner_token}"}
    monkeypatch.setattr(main_module.LocalChatProvider, "chat", chat)
    monkeypatch.setattr(main_module.settings, "runner_token", runner_token)
    monkeypatch.setattr(main_module.settings, "environment_id", environment_id)
    app.dependency_overrides[get_session] = postgres_session
    client = TestClient(app)
    try:
        baseline = client.post(
            f"/events/{event_uid}/synthesis", json={"provider": "local"}
        )
        assert baseline.status_code == 200, baseline.text
        baseline_version_uid = baseline.json()["current"]["version_uid"]

        ordinary_revision = add_evidence(3)
        review_mode = "ordinary"
        ordinary = client.post(
            f"/events/{event_uid}/synthesis", json={"provider": "local"}
        )
        assert ordinary.status_code == 200, ordinary.text
        assert ordinary.json()["status"] == "current"
        assert ordinary.json()["reviewed_revision_uid"] == ordinary_revision
        assert ordinary.json()["current"]["version_uid"] == baseline_version_uid

        uncertain_revision = add_evidence(4)
        review_mode = "uncertain"
        uncertain = client.post(
            f"/events/{event_uid}/synthesis", json={"provider": "local"}
        )
        assert uncertain.status_code == 200, uncertain.text
        assert uncertain.json()["status"] == "unreviewed"
        assert uncertain.json()["target_revision_uid"] == uncertain_revision
        assert uncertain.json()["reviewed_revision_uid"] == ordinary_revision
        assert uncertain.json()["current"]["version_uid"] == baseline_version_uid

        material_revision = add_evidence(5)
        review_mode = "material"
        original_save = event_synthesis_module.save_synthesis_version

        def fail_save(*_args: object, **_kwargs: object) -> None:
            raise SQLAlchemyError("forced PostgreSQL material apply failure")

        monkeypatch.setattr(event_synthesis_module, "save_synthesis_version", fail_save)
        material_failed = client.post(
            f"/events/{event_uid}/synthesis", json={"provider": "local"}
        )
        assert material_failed.status_code == 503, material_failed.text
        material_task = client.get(f"/events/{event_uid}").json()["synthesis"][
            "task"
        ]
        assert material_task["status"] == "apply_failed"
        calls_before_reapply = len(provider_calls)
        monkeypatch.setattr(
            event_synthesis_module, "save_synthesis_version", original_save
        )
        reapplied = client.post(
            f"/generation/requests/{material_task['request_uid']}/reapply"
        )
        assert reapplied.status_code == 200, reapplied.text
        assert len(provider_calls) == calls_before_reapply
        material_state = client.get(f"/events/{event_uid}").json()["synthesis"]
        assert material_state["status"] == "current"
        assert material_state["covered_revision_uid"] == material_revision
        assert material_state["current"]["version_uid"] != baseline_version_uid

        add_evidence(6)
        review_mode = "citation"
        invalid = client.post(
            f"/events/{event_uid}/synthesis", json={"provider": "local"}
        )
        assert invalid.status_code == 502, invalid.text
        with SessionLocal() as session:
            invalid_request = session.scalar(
                select(GenerationRequest)
                .where(GenerationRequest.task_type == "evidence-review")
                .order_by(GenerationRequest.id.desc())
                .limit(1)
            )
            assert invalid_request is not None
            assert session.query(GenerationResult).filter_by(
                request_id=invalid_request.id
            ).count() == 0

        frozen_revision = add_evidence(7)
        queued = client.post(
            f"/events/{event_uid}/synthesis", json={"provider": "legacy"}
        )
        assert queued.status_code == 200, queued.text
        request_uid = queued.json()["task"]["request_uid"]
        assert client.post(
            f"/generation/requests/{request_uid}/approve"
        ).status_code == 200
        claimed_response = client.post(
            "/generation/runner/claim",
            json=runner_identity,
            headers=runner_headers,
        )
        assert claimed_response.status_code == 200, claimed_response.text
        claimed = claimed_response.json()
        assert claimed["claimed"] is True
        comparison = claimed["payload"]["input_data"]
        newest_revision = add_evidence(8)
        completed = client.post(
            f"/generation/runner/attempts/{claimed['attempt_uid']}/complete",
            json={
                "environment_id": environment_id,
                "runner_id": runner_identity["runner_id"],
                "request_uid": request_uid,
                "lease_token": claimed["lease_token"],
                "result": {
                    "result": "ordinary",
                    "reason": "冻结目标仅含普通佐证。",
                    "citations": [
                        {
                            "evidence_version_uid": comparison["new_evidence"][0][
                                "evidence_version_uid"
                            ]
                        }
                    ],
                },
                "input_tokens": 45,
                "output_tokens": 9,
                "runner_exit_code": 0,
                "runner_events": [],
            },
            headers=runner_headers,
        )
        assert completed.status_code == 200, completed.text
        late_state = client.get(f"/events/{event_uid}").json()["synthesis"]
        assert late_state["status"] == "unreviewed"
        assert late_state["target_revision_uid"] == newest_revision
        assert late_state["reviewed_revision_uid"] == frozen_revision
    finally:
        app.dependency_overrides.pop(get_session, None)

    assert provider_calls == [
        "synthesis",
        "ordinary",
        "uncertain",
        "material",
        "citation",
    ]
    with SessionLocal() as session:
        assert [
            review.result
            for review in session.scalars(
                select(EvidenceReview).order_by(EvidenceReview.id)
            )
        ] == ["ordinary", "uncertain", "material", "ordinary"]
        assert session.query(SynthesisVersion).count() == 2
        assert session.query(LLMTask).count() == 0
        assert session.query(GenerationAttempt).filter(
            GenerationAttempt.request_id
            == session.query(GenerationRequest.id)
            .filter_by(uid=material_task["request_uid"])
            .scalar_subquery()
        ).count() == 1
    postgres_engine.dispose()


def test_generation_cancel_expiry_retry_race_on_real_postgres(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from reader_api import generation_lifecycle as lifecycle_module
    from reader_api import generation_runner as runner_module
    from reader_api import main as main_module

    upgrade_baseline(postgres_url)
    postgres_engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=postgres_engine, expire_on_commit=False)
    with SessionLocal() as session:
        source = Source(
            name="Cancel expiry race source",
            url="https://example.com/cancel-expiry-race.xml",
            privacy_class="public",
            external_generation_allowed=True,
        )
        session.add(source)
        session.flush()
        source_policies, policy_fingerprint, privacy_reason = (
            external_generation_policy(session, [source.id])
        )
        assert privacy_reason == ""
        request, created = get_or_create_generation_request(
            session,
            task_type="event-synthesis",
            reason="explicit-user-request",
            target_type="event",
            target_id=1,
            target_uid=str(uuid4()),
            provider="legacy",
            model="runner-test-model",
            prompt_version="runner-prompt-v1",
            schema_version="runner-schema-v1",
            input_fingerprint="8" * 64,
            payload={"input": "race payload", "reasoning_effort": "medium"},
            privacy_status="eligible",
            source_policy_fingerprint=policy_fingerprint,
            source_policies=source_policies,
        )
        assert created is True
        approve_generation_request(session, request)
        control = session.get(GenerationControl, 1)
        assert control is not None
        control.global_pause = False
        control.daily_budget_tokens = 1_000_000
        session.commit()
        request_uid = request.uid

    def postgres_session() -> Iterator[Session]:
        with SessionLocal() as session:
            yield session

    token = "real-postgres-cancel-retry-token"
    environment_id = "reader-p04-cancel-retry-test"
    monkeypatch.setattr(main_module.settings, "runner_token", token)
    monkeypatch.setattr(main_module.settings, "environment_id", environment_id)
    app.dependency_overrides[get_session] = postgres_session

    def identity(runner_id: str) -> dict[str, str]:
        return {
            "environment_id": environment_id,
            "runner_id": runner_id,
            "runner_version": "test-runner-v1",
            "cli_version": "fake-legacy-cli-v1",
        }

    headers = {"Authorization": f"Bearer {token}"}
    try:
        first_response = TestClient(app).post(
            "/generation/runner/claim",
            headers=headers,
            json=identity("runner-initial"),
        )
        assert first_response.status_code == 200, first_response.text
        first = first_response.json()
        assert first["claimed"] is True
        first_lease = {
            "environment_id": environment_id,
            "runner_id": "runner-initial",
            "request_uid": first["request_uid"],
            "lease_token": first["lease_token"],
        }
        future = runner_module.now_utc() + timedelta(minutes=2)
        monkeypatch.setattr(runner_module, "now_utc", lambda: future)
        monkeypatch.setattr(lifecycle_module, "now_utc", lambda: future)
        expiry_check = TestClient(app).post(
            "/generation/runner/claim",
            headers=headers,
            json=identity("runner-expiry-check"),
        )
        assert expiry_check.status_code == 200, expiry_check.text
        assert expiry_check.json() == {"claimed": False}
        expired = TestClient(app).get("/generation/tasks")
        assert expired.status_code == 200, expired.text
        assert expired.json()[0]["status"] == "pending"
        assert expired.json()[0]["approval_status"] == "awaiting"
        approved = TestClient(app).post(
            f"/generation/requests/{request_uid}/approve"
        )
        assert approved.status_code == 200, approved.text
        barrier = Barrier(4)

        def cancel():
            barrier.wait(timeout=10)
            return TestClient(app).post(
                f"/generation/requests/{request_uid}/cancel"
            )

        def claim(runner_id: str):
            barrier.wait(timeout=10)
            return TestClient(app).post(
                "/generation/runner/claim",
                headers=headers,
                json=identity(runner_id),
            )

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(cancel),
                executor.submit(claim, "runner-a"),
                executor.submit(claim, "runner-b"),
            ]
            barrier.wait(timeout=10)
            responses = [future.result(timeout=15) for future in futures]

        assert responses[0].status_code == 200, responses[0].text
        claim_responses = responses[1:]
        assert sum(
            response.status_code == 200
            and response.json().get("claimed") is True
            for response in claim_responses
        ) <= 1
        for path, body in (
            ("heartbeat", first_lease),
            (
                "complete",
                {
                    **first_lease,
                    "result": {},
                    "input_tokens": 1,
                    "output_tokens": 1,
                    "runner_exit_code": 0,
                    "runner_events": [],
                },
            ),
            (
                "fail",
                {
                    **first_lease,
                    "error": "late transport",
                    "failure_class": "transport",
                    "input_tokens": 1,
                    "output_tokens": 1,
                    "runner_exit_code": 1,
                    "runner_events": [],
                },
            ),
        ):
            late = TestClient(app).post(
                f"/generation/runner/attempts/{first['attempt_uid']}/{path}",
                headers=headers,
                json=body,
            )
            assert late.status_code == 409, late.text
    finally:
        app.dependency_overrides.pop(get_session, None)

    with SessionLocal() as session:
        attempts = list(
            session.scalars(
                select(GenerationAttempt).order_by(GenerationAttempt.attempt_no)
            )
        )
        assert attempts[0].status in {"canceled", "expired"}
        assert attempts[0].failure_class in {"canceled", "transport"}
        assert sum(attempt.status == "running" for attempt in attempts) <= 1
        assert sum(attempt.retry_kind == "automatic" for attempt in attempts) <= 1
        assert len(attempts) <= 2
        if len(attempts) == 2:
            assert attempts[1].retry_kind == "automatic"
            assert attempts[1].cancel_requested_at is not None
        admission = session.get(GenerationAdmission, attempts[0].request_id)
        assert admission is not None and admission.canceled_at is not None
        assert session.query(GenerationResult).count() == 0
    postgres_engine.dispose()


def test_locked_generation_control_refreshes_a_previously_loaded_row(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    postgres_engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=postgres_engine, expire_on_commit=False)

    with SessionLocal() as stale_session:
        stale = current_generation_control(stale_session)
        assert stale.global_pause is True
        assert stale.auto_run is False
        assert stale.daily_budget_tokens is None

        with SessionLocal() as concurrent_session:
            current = current_generation_control(concurrent_session)
            current.global_pause = False
            current.auto_run = True
            current.daily_budget_tokens = 123_456
            concurrent_session.commit()

        refreshed = locked_generation_control(stale_session)
        assert refreshed is stale
        assert refreshed.global_pause is False
        assert refreshed.auto_run is True
        assert refreshed.daily_budget_tokens == 123_456
        stale_session.rollback()

    postgres_engine.dispose()


def test_generation_retention_migration_moves_existing_runner_jsonl(
    postgres_url: str,
) -> None:
    config = make_alembic_config(postgres_url)
    command.upgrade(config, "0062_w5_contract_merge")
    engine = create_engine(postgres_url)
    events = [
        {"type": "turn.started"},
        {"type": "turn.completed", "usage": {"input_tokens": 3, "output_tokens": 1}},
    ]
    application_context = {
        "event_uid": "event-migration",
        "target_revision_uid": "revision-migration",
        "snapshot_uid": "snapshot-migration",
        "policy_version": "event-synthesis-policy-v2",
        "prompt_version": "event-synthesis-prompt-v1",
        "schema_version": "event-synthesis-schema-v1",
        "source_coverage_fingerprint": "d" * 64,
        "content_fingerprint": "e" * 64,
        "generation_fingerprint": "f" * 64,
    }
    review_application_context = {
        "event_uid": "event-review-migration",
        "baseline_snapshot_uid": "baseline-snapshot-migration",
        "baseline_revision_uid": "baseline-revision-migration",
        "target_snapshot_uid": "target-snapshot-migration",
        "target_revision_uid": "target-revision-migration",
        "policy_version": "evidence-review-policy-v1",
        "prompt_version": "evidence-review-prompt-v1",
        "schema_version": "evidence-review-schema-v1",
        "comparison_fingerprint": "9" * 64,
    }
    created_at = datetime(2026, 6, 1, tzinfo=timezone.utc)
    with engine.begin() as connection:
        request_id = connection.scalar(
            text(
                "INSERT INTO generation_requests "
                "(uid, request_fingerprint, input_fingerprint, task_type, reason, "
                "target_type, target_id, target_uid, provider, model, prompt_version, "
                "schema_version, privacy_status, privacy_reason, created_at) "
                "VALUES (:uid, :request_fingerprint, :input_fingerprint, "
                "'event-synthesis', 'explicit-user-request', 'event', 1, :target_uid, "
                "'local', 'migration-model', 'migration-prompt-v1', "
                "'migration-schema-v1', 'local', '', :created_at) RETURNING id"
            ),
            {
                "uid": str(uuid4()),
                "request_fingerprint": "a" * 64,
                "input_fingerprint": "b" * 64,
                "target_uid": str(uuid4()),
                "created_at": created_at,
            },
        )
        assert request_id is not None
        connection.execute(
            text(
                "INSERT INTO generation_request_payloads "
                "(request_id, payload_json, payload_fingerprint, created_at) "
                "VALUES (:request_id, CAST(:payload_json AS json), :fingerprint, :created_at)"
            ),
            {
                "request_id": request_id,
                "payload_json": json.dumps(
                    {
                        "input_data": {
                            **application_context,
                            "evidence": [{"private_input": "preserved"}],
                        },
                        "system_prompt": "must-not-be-retained",
                    }
                ),
                "fingerprint": "c" * 64,
                "created_at": created_at,
            },
        )
        review_request_id = connection.scalar(
            text(
                "INSERT INTO generation_requests "
                "(uid, request_fingerprint, input_fingerprint, task_type, reason, "
                "target_type, target_id, target_uid, provider, model, prompt_version, "
                "schema_version, privacy_status, privacy_reason, created_at) "
                "VALUES (:uid, :request_fingerprint, :input_fingerprint, "
                "'evidence-review', 'explicit-user-request', 'event', 2, :target_uid, "
                "'local', 'migration-model', 'migration-prompt-v1', "
                "'migration-schema-v1', 'local', '', :created_at) RETURNING id"
            ),
            {
                "uid": str(uuid4()),
                "request_fingerprint": "7" * 64,
                "input_fingerprint": "8" * 64,
                "target_uid": str(uuid4()),
                "created_at": created_at,
            },
        )
        assert review_request_id is not None
        connection.execute(
            text(
                "INSERT INTO generation_request_payloads "
                "(request_id, payload_json, payload_fingerprint, created_at) "
                "VALUES (:request_id, CAST(:payload_json AS json), :fingerprint, :created_at)"
            ),
            {
                "request_id": review_request_id,
                "payload_json": json.dumps(
                    {
                        "input_data": {
                            **review_application_context,
                            "target_evidence": [
                                {"private_input": "must-not-be-retained"}
                            ],
                        },
                        "system_prompt": "must-not-be-retained",
                    }
                ),
                "fingerprint": "6" * 64,
                "created_at": created_at,
            },
        )
        attempt_id = connection.scalar(
            text(
                "INSERT INTO generation_attempts "
                "(uid, request_id, attempt_no, retry_kind, status, error, failure_class, "
                "runner_exit_code, runner_events_json, started_at, finished_at, created_at, updated_at) "
                "VALUES (:uid, :request_id, 1, 'initial', 'failed', 'transport failed', "
                "'transport', 7, CAST(:events_json AS json), :created_at, :created_at, "
                ":created_at, :created_at) RETURNING id"
            ),
            {
                "uid": str(uuid4()),
                "request_id": request_id,
                "events_json": json.dumps(events),
                "created_at": created_at,
            },
        )
        assert attempt_id is not None

    command.upgrade(config, "head")

    assert "runner_events_json" not in {
        column["name"] for column in inspect(engine).get_columns("generation_attempts")
    }
    with engine.connect() as connection:
        audit = connection.execute(
            text(
                "SELECT events_json, events_fingerprint, purged_at "
                "FROM generation_attempt_runner_audits WHERE attempt_id = :attempt_id"
            ),
            {"attempt_id": attempt_id},
        ).one()
        assert audit.events_json == events
        assert audit.events_fingerprint == stable_hash(events)
        assert audit.purged_at is None
        assert connection.scalar(
            text(
                "SELECT purged_at FROM generation_request_payloads "
                "WHERE request_id = :request_id"
            ),
            {"request_id": request_id},
        ) is None
        retained_context = connection.scalar(
            text(
                "SELECT application_context_json "
                "FROM generation_request_payloads WHERE request_id = :request_id"
            ),
            {"request_id": request_id},
        )
        assert retained_context == application_context
        assert "private_input" not in json.dumps(retained_context)
        assert "must-not-be-retained" not in json.dumps(retained_context)
        retained_review_context = connection.scalar(
            text(
                "SELECT application_context_json "
                "FROM generation_request_payloads WHERE request_id = :request_id"
            ),
            {"request_id": review_request_id},
        )
        assert retained_review_context == review_application_context
        assert "private_input" not in json.dumps(retained_review_context)
        assert "must-not-be-retained" not in json.dumps(retained_review_context)
    engine.dispose()


def test_generation_retention_uses_strict_boundary_on_real_postgres(
    postgres_url: str,
) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    now = datetime(2026, 7, 18, 12, tzinfo=timezone.utc)
    cutoff = now - timedelta(days=30)
    request_ids: list[int] = []
    attempt_ids: list[int] = []
    result_id: int | None = None

    with session_factory() as session:
        for index, created_at in enumerate(
            (
                cutoff - timedelta(microseconds=1),
                cutoff,
                cutoff + timedelta(microseconds=1),
            ),
            start=1,
        ):
            request = GenerationRequest(
                request_fingerprint=f"{index:064x}",
                input_fingerprint=f"{index + 10:064x}",
                task_type="event-synthesis",
                reason="explicit-user-request",
                target_type="event",
                target_id=index,
                target_uid=f"event-retention-{index}",
                provider="local",
                model="retention-postgres",
                prompt_version="retention-postgres-v1",
                schema_version="retention-postgres-v1",
                created_at=created_at,
            )
            session.add(request)
            session.flush()
            request_ids.append(request.id)
            session.add(
                GenerationRequestPayload(
                    request_id=request.id,
                    payload_json={"private_input": f"payload-{index}"},
                    payload_fingerprint=f"{index + 20:064x}",
                    created_at=created_at,
                )
            )
            attempt = GenerationAttempt(
                request_id=request.id,
                attempt_no=1,
                status="complete" if index == 1 else "failed",
                error="" if index == 1 else "transport failed",
                failure_class=None if index == 1 else "transport",
                runner_exit_code=0 if index == 1 else 1,
                input_tokens_actual=10,
                output_tokens_actual=2,
                started_at=created_at,
                finished_at=created_at,
                created_at=created_at,
                updated_at=created_at,
            )
            session.add(attempt)
            session.flush()
            attempt_ids.append(attempt.id)
            events = [{"type": "turn.started", "private": f"jsonl-{index}"}]
            session.add(
                GenerationAttemptRunnerAudit(
                    attempt_id=attempt.id,
                    events_json=events,
                    events_fingerprint=stable_hash(events),
                    created_at=created_at,
                )
            )
            if index == 1:
                result = GenerationResult(
                    request_id=request.id,
                    attempt_id=attempt.id,
                    payload_json={"blocks": []},
                    payload_fingerprint=f"{index + 30:064x}",
                    output_fingerprint=f"{index + 40:064x}",
                    schema_version="retention-postgres-v1",
                    input_tokens=10,
                    output_tokens=2,
                    created_at=created_at,
                )
                session.add(result)
                session.flush()
                result_id = result.id
                session.add(
                    GenerationApplication(
                        request_id=request.id,
                        result_id=result.id,
                        status="pending",
                        created_at=created_at,
                        updated_at=created_at,
                    )
                )
        session.commit()

    result = run_explicit_maintenance(
        GENERATION_RETENTION,
        session_factory=session_factory,
        prepare_database=lambda: None,
        clock=lambda: now,
        retention_batch_size=100,
    )
    assert result.scanned_count == 2
    assert result.processed_count == 2

    with session_factory() as session:
        old_payload = session.get(GenerationRequestPayload, request_ids[0])
        boundary_payload = session.get(GenerationRequestPayload, request_ids[1])
        old_audit = session.get(GenerationAttemptRunnerAudit, attempt_ids[0])
        boundary_audit = session.get(GenerationAttemptRunnerAudit, attempt_ids[1])
        assert old_payload is not None and old_payload.payload_json is None
        assert old_payload.purged_at == now
        assert old_audit is not None and old_audit.events_json is None
        assert old_audit.purged_at == now
        assert boundary_payload is not None
        assert boundary_payload.payload_json == {"private_input": "payload-2"}
        assert boundary_audit is not None
        assert boundary_audit.events_json == [
            {"type": "turn.started", "private": "jsonl-2"}
        ]
        assert session.scalar(select(func.count()).select_from(GenerationRequest)) == 3
        assert session.scalar(select(func.count()).select_from(GenerationAttempt)) == 3
        assert result_id is not None
        retained_result = session.get(GenerationResult, result_id)
        assert retained_result is not None
        assert retained_result.payload_json == {"blocks": []}
        assert session.scalar(
            select(func.count()).select_from(GenerationApplication)
        ) == 1

    second = run_explicit_maintenance(
        GENERATION_RETENTION,
        session_factory=session_factory,
        prepare_database=lambda: None,
        clock=lambda: now,
    )
    assert second.scanned_count == 0
    assert second.processed_count == 0
    fingerprint_index = next(
        index
        for index in inspect(engine).get_indexes("generation_requests")
        if index["name"] == "ix_generation_request_fingerprint"
    )
    assert fingerprint_index["unique"] is False
    with session_factory() as session:
        first_request = session.get(GenerationRequest, request_ids[0])
        assert first_request is not None
        duplicate_request = GenerationRequest(
            request_fingerprint=first_request.request_fingerprint,
            input_fingerprint=first_request.input_fingerprint,
            task_type=first_request.task_type,
            reason=first_request.reason,
            target_type=first_request.target_type,
            target_id=first_request.target_id,
            target_uid=first_request.target_uid,
            provider=first_request.provider,
            model=first_request.model,
            prompt_version=first_request.prompt_version,
            schema_version=first_request.schema_version,
        )
        session.add(duplicate_request)
        session.flush()
        session.add(
            GenerationRequestPayload(
                request_id=duplicate_request.id,
                payload_json={"private_input": "replacement-payload"},
                payload_fingerprint="f" * 64,
                created_at=now,
            )
        )
        session.commit()
        assert session.scalar(
            select(func.count()).select_from(GenerationRequest)
        ) == 4
    engine.dispose()


def test_api_and_workers_start_against_real_postgres_head(postgres_url: str) -> None:
    upgrade_baseline(postgres_url)

    for target in ("api", "fetch", "llm"):
        result = probe_runtime_startup(postgres_url, target)
        assert result.returncode == 0, result.stdout + result.stderr


def test_api_and_workers_reject_unregistered_real_postgres(
    postgres_url: str,
) -> None:
    assert_runtime_targets_fail(postgres_url, "未登记")


def test_api_and_workers_reject_behind_real_postgres(postgres_url: str) -> None:
    upgrade_baseline(postgres_url)

    assert_runtime_targets_fail(
        postgres_url,
        "0002_future_revision",
        expected_head="0002_future_revision",
    )


def test_api_and_workers_reject_unknown_real_postgres(postgres_url: str) -> None:
    upgrade_baseline(postgres_url)
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE alembic_version SET version_num = 'unknown_revision'")
        )
    engine.dispose()

    assert_runtime_targets_fail(postgres_url, "unknown_revision")


def test_preflight_then_stamp_preserves_existing_legacy_data(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        install_frozen_legacy_schema(connection)
        connection.execute(
            text("INSERT INTO folders (name, created_at) VALUES (:name, CURRENT_TIMESTAMP)"),
            {"name": "迁移前数据"},
        )
        add_frozen_legacy_manifest_seed(connection)

    target = database_target(postgres_url)
    before_manifest = collect_database_manifest(target)
    assert "preserved_table_evidence" not in before_manifest["snapshot"]

    report = preflight_legacy_database(postgres_url)
    assert report.ok is True
    with engine.connect() as connection:
        assert "alembic_version" not in inspect(connection).get_table_names()
        assert connection.scalar(text("SELECT count(*) FROM folders WHERE name = '迁移前数据'")) == 1

    stamp_legacy_database(postgres_url)
    stamp_legacy_database(postgres_url)

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == BASELINE_REVISION
        assert connection.scalar(text("SELECT count(*) FROM folders WHERE name = '迁移前数据'")) == 1

    for migration_command in ("upgrade", "upgrade"):
        result = run_migration_cli(postgres_url, migration_command)
        assert result.returncode == 0, result.stdout + result.stderr

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        assert connection.scalar(text("SELECT count(*) FROM folders WHERE name = '迁移前数据'")) == 1
    after_manifest = collect_database_manifest(target)
    assert after_manifest["snapshot"]["counts"]["user_states"] == 0
    assert after_manifest["snapshot"]["legacy_user_state_evidence"][
        "row_count"
    ] == 1
    assert after_manifest["snapshot"]["legacy_preserved_table_evidence"][
        "clusters"
    ]["row_count"] == 1
    assert compare_database_manifests(before_manifest, after_manifest) == {
        "ok": True,
        "mismatches": [],
    }
    engine.dispose()


def test_cli_upgrades_known_strict_legacy_schema_and_runtime_processes_start(
    postgres_url: str,
) -> None:
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        install_strict_legacy_schema(connection)
        connection.execute(
            text("INSERT INTO folders (name, created_at) VALUES (:name, CURRENT_TIMESTAMP)"),
            {"name": "严格旧库数据"},
        )

    for migration_command in ("preflight", "stamp-legacy"):
        result = run_migration_cli(postgres_url, migration_command)
        assert result.returncode == 0, result.stdout + result.stderr

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0000_strict_legacy"
        )
        snapshot = read_postgres_schema(connection)
        for table, column in STRICT_LEGACY_NULLABILITY_COLUMNS:
            assert snapshot.column_nullability[table][column] is False

    for migration_command in ("upgrade", "upgrade"):
        result = run_migration_cli(postgres_url, migration_command)
        assert result.returncode == 0, result.stdout + result.stderr

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        assert connection.scalar(
            text("SELECT count(*) FROM folders WHERE name = '严格旧库数据'")
        ) == 1
        snapshot = read_postgres_schema(connection)
        for table, column in STRICT_LEGACY_NULLABILITY_COLUMNS:
            assert snapshot.column_nullability[table][column] is True
    engine.dispose()

    for target in ("api", "fetch", "llm"):
        result = probe_runtime_startup(postgres_url, target)
        assert result.returncode == 0, result.stdout + result.stderr


def test_cli_upgrades_known_production_legacy_schema_and_preserves_data(
    postgres_url: str,
) -> None:
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        install_production_legacy_schema(connection)
        connection.execute(
            text("INSERT INTO folders (name, created_at) VALUES (:name, CURRENT_TIMESTAMP)"),
            {"name": "生产旧库数据"},
        )
        connection.execute(
            text(
                """
                INSERT INTO sources (
                    id, name, url, site_url, media_type, status, enabled,
                    fetch_full_content, feed_trust_score, last_error, created_at
                ) VALUES (
                    1, 'Production source', 'https://example.com/production.xml', '',
                    'article', 'active', true, false, 0, '', CURRENT_TIMESTAMP
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO raw_entries (
                    id, source_id, external_id, title, url, author,
                    fetched_at, raw_summary, raw_content, content_hash
                ) VALUES (
                    1, 1, 'production-guid:abcdef123456', 'Production legacy title',
                    'https://example.com/production-entry', 'Production author',
                    CURRENT_TIMESTAMP, '<p>Production summary</p>',
                    '<p>Production body</p>',
                    '2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
                )
                """
            )
        )
        production_raw_before = dict(
            connection.execute(
                text(
                    """
                    SELECT id, source_id, external_id, title, url, author,
                           raw_summary, raw_content, content_hash
                    FROM raw_entries
                    WHERE id = 1
                    """
                )
            ).mappings().one()
        )

    for migration_command in ("preflight", "stamp-legacy"):
        result = run_migration_cli(postgres_url, migration_command)
        assert result.returncode == 0, result.stdout + result.stderr

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            PRODUCTION_LEGACY_REVISION
        )
        snapshot = read_postgres_schema(connection)
        for table, column in PRODUCTION_LEGACY_NOT_NULL_COLUMNS:
            assert snapshot.column_nullability[table][column] is False

    for migration_command in ("upgrade", "upgrade"):
        result = run_migration_cli(postgres_url, migration_command)
        assert result.returncode == 0, result.stdout + result.stderr

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == (
            code_head_revisions()[0]
        )
        assert connection.scalar(
            text("SELECT count(*) FROM folders WHERE name = '生产旧库数据'")
        ) == 1
        production_raw_after = connection.execute(
            text(
                """
                SELECT id, source_id, external_id, title, url, author,
                       raw_summary, raw_content, content_hash,
                       source_entry_id, revision_no, payload_fingerprint
                FROM raw_entries
                WHERE id = 1
                """
            )
        ).mappings().one()
        assert {
            key: production_raw_after[key]
            for key in production_raw_before
        } == production_raw_before
        assert production_raw_after["source_entry_id"] is not None
        assert production_raw_after["revision_no"] == 1
        assert production_raw_after["payload_fingerprint"] == (
            raw_mapping_payload_fingerprint(production_raw_before)
        )
        snapshot = read_postgres_schema(connection)
        assert snapshot.column_nullability["raw_entries"]["payload_fingerprint"] is False
        assert connection.scalar(text("SELECT count(*) FROM source_entry_identities")) == 1
        assert connection.scalar(text("SELECT count(*) FROM source_entry_keys")) == 1
        assert connection.scalar(
            text(
                """
                SELECT count(*)
                FROM source_entry_keys
                WHERE identity_kind = 'legacy'
                  AND identity_key = :identity_key
                """
            ),
            {
                "identity_key": legacy_source_entry_key(
                    production_raw_before["external_id"]
                )
            },
        ) == 1
        snapshot = read_postgres_schema(connection)
        for table, column in STRICT_LEGACY_NULLABILITY_COLUMNS:
            assert snapshot.column_nullability[table][column] is True
    engine.dispose()


@pytest.mark.parametrize("strict_column_count", (1, 2, 3))
def test_cli_rejects_partial_strict_legacy_nullability_without_stamping(
    postgres_url: str,
    strict_column_count: int,
) -> None:
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        install_frozen_legacy_schema(connection)
        for table, column in STRICT_LEGACY_NULLABILITY_COLUMNS[:strict_column_count]:
            connection.exec_driver_sql(
                f'ALTER TABLE "{table}" ALTER COLUMN "{column}" SET NOT NULL'
            )

    preflight_result = run_migration_cli(postgres_url, "preflight")
    stamp_result = run_migration_cli(postgres_url, "stamp-legacy")

    assert preflight_result.returncode != 0, preflight_result.stdout
    assert "legacy schema preflight 失败" in preflight_result.stdout
    assert stamp_result.returncode != 0, stamp_result.stdout + stamp_result.stderr
    assert "legacy schema preflight 失败" in stamp_result.stdout + stamp_result.stderr
    with engine.connect() as connection:
        assert "alembic_version" not in inspect(connection).get_table_names()
        snapshot = read_postgres_schema(connection)
        for index, (table, column) in enumerate(STRICT_LEGACY_NULLABILITY_COLUMNS):
            assert snapshot.column_nullability[table][column] is (
                index >= strict_column_count
            )
    engine.dispose()


def test_failed_preflight_does_not_stamp_or_repair_schema(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        install_frozen_legacy_schema(connection)
        connection.execute(text("CREATE TABLE legacy_audit (id INTEGER)"))
        connection.execute(text("ALTER TABLE sources ADD COLUMN legacy_note TEXT"))
        connection.execute(text("DROP INDEX ix_content_items_embedding_hnsw"))

    report = preflight_legacy_database(postgres_url)

    assert report.ok is False
    assert "存在额外表 legacy_audit" in report.errors
    assert "sources 存在额外列 legacy_note" in report.errors
    assert any("ix_content_items_embedding_hnsw" in error for error in report.errors)
    with engine.connect() as connection:
        assert "alembic_version" not in inspect(connection).get_table_names()
        assert "legacy_audit" in inspect(connection).get_table_names()
        assert "legacy_note" in {
            column["name"] for column in inspect(connection).get_columns("sources")
        }
        assert connection.scalar(
            text("SELECT to_regclass('public.ix_content_items_embedding_hnsw')")
        ) is None
    with pytest.raises(RuntimeError, match="preflight"):
        stamp_legacy_database(postgres_url)
    engine.dispose()


def test_preflight_rejects_missing_legacy_id_autoincrement(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        "ALTER TABLE folders ALTER COLUMN id DROP DEFAULT",
        ("folders.id", "自增"),
    )


def test_preflight_rejects_malformed_alembic_version_table(postgres_url: str) -> None:
    assert_preflight_rejects_alembic_version_drift(
        postgres_url,
        """
        CREATE TABLE alembic_version (
            version_num TEXT NOT NULL PRIMARY KEY,
            unexpected_note TEXT
        )
        """,
        ("alembic_version.version_num", "类型"),
        ("alembic_version", "额外列", "unexpected_note"),
    )


def test_preflight_rejects_alembic_version_foreign_key(postgres_url: str) -> None:
    assert_preflight_rejects_alembic_version_drift(
        postgres_url,
        """
        CREATE TABLE alembic_version (
            version_num VARCHAR(32) NOT NULL PRIMARY KEY,
            CONSTRAINT alembic_version_self_fkey
            FOREIGN KEY (version_num)
            REFERENCES alembic_version(version_num)
        )
        """,
        ("alembic_version", "额外外键"),
    )


def test_preflight_rejects_alembic_version_unique_constraint(
    postgres_url: str,
) -> None:
    assert_preflight_rejects_alembic_version_drift(
        postgres_url,
        (
            """
            CREATE TABLE alembic_version (
                version_num VARCHAR(32) NOT NULL PRIMARY KEY
            )
            """,
            """
            ALTER TABLE alembic_version
            ADD CONSTRAINT alembic_version_extra_key UNIQUE (version_num)
            """,
        ),
        ("alembic_version", "额外唯一约束"),
    )


def test_preflight_rejects_deferred_alembic_version_primary_key(
    postgres_url: str,
) -> None:
    assert_preflight_rejects_alembic_version_drift(
        postgres_url,
        """
        CREATE TABLE alembic_version (
            version_num VARCHAR(32) NOT NULL,
            CONSTRAINT alembic_version_pk PRIMARY KEY (version_num)
            DEFERRABLE INITIALLY DEFERRED
        )
        """,
        ("alembic_version", "主键", "DEFERRED"),
    )


def test_preflight_rejects_alembic_version_check_constraint(
    postgres_url: str,
) -> None:
    assert_preflight_rejects_alembic_version_drift(
        postgres_url,
        """
        CREATE TABLE alembic_version (
            version_num VARCHAR(32) NOT NULL PRIMARY KEY,
            CONSTRAINT alembic_version_future_check
            CHECK (version_num <> 'future') NOT VALID
        )
        """,
        ("alembic_version", "CHECK"),
    )


def test_preflight_reports_alembic_version_without_revision_column(postgres_url: str) -> None:
    with frozen_legacy_schema(postgres_url) as connection:
        connection.execute(text("CREATE TABLE alembic_version (unexpected_note TEXT)"))

    report = preflight_legacy_database(postgres_url)

    assert report.ok is False
    assert "alembic_version.version_num 缺少列" in report.errors
    assert report.current_revision == "<invalid>"


def test_preflight_rejects_invalid_and_unready_required_index(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        (
            "SET LOCAL allow_system_table_mods = on",
            """
            UPDATE pg_catalog.pg_index
            SET indisvalid = false,
                indisready = false
            WHERE indexrelid =
                to_regclass('public.ix_content_items_embedding_hnsw')
            """,
        ),
        ("ix_content_items_embedding_hnsw", "无效"),
        ("ix_content_items_embedding_hnsw", "未就绪"),
    )


def test_preflight_rejects_invalid_and_unready_unique_backing_index(
    postgres_url: str,
) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        (
            "SET LOCAL allow_system_table_mods = on",
            """
            UPDATE pg_catalog.pg_index
            SET indisvalid = false,
                indisready = false
            WHERE indexrelid = to_regclass('public.folders_name_key')
            """,
        ),
        ("folders", "IndexState", "is_valid=False", "is_ready=False"),
    )


def test_preflight_rejects_invalid_and_unready_primary_key_backing_index(
    postgres_url: str,
) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        (
            "SET LOCAL allow_system_table_mods = on",
            """
            UPDATE pg_catalog.pg_index
            SET indisvalid = false,
                indisready = false
            WHERE indexrelid = to_regclass('public.llm_tasks_pkey')
            """,
        ),
        (
            "llm_tasks",
            "主键",
            "IndexState",
            "is_valid=False",
            "is_ready=False",
        ),
    )


def test_preflight_rejects_unexpected_foreign_key(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        ALTER TABLE llm_tasks
        ADD CONSTRAINT llm_tasks_object_id_extra_fkey
        FOREIGN KEY (object_id) REFERENCES folders(id)
        """,
        ("llm_tasks", "额外外键"),
    )


def test_preflight_rejects_duplicate_equivalent_unique_constraint(
    postgres_url: str,
) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        ALTER TABLE folders
        ADD CONSTRAINT folders_name_duplicate_key UNIQUE (name)
        """,
        ("folders", "额外唯一约束"),
    )


def test_preflight_rejects_deferred_unique_constraint(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        ALTER TABLE folders
        DROP CONSTRAINT folders_name_key,
        ADD CONSTRAINT folders_name_key UNIQUE (name)
        DEFERRABLE INITIALLY DEFERRED
        """,
        ("folders", "DEFERRED"),
    )


def test_preflight_rejects_deferred_managed_primary_key(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        ALTER TABLE llm_tasks
        DROP CONSTRAINT llm_tasks_pkey,
        ADD CONSTRAINT llm_tasks_pkey PRIMARY KEY (id)
        DEFERRABLE INITIALLY DEFERRED
        """,
        ("llm_tasks", "主键", "DEFERRED"),
    )


def test_preflight_rejects_unexpected_check_constraint(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        ALTER TABLE llm_tasks
        ADD CONSTRAINT llm_tasks_object_id_nonnegative
        CHECK (object_id >= 0) NOT VALID
        """,
        ("llm_tasks", "CHECK"),
    )


def test_preflight_rejects_unexpected_exclusion_constraint(
    postgres_url: str,
) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        ALTER TABLE llm_tasks
        ADD CONSTRAINT llm_tasks_id_exclude
        EXCLUDE USING btree (id WITH =)
        """,
        ("llm_tasks", "EXCLUDE"),
    )


def test_preflight_rejects_standalone_unique_index(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        CREATE UNIQUE INDEX llm_tasks_object_id_unique_idx
        ON llm_tasks (object_id)
        """,
        ("llm_tasks", "独立 UNIQUE 索引", "llm_tasks_object_id_unique_idx"),
    )


def test_preflight_rejects_duplicate_equivalent_foreign_key(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        ALTER TABLE documents
        ADD CONSTRAINT documents_raw_entry_id_duplicate_fkey
        FOREIGN KEY (raw_entry_id) REFERENCES raw_entries(id)
        """,
        ("documents", "额外外键"),
    )


def test_preflight_rejects_destructive_foreign_key_action(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        ALTER TABLE documents
        DROP CONSTRAINT documents_raw_entry_id_fkey,
        ADD CONSTRAINT documents_raw_entry_id_fkey
        FOREIGN KEY (raw_entry_id) REFERENCES raw_entries(id)
        ON DELETE CASCADE
        """,
        ("documents", "CASCADE"),
    )


def test_preflight_rejects_foreign_key_update_action_drift(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        ALTER TABLE documents
        DROP CONSTRAINT documents_raw_entry_id_fkey,
        ADD CONSTRAINT documents_raw_entry_id_fkey
        FOREIGN KEY (raw_entry_id) REFERENCES raw_entries(id)
        ON UPDATE CASCADE
        """,
        ("documents", "CASCADE"),
    )


def test_preflight_rejects_deferred_foreign_key_drift(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        ALTER TABLE documents
        DROP CONSTRAINT documents_raw_entry_id_fkey,
        ADD CONSTRAINT documents_raw_entry_id_fkey
        FOREIGN KEY (raw_entry_id) REFERENCES raw_entries(id)
        DEFERRABLE INITIALLY DEFERRED
        """,
        ("documents", "DEFERRED"),
    )


def test_preflight_rejects_unvalidated_foreign_key(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        ALTER TABLE documents
        DROP CONSTRAINT documents_raw_entry_id_fkey,
        ADD CONSTRAINT documents_raw_entry_id_fkey
        FOREIGN KEY (raw_entry_id) REFERENCES raw_entries(id)
        NOT VALID
        """,
        ("documents", "is_validated=False"),
    )


def test_preflight_rejects_foreign_key_match_type_drift(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        """
        ALTER TABLE documents
        DROP CONSTRAINT documents_raw_entry_id_fkey,
        ADD CONSTRAINT documents_raw_entry_id_fkey
        FOREIGN KEY (raw_entry_id) REFERENCES raw_entries(id)
        MATCH FULL
        """,
        ("documents", "FULL"),
    )


def test_preflight_rejects_cross_schema_foreign_key_target(postgres_url: str) -> None:
    assert_preflight_rejects_schema_drift(
        postgres_url,
        (
            "CREATE SCHEMA foreign_key_shadow",
            "CREATE TABLE foreign_key_shadow.raw_entries (id INTEGER PRIMARY KEY)",
            """
            ALTER TABLE documents
            DROP CONSTRAINT documents_raw_entry_id_fkey,
            ADD CONSTRAINT documents_raw_entry_id_fkey
            FOREIGN KEY (raw_entry_id)
            REFERENCES foreign_key_shadow.raw_entries(id)
            """,
        ),
        ("documents", "referred_schema_is_current=False"),
    )


def test_report_generation_lifecycle_on_real_postgres(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from reader_api import main as main_module

    upgrade_baseline(postgres_url)
    postgres_engine = create_engine(postgres_url)
    SessionLocal = sessionmaker(bind=postgres_engine, expire_on_commit=False)
    published_at = datetime(2026, 7, 16, 8, tzinfo=timezone.utc)
    vector = [1.0, *([0.0] * 2559)]
    vector_text = "[" + ",".join(str(value) for value in vector) + "]"
    with SessionLocal() as session:
        source = Source(
            name="Postgres report source",
            url="https://example.com/postgres-report.xml",
            status="active",
            media_type="article",
            privacy_class="public",
            external_generation_allowed=True,
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source=source,
            external_id="postgres-report-entry",
            title="Postgres report event",
            url="https://example.com/postgres-report/event",
            raw_content="Postgres report evidence",
            published_at=published_at,
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            document_type="normal_article",
            title=raw.title,
            content_text=raw.raw_content,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_text=raw.raw_content,
            url=raw.url,
            canonical_url=raw.url,
            published_at=published_at,
            content_hash=raw.content_hash,
            embedding_vector=vector_text,
            embedding_model="postgres-report-model",
        )
        session.add(item)
        session.flush()
        with clustering_run(
            session,
            scope_type="report-generation-postgres",
            item_ids=[item.id],
            rule_version="report-generation-postgres-v1",
        ):
            cluster = assign_runtime_cluster(
                session,
                item,
                target_vector=vector,
            )
        control = session.get(GenerationControl, 1)
        assert control is not None
        control.global_pause = False
        control.daily_budget_tokens = 10_000_000
        legacy_start, _legacy_end = report_bounds("day", "2026-07-15")
        session.add(
            LLMTask(
                task_type="report:day",
                provider="local-chat",
                object_type="report",
                object_id=report_key(legacy_start),
                status="complete",
                prompt_version="report-v1",
                model_version="legacy-model",
                result_json=json.dumps(
                    {
                        "title": "Legacy PostgreSQL report",
                        "body": "Legacy body [1]",
                        "cluster_ids": [cluster.id],
                        "citations": [],
                    }
                ),
            )
        )
        session.commit()
        cluster_id = cluster.id

    valid_calls = 0

    class SequencedProvider:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def chat(self, *_args, **_kwargs) -> dict[str, object]:
            nonlocal valid_calls
            valid_calls += 1
            text_value = (
                '{"title":"PostgreSQL report","body":"Verified body [1]"}'
                if valid_calls <= 3
                else '{"title":"Invalid report","body":"Missing citation"}'
            )
            return {
                "text": text_value,
                "usage": {"input_tokens": 80, "output_tokens": 16},
            }

    monkeypatch.setattr(main_module.settings, "llm_task_provider", "local")
    monkeypatch.setattr(main_module, "LocalChatProvider", SequencedProvider)

    def postgres_session() -> Iterator[Session]:
        with SessionLocal() as session:
            yield session

    app.dependency_overrides[get_session] = postgres_session
    client = TestClient(app)
    try:
        for period in ("day", "week", "month"):
            generated = client.post(
                "/reports/generate",
                params={"period": period, "date": "2026-07-16"},
            )
            assert generated.status_code == 200, generated.text
            assert generated.json()["status"] == "ready"
            assert generated.json()["items"] == [cluster_id]

        tasks = client.get("/generation/tasks")
        assert tasks.status_code == 200, tasks.text
        assert {task["task_type"] for task in tasks.json()} == {
            "report:day",
            "report:week",
            "report:month",
        }
        assert {task["status"] for task in tasks.json()} == {"complete"}
        legacy = client.get(
            "/reports", params={"period": "day", "date": "2026-07-15"}
        )
        assert legacy.status_code == 200
        assert legacy.json()["title"] == "Legacy PostgreSQL report"

        with SessionLocal() as session:
            source = session.scalar(
                select(Source).where(
                    Source.url == "https://example.com/postgres-report.xml"
                )
            )
            assert source is not None
            source.name = "Changed PostgreSQL report input"
            session.commit()
        rejected = client.post(
            "/reports/generate",
            params={"period": "day", "date": "2026-07-16"},
        )
        assert rejected.status_code == 502, rejected.text
        assert rejected.json()["detail"] == "报告正文缺少来源引用"
    finally:
        app.dependency_overrides.pop(get_session, None)

    with SessionLocal() as session:
        assert (
            session.query(LLMTask)
            .filter(LLMTask.task_type.like("report:%"))
            .count()
            == 1
        )
        assert session.query(GenerationRequest).count() == 4
        assert session.query(GenerationAttempt).count() == 4
        assert session.query(GenerationResult).count() == 3
        assert session.query(GenerationApplication).count() == 3
        assert session.query(InteractionEvent).count() == 0
    postgres_engine.dispose()
