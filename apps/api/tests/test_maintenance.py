from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from reader_api.db import Base
from reader_api.maintenance import (
    DUPLICATE_FEED_ENTRIES,
    DISABLED_OPERATIONS,
    DISABLED_OPERATION_MESSAGES,
    EMBEDDING_CLUSTERS,
    EXACT_CONTENT_DUPLICATES,
    GENERATION_RETENTION,
    LIST_DUPLICATE_FEED_RELATIONS,
    OVER_SPLIT_DOCUMENTS,
    TITLE_ONLY_CLUSTERS,
    USER_STATE_REPAIR_DISABLED_MESSAGE,
    WINDOWED_CLUSTERS,
    MaintenanceFinalizationError,
    MaintenanceResult,
    REVOKE_DUPLICATE_FEED_RELATION,
    REBUILD_PROJECTIONS,
    main,
    query_duplicate_feed_relations,
    revoke_duplicate_feed_relation_by_id,
    run_explicit_maintenance,
    run_scheduled_generation_retention,
)
from reader_api.models import (
    GenerationAttempt,
    GenerationAttemptRunnerAudit,
    GenerationRequest,
    GenerationRequestPayload,
    MaintenanceRun,
    Source,
    SourceEntryIdentity,
    SourceEntryRelation,
)
from reader_api.generation_lifecycle import stable_hash


def maintenance_session_factory() -> sessionmaker[Session]:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


class FinalCommitFailureSession(Session):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.commit_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1
        if self.commit_calls == 2:
            raise RuntimeError("audit storage unavailable")
        super().commit()


@pytest.mark.parametrize("operation", DISABLED_OPERATIONS)
def test_all_historical_repairs_are_disabled_and_persist_accurate_audit(
    operation: str,
) -> None:
    session_factory = maintenance_session_factory()
    prepare_calls: list[str] = []
    with session_factory() as session:
        session.add(Source(name="preserved", url="https://example.com/preserved"))
        session.commit()

    result = run_explicit_maintenance(
        operation,
        session_factory=session_factory,
        prepare_database=lambda: prepare_calls.append("gate"),
    )

    assert prepare_calls == ["gate"]
    assert result.operation_type == operation
    assert result.start_status == "started"
    assert result.end_status == "failed"
    assert result.processed_count == 0
    assert result.failure_info == DISABLED_OPERATION_MESSAGES[operation]
    assert result.finished_at is not None
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Source)) == 1
        record = session.get(MaintenanceRun, result.run_id)
        assert record is not None
        assert record.operation_type == operation
        assert record.start_status == "started"
        assert record.end_status == "failed"
        assert record.processed_count == 0
        assert record.failure_info == DISABLED_OPERATION_MESSAGES[operation]
        assert record.started_at is not None
        assert record.finished_at is not None


@pytest.mark.parametrize(
    "operation",
    (
        OVER_SPLIT_DOCUMENTS,
        TITLE_ONLY_CLUSTERS,
        WINDOWED_CLUSTERS,
        EMBEDDING_CLUSTERS,
    ),
)
def test_unsafe_user_state_repair_cli_returns_disabled_exit_code(
    operation: str,
    monkeypatch,
    capsys,
) -> None:
    now = datetime.now(timezone.utc)
    result = MaintenanceResult(
        run_id=12,
        operation_type=operation,
        start_status="started",
        end_status="failed",
        processed_count=0,
        failure_info=USER_STATE_REPAIR_DISABLED_MESSAGE,
        started_at=now,
        finished_at=now,
    )
    monkeypatch.setattr(
        "reader_api.maintenance.run_explicit_maintenance",
        lambda _: result,
    )

    assert main([operation]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["run_id"] == 12
    assert payload["operation_type"] == operation
    assert payload["start_status"] == "started"
    assert payload["end_status"] == "failed"
    assert payload["processed_count"] == 0
    assert "User State" in payload["failure_info"]


def test_duplicate_relation_maintenance_succeeds_and_reports_processed_count(
    monkeypatch,
    capsys,
) -> None:
    now = datetime.now(timezone.utc)
    result = MaintenanceResult(
        run_id=13,
        operation_type=DUPLICATE_FEED_ENTRIES,
        start_status="started",
        end_status="succeeded",
        processed_count=2,
        failure_info="",
        started_at=now,
        finished_at=now,
    )
    monkeypatch.setattr(
        "reader_api.maintenance.run_explicit_maintenance",
        lambda _: result,
    )

    assert main([DUPLICATE_FEED_ENTRIES]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["end_status"] == "succeeded"
    assert payload["processed_count"] == 2
    assert payload["failure_info"] == ""


def test_duplicate_relation_maintenance_persists_success_audit(monkeypatch) -> None:
    session_factory = maintenance_session_factory()
    monkeypatch.setattr(
        "reader_api.maintenance.record_duplicate_feed_relations",
        lambda _session: 3,
    )

    result = run_explicit_maintenance(
        DUPLICATE_FEED_ENTRIES,
        session_factory=session_factory,
        prepare_database=lambda: None,
    )

    assert result.end_status == "succeeded"
    assert result.processed_count == 3
    assert result.failure_info == ""
    with session_factory() as session:
        record = session.get(MaintenanceRun, result.run_id)
        assert record is not None
        assert record.end_status == "succeeded"
        assert record.processed_count == 3
        assert record.finished_at is not None


def test_exact_content_duplicate_maintenance_persists_success_audit(monkeypatch) -> None:
    session_factory = maintenance_session_factory()
    monkeypatch.setattr(
        "reader_api.maintenance.repair_exact_content_duplicates",
        lambda _session: 4,
    )

    result = run_explicit_maintenance(
        EXACT_CONTENT_DUPLICATES,
        session_factory=session_factory,
        prepare_database=lambda: None,
    )

    assert result.end_status == "succeeded"
    assert result.processed_count == 4


def test_projection_rebuild_maintenance_publishes_and_persists_success_audit() -> None:
    session_factory = maintenance_session_factory()

    result = run_explicit_maintenance(
        REBUILD_PROJECTIONS,
        session_factory=session_factory,
        prepare_database=lambda: None,
    )

    assert result.end_status == "succeeded"
    assert result.processed_count == 0
    with session_factory() as session:
        record = session.get(MaintenanceRun, result.run_id)
        assert record is not None
        assert record.operation_type == REBUILD_PROJECTIONS
        assert record.end_status == "succeeded"


def test_projection_rebuild_cli_returns_failure_exit_code(monkeypatch, capsys) -> None:
    now = datetime.now(timezone.utc)
    result = MaintenanceResult(
        run_id=32,
        operation_type=REBUILD_PROJECTIONS,
        start_status="started",
        end_status="failed",
        processed_count=0,
        failure_info="projection publication failed",
        started_at=now,
        finished_at=now,
    )
    monkeypatch.setattr(
        "reader_api.maintenance.run_explicit_maintenance",
        lambda _: result,
    )

    assert main([REBUILD_PROJECTIONS]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["operation_type"] == REBUILD_PROJECTIONS
    assert payload["end_status"] == "failed"
    assert payload["failure_info"] == "projection publication failed"


def test_projection_rebuild_verify_and_dry_run_cli_only_report(
    monkeypatch,
    capsys,
) -> None:
    calls: list[str] = []

    def report(*, mode: str) -> dict[str, object]:
        calls.append(mode)
        return {
            "mode": mode,
            "matches": False,
            "differences": {"event_user_states": {"changed": 1}},
        }

    monkeypatch.setattr(
        "reader_api.maintenance.inspect_projection_rebuild_database",
        report,
    )

    assert main([REBUILD_PROJECTIONS, "--verify"]) == 1
    assert json.loads(capsys.readouterr().out)["mode"] == "verify"
    assert main([REBUILD_PROJECTIONS, "--dry-run"]) == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "dry-run"
    assert calls == ["verify", "dry-run"]


def test_duplicate_relation_maintenance_rolls_back_and_audits_failure(
    monkeypatch,
) -> None:
    session_factory = maintenance_session_factory()

    def fail_relation_detection(_session: Session) -> int:
        raise RuntimeError("relation detection failed")

    monkeypatch.setattr(
        "reader_api.maintenance.record_duplicate_feed_relations",
        fail_relation_detection,
    )

    result = run_explicit_maintenance(
        DUPLICATE_FEED_ENTRIES,
        session_factory=session_factory,
        prepare_database=lambda: None,
    )

    assert result.end_status == "failed"
    assert result.processed_count == 0
    assert result.failure_info == "relation detection failed"
    with session_factory() as session:
        record = session.get(MaintenanceRun, result.run_id)
        assert record is not None
        assert record.end_status == "failed"
        assert record.failure_info == "relation detection failed"


def test_duplicate_relation_query_and_revoke_are_available_from_maintenance() -> None:
    session_factory = maintenance_session_factory()
    with session_factory() as session:
        source = Source(name="Relation CLI", url="https://example.com/relation-cli")
        session.add(source)
        session.flush()
        duplicate = SourceEntryIdentity(source_id=source.id)
        canonical = SourceEntryIdentity(source_id=source.id)
        session.add_all([duplicate, canonical])
        session.flush()
        relation = SourceEntryRelation(
            source_entry_id=duplicate.id,
            canonical_source_entry_id=canonical.id,
            relation_type="duplicate",
            reason="test relation",
            rule_version="test-v1",
            active=True,
        )
        session.add(relation)
        session.commit()
        relation_id = relation.id

    rows = query_duplicate_feed_relations(
        session_factory=session_factory,
        prepare_database=lambda: None,
    )
    assert [row["id"] for row in rows] == [relation_id]
    assert rows[0]["active"] is True

    revoked = revoke_duplicate_feed_relation_by_id(
        relation_id,
        session_factory=session_factory,
        prepare_database=lambda: None,
    )
    assert revoked["id"] == relation_id
    assert revoked["active"] is False
    assert revoked["revoked_at"] is not None
    assert query_duplicate_feed_relations(
        session_factory=session_factory,
        prepare_database=lambda: None,
    ) == []
    assert [
        row["id"]
        for row in query_duplicate_feed_relations(
            include_revoked=True,
            session_factory=session_factory,
            prepare_database=lambda: None,
        )
    ] == [relation_id]


def test_relation_query_and_revoke_cli_emit_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "reader_api.maintenance.query_duplicate_feed_relations",
        lambda **_kwargs: [{"id": 41, "active": True}],
    )
    assert main([LIST_DUPLICATE_FEED_RELATIONS]) == 0
    assert json.loads(capsys.readouterr().out) == [{"id": 41, "active": True}]

    monkeypatch.setattr(
        "reader_api.maintenance.revoke_duplicate_feed_relation_by_id",
        lambda relation_id, **_kwargs: {"id": relation_id, "active": False},
    )
    assert main([REVOKE_DUPLICATE_FEED_RELATION, "41"]) == 0
    assert json.loads(capsys.readouterr().out) == {"id": 41, "active": False}


def test_finalization_failure_preserves_started_audit_identity() -> None:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(
        bind=engine,
        class_=FinalCommitFailureSession,
        expire_on_commit=False,
    )

    with pytest.raises(MaintenanceFinalizationError) as captured:
        run_explicit_maintenance(
            OVER_SPLIT_DOCUMENTS,
            session_factory=session_factory,
            prepare_database=lambda: None,
        )

    error = captured.value
    assert error.run_id > 0
    assert error.operation_type == OVER_SPLIT_DOCUMENTS
    assert str(error) == "audit storage unavailable"
    with session_factory() as session:
        record = session.get(MaintenanceRun, error.run_id)
        assert record is not None
        assert record.start_status == "started"
        assert record.end_status == ""
        assert record.processed_count == 0
        assert record.failure_info == ""
        assert record.started_at.replace(tzinfo=timezone.utc) == error.started_at


def test_cli_reports_persisted_started_record_when_finalization_is_unconfirmed(
    monkeypatch,
    capsys,
) -> None:
    started_at = datetime.now(timezone.utc)

    def fail_after_start(_: str) -> MaintenanceResult:
        raise MaintenanceFinalizationError(
            run_id=23,
            operation_type=OVER_SPLIT_DOCUMENTS,
            started_at=started_at,
            cause=RuntimeError("audit storage unavailable"),
        )

    monkeypatch.setattr(
        "reader_api.maintenance.run_explicit_maintenance",
        fail_after_start,
    )

    assert main([OVER_SPLIT_DOCUMENTS]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "run_id": 23,
        "operation_type": OVER_SPLIT_DOCUMENTS,
        "start_status": "started",
        "end_status": "unconfirmed",
        "scanned_count": 0,
        "processed_count": 0,
        "failure_info": "结束审计未确认：audit storage unavailable",
        "started_at": started_at.isoformat(),
        "finished_at": None,
    }


def test_generation_retention_strict_30_day_boundary_is_bounded_and_idempotent() -> None:
    session_factory = maintenance_session_factory()
    now = datetime(2026, 7, 18, 12, tzinfo=timezone.utc)
    cutoff = now - timedelta(days=30)

    with session_factory() as session:
        for index, created_at in enumerate(
            (cutoff - timedelta(microseconds=1), cutoff, cutoff + timedelta(microseconds=1)),
            start=1,
        ):
            request = GenerationRequest(
                request_fingerprint=f"{index:064x}",
                input_fingerprint=f"{index + 10:064x}",
                task_type="event-synthesis",
                reason="explicit-user-request",
                target_type="event",
                target_id=index,
                target_uid=f"event-{index}",
                provider="local",
                model="retention-test",
                prompt_version="retention-test-v1",
                schema_version="retention-test-v1",
                created_at=created_at,
            )
            session.add(request)
            session.flush()
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
                status="failed",
                error="transport failed",
                failure_class="transport",
                started_at=created_at,
                finished_at=created_at,
                created_at=created_at,
            )
            session.add(attempt)
            session.flush()
            events = [{"type": "turn.started", "private": f"jsonl-{index}"}]
            session.add(
                GenerationAttemptRunnerAudit(
                    attempt_id=attempt.id,
                    events_json=events,
                    events_fingerprint=stable_hash(events),
                    created_at=created_at,
                )
            )
        session.commit()

    result = run_explicit_maintenance(
        GENERATION_RETENTION,
        session_factory=session_factory,
        prepare_database=lambda: None,
        clock=lambda: now,
        retention_batch_size=2,
    )

    assert result.end_status == "succeeded"
    assert result.scanned_count == 2
    assert result.processed_count == 2
    with session_factory() as session:
        payloads = session.scalars(
            select(GenerationRequestPayload).order_by(GenerationRequestPayload.request_id)
        ).all()
        audits = session.scalars(
            select(GenerationAttemptRunnerAudit).order_by(
                GenerationAttemptRunnerAudit.attempt_id
            )
        ).all()
        assert payloads[0].payload_json is None
        assert payloads[0].purged_at.replace(tzinfo=timezone.utc) == now
        assert audits[0].events_json is None
        assert audits[0].purged_at.replace(tzinfo=timezone.utc) == now
        assert payloads[1].payload_json == {"private_input": "payload-2"}
        assert audits[1].events_json == [
            {"type": "turn.started", "private": "jsonl-2"}
        ]
        assert session.scalar(select(func.count()).select_from(GenerationRequest)) == 3
        assert session.scalar(select(func.count()).select_from(GenerationAttempt)) == 3

    second = run_explicit_maintenance(
        GENERATION_RETENTION,
        session_factory=session_factory,
        prepare_database=lambda: None,
        clock=lambda: now,
        retention_batch_size=10,
    )
    assert second.scanned_count == 0
    assert second.processed_count == 0


def test_scheduled_generation_retention_retries_failures_and_runs_once_per_day(
    monkeypatch,
) -> None:
    session_factory = maintenance_session_factory()
    now = datetime(2026, 7, 18, 12, tzinfo=timezone.utc)
    calls = 0

    def fail_once(*_args, **_kwargs) -> tuple[int, int]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("retention storage unavailable")
        return 0, 0

    monkeypatch.setattr(
        "reader_api.maintenance.purge_expired_generation_data",
        fail_once,
    )
    first = run_scheduled_generation_retention(
        session_factory=session_factory,
        prepare_database=lambda: None,
        clock=lambda: now,
    )
    second = run_scheduled_generation_retention(
        session_factory=session_factory,
        prepare_database=lambda: None,
        clock=lambda: now,
    )
    third = run_scheduled_generation_retention(
        session_factory=session_factory,
        prepare_database=lambda: None,
        clock=lambda: now,
    )

    assert first is not None and first.end_status == "failed"
    assert second is not None and second.end_status == "succeeded"
    assert third is None
    assert calls == 2
