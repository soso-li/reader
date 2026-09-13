from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

import reader_api.bulk_read as bulk_read_module
from reader_api.db import Base, engine
from reader_api.event_interactions import operation_lock_key
from reader_api.main import app
from reader_api.models import (
    Event,
    EventUserState,
    FeedMetric,
    InteractionEvent,
    now_utc,
)
from tests.test_event_interactions import create_event_fixture


def reset_database() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def batch_mark(fixture: dict[str, object], operation_id: str) -> dict[str, str]:
    return {
        "event_uid": str(fixture["event_uid"]),
        "observed_revision_uid": str(fixture["revision_uid"]),
        "operation_id": operation_id,
    }


def test_batch_marks_apply_summary_seen_per_target() -> None:
    reset_database()
    fixtures = [
        create_event_fixture(suffix=f"batch-happy-{index}") for index in range(3)
    ]
    operations = [str(uuid4()) for _ in fixtures]
    response = TestClient(app).post(
        "/event-user-state/batch",
        json={
            "marks": [
                batch_mark(fixture, operation)
                for fixture, operation in zip(fixtures, operations)
            ]
        },
    )

    assert response.status_code == 200
    results = response.json()["results"]
    assert [entry["operation_id"] for entry in results] == operations
    for fixture, entry in zip(fixtures, results):
        assert "error" not in entry
        assert entry["result"]["event_uid"] == fixture["event_uid"]
        assert entry["result"]["value"] == "summary_seen"
        assert entry["result"]["read_status"] == "summary_seen"
        assert entry["result"]["seen_revision_uid"] == fixture["revision_uid"]
    with sessionmaker(bind=engine)() as session:
        states = session.scalars(select(EventUserState)).all()
        assert {state.read_status for state in states} == {"summary_seen"}
        assert len(states) == len(fixtures)
        interactions = session.scalars(select(InteractionEvent)).all()
        assert len(interactions) == len(fixtures)
        assert {interaction.set_value for interaction in interactions} == {
            "summary_seen"
        }
        for interaction in interactions:
            assert isinstance(interaction.payload.get("result"), dict)
        for fixture in fixtures:
            metric = session.scalar(
                select(FeedMetric).where(
                    FeedMetric.source_id == fixture["source_id"]
                )
            )
            assert metric is not None
            assert metric.read_count == 1


def test_batch_replays_identical_body_without_new_writes() -> None:
    reset_database()
    fixture = create_event_fixture(suffix="batch-replay")
    payload = {"marks": [batch_mark(fixture, str(uuid4()))]}
    client = TestClient(app)

    first = client.post("/event-user-state/batch", json=payload)
    second = client.post("/event-user-state/batch", json=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json() == first.json()
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 1
        metric = session.scalar(
            select(FeedMetric).where(FeedMetric.source_id == fixture["source_id"])
        )
        assert metric is not None
        assert metric.read_count == 1


def test_batch_mixes_replayed_and_fresh_targets() -> None:
    reset_database()
    first = create_event_fixture(suffix="batch-mixed-a")
    second = create_event_fixture(suffix="batch-mixed-b")
    first_operation = str(uuid4())
    second_operation = str(uuid4())
    client = TestClient(app)

    initial = client.post(
        "/event-user-state/batch",
        json={"marks": [batch_mark(first, first_operation)]},
    )
    mixed = client.post(
        "/event-user-state/batch",
        json={
            "marks": [
                batch_mark(first, first_operation),
                batch_mark(second, second_operation),
            ]
        },
    )

    assert mixed.status_code == 200
    results = mixed.json()["results"]
    assert results[0] == initial.json()["results"][0]
    assert results[1]["result"]["event_uid"] == second["event_uid"]
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 2


def test_batch_shape_mismatch_fails_one_target_and_applies_the_rest() -> None:
    reset_database()
    starred = create_event_fixture(suffix="batch-shape-starred")
    fresh = create_event_fixture(suffix="batch-shape-fresh")
    reused_operation = str(uuid4())
    client = TestClient(app)
    single = client.post(
        "/event-user-state",
        json={
            "event_uid": starred["event_uid"],
            "observed_revision_uid": starred["revision_uid"],
            "operation_id": reused_operation,
            "action": "starred_set",
            "value": True,
        },
    )
    assert single.status_code == 200

    response = client.post(
        "/event-user-state/batch",
        json={
            "marks": [
                batch_mark(starred, reused_operation),
                batch_mark(fresh, str(uuid4())),
            ]
        },
    )

    assert response.status_code == 200
    results = response.json()["results"]
    assert results[0]["error"] == "operation_id 已用于另一项操作"
    assert "result" not in results[0]
    assert results[1]["result"]["read_status"] == "summary_seen"
    with sessionmaker(bind=engine)() as session:
        fresh_state = session.scalar(
            select(EventUserState).where(
                EventUserState.event_id == fresh["event_id"]
            )
        )
        assert fresh_state is not None
        assert fresh_state.read_status == "summary_seen"
        starred_state = session.scalar(
            select(EventUserState).where(
                EventUserState.event_id == starred["event_id"]
            )
        )
        assert starred_state is not None
        assert starred_state.read_status == "unread"


def test_batch_locks_each_operation_in_canonical_order_and_updates_metrics_once(
    monkeypatch,
) -> None:
    reset_database()
    fixtures = [
        create_event_fixture(suffix=f"batch-lock-{index}") for index in range(3)
    ]
    operations = [str(uuid4()) for _ in fixtures]
    locked: list[str] = []
    metric_calls: list[int] = []
    original_apply = bulk_read_module.apply_metric_deltas

    def record_lock(session, operation_id: str) -> None:
        # SQLite 下 advisory 锁本身是 no-op，这里只断言调用序。
        locked.append(operation_id)

    def record_apply(session, metric_deltas, locked_metrics) -> None:
        metric_calls.append(len(metric_deltas))
        original_apply(session, metric_deltas, locked_metrics)

    monkeypatch.setattr(bulk_read_module, "lock_operation_id", record_lock)
    monkeypatch.setattr(bulk_read_module, "apply_metric_deltas", record_apply)
    response = TestClient(app).post(
        "/event-user-state/batch",
        json={
            "marks": [
                batch_mark(fixture, operation)
                for fixture, operation in zip(fixtures, operations)
            ]
        },
    )

    assert response.status_code == 200
    assert locked == sorted(operations, key=operation_lock_key)
    assert metric_calls == [3]


def test_batch_reports_dead_targets_without_failing_siblings() -> None:
    reset_database()
    live = create_event_fixture(suffix="batch-dead-live")
    superseded = create_event_fixture(suffix="batch-dead-superseded")
    deleted_source = create_event_fixture(suffix="batch-dead-source")
    with sessionmaker(bind=engine)() as session:
        event = session.get(Event, superseded["event_id"])
        assert event is not None
        event.status = "superseded"
        event.superseded_at = now_utc()
        session.commit()
    client = TestClient(app)
    assert (
        client.delete(f"/sources/{deleted_source['source_id']}").status_code == 204
    )

    response = client.post(
        "/event-user-state/batch",
        json={
            "marks": [
                batch_mark(live, str(uuid4())),
                batch_mark(superseded, str(uuid4())),
                batch_mark(deleted_source, str(uuid4())),
                {
                    "event_uid": "missing-event-uid",
                    "observed_revision_uid": str(live["revision_uid"]),
                    "operation_id": str(uuid4()),
                },
            ]
        },
    )

    assert response.status_code == 200
    results = response.json()["results"]
    assert results[0]["result"]["read_status"] == "summary_seen"
    assert results[1]["error"] == "Event 已被后继事件取代，请刷新"
    assert results[2]["error"] == "Event 来源已失效，请刷新"
    assert results[3]["error"] == "Event 不存在"
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 1


def test_batch_rejects_oversized_and_duplicate_payloads() -> None:
    reset_database()
    fixture = create_event_fixture(suffix="batch-cap")
    oversized = TestClient(app).post(
        "/event-user-state/batch",
        json={
            "marks": [batch_mark(fixture, str(uuid4())) for _ in range(51)]
        },
    )
    duplicate_operation = str(uuid4())
    duplicated = TestClient(app).post(
        "/event-user-state/batch",
        json={
            "marks": [
                batch_mark(fixture, duplicate_operation),
                batch_mark(fixture, duplicate_operation),
            ]
        },
    )

    assert oversized.status_code == 422
    assert duplicated.status_code == 422
