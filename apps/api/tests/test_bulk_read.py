from __future__ import annotations

import json

from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from reader_api.bulk_read import bulk_read_batch_key
from reader_api.db import Base, engine
from reader_api.main import app
from reader_api.models import (
    EventUserState,
    FeedMetric,
    InteractionEvent,
    UserState,
)
from tests.test_event_interactions import (
    append_current_revision,
    create_event_fixture,
)


def reset_database() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def stored_manifest(bulk_read_redis, batch_id: str) -> dict[str, object]:
    raw = bulk_read_redis.get(bulk_read_batch_key(batch_id))
    assert raw is not None
    return json.loads(raw)


def replace_manifest(
    bulk_read_redis,
    batch_id: str,
    manifest: dict[str, object],
) -> None:
    bulk_read_redis.values[bulk_read_batch_key(batch_id)] = json.dumps(
        manifest
    ).encode()


def test_prepare_stores_frozen_event_batch_without_mutating_user_state(
    bulk_read_redis,
) -> None:
    reset_database()
    first = create_event_fixture(suffix="bulk-prepare-first")
    second = create_event_fixture(suffix="bulk-prepare-second")

    response = TestClient(app).post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "event"},
    )

    assert response.status_code == 200
    prepared = response.json()
    assert prepared["target_count"] == 2
    assert len(prepared["batch_id"]) == 36
    targets = stored_manifest(bulk_read_redis, prepared["batch_id"])["targets"]
    assert {
        (target["event_uid"], target["observed_revision_uid"])
        for target in targets
    } == {
        (first["event_uid"], first["revision_uid"]),
        (second["event_uid"], second["revision_uid"]),
    }
    assert all(target["target_kind"] == "event" for target in targets)
    assert len({target["operation_id"] for target in targets}) == 2
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0
        assert session.scalar(select(func.count(EventUserState.id))) == 0
        assert session.scalar(select(func.count(UserState.id))) == 0


def test_confirm_marks_only_the_server_prepared_revision_and_not_new_content() -> None:
    reset_database()
    prepared_fixture = create_event_fixture(suffix="bulk-old-revision")
    client = TestClient(app)
    prepared = client.post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "event"},
    ).json()

    newer_revision = append_current_revision(
        prepared_fixture,
        suffix="after-bulk-prepare",
    )
    new_event = create_event_fixture(suffix="bulk-new-event")
    response = client.post(
        "/user-state/bulk-read",
        json={"batch_id": prepared["batch_id"]},
    )

    assert response.status_code == 200
    assert response.json() == {"updated": 1}
    prepared_detail = client.get(
        f"/clusters/{prepared_fixture['cluster_id']}"
    ).json()
    new_detail = client.get(f"/clusters/{new_event['cluster_id']}").json()
    assert prepared_detail["read_status"] == "summary_seen"
    assert prepared_detail["seen_revision_uid"] == prepared_fixture["revision_uid"]
    assert prepared_detail["current_revision_uid"] == newer_revision["uid"]
    assert prepared_detail["current_revision_differs_from_seen"] is True
    assert new_detail["read_status"] == "unread"


def test_confirm_rejects_items_whose_source_was_deleted_after_prepare() -> None:
    reset_database()
    fixture = create_event_fixture(suffix="bulk-deleted-source")
    client = TestClient(app)
    prepared = client.post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "item"},
    ).json()
    assert prepared["target_count"] == 1

    assert client.delete(f"/sources/{fixture['source_id']}").status_code == 204
    response = client.post(
        "/user-state/bulk-read",
        json={"batch_id": prepared["batch_id"]},
    )

    assert response.status_code == 404
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0
        assert session.scalar(select(func.count(UserState.id))) == 0


def test_confirm_rejects_events_whose_observed_source_was_deleted() -> None:
    reset_database()
    fixture = create_event_fixture(suffix="bulk-event-deleted-source")
    client = TestClient(app)
    prepared = client.post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "event"},
    ).json()

    assert client.delete(f"/sources/{fixture['source_id']}").status_code == 204
    response = client.post(
        "/user-state/bulk-read",
        json={"batch_id": prepared["batch_id"]},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "Event 来源已失效，请刷新"
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0
        assert session.scalar(select(func.count(EventUserState.id))) == 0


def test_confirm_rejects_corrupt_revision_ownership_atomically(
    bulk_read_redis,
) -> None:
    reset_database()
    create_event_fixture(suffix="bulk-valid")
    create_event_fixture(suffix="bulk-invalid")
    client = TestClient(app)
    prepared = client.post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "event"},
    ).json()
    manifest = stored_manifest(bulk_read_redis, prepared["batch_id"])
    targets = manifest["targets"]
    targets[1]["observed_revision_uid"] = targets[0]["observed_revision_uid"]
    replace_manifest(bulk_read_redis, prepared["batch_id"], manifest)

    response = client.post(
        "/user-state/bulk-read",
        json={"batch_id": prepared["batch_id"]},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "observed revision 不属于目标 Event"
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0
        assert session.scalar(select(func.count(EventUserState.id))) == 0
        assert sum(
            metric.read_count or 0
            for metric in session.scalars(select(FeedMetric)).all()
        ) == 0


def test_confirm_retries_the_same_server_batch_idempotently() -> None:
    reset_database()
    create_event_fixture(suffix="bulk-retry-first")
    create_event_fixture(suffix="bulk-retry-second")
    client = TestClient(app)
    prepared = client.post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "event"},
    ).json()
    confirmation = {"batch_id": prepared["batch_id"]}

    assert client.post(
        "/user-state/bulk-read",
        json=confirmation,
    ).json() == {"updated": 2}
    assert client.post(
        "/user-state/bulk-read",
        json=confirmation,
    ).json() == {"updated": 2}

    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 2
        assert session.scalar(select(func.count(EventUserState.id))) == 2
        assert sum(
            metric.read_count or 0
            for metric in session.scalars(select(FeedMetric)).all()
        ) == 2


def test_confirm_uses_one_batch_lock_instead_of_per_operation_locks(
    monkeypatch,
) -> None:
    reset_database()
    create_event_fixture(suffix="bulk-lock-first")
    create_event_fixture(suffix="bulk-lock-second")
    create_event_fixture(suffix="bulk-lock-third")
    client = TestClient(app)
    prepared = client.post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "event"},
    ).json()
    locks: list[str] = []
    monkeypatch.setattr(
        "reader_api.bulk_read.lock_operation_id",
        lambda _session, operation_id: locks.append(operation_id),
    )

    response = client.post(
        "/user-state/bulk-read",
        json={"batch_id": prepared["batch_id"]},
    )

    assert response.status_code == 200
    assert locks == [f"bulk-read-batch:{prepared['batch_id']}"]


def test_confirm_rejects_semantically_changed_completed_batch(
    bulk_read_redis,
) -> None:
    reset_database()
    create_event_fixture(suffix="bulk-operation-conflict")
    client = TestClient(app)
    prepared = client.post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "event"},
    ).json()
    confirmation = {"batch_id": prepared["batch_id"]}
    assert client.post("/user-state/bulk-read", json=confirmation).status_code == 200
    manifest = stored_manifest(bulk_read_redis, prepared["batch_id"])
    manifest["targets"][0]["event_uid"] = (
        "00000000-0000-4000-8000-000000000000"
    )
    replace_manifest(bulk_read_redis, prepared["batch_id"], manifest)

    response = client.post("/user-state/bulk-read", json=confirmation)

    assert response.status_code == 409
    assert response.json()["detail"] == "operation_id 已用于另一项操作"
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 1


def test_empty_prepare_and_expired_confirmation_are_explicit() -> None:
    reset_database()
    client = TestClient(app)

    prepared = client.post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "event"},
    )
    expired = client.post(
        "/user-state/bulk-read",
        json={"batch_id": "99999999-9999-4999-8999-999999999999"},
    )

    assert prepared.status_code == 200
    assert prepared.json() == {"target_count": 0}
    assert expired.status_code == 409
    assert expired.json()["detail"] == "批量已读确认已过期，请重新准备"


def test_confirm_accepts_only_an_opaque_batch_id() -> None:
    reset_database()

    response = TestClient(app).post(
        "/user-state/bulk-read",
        json={"object_type": "item", "source_id": 1},
    )

    assert response.status_code == 422
