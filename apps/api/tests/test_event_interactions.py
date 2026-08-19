from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import func, select, text
from sqlalchemy.orm import sessionmaker

from reader_api.clustering_run import clustering_run
from reader_api.db import Base, engine
from reader_api.main import app
from reader_api.models import (
    Cluster,
    ClusterEventProjection,
    ClusterItem,
    ContentItem,
    Document,
    Event,
    EventEvidenceVersion,
    EventRevision,
    EventRevisionEvidence,
    EventUserState,
    FeedMetric,
    InteractionEvent,
    MigrationBaseline,
    Source,
    TopicGroup,
    UserState,
)
from reader_api.projection_rebuild import inspect_projection_rebuild
from reader_api.report_generation import report_clusters
from tests.factories import assign_publishable_cluster as assign_cluster, make_raw_entry


def create_event_fixture(
    *,
    suffix: str = "one",
    baseline: tuple[str, bool, bool] | None = None,
) -> dict[str, object]:
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        source = Source(
            name=f"Interaction source {suffix}",
            url=f"https://example.com/interaction-{suffix}.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id=f"interaction-{suffix}",
            title=f"Interaction {suffix}",
            url=f"https://example.com/interaction-{suffix}",
            raw_content=f"Interaction evidence {suffix}",
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
            scope_type=f"event-interaction-{suffix}",
            item_ids=[item.id],
            rule_version="event-interaction-test-v1",
        ):
            cluster = assign_cluster(session, item)

        projection = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.cluster_id == cluster.id
            )
        )
        assert projection is not None
        event = session.get(Event, projection.event_id)
        revision = session.get(EventRevision, projection.event_revision_id)
        assert event is not None
        assert revision is not None
        evidence_version = session.scalar(
            select(EventEvidenceVersion)
            .join(
                EventRevisionEvidence,
                EventRevisionEvidence.evidence_version_id
                == EventEvidenceVersion.id,
            )
            .where(EventRevisionEvidence.revision_id == revision.id)
        )
        assert evidence_version is not None

        if baseline is not None:
            read_status, read_later, starred = baseline
            source_updated_at = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)
            historical_state_id = 800000 + cluster.id
            migration = MigrationBaseline(
                idempotency_key=hashlib.sha256(
                    f"legacy-user-state-baseline-v1:{historical_state_id}".encode()
                ).hexdigest(),
                migration_version="legacy-user-state-baseline-v1",
                legacy_user_state_id=historical_state_id,
                legacy_object_type="cluster",
                legacy_object_id=cluster.id,
                resolved_event_id=event.id,
                resolved_revision_id=revision.id,
                read_status=read_status,
                read_later=read_later,
                starred=starred,
                source_updated_at=source_updated_at,
                recorded_at=source_updated_at,
            )
            session.add(migration)
            session.flush()
            session.add(
                EventUserState(
                    baseline_id=migration.id,
                    event_id=event.id,
                    seen_revision_id=(
                        revision.id if read_status in {"summary_seen", "original_opened"} else None
                    ),
                    read_status=read_status,
                    read_later=read_later,
                    starred=starred,
                    updated_at=source_updated_at,
                )
            )

        result: dict[str, object] = {
            "source_id": source.id,
            "item_id": item.id,
            "cluster_id": cluster.id,
            "event_id": event.id,
            "event_uid": event.uid,
            "revision_id": revision.id,
            "revision_uid": revision.uid,
            "evidence_version_uid": evidence_version.uid,
        }
        session.commit()
        return result


def mutation_payload(
    fixture: dict[str, object],
    *,
    operation_id: str,
    action: str,
    value: object,
    source_id: int | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "event_uid": fixture["event_uid"],
        "observed_revision_uid": fixture["revision_uid"],
        "operation_id": operation_id,
        "action": action,
        "value": value,
    }
    if source_id is not None:
        payload["source_id"] = source_id
    return payload


def test_unread_cluster_cursor_survives_seen_rows_and_newer_arrivals() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixtures = [create_event_fixture(suffix=f"cursor-{index}") for index in range(4)]
    baseline = datetime(2026, 8, 1, tzinfo=timezone.utc)
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        for index, fixture in enumerate(fixtures):
            cluster = session.get(Cluster, fixture["cluster_id"])
            assert cluster is not None
            cluster.first_seen_at = baseline + timedelta(hours=index)
            cluster.last_seen_at = cluster.first_seen_at
        session.commit()

    client = TestClient(app)
    first_page = client.get(
        "/clusters",
        params={"read_status": "unread", "limit": 2},
    ).json()
    assert [row["id"] for row in first_page] == [
        fixtures[3]["cluster_id"],
        fixtures[2]["cluster_id"],
    ]
    for fixture in fixtures[2:]:
        response = client.post(
            "/event-user-state",
            json=mutation_payload(
                fixture,
                operation_id=str(uuid4()),
                action="read_status_set",
                value="summary_seen",
            ),
        )
        assert response.status_code == 200

    newer = create_event_fixture(suffix="cursor-newer")
    with SessionLocal() as session:
        cluster = session.get(Cluster, newer["cluster_id"])
        assert cluster is not None
        cluster.first_seen_at = baseline + timedelta(hours=4)
        cluster.last_seen_at = cluster.first_seen_at
        session.commit()

    second_page = client.get(
        "/clusters",
        params={
            "read_status": "unread",
            "limit": 2,
            "cursor_id": first_page[-1]["id"],
        },
    )

    assert second_page.status_code == 200
    assert [row["id"] for row in second_page.json()] == [
        fixtures[1]["cluster_id"],
        fixtures[0]["cluster_id"],
    ]


def object_mutation_payload(
    *,
    operation_id: str,
    field: str,
    value: object,
) -> dict[str, object]:
    return {
        "operation_id": operation_id,
        field: value,
    }


def test_event_mutation_does_not_create_cluster_user_state() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(suffix="event-only")

    response = TestClient(app).post(
        "/event-user-state",
        json=mutation_payload(
            fixture,
            operation_id="12121212-1212-4212-8212-121212121212",
            action="starred_set",
            value=True,
        ),
    )

    assert response.status_code == 200
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(
            select(func.count(UserState.id)).where(
                UserState.object_type == "cluster"
            )
        ) == 0


def append_current_revision(
    fixture: dict[str, object],
    *,
    suffix: str,
) -> dict[str, object]:
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        event = session.get(Event, fixture["event_id"])
        previous = session.get(EventRevision, fixture["revision_id"])
        assert event is not None
        assert previous is not None
        revision = EventRevision(
            uid=str(uuid4()),
            event_id=event.id,
            revision_no=previous.revision_no + 1,
            evidence_fingerprint=hashlib.sha256(
                f"event-interaction-revision:{event.id}:{suffix}".encode()
            ).hexdigest(),
            title_snapshot=f"Interaction revision {suffix}",
            event_time_snapshot=previous.event_time_snapshot,
        )
        session.add(revision)
        session.flush()
        previous_evidence = session.scalars(
            select(EventRevisionEvidence).where(
                EventRevisionEvidence.revision_id == previous.id
            )
        ).all()
        session.add_all(
            EventRevisionEvidence(
                revision_id=revision.id,
                evidence_version_id=evidence.evidence_version_id,
                evidence_type=evidence.evidence_type,
                role=evidence.role,
            )
            for evidence in previous_evidence
        )
        event.current_revision_id = revision.id
        session.commit()
        return {"id": revision.id, "uid": revision.uid}


def test_event_starred_and_read_later_are_revision_bound_atomic_and_idempotent() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture()
    client = TestClient(app)

    starred = mutation_payload(
        fixture,
        operation_id="11111111-1111-4111-8111-111111111111",
        action="starred_set",
        value=True,
    )
    first_response = client.post("/event-user-state", json=starred)
    assert first_response.status_code == 200
    first = first_response.json()
    assert first == {
        "operation_id": starred["operation_id"],
        "event_uid": fixture["event_uid"],
        "observed_revision_uid": fixture["revision_uid"],
        "action": "starred_set",
        "value": True,
        "read_later": False,
        "starred": True,
        "updated_at": first["updated_at"],
    }

    duplicate = client.post("/event-user-state", json=starred)
    assert duplicate.status_code == 200
    assert duplicate.json() == first

    read_later = mutation_payload(
        fixture,
        operation_id="22222222-2222-4222-8222-222222222222",
        action="read_later_set",
        value=True,
    )
    later_response = client.post("/event-user-state", json=read_later)
    assert later_response.status_code == 200
    assert later_response.json()["read_later"] is True

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        interactions = session.scalars(
            select(InteractionEvent).order_by(InteractionEvent.recorded_at)
        ).all()
        assert len(interactions) == 2
        assert interactions[0].event_id == fixture["event_id"]
        assert interactions[0].observed_revision_id == fixture["revision_id"]
        assert interactions[0].set_value is True
        assert interactions[0].payload["result"] == first
        event_state = session.scalar(select(EventUserState))
        assert event_state is not None
        assert event_state.starred is True
        assert event_state.read_later is True
        assert session.scalar(
            select(func.count(UserState.id)).where(
                UserState.object_type == "cluster"
            )
        ) == 0
        metric = session.scalar(
            select(FeedMetric).where(FeedMetric.source_id == fixture["source_id"])
        )
        assert metric is not None
        assert metric.starred_count == 1
        assert metric.read_later_count == 1


def test_event_mutation_rejects_missing_unknown_wrong_or_reused_identity() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    first = create_event_fixture(suffix="first")
    second = create_event_fixture(suffix="second")
    client = TestClient(app)
    valid = mutation_payload(
        first,
        operation_id="33333333-3333-4333-8333-333333333333",
        action="starred_set",
        value=True,
    )

    missing_revision = dict(valid)
    missing_revision.pop("observed_revision_uid")
    assert client.post("/event-user-state", json=missing_revision).status_code == 422

    unknown_event = dict(valid, event_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    assert client.post("/event-user-state", json=unknown_event).status_code == 404

    unknown_revision = dict(
        valid,
        observed_revision_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    )
    assert client.post("/event-user-state", json=unknown_revision).status_code == 404

    wrong_owner = dict(valid, observed_revision_uid=second["revision_uid"])
    assert client.post("/event-user-state", json=wrong_owner).status_code == 409

    unknown_action = dict(valid, action="toggle_starred")
    assert client.post("/event-user-state", json=unknown_action).status_code == 400

    assert client.post("/event-user-state", json=valid).status_code == 200
    reused = dict(valid, action="read_later_set")
    assert client.post("/event-user-state", json=reused).status_code == 409

    cluster_id = int(first["cluster_id"])
    bypass = client.patch(
        f"/user-state/cluster/{cluster_id}", json={"starred": False}
    )
    assert bypass.status_code == 400
    assert bypass.json()["detail"] == "不支持的状态对象类型"
    assert (
        client.patch(
            f"/user-state/cluster/{cluster_id}",
            json={"read_status": "summary_seen"},
        ).status_code
        == 400
    )


def test_baseline_nondefault_state_keeps_original_retry_result_and_metric_floor() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(
        suffix="baseline",
        baseline=("summary_seen", True, True),
    )
    client = TestClient(app)
    clear_star = mutation_payload(
        fixture,
        operation_id="44444444-4444-4444-8444-444444444444",
        action="starred_set",
        value=False,
    )
    original = client.post("/event-user-state", json=clear_star).json()
    assert original["starred"] is False
    assert original["read_later"] is True

    restore_star = mutation_payload(
        fixture,
        operation_id="55555555-5555-4555-8555-555555555555",
        action="starred_set",
        value=True,
    )
    assert client.post("/event-user-state", json=restore_star).json()["starred"] is True
    assert client.post("/event-user-state", json=clear_star).json() == original

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 2
        state = session.scalar(select(EventUserState))
        assert state is not None
        assert state.starred is True
        metric = session.scalar(select(FeedMetric))
        assert metric is not None
        assert metric.starred_count == 1


def test_baseline_summary_seen_first_original_open_only_adds_opened_metric() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(
        suffix="baseline-reading",
        baseline=("summary_seen", False, False),
    )
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        session.add(
            FeedMetric(
                source_id=int(fixture["source_id"]),
                read_count=7,
                opened_count=3,
            )
        )
        session.commit()

    response = TestClient(app).post(
        "/event-user-state",
        json=mutation_payload(
            fixture,
            operation_id="56565656-5656-4656-8656-565656565656",
            action="read_status_set",
            value="original_opened",
            source_id=int(fixture["source_id"]),
        ),
    )
    assert response.status_code == 200

    repeated = TestClient(app).post(
        "/event-user-state",
        json=mutation_payload(
            fixture,
            operation_id="57575757-5757-4757-8757-575757575757",
            action="read_status_set",
            value="original_opened",
            source_id=int(fixture["source_id"]),
        ),
    )
    assert repeated.status_code == 200

    with SessionLocal() as session:
        metric = session.scalar(
            select(FeedMetric).where(
                FeedMetric.source_id == fixture["source_id"]
            )
        )
        assert metric is not None
        assert metric.read_count == 7
        assert metric.opened_count == 4
        interactions = session.scalars(select(InteractionEvent)).all()
        assert len(interactions) == 2
        interactions_by_operation = {
            interaction.operation_id: interaction for interaction in interactions
        }
        first = interactions_by_operation["56565656-5656-4656-8656-565656565656"]
        second = interactions_by_operation["57575757-5757-4757-8757-575757575757"]
        assert first.payload["read_metric_source_ids"] == []
        assert first.payload["opened_metric_source_ids"] == [
            fixture["source_id"]
        ]
        assert first.payload["metric_delta"] == {
            str(fixture["source_id"]): {"opened_count": 1}
        }
        assert second.payload["read_metric_source_ids"] == []
        assert second.payload["opened_metric_source_ids"] == [
            fixture["source_id"]
        ]
        assert second.payload["metric_delta"] == {}


def test_baseline_read_metric_floor_survives_explicit_unread_then_seen() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(
        suffix="baseline-unread-reading",
        baseline=("summary_seen", False, False),
    )
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        session.add(
            FeedMetric(
                source_id=int(fixture["source_id"]),
                read_count=7,
                opened_count=3,
            )
        )
        session.commit()

    client = TestClient(app)
    unread = client.post(
        "/event-user-state",
        json=mutation_payload(
            fixture,
            operation_id="58585858-5858-4858-8858-585858585858",
            action="read_status_set",
            value="unread",
        ),
    )
    assert unread.status_code == 200
    assert unread.json()["read_status"] == "unread"
    seen = client.post(
        "/event-user-state",
        json=mutation_payload(
            fixture,
            operation_id="59595959-5959-4959-8959-595959595959",
            action="read_status_set",
            value="summary_seen",
        ),
    )
    assert seen.status_code == 200
    assert seen.json()["read_status"] == "summary_seen"

    with SessionLocal() as session:
        metric = session.scalar(
            select(FeedMetric).where(
                FeedMetric.source_id == fixture["source_id"]
            )
        )
        assert metric is not None
        assert metric.read_count == 7
        assert metric.opened_count == 3
        interactions = session.scalars(
            select(InteractionEvent).order_by(InteractionEvent.id)
        ).all()
        assert len(interactions) == 2
        assert interactions[0].payload["metric_delta"] == {}
        assert interactions[1].payload["read_metric_source_ids"] == []
        assert interactions[1].payload["metric_delta"] == {}


def test_event_mutation_rolls_back_every_projection_when_metric_write_fails() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(suffix="rollback")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TRIGGER fail_event_metric BEFORE INSERT ON feed_metrics "
                "BEGIN SELECT RAISE(ABORT, 'forced metric failure'); END"
            )
        )

    client = TestClient(app, raise_server_exceptions=False)
    response = client.post(
        "/event-user-state",
        json=mutation_payload(
            fixture,
            operation_id="66666666-6666-4666-8666-666666666666",
            action="read_later_set",
            value=True,
        ),
    )
    assert response.status_code == 500

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0
        assert session.scalar(select(func.count(EventUserState.id))) == 0
        assert session.scalar(select(func.count(UserState.id))) == 0
        assert session.scalar(select(func.count(FeedMetric.id))) == 0


def test_item_report_and_topic_actions_append_honest_object_interactions() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(suffix="legacy")
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        topic = TopicGroup(name="Legacy topic", query="reader")
        session.add(topic)
        session.commit()
        topic_id = topic.id

    client = TestClient(app)
    assert client.get("/items").status_code == 200
    assert client.get("/topics").status_code == 200
    with SessionLocal() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0

    actions = [
        (
            "item",
            int(fixture["item_id"]),
            object_mutation_payload(
                operation_id="71111111-1111-4111-8111-111111111111",
                field="read_status",
                value="summary_seen",
            ),
            "read_status_set",
            "summary_seen",
        ),
        (
            "item",
            int(fixture["item_id"]),
            object_mutation_payload(
                operation_id="72222222-2222-4222-8222-222222222222",
                field="read_status",
                value="original_opened",
            ),
            "read_status_set",
            "original_opened",
        ),
        (
            "item",
            int(fixture["item_id"]),
            object_mutation_payload(
                operation_id="73333333-3333-4333-8333-333333333333",
                field="read_status",
                value="unread",
            ),
            "read_status_set",
            "unread",
        ),
        (
            "item",
            int(fixture["item_id"]),
            object_mutation_payload(
                operation_id="74444444-4444-4444-8444-444444444444",
                field="starred",
                value=True,
            ),
            "starred_set",
            True,
        ),
        (
            "report",
            20260714,
            object_mutation_payload(
                operation_id="75555555-5555-4555-8555-555555555555",
                field="read_later",
                value=True,
            ),
            "read_later_set",
            True,
        ),
        (
            "topic",
            topic_id,
            object_mutation_payload(
                operation_id="76666666-6666-4666-8666-666666666666",
                field="read_status",
                value="summary_seen",
            ),
            "read_status_set",
            "summary_seen",
        ),
    ]
    responses: list[dict[str, object]] = []
    for object_type, object_id, payload, _, _ in actions:
        response = client.patch(
            f"/user-state/{object_type}/{object_id}",
            json=payload,
        )
        assert response.status_code == 200
        responses.append(response.json())

    with SessionLocal() as session:
        interactions = session.scalars(
            select(InteractionEvent).order_by(InteractionEvent.recorded_at)
        ).all()
        assert len(interactions) == len(actions)
        for interaction, action, response in zip(interactions, actions, responses):
            object_type, object_id, payload, action_name, set_value = action
            assert interaction.operation_id == payload["operation_id"]
            assert interaction.target_kind == "legacy"
            assert interaction.object_type == object_type
            assert interaction.object_id == object_id
            assert interaction.event_id is None
            assert interaction.observed_revision_id is None
            assert interaction.action == action_name
            assert interaction.set_value == set_value
            assert interaction.occurred_at is not None
            assert interaction.recorded_at is not None
            assert interaction.occurred_at <= interaction.recorded_at
            assert interaction.payload["metric_source_id"] == (
                fixture["source_id"] if object_type == "item" else None
            )
            assert interaction.payload["result"] == response
        metric = session.scalar(
            select(FeedMetric).where(
                FeedMetric.source_id == fixture["source_id"]
            )
        )
        assert metric is not None
        assert metric.read_count == 0
        assert metric.opened_count == 0
        assert metric.starred_count == 1


def test_object_interaction_operation_is_idempotent_and_cannot_change_meaning() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(suffix="object-idempotent")
    client = TestClient(app)
    payload = object_mutation_payload(
        operation_id="77777777-1111-4111-8111-111111111111",
        field="read_later",
        value=True,
    )

    first = client.patch(
        f"/user-state/item/{fixture['item_id']}", json=payload
    )
    duplicate = client.patch(
        f"/user-state/item/{fixture['item_id']}", json=payload
    )

    assert first.status_code == 200
    assert duplicate.status_code == 200
    assert duplicate.json() == first.json()
    assert (
        client.patch(
            f"/user-state/item/{fixture['item_id']}",
            json={**payload, "read_later": False},
        ).status_code
        == 409
    )
    assert (
        client.patch(
            "/user-state/report/20260714", json=payload
        ).status_code
        == 409
    )

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 1
        metric = session.scalar(select(FeedMetric))
        assert metric is not None
        assert metric.read_later_count == 1


def test_object_interactions_require_one_explicit_operation_and_real_target() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    assert (
        client.patch(
            "/user-state/item/999",
            json=object_mutation_payload(
                operation_id="78888888-8888-4888-8888-888888888888",
                field="starred",
                value=True,
            ),
        ).status_code
        == 404
    )
    assert (
        client.patch(
            "/user-state/topic/999",
            json=object_mutation_payload(
                operation_id="79999999-9999-4999-8999-999999999999",
                field="read_status",
                value="summary_seen",
            ),
        ).status_code
        == 404
    )
    assert (
        client.patch(
            "/user-state/report/20260714",
            json={"starred": True},
        ).status_code
        == 400
    )
    assert (
        client.patch(
            "/user-state/report/20260714",
            json={
                "operation_id": "70000000-0000-4000-8000-000000000000",
                "starred": True,
                "read_later": True,
            },
        ).status_code
        == 400
    )

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0
        assert session.scalar(select(func.count(UserState.id))) == 0
        assert session.scalar(select(func.count(FeedMetric.id))) == 0


def test_event_interaction_rejects_a_deleted_observed_source() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(suffix="deleted-observed-source")
    client = TestClient(app)
    assert client.delete(f"/sources/{fixture['source_id']}").status_code == 204

    response = client.post(
        "/event-user-state",
        json=mutation_payload(
            fixture,
            operation_id="70111111-1111-4111-8111-111111111111",
            action="starred_set",
            value=True,
        ),
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "Event 来源已失效，请刷新"
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0
        assert session.scalar(select(func.count(EventUserState.id))) == 0


def test_object_interaction_rolls_back_state_and_metric_when_ledger_write_fails() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(suffix="object-rollback")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TRIGGER fail_object_interaction BEFORE INSERT ON interaction_events "
                "BEGIN SELECT RAISE(ABORT, 'forced interaction failure'); END"
            )
        )

    response = TestClient(app, raise_server_exceptions=False).patch(
        f"/user-state/item/{fixture['item_id']}",
        json=object_mutation_payload(
            operation_id="7aaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            field="starred",
            value=True,
        ),
    )

    assert response.status_code == 500
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0
        assert session.scalar(select(func.count(UserState.id))) == 0
        assert session.scalar(select(func.count(FeedMetric.id))) == 0


def test_event_reading_keeps_observed_history_and_advances_seen_revision_only_explicitly() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(suffix="reading-revision")
    client = TestClient(app)

    summary_seen = mutation_payload(
        fixture,
        operation_id="a1111111-1111-4111-8111-111111111111",
        action="read_status_set",
        value="summary_seen",
    )
    first = client.post("/event-user-state", json=summary_seen)
    assert first.status_code == 200
    assert first.json()["read_status"] == "summary_seen"
    assert first.json()["seen_revision_uid"] == fixture["revision_uid"]
    assert first.json()["current_revision_differs_from_seen"] is False
    assert client.post("/event-user-state", json=summary_seen).json() == first.json()

    newer = append_current_revision(fixture, suffix="new-current")
    detail = client.get(f"/clusters/{fixture['cluster_id']}").json()
    assert detail["current_revision_uid"] == newer["uid"]
    assert detail["seen_revision_uid"] == fixture["revision_uid"]
    assert detail["current_revision_differs_from_seen"] is True
    listed = next(
        cluster
        for cluster in client.get("/clusters").json()
        if cluster["id"] == fixture["cluster_id"]
    )
    assert listed["current_revision_uid"] == newer["uid"]
    assert listed["seen_revision_uid"] == fixture["revision_uid"]
    assert listed["current_revision_differs_from_seen"] is True

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 1

    opened_old = mutation_payload(
        fixture,
        operation_id="a2222222-2222-4222-8222-222222222222",
        action="read_status_set",
        value="original_opened",
        source_id=int(fixture["source_id"]),
    )
    opened = client.post("/event-user-state", json=opened_old)
    assert opened.status_code == 200
    assert opened.json()["read_status"] == "original_opened"
    assert opened.json()["seen_revision_uid"] == fixture["revision_uid"]
    assert opened.json()["current_revision_differs_from_seen"] is True
    assert client.post("/event-user-state", json=opened_old).json() == opened.json()

    seen_new = dict(
        summary_seen,
        operation_id="a3333333-3333-4333-8333-333333333333",
        observed_revision_uid=newer["uid"],
    )
    advanced = client.post("/event-user-state", json=seen_new)
    assert advanced.status_code == 200
    assert advanced.json()["read_status"] == "original_opened"
    assert advanced.json()["seen_revision_uid"] == newer["uid"]
    assert advanced.json()["current_revision_differs_from_seen"] is False

    stale_again = dict(
        summary_seen,
        operation_id="a4444444-4444-4444-8444-444444444444",
    )
    stale = client.post("/event-user-state", json=stale_again)
    assert stale.status_code == 200
    assert stale.json()["seen_revision_uid"] == newer["uid"]

    unread = dict(
        seen_new,
        operation_id="a5555555-5555-4555-8555-555555555555",
        value="unread",
    )
    reset = client.post("/event-user-state", json=unread)
    assert reset.status_code == 200
    assert reset.json()["read_status"] == "unread"
    assert reset.json()["seen_revision_uid"] == newer["uid"]

    with SessionLocal() as session:
        interactions = session.scalars(
            select(InteractionEvent).order_by(InteractionEvent.recorded_at)
        ).all()
        assert len(interactions) == 5
        assert [interaction.action for interaction in interactions] == [
            "read_status_set"
        ] * 5
        assert interactions[0].observed_revision_id == fixture["revision_id"]
        assert interactions[2].observed_revision_id == newer["id"]
        state = session.scalar(select(EventUserState))
        assert state is not None
        assert state.read_status == "unread"
        assert state.seen_revision_id == newer["id"]
        assert session.scalar(
            select(func.count(UserState.id)).where(
                UserState.object_type == "cluster"
            )
        ) == 0
        metric = session.scalar(select(FeedMetric))
        assert metric is not None
        assert metric.read_count == 0
        assert metric.opened_count == 0


def test_revision_difference_does_not_change_cluster_time_order() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    older = create_event_fixture(
        suffix="timeline-older",
        baseline=("summary_seen", False, False),
    )
    newer = create_event_fixture(suffix="timeline-newer")
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        older_cluster = session.get(Cluster, older["cluster_id"])
        newer_cluster = session.get(Cluster, newer["cluster_id"])
        assert older_cluster is not None
        assert newer_cluster is not None
        older_cluster.first_seen_at = datetime(2026, 7, 14, tzinfo=timezone.utc)
        newer_cluster.first_seen_at = datetime(2026, 7, 15, tzinfo=timezone.utc)
        session.commit()

    client = TestClient(app)
    before = [row["id"] for row in client.get("/clusters").json()]
    append_current_revision(older, suffix="timeline-difference")
    after_rows = client.get("/clusters").json()

    assert before == [newer["cluster_id"], older["cluster_id"]]
    assert [row["id"] for row in after_rows] == before
    assert next(
        row for row in after_rows if row["id"] == older["cluster_id"]
    )["current_revision_differs_from_seen"] is True


def test_event_original_open_validates_source_and_reading_rolls_back_atomically() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(suffix="reading-rollback")
    other = create_event_fixture(suffix="reading-other")
    client = TestClient(app, raise_server_exceptions=False)

    wrong_source = mutation_payload(
        fixture,
        operation_id="b1111111-1111-4111-8111-111111111111",
        action="read_status_set",
        value="original_opened",
        source_id=int(other["source_id"]),
    )
    assert client.post("/event-user-state", json=wrong_source).status_code == 409

    missing_source = dict(wrong_source)
    missing_source["operation_id"] = "b2222222-2222-4222-8222-222222222222"
    missing_source.pop("source_id")
    assert client.post("/event-user-state", json=missing_source).status_code == 400

    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TRIGGER fail_read_metric BEFORE INSERT ON feed_metrics "
                "BEGIN SELECT RAISE(ABORT, 'forced read metric failure'); END"
            )
        )

    response = client.post(
        "/event-user-state",
        json=mutation_payload(
            fixture,
            operation_id="b3333333-3333-4333-8333-333333333333",
            action="read_status_set",
            value="summary_seen",
        ),
    )
    assert response.status_code == 500

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0
        assert session.scalar(select(func.count(EventUserState.id))) == 0
        assert session.scalar(select(func.count(UserState.id))) == 0
        assert session.scalar(select(func.count(FeedMetric.id))) == 0


def test_event_original_open_audits_the_exact_rendered_evidence() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(suffix="reading-evidence")
    other = create_event_fixture(suffix="reading-other-evidence")
    client = TestClient(app)
    operation_id = "b4444444-4444-4444-8444-444444444444"
    payload = mutation_payload(
        fixture,
        operation_id=operation_id,
        action="read_status_set",
        value="original_opened",
        source_id=int(fixture["source_id"]),
    )
    payload["evidence_version_uid"] = fixture["evidence_version_uid"]

    response = client.post("/event-user-state", json=payload)

    assert response.status_code == 200
    assert response.json()["evidence_version_uid"] == fixture[
        "evidence_version_uid"
    ]
    assert client.post("/event-user-state", json=payload).json() == response.json()
    conflicting = dict(payload, evidence_version_uid=other["evidence_version_uid"])
    assert client.post("/event-user-state", json=conflicting).status_code == 409
    wrong_evidence = dict(
        conflicting,
        operation_id="b5555555-5555-4555-8555-555555555555",
    )
    assert client.post("/event-user-state", json=wrong_evidence).status_code == 409
    with sessionmaker(bind=engine)() as session:
        interaction = session.scalar(
            select(InteractionEvent).where(
                InteractionEvent.operation_id == operation_id
            )
        )
        assert interaction is not None
        assert interaction.payload["opened_evidence_version_uid"] == fixture[
            "evidence_version_uid"
        ]


def test_cluster_detail_identifies_current_source_view_evidence() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(suffix="source-view-evidence")

    detail = TestClient(app).get(f"/clusters/{fixture['cluster_id']}")

    assert detail.status_code == 200
    assert detail.json()["synthesis"]["source_view_revision_uid"] == fixture[
        "revision_uid"
    ]
    assert detail.json()["source_view_evidence"] == [
        {
            "evidence_version_uid": fixture["evidence_version_uid"],
            "source_id": fixture["source_id"],
            "legacy_content_item_id_snapshot": fixture["item_id"],
        }
    ]


def test_cluster_detail_projects_items_from_the_fixed_event_revision() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(suffix="source-view-snapshot")
    with sessionmaker(bind=engine)() as session:
        item = session.get(ContentItem, fixture["item_id"])
        assert item is not None
        item.title = "Mutable projection title"
        item.content_text = "Mutable projection content"
        session.commit()

    detail = TestClient(app).get(f"/clusters/{fixture['cluster_id']}")

    assert detail.status_code == 200
    assert detail.json()["synthesis"]["source_view_revision_uid"] == fixture[
        "revision_uid"
    ]
    assert [
        {
            "id": item["id"],
            "title": item["title"],
            "content_text": item["content_text"],
        }
        for item in detail.json()["items"]
    ] == [
        {
            "id": fixture["item_id"],
            "title": "Interaction source-view-snapshot",
            "content_text": "Interaction evidence source-view-snapshot",
        }
    ]


def test_uninterested_event_is_recoverable_reasoned_and_yields_to_rules() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fixture = create_event_fixture(
        suffix="uninterested",
        baseline=("unread", True, True),
    )
    client = TestClient(app)
    initial = client.post(
        "/uninterested",
        json={
            "operation_id": "c1111111-1111-4111-8111-111111111111",
            "target_type": "article",
            "item_id": fixture["item_id"],
            "value": True,
        },
    )

    assert initial.status_code == 200
    assert initial.json()["target_kind"] == "event"
    assert initial.json()["affected_item_ids"] == [fixture["item_id"]]
    reasoned_payload = {
        "operation_id": "c2222222-2222-4222-8222-222222222222",
        "target_type": "event",
        "event_uid": fixture["event_uid"],
        "observed_revision_uid": fixture["revision_uid"],
        "value": True,
        "reason": "repetitive",
    }
    reasoned = client.post("/uninterested", json=reasoned_payload)
    assert reasoned.status_code == 200
    assert client.post("/uninterested", json=reasoned_payload).json() == reasoned.json()
    assert client.post(
        "/uninterested",
        json={
            **reasoned_payload,
            "operation_id": "c3333333-3333-4333-8333-333333333333",
            "reason": "other",
        },
    ).status_code == 422

    assert client.get("/clusters").json() == []
    assert client.get("/items", params={"source_id": fixture["source_id"]}).json() == []
    assert client.get("/search", params={"q": "Interaction"}).json() == []
    assert client.get("/items", params={"read_later": True}).json() == []
    source = next(
        row for row in client.get("/sources").json()
        if row["id"] == fixture["source_id"]
    )
    assert source["unread_count"] == 0
    direct = client.get(f"/items/{fixture['item_id']}").json()
    assert direct["uninterested"] is True
    event_detail = client.get(f"/clusters/{fixture['cluster_id']}").json()
    assert event_detail["read_status"] == "unread"
    assert event_detail["read_later"] is True
    assert event_detail["starred"] is True
    bucket = client.get("/uninterested-targets").json()
    assert bucket["count"] == 1
    assert bucket["items"][0]["target_kind"] == "event"
    assert bucket["items"][0]["reason"] == "repetitive"
    assert client.get(
        "/uninterested-targets", params={"reason": "repetitive"}
    ).json()["count"] == 1
    assert client.get(
        "/uninterested-targets", params={"reason": "promotion"}
    ).json()["count"] == 0
    assert client.get(
        "/uninterested-targets", params={"q": "Interaction"}
    ).json()["count"] == 1
    assert client.get(
        "/uninterested-targets", params={"source_id": fixture["source_id"]}
    ).json()["count"] == 1
    with sessionmaker(bind=engine)() as session:
        assert int(fixture["cluster_id"]) not in {
            cluster.id
            for cluster in report_clusters(
                session,
                datetime(2020, 1, 1, tzinfo=timezone.utc),
                datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
        }

    rule = client.post(
        "/filter-rules",
        json={
            "match_type": "literal",
            "pattern": "Interaction uninterested",
        },
    ).json()
    assert client.get("/uninterested-targets").json()["count"] == 0
    filtered = client.get("/filtered-items").json()["items"]
    assert filtered[0]["uninterested"] is True
    assert filtered[0]["uninterested_reason"] == "repetitive"
    assert client.post(
        "/uninterested",
        json={
            "operation_id": "c6666666-6666-4666-8666-666666666666",
            "target_type": "event",
            "event_uid": fixture["event_uid"],
            "observed_revision_uid": fixture["revision_uid"],
            "value": False,
        },
    ).status_code == 200
    assert client.get("/clusters").json() == []
    assert client.get("/uninterested-targets").json()["count"] == 0
    assert client.post(
        "/uninterested",
        json={
            "operation_id": "c7777777-7777-4777-8777-777777777777",
            "target_type": "event",
            "event_uid": fixture["event_uid"],
            "observed_revision_uid": fixture["revision_uid"],
            "value": True,
            "reason": "repetitive",
        },
    ).status_code == 200
    assert client.get("/uninterested-targets").json()["count"] == 0

    with sessionmaker(bind=engine)() as session:
        raw = make_raw_entry(
            source_id=int(fixture["source_id"]),
            external_id="uninterested-follow-up",
            title="A genuinely new follow-up",
            raw_content="Fresh evidence",
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
        follow_up = ContentItem(
            document_id=document.id,
            source_id=int(fixture["source_id"]),
            title=raw.title,
            content_text=raw.raw_content,
            url=raw.url,
            canonical_url=raw.url,
            content_hash=raw.content_hash,
        )
        session.add(follow_up)
        session.flush()
        session.add(
            ClusterItem(
                cluster_id=int(fixture["cluster_id"]),
                content_item_id=follow_up.id,
            )
        )
        session.commit()
        follow_up_id = follow_up.id
    assert client.get("/uninterested-targets").json()["count"] == 1
    assert client.get(f"/items/{follow_up_id}").json()["uninterested"] is True
    assert client.get("/items", params={"source_id": fixture["source_id"]}).json() == []

    assert client.patch(
        f"/filter-rules/{rule['id']}", json={"enabled": False}
    ).status_code == 200
    assert client.get("/uninterested-targets").json()["count"] == 1
    restored = client.post(
        "/uninterested",
        json={
            "operation_id": "c4444444-4444-4444-8444-444444444444",
            "target_type": "event",
            "event_uid": fixture["event_uid"],
            "observed_revision_uid": fixture["revision_uid"],
            "value": False,
        },
    )
    assert restored.status_code == 200
    assert restored.json()["uninterested"] is False
    assert client.get("/uninterested-targets").json()["count"] == 0
    visible = client.get(f"/clusters/{fixture['cluster_id']}").json()
    assert {
        key: visible[key] for key in ("read_status", "read_later", "starred")
    } == {"read_status": "unread", "read_later": True, "starred": True}
    with sessionmaker(bind=engine)() as session:
        report = inspect_projection_rebuild(session)
        assert report["matches"] is True
        assert session.scalar(select(func.count(FeedMetric.id))) == 0


def test_uninterested_article_without_an_event_moves_only_itself() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with sessionmaker(bind=engine)() as session:
        source = Source(
            name="Standalone",
            url="https://example.com/standalone.xml",
            status="active",
            media_type="notification",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="standalone",
            title="Standalone alert",
            raw_content="Standalone body",
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
        session.commit()
        item_id = item.id

    client = TestClient(app)
    marked = client.post(
        "/uninterested",
        json={
            "operation_id": "c5555555-5555-4555-8555-555555555555",
            "target_type": "article",
            "item_id": item_id,
            "value": True,
            "reason": "other",
            "note": "不是我需要的通知",
        },
    )
    assert marked.status_code == 200
    assert marked.json()["target_kind"] == "item"
    assert client.get("/items", params={"media_type": "notification"}).json() == []
    target = client.get("/uninterested-targets").json()["items"][0]
    assert target["item_id"] == item_id
    assert target["note"] == "不是我需要的通知"


def test_uninterested_article_does_not_reenter_ordinary_views_after_clustering(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with sessionmaker(bind=engine)() as session:
        source = Source(
            name="Late cluster",
            url="https://example.com/late-cluster.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="late-cluster",
            title="Late clustered article",
            raw_content="Late clustered body",
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
        session.commit()
        item_id = item.id
        source_id = source.id

    client = TestClient(app)
    marked = client.post(
        "/uninterested",
        json={
            "operation_id": "c8888888-8888-4888-8888-888888888888",
            "target_type": "article",
            "item_id": item_id,
            "value": True,
        },
    )
    assert marked.json()["target_kind"] == "item"

    with sessionmaker(bind=engine)() as session:
        item = session.get(ContentItem, item_id)
        assert item is not None
        with clustering_run(
            session,
            scope_type="late-uninterested-cluster",
            item_ids=[item.id],
            rule_version="late-uninterested-cluster-v1",
        ):
            cluster = assign_cluster(session, item)
        session.commit()
        cluster_id = cluster.id

    assert client.get("/clusters").json() == []
    assert client.get("/clusters", params={"source_id": source_id}).json() == []
    assert client.get("/clusters", params={"q": "Late clustered"}).json() == []
    assert client.get("/clusters/count", params={"source_id": source_id}).json() == {
        "count": 0
    }
    source = next(
        row for row in client.get("/sources/navigation").json()
        if row["id"] == source_id
    )
    assert source["unread_count"] == 0
    prepared = client.post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "event", "source_id": source_id},
    ).json()
    assert prepared["target_count"] == 0

    def unexpected_model_call(*_args, **_kwargs):
        raise AssertionError("hidden content reached the model")

    monkeypatch.setattr("reader_api.main.local_llm", unexpected_model_call)
    assistant = client.get(
        "/assistant", params={"q": "总结", "cluster_id": cluster_id}
    )
    assert assistant.status_code == 200
    assert assistant.json()["citations"] == []
    synthesis = client.post(f"/clusters/{cluster_id}/synthesize")
    assert synthesis.status_code == 400
    assert synthesis.json()["detail"] == "事件聚类没有可合成的来源"

    with sessionmaker(bind=engine)() as session:
        source = session.get(Source, source_id)
        assert source is not None
        raw = make_raw_entry(
            source_id=source.id,
            external_id="late-cluster-visible",
            title="Late clustered visible follow-up",
            raw_content="Late clustered visible body",
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
        visible_item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_text=raw.raw_content,
            url=raw.url,
            canonical_url=raw.url,
            content_hash=raw.content_hash,
        )
        session.add(visible_item)
        session.flush()
        with clustering_run(
            session,
            scope_type="late-uninterested-cluster-follow-up",
            item_ids=[visible_item.id],
            rule_version="late-uninterested-cluster-v1",
        ):
            session.add(ClusterItem(
                cluster_id=cluster_id,
                content_item_id=visible_item.id,
            ))
            session.flush()
        session.commit()
        visible_item_id = visible_item.id

    detail = client.get(f"/clusters/{cluster_id}").json()
    assert detail["item_count"] == 1
    assert [item["id"] for item in detail["items"]] == [visible_item_id]
    searched = client.get(
        "/clusters", params={"q": "Late clustered"}
    ).json()
    assert [item["id"] for item in searched[0]["items"]] == [visible_item_id]

    marked_event = client.post(
        "/uninterested",
        json={
            "operation_id": "c9999999-9999-4999-8999-999999999999",
            "target_type": "event",
            "event_uid": detail["event_uid"],
            "observed_revision_uid": detail["current_revision_uid"],
            "value": True,
        },
    )
    assert marked_event.status_code == 200
    bucket = client.get("/uninterested-targets").json()
    assert bucket["count"] == 1
    assert bucket["items"][0]["target_kind"] == "event"
