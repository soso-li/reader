from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime, timezone
from threading import Event as ThreadEvent
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from reader_api.clustering_run import clustering_run
from reader_api.db import Base, engine
from reader_api.digest import content_hash
from reader_api.event_synthesis import (
    EVIDENCE_REVIEW_OUTPUT_SCHEMA,
    SYNTHESIS_OUTPUT_SCHEMA,
    SynthesisValidationError,
    create_evidence_snapshot,
    save_evidence_review,
    synthesis_candidate_version_rows,
    synthesis_evidence_rows,
    synthesis_fingerprints_for_rows,
    validate_synthesis_output,
)
from reader_api.event_projection import (
    ProjectionEvidence,
    canonical_evidence_fingerprint,
)
from reader_api.main import app
from reader_api.generation_lifecycle import start_generation_attempt
from reader_api.report_generation import report_clusters
from reader_api.models import (
    Cluster,
    ClusterEventProjection,
    ClusterItem,
    ClusteringRun,
    ContentItem,
    Document,
    EvidenceReview,
    EvidenceReviewCitation,
    EvidenceSnapshot,
    EvidenceSnapshotMember,
    Event,
    EventEvidenceVersion,
    EventRevision,
    EventRevisionEvidence,
    EventUserState,
    GenerationApplication,
    GenerationAdmission,
    GenerationAttempt,
    GenerationControl,
    GenerationRequest,
    GenerationRequestPayload,
    GenerationResult,
    InteractionEvent,
    LLMTask,
    Source,
    SynthesisBlock,
    SynthesisCitation,
    SynthesisVersion,
    TopicGroup,
)
from tests.factories import assign_publishable_cluster, make_raw_entry


NOW = datetime(2026, 7, 16, 8, 0, tzinfo=timezone.utc)


def add_source_item(
    session, index: int, *, embedding_vector: str = "[1.0,0.0]"
) -> ContentItem:
    source = Source(
        name=f"Source {index}",
        url=f"https://example.com/{index}.xml",
        site_url=f"https://example.com/{index}",
        privacy_class="public",
        external_generation_allowed=True,
    )
    raw = make_raw_entry(
        source=source,
        external_id=f"story-{index}",
        title=f"Claim {index}",
        url=f"https://example.com/story/{index}",
        author=f"Author {index}",
        published_at=NOW,
        raw_content=f"Evidence body {index}",
    )
    document = Document(
        raw_entry=raw,
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
        published_at=NOW,
        content_hash=content_hash(raw.title, raw.raw_content, raw.url),
        embedding_vector=embedding_vector,
        embedding_model="test-synthesis-model",
    )
    session.add(item)
    session.flush()
    return item


def seed_multi_source_event(*, generation_enabled: bool = True) -> dict[str, object]:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        if generation_enabled:
            session.add(
                GenerationControl(
                    id=1,
                    global_pause=False,
                    auto_run=False,
                    daily_budget_tokens=10_000_000,
                    input_estimator="unicode-codepoints-v1",
                    output_reserve_tokens=0,
                    day_timezone="Asia/Shanghai",
                )
            )
        cluster = None
        items: list[ContentItem] = []
        item_ids: list[int] = []
        for index in range(1, 3):
            item = add_source_item(session, index)
            items.append(item)
            item_ids.append(item.id)
        with clustering_run(
            session,
            scope_type="event-synthesis-test",
            item_ids=item_ids,
            rule_version="event-synthesis-test-v1",
        ):
            for item in items:
                cluster = assign_publishable_cluster(session, item)
        session.commit()
        assert cluster is not None
        event = session.scalar(
            select(Event)
            .where(Event.current_revision_id.is_not(None))
            .order_by(Event.id.desc())
            .limit(1)
        )
        assert event is not None
        return {
            "event_id": event.id,
            "event_uid": event.uid,
            "revision_id": event.current_revision_id,
            "item_id": item_ids[0],
            "cluster_id": cluster.id,
        }


def add_new_source_evidence(
    fixture: dict[str, object], *, index: int, scope: str
) -> str:
    with sessionmaker(bind=engine)() as session:
        item = add_source_item(session, index)
        with clustering_run(
            session,
            scope_type=scope,
            item_ids=[item.id],
            rule_version=f"{scope}-v1",
        ):
            assigned = assign_publishable_cluster(session, item)
        event = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        assert event is not None and assigned.id == fixture["cluster_id"]
        revision = session.get(EventRevision, event.current_revision_id)
        assert revision is not None
        revision_uid = revision.uid
        session.commit()
        return revision_uid


def add_distinct_multi_source_event() -> str:
    with sessionmaker(bind=engine)() as session:
        items = [
            add_source_item(session, index, embedding_vector="[0.0,1.0]")
            for index in range(3, 5)
        ]
        with clustering_run(
            session,
            scope_type="event-synthesis-second-event",
            item_ids=[item.id for item in items],
            rule_version="event-synthesis-second-event-v1",
        ):
            for item in items:
                assign_publishable_cluster(session, item)
        event = session.scalar(select(Event).order_by(Event.id.desc()).limit(1))
        assert event is not None
        session.commit()
        return event.uid


def valid_blocks(version_uids: list[str]) -> list[dict[str, object]]:
    return [
        {
            "kind": "summary",
            "body": "两份来源共同确认了事件。",
            "citations": [
                {"evidence_version_uid": uid, "side": "support"} for uid in version_uids
            ],
        },
        {
            "kind": "fact",
            "body": "已确认的事实。",
            "citations": [{"evidence_version_uid": version_uids[0], "side": "support"}],
        },
        {
            "kind": "viewpoint",
            "body": "来源认为影响有限。",
            "attribution": "Source 1",
            "citations": [
                {"evidence_version_uid": version_uids[0], "side": "attributed"}
            ],
        },
        {
            "kind": "disagreement",
            "body": "两个来源对影响范围存在分歧。",
            "citations": [
                {"evidence_version_uid": version_uids[0], "side": "position_a"},
                {"evidence_version_uid": version_uids[1], "side": "position_b"},
            ],
        },
        {
            "kind": "uncertainty",
            "body": "后续影响仍未确定。",
            "citations": [
                {"evidence_version_uid": version_uids[1], "side": "uncertain"}
            ],
        },
    ]


def material_review_payload(task: dict[str, object]) -> dict[str, object]:
    request = task["request"]
    assert isinstance(request, dict)
    comparison = request["input_data"]
    assert isinstance(comparison, dict)
    target = comparison["target_evidence"]
    new = comparison["new_evidence"]
    assert isinstance(target, list) and isinstance(new, list)
    cited = new[-1] if new else target[-1]
    return {
        "result": "material",
        "reason": "固定目标包含实质变化。",
        "citations": [{"evidence_version_uid": cited["evidence_version_uid"]}],
        "synthesis": {
            "blocks": valid_blocks(
                [row["evidence_version_uid"] for row in target]
            )
        },
    }


def append_event_revision(
    session: Session,
    event: Event,
    links: list[EventRevisionEvidence],
    roles: list[str],
) -> EventRevision:
    current = session.get(EventRevision, event.current_revision_id)
    assert current is not None
    versions = [
        session.get(EventEvidenceVersion, link.evidence_version_id) for link in links
    ]
    assert all(version is not None for version in versions)
    projected = [
        ProjectionEvidence(
            version=version,
            evidence_type=link.evidence_type,
            role=role,
        )
        for link, version, role in zip(links, versions, roles, strict=True)
        if version is not None
    ]
    revision = EventRevision(
        uid=str(uuid4()),
        event_id=event.id,
        revision_no=current.revision_no + 1,
        evidence_fingerprint=canonical_evidence_fingerprint(projected),
        title_snapshot=current.title_snapshot,
        event_time_snapshot=current.event_time_snapshot,
    )
    session.add(revision)
    session.flush()
    session.add_all(
        EventRevisionEvidence(
            revision_id=revision.id,
            evidence_version_id=link.evidence_version_id,
            evidence_type=link.evidence_type,
            role=role,
        )
        for link, role in zip(links, roles, strict=True)
    )
    event.current_revision_id = revision.id
    session.flush()
    return revision


def recur_event_to_original_evidence(event_uid: str) -> tuple[str, str]:
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        event = session.scalar(select(Event).where(Event.uid == event_uid))
        assert event is not None
        original = session.get(EventRevision, event.current_revision_id)
        assert original is not None
        links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == original.id)
                .order_by(EventRevisionEvidence.id)
            )
        )
        original_roles = [link.role for link in links]
        changed_roles = list(original_roles)
        changed_roles[0] = (
            "challenge" if changed_roles[0] != "challenge" else "opinion"
        )
        append_event_revision(session, event, links, changed_roles)
        recurrent = append_event_revision(session, event, links, original_roles)
        session.commit()
        return original.uid, recurrent.uid


def clone_event_from_revision(session: Session, revision_id: int) -> Event:
    original = session.get(EventRevision, revision_id)
    assert original is not None
    links = list(
        session.scalars(
            select(EventRevisionEvidence)
            .where(EventRevisionEvidence.revision_id == original.id)
            .order_by(EventRevisionEvidence.id)
        )
    )
    event = Event(uid=str(uuid4()), status="active")
    session.add(event)
    session.flush()
    revision = EventRevision(
        uid=str(uuid4()),
        event_id=event.id,
        revision_no=1,
        evidence_fingerprint=original.evidence_fingerprint,
        title_snapshot=original.title_snapshot,
        event_time_snapshot=original.event_time_snapshot,
    )
    session.add(revision)
    session.flush()
    session.add_all(
        EventRevisionEvidence(
            revision_id=revision.id,
            evidence_version_id=link.evidence_version_id,
            evidence_type=link.evidence_type,
            role=link.role,
        )
        for link in links
    )
    event.current_revision_id = revision.id
    session.flush()
    return event


@pytest.mark.parametrize(
    "payload",
    [
        {
            "blocks": [
                {
                    "kind": "fact",
                    "body": "fact",
                    "citations": [
                        {"evidence_version_uid": "version-1", "side": "support"}
                    ],
                }
            ],
            "unexpected": True,
        },
        {
            "blocks": [
                {
                    "kind": "fact",
                    "body": "fact",
                    "citations": [
                        {"evidence_version_uid": "version-1", "side": "support"}
                    ],
                    "unexpected": True,
                }
            ]
        },
        {
            "blocks": [
                {
                    "kind": "fact",
                    "body": "fact",
                    "citations": [
                        {
                            "evidence_version_uid": "version-1",
                            "side": "support",
                            "unexpected": True,
                        }
                    ],
                }
            ]
        },
    ],
)
def test_synthesis_output_rejects_unknown_fields_at_every_level(
    payload: dict[str, object],
) -> None:
    with pytest.raises(SynthesisValidationError):
        validate_synthesis_output(payload, {"version-1"})

    block_schema = SYNTHESIS_OUTPUT_SCHEMA["properties"]["blocks"]["items"]
    citation_schema = block_schema["properties"]["citations"]["items"]
    assert SYNTHESIS_OUTPUT_SCHEMA["additionalProperties"] is False
    assert block_schema["additionalProperties"] is False
    assert citation_schema["additionalProperties"] is False


def version_uids_from_input(input_text: str) -> list[str]:
    data = json.loads(input_text)
    return [row["evidence_version_uid"] for row in data["evidence"]]


def generate_valid_synthesis(
    monkeypatch: pytest.MonkeyPatch, event_uid: str
) -> TestClient:
    def chat(_: object, model: str, __: str, input_text: str) -> dict[str, object]:
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    client = TestClient(app)
    response = client.post(
        f"/events/{event_uid}/synthesis", json={"provider": "local"}
    )
    assert response.status_code == 200
    return client


def mark_event_seen(
    client: TestClient,
    fixture: dict[str, object],
    revision_uid: str,
    operation_id: str,
) -> dict[str, object]:
    response = client.post(
        "/event-user-state",
        json={
            "event_uid": fixture["event_uid"],
            "observed_revision_uid": revision_uid,
            "operation_id": operation_id,
            "action": "read_status_set",
            "value": "summary_seen",
        },
    )
    assert response.status_code == 200
    return response.json()


def clear_current_synthesis_pointer(event_uid: str) -> None:
    """Make history fixtures enter the pre-synthesis path without faking LLM output."""
    with sessionmaker(bind=engine)() as session:
        event = session.scalar(select(Event).where(Event.uid == event_uid))
        assert event is not None
        event.current_synthesis_version_id = None
        session.commit()


def test_event_synthesis_is_missing_without_calling_a_model(monkeypatch) -> None:
    fixture = seed_multi_source_event()

    def fail(*_: object, **__: object) -> None:
        raise AssertionError("opening an Event must not call a model")

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", fail)
    body = TestClient(app).get(f"/events/{fixture['event_uid']}").json()

    assert body["synthesis"] == {
        "status": "missing",
        "current_revision_uid": body["current_revision"]["revision_uid"],
        "source_view_revision_uid": body["current_revision"]["revision_uid"],
        "covered_revision_uid": None,
        "reviewed_revision_uid": None,
        "new_source_count": 0,
        "unreviewed_evidence_count": 0,
        "unreviewed_source_count": 0,
        "target_revision_uid": body["current_revision"]["revision_uid"],
        "source_count": 2,
        "can_generate": True,
        "default_view": "source",
        "task_status": "idle",
        "task": None,
        "current": None,
    }


def test_direct_model_endpoints_hide_transport_diagnostics(monkeypatch) -> None:
    fixture = seed_multi_source_event()

    def fail(*_: object, **__: object) -> None:
        raise RuntimeError("connection refused: http://token@private.example/models")

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", fail)
    client = TestClient(app)
    responses = [
        client.post(
            f"/clusters/{fixture['cluster_id']}/synthesize",
            json={"provider": "local"},
        ),
        client.get(
            "/assistant",
            params={"q": "发生了什么", "cluster_id": fixture["cluster_id"]},
        ),
        client.post(
            "/ai/chat",
            json={"model_type": "llm", "input": "ping"},
        ),
    ]

    assert all(response.status_code == 502 for response in responses)
    assert all(
        response.json()["detail"] == "本地模型服务未连接，请检查 LM Studio"
        for response in responses
    )
    assert all("token@private.example" not in response.text for response in responses)

    saved = client.patch(
        "/ai/settings",
        json={
            "synthesis_remote_base_url": "https://api.example.com",
            "synthesis_remote_model": "remote-model",
            "synthesis_remote_api_key": "secret",
            "translation_provider": "openai_compatible",
            "translation_base_url": "https://translate.example.com",
            "translation_model": "translation-model",
            "translation_api_key": "secret",
        },
    )
    assert saved.status_code == 200
    monkeypatch.setattr(
        "reader_api.llm.OpenAICompatibleChatProvider.chat",
        fail,
    )
    remote = client.post(
        f"/clusters/{fixture['cluster_id']}/synthesize",
        json={"provider": "openai_compatible"},
    )
    translation = client.post(
        "/ai/chat",
        json={"model_type": "translation", "input": "ping"},
    )
    assert remote.json()["detail"] == "云端合成服务不可用，请检查地址、模型和密钥"
    assert translation.json()["detail"] == "云端翻译服务不可用，请检查地址、模型和密钥"
    assert "token@private.example" not in remote.text + translation.text
    with sessionmaker(bind=engine)() as session:
        assert {
            attempt.error for attempt in session.scalars(select(GenerationAttempt))
        } == {
            "本地模型服务未连接，请检查 LM Studio",
            "云端合成服务不可用，请检查地址、模型和密钥",
        }


def test_source_pause_preserves_event_synthesis_topology_and_embeddings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    before_detail = client.get(f"/events/{fixture['event_uid']}").json()

    def stored_state() -> dict[str, tuple[object, ...]]:
        with sessionmaker(bind=engine)() as session:
            return {
                "memberships": tuple(
                    session.execute(
                        select(
                            ClusterItem.cluster_id,
                            ClusterItem.content_item_id,
                        ).order_by(
                            ClusterItem.cluster_id,
                            ClusterItem.content_item_id,
                        )
                    ).all()
                ),
                "embeddings": tuple(
                    session.execute(
                        select(
                            ContentItem.id,
                            ContentItem.embedding_vector,
                            ContentItem.embedding_model,
                        ).order_by(ContentItem.id)
                    ).all()
                ),
                "events": tuple(session.scalars(select(Event.uid).order_by(Event.id))),
                "revisions": tuple(
                    session.scalars(select(EventRevision.uid).order_by(EventRevision.id))
                ),
                "syntheses": tuple(
                    session.scalars(
                        select(SynthesisVersion.uid).order_by(SynthesisVersion.id)
                    )
                ),
                "runs": tuple(
                    session.scalars(select(ClusteringRun.id).order_by(ClusteringRun.id))
                ),
            }

    before_state = stored_state()
    source_ids = [row["id"] for row in client.get("/sources").json()]
    assert len(source_ids) == 2

    paused = client.patch(f"/sources/{source_ids[0]}", json={"enabled": False})
    assert paused.status_code == 200
    after_detail = client.get(f"/events/{fixture['event_uid']}").json()
    listed = next(
        row for row in client.get("/clusters").json()
        if row["id"] == fixture["cluster_id"]
    )

    assert after_detail["current_revision"]["revision_uid"] == before_detail[
        "current_revision"
    ]["revision_uid"]
    assert after_detail["synthesis"]["current"]["version_uid"] == before_detail[
        "synthesis"
    ]["current"]["version_uid"]
    assert after_detail["synthesis"]["source_count"] == 2
    assert listed["event_uid"] == fixture["event_uid"]
    assert listed["item_count"] == 2
    assert stored_state() == before_state

    restored = client.patch(f"/sources/{source_ids[0]}", json={"enabled": True})
    assert restored.status_code == 200
    assert stored_state() == before_state


def test_future_synthesis_excludes_filtered_evidence_without_rewriting_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = seed_multi_source_event()
    add_new_source_evidence(
        fixture,
        index=3,
        scope="event-synthesis-filtered-evidence",
    )
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    original = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    original_uid = original["current"]["version_uid"]

    filtered = client.post(
        "/filter-rules",
        json={"match_type": "literal", "pattern": "Evidence body 1"},
    )
    assert filtered.status_code == 200
    assert filtered.json()["match_count"] == 1

    detail = client.get(f"/events/{fixture['event_uid']}").json()
    listed = next(
        row for row in client.get("/clusters").json()
        if row["id"] == fixture["cluster_id"]
    )
    assert detail["synthesis"]["current"]["version_uid"] == original_uid
    assert listed["synthesis_freshness"]["status"] == detail["synthesis"]["status"]

    with sessionmaker(bind=engine)() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        original_version = session.scalar(
            select(SynthesisVersion).where(SynthesisVersion.uid == original_uid)
        )
        assert event is not None and original_version is not None
        revision = session.get(EventRevision, event.current_revision_id)
        assert revision is not None

        original_members = list(
            session.scalars(
                select(EvidenceSnapshotMember).where(
                    EvidenceSnapshotMember.snapshot_id == original_version.snapshot_id
                )
            )
        )
        snapshot, evidence, source_count, _fingerprint = create_evidence_snapshot(
            session, event, revision
        )
        new_members = list(
            session.scalars(
                select(EvidenceSnapshotMember).where(
                    EvidenceSnapshotMember.snapshot_id == snapshot.id
                )
            )
        )

        assert len(original_members) == 3
        assert len(new_members) == 2
        assert source_count == 2
        assert {row["title"] for row in evidence} == {"Claim 2", "Claim 3"}
        assert event.current_synthesis_version_id == original_version.id


def test_external_generation_blocks_unclassified_sources_before_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = seed_multi_source_event()
    client = TestClient(app)
    source_ids = [source["id"] for source in client.get("/sources").json()]
    assert client.post(
        "/sources/bulk",
        json={
            "ids": source_ids,
            "set": {"privacy_class": "unclassified"},
        },
    ).status_code == 200
    assert client.patch(
        "/ai/settings",
        json={
            "synthesis_remote_base_url": "https://api.example.com",
            "synthesis_remote_model": "remote-model",
            "synthesis_remote_api_key": "secret",
        },
    ).status_code == 200

    blocked = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "openai_compatible"},
    )

    assert blocked.status_code == 403, blocked.text
    assert blocked.json()["detail"] == "包含未分类来源，不能发送给外部生成服务"
    tasks = client.get("/generation/tasks").json()
    assert len(tasks) == 1
    assert tasks[0]["status"] == "blocked"
    assert tasks[0]["privacy_status"] == "blocked"
    assert tasks[0]["privacy_reason"] == blocked.json()["detail"]
    assert tasks[0]["input_tokens_estimated"] is None
    assert tasks[0]["output_tokens_reserved"] is None
    approval = client.post(
        f"/generation/requests/{tasks[0]['request_uid']}/approve"
    )
    assert approval.status_code == 409
    assert approval.json()["detail"] == blocked.json()["detail"]
    assert {
        (source["source_name"], source["privacy_class"], source["external_generation_allowed"])
        for source in tasks[0]["sources"]
    } == {
        ("Source 1", "unclassified", False),
        ("Source 2", "unclassified", False),
    }
    assert "Evidence body" not in json.dumps(tasks, ensure_ascii=False)
    with sessionmaker(bind=engine)() as session:
        assert session.query(GenerationRequest).count() == 1
        assert session.query(GenerationRequestPayload).count() == 0
        assert session.query(GenerationAttempt).count() == 0
        assert session.query(EvidenceSnapshot).count() == 0
        assert session.query(LLMTask).count() == 0

    def local_chat(_: object, __: str, ___: str, input_text: str) -> dict[str, object]:
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", local_chat)
    local = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert local.status_code == 200
    assert local.json()["status"] == "current"



def test_remote_generation_blocks_mixed_private_sources_then_uses_generation_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = seed_multi_source_event()
    client = TestClient(app)
    sources = client.get("/sources").json()
    source_ids = [source["id"] for source in sources]
    assert client.post(
        "/sources/bulk",
        json={
            "ids": source_ids,
            "set": {
                "privacy_class": "public",
                "external_generation_allowed": True,
            },
        },
    ).status_code == 200
    assert client.patch(
        f"/sources/{source_ids[0]}", json={"privacy_class": "private"}
    ).status_code == 200
    assert client.patch(
        "/ai/settings",
        json={
            "synthesis_remote_base_url": "https://api.example.com",
            "synthesis_remote_model": "remote-model",
            "synthesis_remote_api_key": "secret",
        },
    ).status_code == 200

    def fail(*_: object, **__: object) -> None:
        raise AssertionError("私密来源不得调用远端模型")

    monkeypatch.setattr("reader_api.llm.OpenAICompatibleChatProvider.chat", fail)
    blocked = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "openai_compatible"},
    )
    assert blocked.status_code == 403
    assert blocked.json()["detail"] == "包含私密来源，只能使用本地 LM Studio"
    with sessionmaker(bind=engine)() as session:
        assert session.query(GenerationRequestPayload).count() == 0
        assert session.query(EvidenceSnapshot).count() == 0

    assert client.patch(
        f"/sources/{source_ids[0]}",
        json={"privacy_class": "public", "external_generation_allowed": True},
    ).status_code == 200

    def remote_chat(
        _: object, __: str, ___: str, input_text: str
    ) -> dict[str, object]:
        return {
            "output": json.dumps(
                {"blocks": valid_blocks(version_uids_from_input(input_text))},
                ensure_ascii=False,
            ),
            "usage": {"input_tokens": 80, "output_tokens": 25},
        }

    monkeypatch.setattr(
        "reader_api.llm.OpenAICompatibleChatProvider.chat", remote_chat
    )
    generated = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "openai_compatible"},
    )
    assert generated.status_code == 200, generated.text
    assert generated.json()["current"]["provider"] == "openai_compatible"
    assert generated.json()["task"]["task_type"] == "event-synthesis"
    assert generated.json()["task"]["status"] == "complete"
    with sessionmaker(bind=engine)() as session:
        assert session.query(GenerationRequest).count() == 2
        assert session.query(GenerationRequestPayload).count() == 1
        assert session.query(GenerationAttempt).count() == 1
        assert session.query(GenerationResult).count() == 1
        assert session.query(GenerationApplication).filter_by(status="applied").count() == 1
        assert session.query(LLMTask).count() == 0




def test_reconciliation_revision_and_read_paths_never_create_model_work(
    monkeypatch,
) -> None:
    def fail(*_: object, **__: object) -> None:
        raise AssertionError("deterministic Event paths must not call a model")

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", fail)
    monkeypatch.setattr("reader_api.llm.OpenAICompatibleChatProvider.chat", fail)
    fixture = seed_multi_source_event()
    client = TestClient(app)
    assert client.get("/clusters").status_code == 200
    detail = client.get(f"/events/{fixture['event_uid']}")
    assert detail.status_code == 200
    revision_uid = detail.json()["current_revision"]["revision_uid"]
    assert client.get(
        f"/events/{fixture['event_uid']}/revisions/{revision_uid}"
    ).status_code == 200

    Session = sessionmaker(bind=engine)
    with Session() as session:
        item = session.get(ContentItem, fixture["item_id"])
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert item is not None and event is not None
        old_revision_id = event.current_revision_id
        with clustering_run(
            session,
            scope_type="event-synthesis-zero-model",
            item_ids=[item.id],
            rule_version="event-synthesis-zero-model-v1",
        ):
            item.content_text = "Changed evidence without semantic adjudication"
            item.content_hash = content_hash(item.title, item.content_text, item.url)
        session.commit()
        session.refresh(event)
        assert event.current_revision_id != old_revision_id
        assert session.query(LLMTask).count() == 0
        assert session.query(EvidenceSnapshot).count() == 0

    assert client.get(f"/events/{fixture['event_uid']}").status_code == 200
    with Session() as session:
        assert session.query(LLMTask).count() == 0


def test_local_generation_freezes_evidence_and_publishes_cited_blocks(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    seen_inputs: list[dict[str, object]] = []

    def chat(_: object, model: str, __: str, input_text: str) -> dict[str, object]:
        seen_inputs.append(json.loads(input_text))
        with sessionmaker(bind=engine)() as audit:
            attempt = audit.scalar(select(GenerationAttempt))
            request = audit.scalar(select(GenerationRequest))
            event = audit.scalar(
                select(Event).where(Event.uid == fixture["event_uid"])
            )
            assert attempt is not None and attempt.status == "running"
            assert request is not None and request.provider == "local"
            assert event is not None and event.current_synthesis_version_id is None
            assert audit.query(EvidenceSnapshot).count() == 1
            assert audit.query(EvidenceSnapshotMember).count() == 2
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    client = TestClient(app)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "current"
    assert body["task_status"] == "complete"
    assert body["current"]["provider"] == "local"
    assert [block["kind"] for block in body["current"]["blocks"]] == [
        "summary",
        "fact",
        "viewpoint",
        "disagreement",
        "uncertainty",
    ]
    assert len(seen_inputs) == 1
    assert seen_inputs[0]["target_revision_uid"] == body["target_revision_uid"]
    assert len(seen_inputs[0]["evidence"]) == 2

    Session = sessionmaker(bind=engine)
    with Session() as session:
        snapshot = session.scalar(select(EvidenceSnapshot))
        version = session.scalar(select(SynthesisVersion))
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert snapshot is not None and version is not None and event is not None
        frozen = list(
            session.scalars(
                select(EvidenceSnapshotMember.evidence_version_id)
                .where(EvidenceSnapshotMember.snapshot_id == snapshot.id)
                .order_by(EvidenceSnapshotMember.position)
            )
        )
        assert len(frozen) == 2
        assert version.snapshot_id == snapshot.id
        assert body["current"]["snapshot_created_at"].rstrip("Z") == (
            snapshot.created_at.isoformat()
        )
        assert event.current_synthesis_version_id == version.id
        assert session.query(SynthesisBlock).count() == 5
        assert session.query(SynthesisCitation).count() == 7
        request = session.scalar(select(GenerationRequest))
        payload = session.scalar(select(GenerationRequestPayload))
        attempt = session.scalar(select(GenerationAttempt))
        result = session.scalar(select(GenerationResult))
        application = session.scalar(select(GenerationApplication))
        assert request is not None and payload is not None
        assert attempt is not None and attempt.status == "complete"
        assert result is not None and application is not None
        assert application.status == "applied"
        assert payload.payload_json["input_data"]["generation_fingerprint"] == (
            version.generation_fingerprint
        )
        assert session.query(LLMTask).count() == 0


def test_local_generation_exposes_request_result_and_application_audit(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()

    def chat(_: object, model: str, __: str, input_text: str) -> dict[str, object]:
        return {
            "output": json.dumps(
                {"blocks": valid_blocks(version_uids_from_input(input_text))},
                ensure_ascii=False,
            ),
            "usage": {"input_tokens": 321, "output_tokens": 87},
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    client = TestClient(app)

    generated = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )

    assert generated.status_code == 200
    task = generated.json()["task"]
    assert task["task_type"] == "event-synthesis"
    assert task["reason"] == "explicit-user-request"
    assert task["target_type"] == "event"
    assert task["target_uid"] == fixture["event_uid"]
    assert task["provider"] == "local"
    assert task["status"] == "complete"
    assert task["application_status"] == "applied"
    assert task["result_uid"]
    assert task["input_tokens"] == 321
    assert task["output_tokens"] == 87
    assert task["created_at"]
    assert task["started_at"]
    assert task["finished_at"]
    assert task["error"] == ""

    overview = client.get("/generation/tasks")
    assert overview.status_code == 200
    assert overview.json() == [task]


def test_local_generation_is_blocked_by_default_pause_before_attempt(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event(generation_enabled=False)

    def fail(*_: object, **__: object) -> None:
        raise AssertionError("paused admission must not call the provider")

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", fail)
    response = TestClient(app).post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "local"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "missing"
    assert body["task_status"] == "blocked"
    assert body["task"]["status"] == "blocked"
    assert body["task"]["approval_status"] == "approved"
    assert body["task"]["admission_status"] == "blocked_paused"
    assert body["task"]["admission_reason"] == "生成任务已全局暂停"
    assert body["task"]["input_tokens_estimated"] > 0
    assert body["task"]["output_tokens_reserved"] == 0
    assert body["task"]["input_tokens"] is None
    assert body["task"]["output_tokens"] is None
    with sessionmaker(bind=engine)() as session:
        assert session.query(GenerationRequest).count() == 1
        assert session.query(GenerationAdmission).count() == 1
        assert session.query(GenerationAttempt).count() == 0
        assert session.query(GenerationResult).count() == 0
        assert session.query(LLMTask).count() == 0


def test_local_generation_is_blocked_before_attempt_when_budget_is_unconfigured(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event(generation_enabled=False)
    client = TestClient(app)
    assert client.patch(
        "/generation/control", json={"global_pause": False}
    ).status_code == 200

    def fail(*_: object, **__: object) -> None:
        raise AssertionError("unconfigured budget must not call the provider")

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", fail)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "local"},
    )

    assert response.status_code == 200
    task = response.json()["task"]
    assert task["status"] == "blocked"
    assert task["approval_status"] == "approved"
    assert task["admission_status"] == "blocked_budget_unconfigured"
    assert task["admission_reason"] == "每日 Token 预算尚未配置"
    with sessionmaker(bind=engine)() as session:
        assert session.query(GenerationAttempt).count() == 0
        assert session.query(GenerationResult).count() == 0


def test_configured_budget_admits_one_attempt_and_accounts_actual_usage(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = TestClient(app)
    configured = client.patch(
        "/generation/control",
        json={
            "global_pause": False,
            "daily_budget_tokens": 100_000,
            "output_reserve_tokens": 200,
        },
    )
    assert configured.status_code == 200

    def chat(_: object, __: str, ___: str, input_text: str) -> dict[str, object]:
        return {
            "output": json.dumps(
                {"blocks": valid_blocks(version_uids_from_input(input_text))},
                ensure_ascii=False,
            ),
            "usage": {"input_tokens": 321, "output_tokens": 87},
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "local"},
    )

    assert response.status_code == 200
    task = response.json()["task"]
    assert task["status"] == "complete"
    assert task["approval_status"] == "consumed"
    assert task["admission_status"] == "admitted"
    assert task["admission_reason"] == ""
    assert task["input_tokens_estimated"] > 0
    assert task["output_tokens_reserved"] == 200
    assert task["input_tokens"] == 321
    assert task["output_tokens"] == 87
    with sessionmaker(bind=engine)() as session:
        attempt = session.scalar(select(GenerationAttempt))
        assert attempt is not None
        assert attempt.estimator_version == "unicode-codepoints-v1"
        assert attempt.input_tokens_estimated == task["input_tokens_estimated"]
        assert attempt.output_tokens_reserved == 200
        assert attempt.input_tokens_actual == 321
        assert attempt.output_tokens_actual == 87
    control = client.get("/generation/control").json()
    assert control["used_tokens"] == 408
    assert control["reserved_tokens"] == 0
    assert control["remaining_tokens"] == 100_000 - 408
    repeated_approval = client.post(
        f"/generation/requests/{task['request_uid']}/approve"
    )
    assert repeated_approval.status_code == 409
    assert repeated_approval.json()["detail"] == "已有生成结果，无需再次批准"


def test_insufficient_budget_blocks_before_attempt(monkeypatch) -> None:
    fixture = seed_multi_source_event()
    client = TestClient(app)
    assert client.patch(
        "/generation/control",
        json={
            "global_pause": False,
            "daily_budget_tokens": 1,
            "output_reserve_tokens": 0,
        },
    ).status_code == 200

    def fail(*_: object, **__: object) -> None:
        raise AssertionError("insufficient budget must not call the provider")

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", fail)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "local"},
    )

    assert response.status_code == 200
    task = response.json()["task"]
    assert task["status"] == "blocked"
    assert task["approval_status"] == "approved"
    assert task["admission_status"] == "blocked_budget"
    assert task["admission_reason"] == "今日 Token 预算不足"
    with sessionmaker(bind=engine)() as session:
        assert session.query(GenerationAttempt).count() == 0
        assert session.query(GenerationResult).count() == 0


def test_completed_unknown_usage_stays_unknown_and_releases_reserve(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = TestClient(app)
    assert client.patch(
        "/generation/control",
        json={
            "global_pause": False,
            "daily_budget_tokens": 100_000,
            "output_reserve_tokens": 200,
        },
    ).status_code == 200

    def chat(_: object, __: str, ___: str, input_text: str) -> dict[str, object]:
        return {
            "output": json.dumps(
                {"blocks": valid_blocks(version_uids_from_input(input_text))},
                ensure_ascii=False,
            )
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "local"},
    )

    assert response.status_code == 200
    task = response.json()["task"]
    assert task["status"] == "complete"
    assert task["input_tokens"] is None
    assert task["output_tokens"] is None
    with sessionmaker(bind=engine)() as session:
        attempt = session.scalar(select(GenerationAttempt))
        assert attempt is not None
        assert attempt.input_tokens_actual is None
        assert attempt.output_tokens_actual is None
    control = client.get("/generation/control").json()
    assert control["used_tokens"] == 0
    assert control["reserved_tokens"] == 0
    assert control["remaining_tokens"] == 100_000


def test_failed_generation_accounts_provider_usage_without_guessing_unknowns(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = TestClient(app)
    assert client.patch(
        "/generation/control",
        json={
            "global_pause": False,
            "daily_budget_tokens": 100_000,
            "output_reserve_tokens": 200,
        },
    ).status_code == 200

    def chat(*_: object, **__: object) -> dict[str, object]:
        return {
            "output": json.dumps({"blocks": []}),
            "usage": {"input_tokens": 321, "output_tokens": 87},
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "local"},
    )

    assert response.status_code == 502
    with sessionmaker(bind=engine)() as session:
        attempt = session.scalar(select(GenerationAttempt))
        assert attempt is not None and attempt.status == "failed"
        assert attempt.input_tokens_actual == 321
        assert attempt.output_tokens_actual == 87
    task = client.get("/generation/tasks").json()[0]
    assert task["input_tokens"] == 321
    assert task["output_tokens"] == 87
    control = client.get("/generation/control").json()
    assert control["used_tokens"] == 408
    assert control["reserved_tokens"] == 0
    assert control["remaining_tokens"] == 100_000 - 408


def test_failed_generation_discards_provider_usage_outside_database_range(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = TestClient(app)
    assert client.patch(
        "/generation/control",
        json={"global_pause": False, "daily_budget_tokens": 100_000},
    ).status_code == 200

    def chat(*_: object, **__: object) -> dict[str, object]:
        return {
            "output": json.dumps({"blocks": []}),
            "usage": {
                "input_tokens": 2_147_483_648,
                "output_tokens": 87,
            },
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "local"},
    )

    assert response.status_code == 502
    with sessionmaker(bind=engine)() as session:
        attempt = session.scalar(select(GenerationAttempt))
        assert attempt is not None and attempt.status == "failed"
        assert attempt.input_tokens_actual is None
        assert attempt.output_tokens_actual == 87
    control = client.get("/generation/control").json()
    assert control["used_tokens"] == 87
    assert control["reserved_tokens"] == 0


def test_new_blocked_decision_does_not_mix_an_older_failed_attempt_usage(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = TestClient(app)

    def chat(*_: object, **__: object) -> dict[str, object]:
        return {
            "output": json.dumps({"blocks": []}),
            "usage": {"input_tokens": 321, "output_tokens": 87},
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    failed = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "local"},
    )
    assert failed.status_code == 502
    assert client.patch(
        "/generation/control", json={"global_pause": True}
    ).status_code == 200

    blocked = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "local"},
    )

    assert blocked.status_code == 200
    task = blocked.json()["task"]
    assert task["status"] == "blocked"
    assert task["admission_status"] == "blocked_paused"
    assert task["input_tokens"] is None
    assert task["output_tokens"] is None
    assert task["started_at"] is None
    assert task["finished_at"] is None
    assert task["error"] == ""
    with sessionmaker(bind=engine)() as session:
        attempt = session.scalar(select(GenerationAttempt))
        assert attempt is not None and attempt.status == "failed"
        assert attempt.input_tokens_actual == 321
        assert attempt.output_tokens_actual == 87


def test_actual_usage_over_reserve_blocks_the_next_request(monkeypatch) -> None:
    fixture = seed_multi_source_event()
    second_event_uid = add_distinct_multi_source_event()
    assert second_event_uid != fixture["event_uid"]
    client = TestClient(app)
    assert client.patch(
        "/generation/control",
        json={
            "global_pause": False,
            "daily_budget_tokens": 100_000,
            "output_reserve_tokens": 100,
        },
    ).status_code == 200
    calls = 0

    def chat(_: object, __: str, ___: str, input_text: str) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {
            "output": json.dumps(
                {"blocks": valid_blocks(version_uids_from_input(input_text))},
                ensure_ascii=False,
            ),
            "usage": {"input_tokens": 99_900, "output_tokens": 200},
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    first = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "local"},
    )
    second = client.post(
        f"/events/{second_event_uid}/synthesis",
        json={"provider": "local"},
    )

    assert first.status_code == 200
    assert first.json()["task"]["status"] == "complete"
    assert second.status_code == 200
    assert second.json()["task"]["status"] == "blocked"
    assert second.json()["task"]["admission_status"] == "blocked_budget"
    assert calls == 1
    with sessionmaker(bind=engine)() as session:
        attempts = session.scalars(
            select(GenerationAttempt).order_by(GenerationAttempt.id)
        ).all()
        assert len(attempts) == 1
        assert attempts[0].input_tokens_estimated is not None
        assert attempts[0].output_tokens_reserved == 100
        assert attempts[0].input_tokens_actual == 99_900
        assert attempts[0].output_tokens_actual == 200
    control = client.get("/generation/control").json()
    assert control["used_tokens"] == 100_100
    assert control["remaining_tokens"] == 0


def test_global_pause_blocks_new_work_without_removing_completed_artifacts(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    second_event_uid = add_distinct_multi_source_event()
    client = TestClient(app)
    calls = 0

    def chat(_: object, __: str, ___: str, input_text: str) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {
            "output": json.dumps(
                {"blocks": valid_blocks(version_uids_from_input(input_text))},
                ensure_ascii=False,
            ),
            "usage": {"input_tokens": 100, "output_tokens": 50},
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    completed = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "local"},
    )
    assert completed.status_code == 200
    assert completed.json()["task"]["status"] == "complete"

    paused = client.patch("/generation/control", json={"global_pause": True})
    assert paused.status_code == 200
    blocked = client.post(
        f"/events/{second_event_uid}/synthesis",
        json={"provider": "local"},
    )

    assert blocked.status_code == 200
    assert blocked.json()["task"]["admission_status"] == "blocked_paused"
    assert calls == 1
    with sessionmaker(bind=engine)() as session:
        first_event = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        assert first_event is not None
        assert first_event.current_synthesis_version_id is not None
        assert session.query(GenerationRequest).count() == 2
        assert session.query(GenerationAttempt).count() == 1
        assert session.query(GenerationResult).count() == 1
        assert session.query(SynthesisVersion).count() == 1


def test_omitted_local_provider_uses_admission_and_respects_default_pause(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event(generation_enabled=False)

    def chat(*_: object, **__: object) -> dict[str, object]:
        raise AssertionError("default pause must block the provider")

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    response = TestClient(app).post(
        f"/events/{fixture['event_uid']}/synthesis", json={}
    )

    assert response.status_code == 200
    assert response.json()["task"]["admission_status"] == "blocked_paused"
    with sessionmaker(bind=engine)() as session:
        assert session.query(GenerationRequest).count() == 1
        assert session.query(GenerationAttempt).count() == 0
        assert session.query(LLMTask).count() == 0



def test_local_generation_reuses_request_until_evidence_changes(monkeypatch) -> None:
    fixture = seed_multi_source_event()
    calls = 0

    def chat(_: object, model: str, __: str, input_text: str) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    client = TestClient(app)
    event_url = f"/events/{fixture['event_uid']}"

    first = client.post(f"{event_url}/synthesis", json={"provider": "local"})
    repeated = client.post(f"{event_url}/synthesis", json={"provider": "local"})

    assert first.status_code == repeated.status_code == 200
    assert calls == 1
    first_task = client.get("/generation/tasks").json()
    assert len(first_task) == 1

    with sessionmaker(bind=engine)() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None
        links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == event.current_revision_id)
                .order_by(EventRevisionEvidence.id)
            )
        )
        roles = [link.role for link in links]
        roles[0] = "challenge"
        append_event_revision(session, event, links, roles)
        event.current_synthesis_version_id = None
        session.commit()

    changed = client.post(f"{event_url}/synthesis", json={"provider": "local"})

    assert changed.status_code == 200
    assert calls == 2
    tasks = client.get("/generation/tasks").json()
    assert len(tasks) == 2
    assert tasks[0]["request_uid"] != tasks[1]["request_uid"]


def test_local_result_survives_apply_failure_and_is_reused(monkeypatch) -> None:
    from reader_api import event_synthesis as event_synthesis_module

    fixture = seed_multi_source_event()
    calls = 0

    def chat(_: object, model: str, __: str, input_text: str) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    original_save = event_synthesis_module.save_synthesis_version

    def fail_apply(*_args: object, **_kwargs: object) -> None:
        raise SQLAlchemyError("forced apply failure")

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    monkeypatch.setattr(event_synthesis_module, "save_synthesis_version", fail_apply)
    client = TestClient(app)
    event_url = f"/events/{fixture['event_uid']}"

    failed = client.post(f"{event_url}/synthesis", json={"provider": "local"})

    assert failed.status_code == 503
    frozen = client.get("/generation/tasks").json()[0]
    assert frozen["status"] == "apply_failed"
    assert frozen["application_status"] == "failed"
    assert frozen["result_uid"]
    assert client.get(event_url).json()["synthesis"]["status"] == "missing"

    monkeypatch.setattr(event_synthesis_module, "save_synthesis_version", original_save)

    def no_second_call(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("a valid frozen Result must be reused")

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", no_second_call)
    retried = client.post(f"{event_url}/synthesis", json={"provider": "local"})

    assert retried.status_code == 200
    assert retried.json()["status"] == "current"
    assert calls == 1
    applied = client.get("/generation/tasks").json()[0]
    assert applied["request_uid"] == frozen["request_uid"]
    assert applied["result_uid"] == frozen["result_uid"]
    assert applied["application_status"] == "applied"


def test_added_evidence_is_unreviewed_without_model_or_new_artifacts(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    initial = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    initial_version_uid = initial["current"]["version_uid"]
    initial_snapshot_uid = initial["current"]["snapshot_uid"]
    initial_revision_uid = initial["current"]["target_revision_uid"]
    initial_citations = initial["current"]["blocks"][0]["citations"]

    def fail(*_: object, **__: object) -> None:
        raise AssertionError("detecting unreviewed evidence must not call a model")

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", fail)
    monkeypatch.setattr(
        "reader_api.llm.OpenAICompatibleChatProvider.chat", fail
    )
    Session = sessionmaker(bind=engine)
    with Session() as session:
        artifact_counts = (
            session.query(EvidenceSnapshot).count(),
            session.query(SynthesisVersion).count(),
            session.query(LLMTask).count(),
        )
        item = add_source_item(session, 3)
        with clustering_run(
            session,
            scope_type="event-synthesis-unreviewed",
            item_ids=[item.id],
            rule_version="event-synthesis-unreviewed-v1",
        ):
            assigned = assign_publishable_cluster(session, item)
        session.commit()
        event = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        assert event is not None
        assert assigned.id == fixture["cluster_id"]
        assert event.current_revision_id != fixture["revision_id"]
        current_revision_uid = session.get(
            EventRevision, event.current_revision_id
        ).uid

    event_detail = client.get(f"/events/{fixture['event_uid']}")
    cluster_detail = client.get(f"/clusters/{fixture['cluster_id']}")
    cluster_list = client.get("/clusters")

    assert event_detail.status_code == cluster_detail.status_code == 200
    assert cluster_list.status_code == 200
    state = event_detail.json()["synthesis"]
    assert state["status"] == "unreviewed"
    assert state["target_revision_uid"] == current_revision_uid
    assert state["current_revision_uid"] == current_revision_uid
    assert state["covered_revision_uid"] == initial_revision_uid
    assert state["unreviewed_evidence_count"] == 1
    assert state["unreviewed_source_count"] == 1
    assert state["source_count"] == 3
    assert state["task_status"] == "complete"
    assert state["current"]["version_uid"] == initial_version_uid
    assert state["current"]["snapshot_uid"] == initial_snapshot_uid
    assert state["current"]["source_count"] == 2
    assert state["current"]["blocks"][0]["citations"] == initial_citations
    assert cluster_detail.json()["synthesis"]["status"] == "unreviewed"
    listed = next(
        cluster
        for cluster in cluster_list.json()
        if cluster["id"] == fixture["cluster_id"]
    )
    expected_freshness = {
        "status": "unreviewed",
        "current_revision_uid": current_revision_uid,
        "covered_revision_uid": initial_revision_uid,
        "reviewed_revision_uid": initial_revision_uid,
        "new_source_count": 0,
        "unreviewed_evidence_count": 1,
        "unreviewed_source_count": 1,
    }
    assert listed["synthesis_freshness"] == expected_freshness
    assert cluster_detail.json()["synthesis_freshness"] == expected_freshness
    assert {
        key: state[key] for key in expected_freshness
    } == expected_freshness

    revision = client.get(
        f"/events/{fixture['event_uid']}/revisions/{current_revision_uid}"
    )
    assert revision.status_code == 200
    assert len(revision.json()["evidence"]) == 3
    with Session() as session:
        assert (
            session.query(EvidenceSnapshot).count(),
            session.query(SynthesisVersion).count(),
            session.query(LLMTask).count(),
        ) == artifact_counts


def test_local_ordinary_review_advances_only_the_reviewed_watermark(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    before = client.get(f"/events/{fixture['event_uid']}").json()
    old_synthesis = before["synthesis"]["current"]
    observed_revision_uid = before["current_revision"]["revision_uid"]
    for action, value in (
        ("read_status_set", "summary_seen"),
        ("starred_set", True),
        ("read_later_set", True),
    ):
        mutation = client.post(
            "/event-user-state",
            json={
                "event_uid": fixture["event_uid"],
                "observed_revision_uid": observed_revision_uid,
                "operation_id": str(uuid4()),
                "action": action,
                "value": value,
            },
        )
        assert mutation.status_code == 200

    Session = sessionmaker(bind=engine)
    with Session() as session:
        item = add_source_item(session, 3)
        with clustering_run(
            session,
            scope_type="event-synthesis-ordinary-review",
            item_ids=[item.id],
            rule_version="event-synthesis-ordinary-review-v1",
        ):
            assign_publishable_cluster(session, item)
        session.commit()
        event = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        assert event is not None
        current_revision = session.get(EventRevision, event.current_revision_id)
        assert current_revision is not None
        target_revision_uid = current_revision.uid
        synthesis_pointer = event.current_synthesis_version_id
        interaction_count = session.query(InteractionEvent).count()
        topic_count = session.query(TopicGroup).count()
        state_before = session.scalar(
            select(EventUserState).where(EventUserState.event_id == event.id)
        )
        assert state_before is not None
        user_state_before = (
            state_before.seen_revision_id,
            state_before.read_status,
            state_before.starred,
            state_before.read_later,
        )
        source_rows_before = list(
            session.execute(
                select(
                    Source.id,
                    Source.name,
                    Source.url,
                    Source.status,
                    Source.created_at,
                ).order_by(Source.id)
            )
        )
    order_before = [row["id"] for row in client.get("/clusters").json()]
    unread_before = [
        row["id"]
        for row in client.get("/clusters", params={"read_status": "unread"}).json()
    ]
    assert fixture["cluster_id"] not in unread_before

    def ordinary(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        assert comparison["baseline_snapshot_uid"] == old_synthesis["snapshot_uid"]
        assert comparison["target_revision_uid"] == target_revision_uid
        assert len(comparison["new_evidence"]) == 1
        return {
            "result": "ordinary",
            "reason": "新增来源只佐证既有事实。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", ordinary)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )

    assert response.status_code == 200
    state = response.json()
    assert state["status"] == "current"
    assert state["covered_revision_uid"] == old_synthesis["target_revision_uid"]
    assert state["reviewed_revision_uid"] == target_revision_uid
    assert state["new_source_count"] == 1
    assert state["current"]["version_uid"] == old_synthesis["version_uid"]
    assert state["current"]["snapshot_uid"] == old_synthesis["snapshot_uid"]
    assert state["current"]["blocks"] == old_synthesis["blocks"]
    listed = next(
        row
        for row in client.get("/clusters").json()
        if row["id"] == fixture["cluster_id"]
    )
    assert listed["synthesis_freshness"] == {
        "status": "current",
        "current_revision_uid": target_revision_uid,
        "covered_revision_uid": old_synthesis["target_revision_uid"],
        "reviewed_revision_uid": target_revision_uid,
        "new_source_count": 1,
        "unreviewed_evidence_count": 0,
        "unreviewed_source_count": 0,
    }
    assert [row["id"] for row in client.get("/clusters").json()] == order_before
    unread_after = [
        row["id"]
        for row in client.get("/clusters", params={"read_status": "unread"}).json()
    ]
    assert unread_after == unread_before
    with Session() as session:
        event = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        assert event is not None
        assert event.current_synthesis_version_id == synthesis_pointer
        assert session.query(SynthesisVersion).count() == 1
        assert session.query(InteractionEvent).count() == interaction_count
        assert session.query(EvidenceReview).count() == 1
        assert session.query(EvidenceReviewCitation).count() == 1
        assert session.query(EvidenceSnapshot).count() == 2
        assert session.query(LLMTask).count() == 0
        requests = list(
            session.scalars(select(GenerationRequest).order_by(GenerationRequest.id))
        )
        assert [request.task_type for request in requests] == [
            "event-synthesis",
            "evidence-review",
        ]
        review_application = session.scalar(
            select(GenerationApplication)
            .where(GenerationApplication.request_id == requests[-1].id)
        )
        assert review_application is not None
        assert review_application.status == "applied"
        assert review_application.artifact_type == "evidence-review"
        assert session.query(TopicGroup).count() == topic_count
        review = session.get(EvidenceReview, event.reviewed_evidence_review_id)
        assert review is not None
        assert review.result == "ordinary"
        assert review.reason == "新增来源只佐证既有事实。"
        assert review.provider == "local"
        assert review.model
        assert review.policy_version == "evidence-review-policy-v1"
        assert review.baseline_snapshot_id != review.target_snapshot_id
        assert review.target_revision_id == event.current_revision_id
        assert len(review.comparison_fingerprint) == 64
        state_after = session.scalar(
            select(EventUserState).where(EventUserState.event_id == event.id)
        )
        assert state_after is not None
        assert (
            state_after.seen_revision_id,
            state_after.read_status,
            state_after.starred,
            state_after.read_later,
        ) == user_state_before
        assert list(
            session.execute(
                select(
                    Source.id,
                    Source.name,
                    Source.url,
                    Source.status,
                    Source.created_at,
                ).order_by(Source.id)
            )
        ) == source_rows_before


def test_uncertain_review_is_audited_without_advancing_or_rewriting(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    original = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    add_new_source_evidence(fixture, index=3, scope="event-review-uncertain")

    with sessionmaker(bind=engine)() as session:
        interaction_count = session.query(InteractionEvent).count()

    calls = 0

    def uncertain(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        nonlocal calls
        calls += 1
        comparison = json.loads(input_text)
        return {
            "result": "uncertain",
            "reason": "新增证据不足以可靠判断变化性质。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", uncertain)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )

    assert response.status_code == 200
    state = response.json()
    assert state["status"] == "unreviewed"
    assert state["task_status"] == "complete"
    assert state["covered_revision_uid"] == original["covered_revision_uid"]
    assert state["reviewed_revision_uid"] == original["reviewed_revision_uid"]
    assert state["current"]["version_uid"] == original["current"]["version_uid"]
    assert calls == 1
    assert client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    ).status_code == 200
    assert calls == 1

    with sessionmaker(bind=engine)() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        review = session.scalar(select(EvidenceReview))
        assert event is not None and review is not None
        assert event.reviewed_evidence_review_id is None
        assert review.result == "uncertain"
        assert session.query(EvidenceReviewCitation).count() == 1
        assert session.query(SynthesisVersion).count() == 1
        assert session.query(InteractionEvent).count() == interaction_count


def test_material_review_without_a_valid_rewrite_keeps_the_old_draft_stale(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    original = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    target_revision_uid = add_new_source_evidence(
        fixture, index=3, scope="event-review-material-invalid"
    )

    def material_without_rewrite(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        return {
            "result": "material",
            "reason": "新增证据改变了事件的核心事实。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr(
        "reader_api.main.LocalChatProvider.chat", material_without_rewrite
    )
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )

    assert response.status_code == 502
    state = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    assert state["status"] == "stale"
    assert state["task_status"] == "failed"
    assert state["current"]["version_uid"] == original["current"]["version_uid"]
    assert state["covered_revision_uid"] == original["covered_revision_uid"]
    assert state["reviewed_revision_uid"] == original["reviewed_revision_uid"]
    listed = next(
        row for row in client.get("/clusters").json() if row["id"] == fixture["cluster_id"]
    )
    cluster_detail = client.get(f"/clusters/{fixture['cluster_id']}").json()
    assert listed["synthesis_freshness"]["status"] == "stale"
    assert cluster_detail["synthesis_freshness"]["status"] == "stale"

    with sessionmaker(bind=engine)() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        review = session.scalar(select(EvidenceReview))
        request = session.scalar(
            select(GenerationRequest).where(
                GenerationRequest.task_type == "evidence-review"
            )
        )
        assert event is not None and review is not None and request is not None
        assert review.result == "material"
        assert session.get(EventRevision, review.target_revision_id).uid == target_revision_uid
        assert event.reviewed_evidence_review_id is None
        assert session.query(GenerationAttempt).filter_by(
            request_id=request.id, status="failed"
        ).count() == 1
        assert session.query(GenerationResult).filter_by(request_id=request.id).count() == 0
        assert session.query(LLMTask).count() == 0
        assert session.query(SynthesisVersion).count() == 1


def test_material_review_and_rewrite_publish_atomically_in_one_provider_call(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    original = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    target_revision_uid = add_new_source_evidence(
        fixture, index=3, scope="event-review-material-current"
    )

    with sessionmaker(bind=engine)() as session:
        old_version = session.scalar(select(SynthesisVersion))
        assert old_version is not None
        old_version_id = old_version.id
        old_snapshot_id = old_version.snapshot_id
        old_blocks = list(
            session.execute(
                select(SynthesisBlock.kind, SynthesisBlock.body)
                .where(SynthesisBlock.synthesis_version_id == old_version.id)
                .order_by(SynthesisBlock.position)
            )
        )
        old_citations = list(
            session.execute(
                select(
                    SynthesisCitation.block_id,
                    SynthesisCitation.evidence_version_id,
                    SynthesisCitation.side,
                )
                .where(SynthesisCitation.synthesis_version_id == old_version.id)
                .order_by(SynthesisCitation.block_id, SynthesisCitation.position)
            )
        )
        interaction_count = session.query(InteractionEvent).count()
        topic_count = session.query(TopicGroup).count()
        user_state_count = session.query(EventUserState).count()
    order_before = [row["id"] for row in client.get("/clusters").json()]

    calls = 0

    def material_with_rewrite(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        nonlocal calls
        calls += 1
        comparison = json.loads(input_text)
        version_uids = [
            row["evidence_version_uid"] for row in comparison["target_evidence"]
        ]
        return {
            "result": "material",
            "reason": "新增证据改变了事件的核心事实。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
            "synthesis": {"blocks": valid_blocks(version_uids)},
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", material_with_rewrite)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )

    assert response.status_code == 200
    state = response.json()
    assert state["status"] == "current"
    assert state["task_status"] == "complete"
    assert state["covered_revision_uid"] == target_revision_uid
    assert state["reviewed_revision_uid"] == target_revision_uid
    assert state["current"]["target_revision_uid"] == target_revision_uid
    assert state["current"]["source_count"] == 3
    assert state["current"]["version_uid"] != original["current"]["version_uid"]
    assert state["current"]["snapshot_uid"] != original["current"]["snapshot_uid"]
    assert calls == 1
    assert client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    ).status_code == 200
    assert calls == 1
    assert [row["id"] for row in client.get("/clusters").json()] == order_before

    with sessionmaker(bind=engine)() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        review = session.scalar(select(EvidenceReview))
        versions = list(session.scalars(select(SynthesisVersion).order_by(SynthesisVersion.id)))
        assert event is not None and review is not None and len(versions) == 2
        assert review.result == "material"
        assert event.reviewed_evidence_review_id == review.id
        assert event.current_synthesis_version_id == versions[-1].id
        assert versions[0].id == old_version_id
        assert versions[0].snapshot_id == old_snapshot_id
        assert list(
            session.execute(
                select(SynthesisBlock.kind, SynthesisBlock.body)
                .where(SynthesisBlock.synthesis_version_id == old_version_id)
                .order_by(SynthesisBlock.position)
            )
        ) == old_blocks
        assert list(
            session.execute(
                select(
                    SynthesisCitation.block_id,
                    SynthesisCitation.evidence_version_id,
                    SynthesisCitation.side,
                )
                .where(SynthesisCitation.synthesis_version_id == old_version_id)
                .order_by(SynthesisCitation.block_id, SynthesisCitation.position)
            )
        ) == old_citations
        assert session.query(InteractionEvent).count() == interaction_count
        assert session.query(TopicGroup).count() == topic_count
        assert session.query(EventUserState).count() == user_state_count


def test_failed_material_rewrite_can_be_explicitly_retried_without_re_reviewing(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    target_revision_uid = add_new_source_evidence(
        fixture, index=3, scope="event-review-material-retry"
    )
    calls: list[str] = []

    def first_call(
        _: object, model: str, system_prompt: str, input_text: str
    ) -> dict[str, object]:
        calls.append(system_prompt)
        comparison = json.loads(input_text)
        return {
            "result": "material",
            "reason": "新增证据改变了事件的核心事实。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", first_call)
    failed = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert failed.status_code == 502

    def retry_rewrite(
        _: object, model: str, system_prompt: str, input_text: str
    ) -> dict[str, object]:
        calls.append(system_prompt)
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", retry_rewrite)
    retried = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )

    assert retried.status_code == 200
    assert retried.json()["status"] == "current"
    assert retried.json()["reviewed_revision_uid"] == target_revision_uid
    assert len(calls) == 2
    with sessionmaker(bind=engine)() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        review = session.scalar(select(EvidenceReview))
        assert event is not None and review is not None
        assert event.reviewed_evidence_review_id == review.id
        assert session.query(EvidenceReview).count() == 1
        assert session.query(SynthesisVersion).count() == 2
        attempts = list(
            session.scalars(
                select(GenerationAttempt).order_by(GenerationAttempt.id)
            )
        )
        assert [attempt.status for attempt in attempts] == [
            "complete",
            "failed",
            "complete",
        ]
        assert session.query(LLMTask).count() == 0


@pytest.mark.parametrize(
    "conflicting_field",
    ["reason", "citations", "provider", "model"],
)
def test_material_review_retry_rejects_conflicting_immutable_result_metadata(
    monkeypatch, conflicting_field: str
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    add_new_source_evidence(
        fixture, index=3, scope=f"material-review-conflict-{conflicting_field}"
    )

    def material_without_rewrite(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        return {
            "result": "material",
            "reason": "原始 material 判断。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr(
        "reader_api.main.LocalChatProvider.chat", material_without_rewrite
    )
    assert client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    ).status_code == 502

    with sessionmaker(bind=engine)() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        review = session.scalar(select(EvidenceReview))
        assert event is not None and review is not None
        baseline = session.get(EvidenceSnapshot, review.baseline_snapshot_id)
        target = session.get(EvidenceSnapshot, review.target_snapshot_id)
        assert baseline is not None and target is not None
        citations = list(
            session.scalars(
                select(EventEvidenceVersion.uid)
                .join(
                    EvidenceReviewCitation,
                    EvidenceReviewCitation.evidence_version_id
                    == EventEvidenceVersion.id,
                )
                .where(EvidenceReviewCitation.review_id == review.id)
                .order_by(EvidenceReviewCitation.position)
            )
        )
        values: dict[str, object] = {
            "reason": review.reason,
            "cited_version_uids": citations,
            "provider": review.provider,
            "model": review.model,
        }
        if conflicting_field == "reason":
            values["reason"] = "冲突的 material 判断。"
        elif conflicting_field == "citations":
            alternative = session.scalar(
                select(EventEvidenceVersion.uid)
                .join(
                    EvidenceSnapshotMember,
                    EvidenceSnapshotMember.evidence_version_id
                    == EventEvidenceVersion.id,
                )
                .where(
                    EvidenceSnapshotMember.snapshot_id == target.id,
                    EventEvidenceVersion.uid.not_in(citations),
                )
                .order_by(EvidenceSnapshotMember.position)
                .limit(1)
            )
            assert alternative is not None
            values["cited_version_uids"] = [alternative]
        elif conflicting_field == "provider":
            values["provider"] = "openai_compatible"
        else:
            values["model"] = "conflicting-model"

        with pytest.raises(SynthesisValidationError, match="内容冲突"):
            save_evidence_review(
                session,
                event=event,
                baseline_snapshot=baseline,
                target_snapshot=target,
                comparison_fingerprint=review.comparison_fingerprint,
                result="material",
                reason=str(values["reason"]),
                cited_version_uids=list(values["cited_version_uids"]),
                provider=str(values["provider"]),
                model=str(values["model"]),
            )


def test_rendered_read_targets_follow_stale_source_and_new_synthesis(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    initial = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    old_revision_uid = initial["current"]["target_revision_uid"]
    old_citations = initial["current"]["blocks"][0]["citations"]
    historical_citation = old_citations[0]
    add_new_source_evidence(
        fixture, index=3, scope="event-rendered-read-targets"
    )
    with sessionmaker(bind=engine)() as session:
        event = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        assert event is not None
        links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(
                    EventRevisionEvidence.revision_id == event.current_revision_id
                )
                .order_by(EventRevisionEvidence.id)
            )
        )
        historical_link = next(
            link
            for link in links
            if session.get(EventEvidenceVersion, link.evidence_version_id).uid
            == historical_citation["evidence_version_uid"]
        )
        historical_version = session.get(
            EventEvidenceVersion, historical_link.evidence_version_id
        )
        assert historical_version is not None
        replacement_version = EventEvidenceVersion(
            uid=str(uuid4()),
            evidence_id=historical_version.evidence_id,
            version_fingerprint=content_hash(
                "replacement-version",
                historical_version.content_snapshot,
                historical_version.url_snapshot,
            ),
            raw_entry_id=historical_version.raw_entry_id,
            source_entry_id=historical_version.source_entry_id,
            source_id=historical_version.source_id,
            raw_revision_no=historical_version.raw_revision_no,
            legacy_content_item_id=historical_version.legacy_content_item_id,
            legacy_content_item_id_snapshot=(
                historical_version.legacy_content_item_id_snapshot
            ),
            fragment_fingerprint=historical_version.fragment_fingerprint,
            title_snapshot=historical_version.title_snapshot,
            url_snapshot=f"{historical_version.url_snapshot}?revision=current",
            author_snapshot=historical_version.author_snapshot,
            published_at_snapshot=historical_version.published_at_snapshot,
            content_snapshot=historical_version.content_snapshot,
        )
        session.add(replacement_version)
        session.flush()
        replacement_link = EventRevisionEvidence(
            evidence_version_id=replacement_version.id,
            evidence_type=historical_link.evidence_type,
            role=historical_link.role,
        )
        current_links = [
            replacement_link if link is historical_link else link for link in links
        ]
        current = append_event_revision(
            session,
            event,
            current_links,
            [link.role for link in current_links],
        )
        current_revision_uid = current.uid
        replacement_evidence_uid = replacement_version.uid
        session.commit()

    current_detail = client.get(f"/clusters/{fixture['cluster_id']}").json()
    current_source_evidence = current_detail["source_view_evidence"]
    assert historical_citation["evidence_version_uid"] not in {
        row["evidence_version_uid"] for row in current_source_evidence
    }
    current_same_item_evidence = next(
        row
        for row in current_source_evidence
        if row["legacy_content_item_id_snapshot"]
        == historical_citation["legacy_content_item_id_snapshot"]
    )
    assert current_same_item_evidence["evidence_version_uid"] == (
        replacement_evidence_uid
    )
    current_same_item = next(
        item
        for item in current_detail["items"]
        if item["id"] == historical_citation["legacy_content_item_id_snapshot"]
    )
    assert current_same_item["url"] != historical_citation["url"]

    def readability_source(
        revision_uid: str,
        evidence_version_uid: str,
        source_id: int,
        item_id: int,
        url: str,
    ):
        return client.post(
            f"/events/{fixture['event_uid']}/readability-source",
            json={
                "observed_revision_uid": revision_uid,
                "evidence_version_uid": evidence_version_uid,
                "source_id": source_id,
                "item_id": item_id,
                "url": url,
            },
        )

    frozen_source = readability_source(
        old_revision_uid,
        historical_citation["evidence_version_uid"],
        historical_citation["source"]["source_id"],
        historical_citation["legacy_content_item_id_snapshot"],
        historical_citation["url"],
    )
    assert frozen_source.status_code == 200
    assert frozen_source.json()["url"] == historical_citation["url"]
    current_source = readability_source(
        current_revision_uid,
        current_same_item_evidence["evidence_version_uid"],
        current_same_item_evidence["source_id"],
        current_same_item_evidence["legacy_content_item_id_snapshot"],
        current_same_item["url"],
    )
    assert current_source.status_code == 200
    assert current_source.json()["url"] == current_same_item["url"]
    assert current_source.json()["url"] != frozen_source.json()["url"]
    assert readability_source(
        old_revision_uid,
        current_same_item_evidence["evidence_version_uid"],
        current_same_item_evidence["source_id"],
        current_same_item_evidence["legacy_content_item_id_snapshot"],
        current_same_item["url"],
    ).status_code == 409
    assert readability_source(
        old_revision_uid,
        historical_citation["evidence_version_uid"],
        historical_citation["source"]["source_id"],
        historical_citation["legacy_content_item_id_snapshot"] + 1,
        historical_citation["url"],
    ).status_code == 409
    assert readability_source(
        old_revision_uid,
        historical_citation["evidence_version_uid"],
        historical_citation["source"]["source_id"],
        historical_citation["legacy_content_item_id_snapshot"],
        current_same_item["url"],
    ).status_code == 409

    def material_without_rewrite(
        _: object, model: str, system_prompt: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        return {
            "result": "material",
            "reason": "新增证据改变了事件的核心事实。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr(
        "reader_api.main.LocalChatProvider.chat", material_without_rewrite
    )
    assert client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    ).status_code == 502
    stale = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    assert stale["status"] == "stale"
    assert stale["current"]["target_revision_uid"] == old_revision_uid
    assert stale["source_view_revision_uid"] == current_revision_uid

    def mark_seen(operation_id: str, revision_uid: str):
        return client.post(
            "/event-user-state",
            json={
                "event_uid": fixture["event_uid"],
                "observed_revision_uid": revision_uid,
                "operation_id": operation_id,
                "action": "read_status_set",
                "value": "summary_seen",
            },
        )

    def mark_original(
        operation_id: str, revision_uid: str, evidence: dict[str, object]
    ):
        return client.post(
            "/event-user-state",
            json={
                "event_uid": fixture["event_uid"],
                "observed_revision_uid": revision_uid,
                "operation_id": operation_id,
                "action": "read_status_set",
                "value": "original_opened",
                "source_id": evidence["source_id"],
                "evidence_version_uid": evidence["evidence_version_uid"],
            },
        )

    stale_seen = mark_original(
        "issue44-stale-synthesis",
        old_revision_uid,
        {
            "source_id": historical_citation["source"]["source_id"],
            "evidence_version_uid": historical_citation["evidence_version_uid"],
        },
    )
    assert stale_seen.status_code == 200
    assert stale_seen.json()["seen_revision_uid"] == old_revision_uid
    source_seen = mark_original(
        "issue44-current-source",
        current_revision_uid,
        current_same_item_evidence,
    )
    assert source_seen.status_code == 200
    assert source_seen.json()["seen_revision_uid"] == current_revision_uid

    def retry_rewrite(
        _: object, model: str, system_prompt: str, input_text: str
    ) -> dict[str, object]:
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", retry_rewrite)
    refreshed = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert refreshed.status_code == 200
    assert refreshed.json()["current"]["target_revision_uid"] == current_revision_uid
    synthesis_seen = mark_seen("issue44-current-synthesis", current_revision_uid)
    assert synthesis_seen.status_code == 200
    assert mark_seen("issue44-current-synthesis", current_revision_uid).json() == (
        synthesis_seen.json()
    )

    with sessionmaker(bind=engine)() as session:
        interactions = list(
            session.scalars(
                select(InteractionEvent)
                .where(InteractionEvent.operation_id.like("issue44-%"))
                .order_by(InteractionEvent.id)
            )
        )
        assert {
            interaction.operation_id: {
                "revision_uid": session.get(
                    EventRevision, interaction.observed_revision_id
                ).uid,
                "evidence_version_uid": interaction.payload.get(
                    "opened_evidence_version_uid"
                ),
            }
            for interaction in interactions
        } == {
            "issue44-stale-synthesis": {
                "revision_uid": old_revision_uid,
                "evidence_version_uid": historical_citation[
                    "evidence_version_uid"
                ],
            },
            "issue44-current-source": {
                "revision_uid": current_revision_uid,
                "evidence_version_uid": current_same_item_evidence[
                    "evidence_version_uid"
                ],
            },
            "issue44-current-synthesis": {
                "revision_uid": current_revision_uid,
                "evidence_version_uid": None,
            },
        }


def test_remote_ordinary_review_uses_the_strict_shared_contract(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    add_new_source_evidence(
        fixture, index=3, scope="event-review-remote-contract"
    )
    assert client.patch(
        "/ai/settings",
        json={
            "synthesis_remote_base_url": "https://api.example.com",
            "synthesis_remote_model": "remote-review-model",
            "synthesis_remote_api_key": "secret",
        },
    ).status_code == 200

    def ordinary(
        _: object, model: str, system_prompt: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        assert model == "remote-review-model"
        assert "ordinary、material、uncertain" in system_prompt
        return {
            "result": "ordinary",
            "reason": "远端模型确认只构成普通佐证。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr(
        "reader_api.llm.OpenAICompatibleChatProvider.chat", ordinary
    )
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "openai_compatible"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "current"
    with sessionmaker(bind=engine)() as session:
        review = session.scalar(select(EvidenceReview))
        request = session.scalar(
            select(GenerationRequest).where(
                GenerationRequest.task_type == "evidence-review"
            )
        )
        assert review is not None and request is not None
        assert (review.provider, review.model) == (
            "openai_compatible",
            "remote-review-model",
        )
        payload = session.get(GenerationRequestPayload, request.id)
        assert payload is not None
        assert payload.payload_json["output_schema"] == EVIDENCE_REVIEW_OUTPUT_SCHEMA
        assert session.query(LLMTask).count() == 0



def test_consecutive_ordinary_reviews_compare_only_from_the_reviewed_watermark(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    comparisons: list[dict[str, object]] = []

    def ordinary(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        comparisons.append(comparison)
        return {
            "result": "ordinary",
            "reason": "只构成普通佐证。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][-1][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", ordinary)
    add_new_source_evidence(
        fixture, index=3, scope="event-review-consecutive-three"
    )
    first = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert first.status_code == 200
    add_new_source_evidence(
        fixture, index=4, scope="event-review-consecutive-four"
    )
    second = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )

    assert second.status_code == 200
    assert len(comparisons) == 2
    assert comparisons[1]["baseline_snapshot_uid"] == comparisons[0][
        "target_snapshot_uid"
    ]
    assert comparisons[1]["new_source_count"] == 1
    assert [
        row["source_name"] for row in comparisons[1]["new_evidence"]
    ] == ["Source 4"]
    assert second.json()["status"] == "current"
    assert second.json()["new_source_count"] == 2
    detail = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    listed = next(
        row
        for row in client.get("/clusters").json()
        if row["id"] == fixture["cluster_id"]
    )
    cluster_detail = client.get(f"/clusters/{fixture['cluster_id']}").json()
    assert detail["new_source_count"] == 2
    assert listed["synthesis_freshness"]["new_source_count"] == 2
    assert cluster_detail["synthesis_freshness"]["new_source_count"] == 2
    assert cluster_detail["synthesis"]["new_source_count"] == 2
    with sessionmaker(bind=engine)() as session:
        assert session.query(EvidenceReview).count() == 2
        assert session.query(SynthesisVersion).count() == 1


def test_review_ignores_a_watermark_from_a_replaced_synthesis_lineage(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    add_new_source_evidence(
        fixture, index=3, scope="event-review-old-lineage"
    )

    def ordinary(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        return {
            "result": "ordinary",
            "reason": "只构成普通佐证。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][-1][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", ordinary)
    assert client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    ).status_code == 200

    with sessionmaker(bind=engine)() as session:
        event = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        assert event is not None
        links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(
                    EventRevisionEvidence.revision_id == event.current_revision_id
                )
                .order_by(EventRevisionEvidence.id)
            )
        )
        roles = [link.role for link in links]
        roles[0] = "challenge"
        append_event_revision(session, event, links, roles)
        session.commit()
    clear_current_synthesis_pointer(str(fixture["event_uid"]))
    new_lineage = generate_valid_synthesis(
        monkeypatch, str(fixture["event_uid"])
    ).get(f"/events/{fixture['event_uid']}").json()["synthesis"]["current"]
    add_new_source_evidence(
        fixture, index=4, scope="event-review-new-lineage"
    )
    captured: dict[str, object] = {}

    def capture(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        captured.update(comparison)
        return {
            "result": "ordinary",
            "reason": "新合成谱系上的普通佐证。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][-1][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", capture)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )

    assert response.status_code == 200
    assert captured["baseline_snapshot_uid"] == new_lineage["snapshot_uid"]
    assert [row["source_name"] for row in captured["new_evidence"]] == [
        "Source 4"
    ]


@pytest.mark.parametrize(
    ("provider", "envelope"),
    [
        ("local", "output"),
        ("local", "text"),
        ("openai_compatible", "choices"),
    ],
)
def test_review_accepts_real_provider_json_envelopes(
    monkeypatch, provider: str, envelope: str
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    add_new_source_evidence(
        fixture, index=3, scope=f"event-review-envelope-{envelope}"
    )
    if provider == "openai_compatible":
        assert client.patch(
            "/ai/settings",
            json={
                "synthesis_remote_base_url": "https://api.example.com",
                "synthesis_remote_model": "remote-review-model",
                "synthesis_remote_api_key": "secret",
            },
        ).status_code == 200

    def chat(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        review = json.dumps(
            {
                "result": "ordinary",
                "reason": "真实 provider envelope 内的普通佐证。",
                "citations": [
                    {
                        "evidence_version_uid": comparison["new_evidence"][0][
                            "evidence_version_uid"
                        ]
                    }
                ],
            },
            ensure_ascii=False,
        )
        if envelope == "choices":
            return {"choices": [{"message": {"content": review}}]}
        return {envelope: f"```json\n{review}\n```"}

    target = (
        "reader_api.llm.OpenAICompatibleChatProvider.chat"
        if provider == "openai_compatible"
        else "reader_api.main.LocalChatProvider.chat"
    )
    monkeypatch.setattr(target, chat)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": provider}
    )

    assert response.status_code == 200
    assert response.json()["status"] == "current"
    with sessionmaker(bind=engine)() as session:
        assert session.query(EvidenceReview).count() == 1


def test_synchronous_review_response_uses_the_latest_revision_after_model_wait(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    reviewed_revision_uid = add_new_source_evidence(
        fixture, index=3, scope="event-review-sync-race-target"
    )
    provider_started = ThreadEvent()
    release_provider = ThreadEvent()
    reviewed_source: dict[str, int] = {}

    def ordinary_after_wait(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        reviewed_source["id"] = comparison["new_evidence"][0]["source_id"]
        provider_started.set()
        assert release_provider.wait(timeout=5)
        return {
            "result": "ordinary",
            "reason": "固定目标只构成普通佐证。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr(
        "reader_api.main.LocalChatProvider.chat", ordinary_after_wait
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            TestClient(app).post,
            f"/events/{fixture['event_uid']}/synthesis",
            json={"provider": "local"},
        )
        assert provider_started.wait(timeout=5)
        with sessionmaker(bind=engine)() as session:
            source = session.get(Source, reviewed_source["id"])
            assert source is not None
            source.name = "Renamed during model call"
            session.commit()
        newest_revision_uid = add_new_source_evidence(
            fixture, index=4, scope="event-review-sync-race-newest"
        )
        release_provider.set()
        response = future.result(timeout=5)

    assert response.status_code == 200
    assert response.json()["status"] == "unreviewed"
    assert response.json()["current_revision_uid"] == newest_revision_uid
    assert response.json()["reviewed_revision_uid"] == reviewed_revision_uid


@pytest.mark.parametrize("endpoint", ["/clusters", "/clusters/{cluster_id}"])
def test_cluster_response_keeps_one_frozen_synthesis_revision(
    monkeypatch, endpoint: str
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    initial = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    r1_uid = initial["current_revision_uid"]
    advanced: dict[str, str] = {}
    from reader_api import main as main_module

    original_identities = main_module.cluster_event_identities_for

    def freeze_then_advance(session, cluster_ids):
        identities = original_identities(session, cluster_ids)
        if not advanced:
            event = session.scalar(
                select(Event).where(Event.uid == fixture["event_uid"])
            )
            assert event is not None
            links = list(
                session.scalars(
                    select(EventRevisionEvidence)
                    .where(
                        EventRevisionEvidence.revision_id
                        == event.current_revision_id
                    )
                    .order_by(EventRevisionEvidence.id)
                )
            )
            roles = [link.role for link in links]
            roles[0] = "challenge"
            advanced["revision_uid"] = append_event_revision(
                session, event, links, roles
            ).uid
            session.commit()
        return identities

    monkeypatch.setattr(main_module, "cluster_event_identities_for", freeze_then_advance)
    url = endpoint.format(cluster_id=fixture["cluster_id"])

    first_payload = client.get(url).json()
    first = (
        next(row for row in first_payload if row["id"] == fixture["cluster_id"])
        if isinstance(first_payload, list)
        else first_payload
    )

    assert advanced["revision_uid"] != r1_uid
    assert first["current_revision_uid"] == r1_uid
    assert first["synthesis_freshness"]["current_revision_uid"] == r1_uid
    if not isinstance(first_payload, list):
        assert first["synthesis"]["current_revision_uid"] == r1_uid

    second_payload = client.get(url).json()
    second = (
        next(row for row in second_payload if row["id"] == fixture["cluster_id"])
        if isinstance(second_payload, list)
        else second_payload
    )
    assert second["current_revision_uid"] == advanced["revision_uid"]
    assert second["synthesis_freshness"]["current_revision_uid"] == advanced[
        "revision_uid"
    ]
    if not isinstance(second_payload, list):
        assert second["synthesis"]["current_revision_uid"] == advanced[
            "revision_uid"
        ]



def test_replaced_evidence_version_is_unreviewed(monkeypatch) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    generated = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    Session = sessionmaker(bind=engine)
    with Session() as session:
        item = session.get(ContentItem, fixture["item_id"])
        assert item is not None
        with clustering_run(
            session,
            scope_type="event-synthesis-replaced-evidence",
            item_ids=[item.id],
            rule_version="event-synthesis-replaced-evidence-v1",
        ):
            item.content_text = "A corrected evidence version"
            item.content_hash = content_hash(item.title, item.content_text, item.url)
        session.commit()

    state = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    listed = next(
        row
        for row in client.get("/clusters").json()
        if row["id"] == fixture["cluster_id"]
    )
    assert state["status"] == "unreviewed"
    assert state["current"] is not None
    assert state["current"]["version_uid"] == generated["current"]["version_uid"]
    assert state["unreviewed_evidence_count"] == 1
    assert state["unreviewed_source_count"] == 0
    assert listed["synthesis_freshness"] == {
        "status": "unreviewed",
        "current_revision_uid": state["current_revision_uid"],
        "covered_revision_uid": state["covered_revision_uid"],
        "reviewed_revision_uid": state["covered_revision_uid"],
        "new_source_count": 0,
        "unreviewed_evidence_count": 1,
        "unreviewed_source_count": 0,
    }


def test_changed_evidence_role_is_unreviewed(monkeypatch) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    generated = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    Session = sessionmaker(bind=engine)
    with Session() as session:
        event = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        assert event is not None
        links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == event.current_revision_id)
                .order_by(EventRevisionEvidence.id)
            )
        )
        roles = [link.role for link in links]
        roles[0] = "challenge"
        append_event_revision(session, event, links, roles)
        session.commit()

    state = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    assert state["status"] == "unreviewed"
    assert state["current"]["version_uid"] == generated["current"]["version_uid"]


def test_reordered_evidence_members_keep_the_synthesis_current(monkeypatch) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    generated = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    Session = sessionmaker(bind=engine)
    with Session() as session:
        event = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        assert event is not None
        links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == event.current_revision_id)
                .order_by(EventRevisionEvidence.id)
            )
        )
        reordered = list(reversed(links))
        append_event_revision(
            session,
            event,
            reordered,
            [link.role for link in reordered],
        )
        session.commit()

    state = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    assert state["status"] == "current"
    assert state["current"]["version_uid"] == generated["current"]["version_uid"]
    listed = next(
        cluster
        for cluster in client.get("/clusters").json()
        if cluster["id"] == fixture["cluster_id"]
    )
    assert listed["synthesis_freshness"]["status"] == "current"


def test_list_candidate_query_excludes_nonmatching_synthesis_history(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    first_version_uid = client.get(f"/events/{fixture['event_uid']}").json()[
        "synthesis"
    ]["current"]["version_uid"]
    Session = sessionmaker(bind=engine)

    def append_role(role: str) -> list[str]:
        with Session() as session:
            event = session.scalar(
                select(Event).where(Event.uid == fixture["event_uid"])
            )
            assert event is not None
            links = list(
                session.scalars(
                    select(EventRevisionEvidence)
                    .where(
                        EventRevisionEvidence.revision_id == event.current_revision_id
                    )
                    .order_by(EventRevisionEvidence.id)
                )
            )
            roles = [link.role for link in links]
            original_roles = list(roles)
            roles[0] = role
            append_event_revision(session, event, links, roles)
            session.commit()
            return original_roles

    original_roles = append_role("challenge")
    clear_current_synthesis_pointer(str(fixture["event_uid"]))
    second = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    ).json()["current"]["version_uid"]
    append_role("opinion")
    clear_current_synthesis_pointer(str(fixture["event_uid"]))
    third = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    ).json()["current"]["version_uid"]

    with Session() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None
        links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == event.current_revision_id)
                .order_by(EventRevisionEvidence.id)
            )
        )
        revision = append_event_revision(session, event, links, original_roles)
        session.flush()
        fingerprints = synthesis_fingerprints_for_rows(
            synthesis_evidence_rows(session, revision)
        )
        candidates = synthesis_candidate_version_rows(
            session,
            [(event, revision)],
            {event.id: (fingerprints[1], fingerprints[2], fingerprints[3])},
        )

    candidate_uids = {version.uid for version, _snapshot in candidates}
    assert candidate_uids == {first_version_uid, third}
    assert second not in candidate_uids


def test_list_does_not_treat_a_different_evidence_identity_as_current(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    Session = sessionmaker(bind=engine)
    with Session() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        original_revision = session.get(EventRevision, fixture["revision_id"])
        assert event is not None and original_revision is not None
        original_links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == original_revision.id)
                .order_by(EventRevisionEvidence.id)
            )
        )
        item = add_source_item(session, 3)
        with clustering_run(
            session,
            scope_type="event-synthesis-identity-change",
            item_ids=[item.id],
            rule_version="event-synthesis-identity-change-v1",
        ):
            assign_publishable_cluster(session, item)
        current_links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == event.current_revision_id)
                .order_by(EventRevisionEvidence.id)
            )
        )
        original_version_ids = {link.evidence_version_id for link in original_links}
        replacement_link = next(
            link
            for link in current_links
            if link.evidence_version_id not in original_version_ids
        )
        original_version = session.get(
            EventEvidenceVersion, original_links[0].evidence_version_id
        )
        replacement_version = session.get(
            EventEvidenceVersion, replacement_link.evidence_version_id
        )
        assert original_version is not None and replacement_version is not None
        assert replacement_version.evidence_id != original_version.evidence_id
        same_fingerprint_version = EventEvidenceVersion(
            uid=str(uuid4()),
            evidence_id=replacement_version.evidence_id,
            version_fingerprint=original_version.version_fingerprint,
            raw_entry_id=replacement_version.raw_entry_id,
            source_entry_id=replacement_version.source_entry_id,
            source_id=replacement_version.source_id,
            raw_revision_no=replacement_version.raw_revision_no,
            legacy_content_item_id=replacement_version.legacy_content_item_id,
            legacy_content_item_id_snapshot=(
                replacement_version.legacy_content_item_id_snapshot
            ),
            fragment_fingerprint=replacement_version.fragment_fingerprint,
            title_snapshot=replacement_version.title_snapshot,
            url_snapshot=replacement_version.url_snapshot,
            author_snapshot=replacement_version.author_snapshot,
            published_at_snapshot=replacement_version.published_at_snapshot,
            content_snapshot=replacement_version.content_snapshot,
        )
        session.add(same_fingerprint_version)
        session.flush()
        same_fingerprint_link = EventRevisionEvidence(
            evidence_version_id=same_fingerprint_version.id,
            evidence_type=replacement_link.evidence_type,
            role=replacement_link.role,
        )
        replacement = append_event_revision(
            session,
            event,
            [same_fingerprint_link, original_links[1]],
            [same_fingerprint_link.role, original_links[1].role],
        )
        assert replacement.evidence_fingerprint == original_revision.evidence_fingerprint
        session.commit()

    detail_status = client.get(f"/events/{fixture['event_uid']}").json()[
        "synthesis"
    ]["status"]
    listed_status = next(
        cluster["synthesis_freshness"]["status"]
        for cluster in client.get("/clusters").json()
        if cluster["id"] == fixture["cluster_id"]
    )
    assert detail_status == listed_status == "unreviewed"


def test_completed_generation_is_reused_without_new_artifacts_or_model_call(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()

    def chat(_: object, model: str, __: str, input_text: str) -> dict[str, object]:
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    client = TestClient(app)
    first = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert first.status_code == 200

    def fail(*_: object, **__: object) -> None:
        raise AssertionError("an identical generation must reuse the completed result")

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", fail)
    repeated = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "openai_compatible"},
    )

    assert repeated.status_code == 200
    assert repeated.json()["current"]["version_uid"] == first.json()["current"][
        "version_uid"
    ]
    with sessionmaker(bind=engine)() as session:
        assert session.query(EvidenceSnapshot).count() == 1
        assert session.query(SynthesisVersion).count() == 1
        assert session.query(LLMTask).count() == 0
        assert session.query(GenerationResult).count() == 1


def test_compatible_v1_synthesis_remains_current_after_prompt_v2(
    monkeypatch,
) -> None:
    from reader_api import event_synthesis, main

    fixture = seed_multi_source_event()
    current_prompt_version = event_synthesis.SYNTHESIS_PROMPT_VERSION
    monkeypatch.setattr(
        event_synthesis,
        "SYNTHESIS_PROMPT_VERSION",
        "event-synthesis-prompt-v1",
    )
    monkeypatch.setattr(
        main,
        "SYNTHESIS_PROMPT_VERSION",
        "event-synthesis-prompt-v1",
    )
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    monkeypatch.setattr(
        event_synthesis,
        "SYNTHESIS_PROMPT_VERSION",
        current_prompt_version,
    )
    monkeypatch.setattr(main, "SYNTHESIS_PROMPT_VERSION", current_prompt_version)

    state = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    listed = next(
        row
        for row in client.get("/clusters").json()
        if row["id"] == fixture["cluster_id"]
    )["synthesis_freshness"]

    assert state["status"] == listed["status"] == "current"
    assert state["current"]["prompt_version"] == "event-synthesis-prompt-v1"


def test_compatible_v1_history_is_current_when_event_pointer_is_empty(
    monkeypatch,
) -> None:
    from reader_api import event_synthesis, main

    fixture = seed_multi_source_event()
    current_prompt_version = event_synthesis.SYNTHESIS_PROMPT_VERSION
    monkeypatch.setattr(
        event_synthesis,
        "SYNTHESIS_PROMPT_VERSION",
        "event-synthesis-prompt-v1",
    )
    monkeypatch.setattr(
        main,
        "SYNTHESIS_PROMPT_VERSION",
        "event-synthesis-prompt-v1",
    )
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    with sessionmaker(bind=engine)() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None
        event.current_synthesis_version_id = None
        session.commit()
    monkeypatch.setattr(
        event_synthesis,
        "SYNTHESIS_PROMPT_VERSION",
        current_prompt_version,
    )
    monkeypatch.setattr(main, "SYNTHESIS_PROMPT_VERSION", current_prompt_version)

    state = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    listed = next(
        row
        for row in client.get("/clusters").json()
        if row["id"] == fixture["cluster_id"]
    )["synthesis_freshness"]

    assert state["status"] == listed["status"] == "current"
    assert state["current"]["prompt_version"] == "event-synthesis-prompt-v1"


def test_v1_synthesis_becomes_missing_for_unlisted_prompt_v3(
    monkeypatch,
) -> None:
    from reader_api import event_synthesis, main

    fixture = seed_multi_source_event()
    monkeypatch.setattr(
        event_synthesis,
        "SYNTHESIS_PROMPT_VERSION",
        "event-synthesis-prompt-v1",
    )
    monkeypatch.setattr(
        main,
        "SYNTHESIS_PROMPT_VERSION",
        "event-synthesis-prompt-v1",
    )
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    monkeypatch.setattr(
        event_synthesis,
        "SYNTHESIS_PROMPT_VERSION",
        "event-synthesis-prompt-v3",
    )
    monkeypatch.setattr(
        main,
        "SYNTHESIS_PROMPT_VERSION",
        "event-synthesis-prompt-v3",
    )

    state = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    listed = next(
        row
        for row in client.get("/clusters").json()
        if row["id"] == fixture["cluster_id"]
    )["synthesis_freshness"]

    assert state["status"] == listed["status"] == "missing"
    assert state["current"] is None


def test_completed_generation_is_reused_after_evidence_recurs_a_b_a(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    model_calls = 0

    def chat(_: object, model: str, __: str, input_text: str) -> dict[str, object]:
        nonlocal model_calls
        model_calls += 1
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    client = TestClient(app)
    first = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert first.status_code == 200
    version_uid = first.json()["current"]["version_uid"]
    original_revision_uid = first.json()["current"]["target_revision_uid"]
    old_revision_uid, recurrent_revision_uid = recur_event_to_original_evidence(
        str(fixture["event_uid"])
    )
    assert old_revision_uid == original_revision_uid
    assert recurrent_revision_uid != original_revision_uid

    current = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    assert current["status"] == "current"
    assert current["target_revision_uid"] == recurrent_revision_uid
    assert current["current"]["version_uid"] == version_uid
    assert current["current"]["target_revision_uid"] == original_revision_uid

    repeated = client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "openai_compatible"},
    )
    assert repeated.status_code == 200
    assert repeated.json()["current"]["version_uid"] == version_uid
    assert repeated.json()["current"]["target_revision_uid"] == original_revision_uid
    assert model_calls == 1
    with sessionmaker(bind=engine)() as session:
        assert session.query(EvidenceSnapshot).count() == 1
        assert session.query(SynthesisVersion).count() == 1
        assert session.query(LLMTask).count() == 0
        assert session.query(GenerationRequest).count() == 1


def test_state_finds_historical_generation_when_pointer_still_targets_b(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    model_calls = 0

    def chat(_: object, model: str, __: str, input_text: str) -> dict[str, object]:
        nonlocal model_calls
        model_calls += 1
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    client = TestClient(app)
    first = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert first.status_code == 200
    first_version_uid = first.json()["current"]["version_uid"]
    first_target_uid = first.json()["current"]["target_revision_uid"]

    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None
        links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == fixture["revision_id"])
                .order_by(EventRevisionEvidence.id)
            )
        )
        changed_roles = [link.role for link in links]
        changed_roles[0] = "challenge"
        b_revision = append_event_revision(session, event, links, changed_roles)
        session.commit()
        b_revision_uid = b_revision.uid

    clear_current_synthesis_pointer(str(fixture["event_uid"]))
    second = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert second.status_code == 200
    second_version_uid = second.json()["current"]["version_uid"]
    assert second_version_uid != first_version_uid
    assert second.json()["current"]["target_revision_uid"] == b_revision_uid

    with SessionLocal() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None
        b_version_id = event.current_synthesis_version_id
        links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == fixture["revision_id"])
                .order_by(EventRevisionEvidence.id)
            )
        )
        recurrent = append_event_revision(
            session, event, links, [link.role for link in links]
        )
        session.commit()
        recurrent_uid = recurrent.uid

    state = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    assert state["status"] == "current"
    assert state["target_revision_uid"] == recurrent_uid
    assert state["current"]["version_uid"] == first_version_uid
    assert state["current"]["target_revision_uid"] == first_target_uid
    with SessionLocal() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None
        assert event.current_synthesis_version_id == b_version_id

    repeated = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert repeated.status_code == 200
    assert repeated.json()["current"]["version_uid"] == first_version_uid
    assert repeated.json()["current"]["target_revision_uid"] == first_target_uid
    assert model_calls == 2
    with SessionLocal() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None
        current = session.get(SynthesisVersion, event.current_synthesis_version_id)
        assert current is not None and current.uid == first_version_uid
        assert session.query(EvidenceSnapshot).count() == 2
        assert session.query(SynthesisVersion).count() == 2
        assert session.query(LLMTask).count() == 0
        assert session.query(GenerationRequest).count() == 2


def test_recurrent_historical_synthesis_drops_review_from_physical_pointer(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    synthesis_a = client.get(f"/events/{fixture['event_uid']}").json()[
        "synthesis"
    ]["current"]
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None
        original_links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == fixture["revision_id"])
                .order_by(EventRevisionEvidence.id)
            )
        )
        original_roles = [link.role for link in original_links]
        changed_roles = list(original_roles)
        changed_roles[0] = "challenge"
        append_event_revision(session, event, original_links, changed_roles)
        session.commit()

    clear_current_synthesis_pointer(str(fixture["event_uid"]))
    synthesis_b = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert synthesis_b.status_code == 200
    assert synthesis_b.json()["current"]["version_uid"] != synthesis_a["version_uid"]
    add_new_source_evidence(
        fixture, index=3, scope="event-review-physical-pointer-b"
    )

    def ordinary(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        return {
            "result": "ordinary",
            "reason": "B 谱系上的新增来源只构成普通佐证。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][-1][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", ordinary)
    reviewed = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert reviewed.status_code == 200
    assert reviewed.json()["new_source_count"] == 1

    with SessionLocal() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None
        original_links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == fixture["revision_id"])
                .order_by(EventRevisionEvidence.id)
            )
        )
        recurrent = append_event_revision(
            session, event, original_links, original_roles
        )
        session.commit()
        recurrent_uid = recurrent.uid

    expected = {
        "status": "current",
        "current_revision_uid": recurrent_uid,
        "covered_revision_uid": synthesis_a["target_revision_uid"],
        "reviewed_revision_uid": synthesis_a["target_revision_uid"],
        "new_source_count": 0,
        "unreviewed_evidence_count": 0,
        "unreviewed_source_count": 0,
    }
    event_detail = client.get(f"/events/{fixture['event_uid']}").json()
    cluster_list = next(
        row
        for row in client.get("/clusters").json()
        if row["id"] == fixture["cluster_id"]
    )
    cluster_detail = client.get(f"/clusters/{fixture['cluster_id']}").json()

    assert event_detail["synthesis"]["current"]["version_uid"] == synthesis_a[
        "version_uid"
    ]
    assert {
        key: event_detail["synthesis"][key] for key in expected
    } == expected
    assert cluster_list["synthesis_freshness"] == expected
    assert cluster_detail["synthesis_freshness"] == expected
    assert {
        key: cluster_detail["synthesis"][key] for key in expected
    } == expected

    repointed = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert repointed.status_code == 200
    assert repointed.json()["current"]["version_uid"] == synthesis_a["version_uid"]
    add_new_source_evidence(
        fixture, index=4, scope="event-review-same-lineage-recurrence"
    )
    same_lineage_review = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert same_lineage_review.status_code == 200
    assert same_lineage_review.json()["new_source_count"] == 2
    reviewed_revision_uid = same_lineage_review.json()["reviewed_revision_uid"]

    with SessionLocal() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None
        original_links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == fixture["revision_id"])
                .order_by(EventRevisionEvidence.id)
            )
        )
        recurrent = append_event_revision(
            session, event, original_links, original_roles
        )
        session.commit()
        recurrent_uid = recurrent.uid

    event_detail = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    cluster_list = next(
        row
        for row in client.get("/clusters").json()
        if row["id"] == fixture["cluster_id"]
    )["synthesis_freshness"]
    cluster_detail = client.get(f"/clusters/{fixture['cluster_id']}").json()
    for projection in (
        event_detail,
        cluster_list,
        cluster_detail["synthesis_freshness"],
        cluster_detail["synthesis"],
    ):
        assert projection["current_revision_uid"] == recurrent_uid
        assert projection["reviewed_revision_uid"] == reviewed_revision_uid
        assert projection["new_source_count"] == 0


def test_generation_fingerprint_ignores_evidence_member_insert_order(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    Session = sessionmaker(bind=engine)
    with Session() as session:
        first_event = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        assert first_event is not None
        first_revision = session.get(EventRevision, first_event.current_revision_id)
        assert first_revision is not None
        links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == first_revision.id)
                .order_by(EventRevisionEvidence.id)
            )
        )
        second_event = Event(
            uid="39000000-0000-4000-8000-000000000001", status="active"
        )
        session.add(second_event)
        session.flush()
        second_revision = EventRevision(
            uid="39000000-0000-4000-8000-000000000002",
            event_id=second_event.id,
            revision_no=1,
            evidence_fingerprint=first_revision.evidence_fingerprint,
            title_snapshot=first_revision.title_snapshot,
            event_time_snapshot=first_revision.event_time_snapshot,
        )
        session.add(second_revision)
        session.flush()
        session.add_all(
            EventRevisionEvidence(
                revision_id=second_revision.id,
                evidence_version_id=link.evidence_version_id,
                evidence_type=link.evidence_type,
                role=link.role,
            )
            for link in reversed(links)
        )
        second_event.current_revision_id = second_revision.id
        session.commit()
        second_event_uid = second_event.uid

    fingerprints: list[str] = []

    def chat(_: object, model: str, __: str, input_text: str) -> dict[str, object]:
        input_data = json.loads(input_text)
        fingerprints.append(input_data["generation_fingerprint"])
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    client = TestClient(app)
    for event_uid in (fixture["event_uid"], second_event_uid):
        response = client.post(
            f"/events/{event_uid}/synthesis", json={"provider": "local"}
        )
        assert response.status_code == 200

    assert len(fingerprints) == 2
    assert fingerprints[0] == fingerprints[1]


@pytest.mark.parametrize(
    "result_builder",
    [
        lambda uids: [{"kind": "fact", "body": "fact", "citations": []}],
        lambda uids: [
            {
                "kind": "fact",
                "body": "",
                "citations": [{"evidence_version_uid": uids[0], "side": "support"}],
            }
        ],
        lambda uids: [
            {
                "kind": "other",
                "body": "fact",
                "citations": [{"evidence_version_uid": uids[0], "side": "support"}],
            }
        ],
        lambda uids: [
            {
                "kind": "fact",
                "body": "fact",
                "citations": [{"evidence_version_uid": "unknown", "side": "support"}],
            }
        ],
        lambda uids: [
            {
                "kind": "disagreement",
                "body": "conflict",
                "citations": [
                    {"evidence_version_uid": uids[0], "side": "position_a"},
                    {"evidence_version_uid": uids[1], "side": "position_a"},
                ],
            }
        ],
        lambda uids: [
            {
                "kind": "viewpoint",
                "body": "view",
                "citations": [{"evidence_version_uid": uids[0], "side": "attributed"}],
            }
        ],
        lambda uids: [
            {
                "kind": "fact",
                "body": 42,
                "citations": [{"evidence_version_uid": uids[0], "side": "support"}],
            }
        ],
        lambda uids: [
            {
                "kind": "viewpoint",
                "body": "view",
                "attribution": {"source": "Source 1"},
                "citations": [{"evidence_version_uid": uids[0], "side": "attributed"}],
            }
        ],
        lambda uids: [
            {
                "kind": "fact",
                "body": "fact",
                "citations": [{"evidence_version_uid": uids[0], "side": ["support"]}],
            }
        ],
        lambda uids: [
            {
                "kind": "fact",
                "body": "fact",
                "citations": [{"evidence_version_uid": uids[0], "side": "x" * 41}],
            }
        ],
    ],
)
def test_invalid_provider_output_does_not_advance_current_pointer(
    monkeypatch, result_builder
) -> None:
    fixture = seed_multi_source_event()

    def chat(_: object, model: str, __: str, input_text: str) -> dict[str, object]:
        return {"blocks": result_builder(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    client = TestClient(app)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )

    assert response.status_code == 502
    assert response.json()["detail"] == "模型返回的合成内容无法使用，请重试"
    assert (
        client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]["status"]
        == "missing"
    )
    Session = sessionmaker(bind=engine)
    with Session() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None and event.current_synthesis_version_id is None
        assert session.query(EvidenceSnapshot).count() == 1
        assert session.query(EvidenceSnapshotMember).count() == 2
        assert session.query(SynthesisVersion).count() == 0
        attempt = session.scalar(select(GenerationAttempt))
        assert attempt is not None and attempt.status == "failed"
        assert attempt.error
        assert session.query(GenerationResult).count() == 0



def test_explicit_remote_provider_uses_the_same_schema(monkeypatch) -> None:
    fixture = seed_multi_source_event()
    Session = sessionmaker(bind=engine)
    with Session() as session:
        from reader_api.ai_runtime import save_ai_settings

        save_ai_settings(
            session,
            {
                "synthesis_remote_base_url": "https://api.example.com",
                "synthesis_remote_model": "remote-model",
                "synthesis_remote_api_key": "secret",
            },
        )
        session.commit()

    def chat(
        _: object, model: str, system_prompt: str, input_text: str
    ) -> dict[str, object]:
        assert model == "remote-model"
        assert "每个 block 只允许 kind、body、attribution、citations" in system_prompt
        assert "每个 citation 只允许 evidence_version_uid、side" in system_prompt
        assert "不得输出任何其他字段" in system_prompt
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.llm.OpenAICompatibleChatProvider.chat", chat)
    body = TestClient(app).post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "openai_compatible"},
    )

    assert body.status_code == 200
    assert body.json()["current"]["provider"] == "openai_compatible"
    assert body.json()["current"]["model"] == "remote-model"


def test_cleared_default_remote_key_fails_closed_before_model_call(monkeypatch) -> None:
    fixture = seed_multi_source_event()
    client = TestClient(app)
    saved = client.patch(
        "/ai/settings",
        json={
            "synthesis_provider": "openai_compatible",
            "synthesis_remote_base_url": "https://api.example.com",
            "synthesis_remote_model": "remote-model",
            "synthesis_remote_api_key": "secret",
        },
    )
    assert saved.status_code == 200
    cleared = client.patch(
        "/ai/settings", json={"clear_synthesis_remote_api_key": True}
    )
    assert cleared.status_code == 200
    assert cleared.json()["task_provider"] == "local"
    assert cleared.json()["synthesis_provider"] == "openai_compatible"
    assert cleared.json()["synthesis_remote_api_key_configured"] is False

    def fail(*_: object, **__: object) -> None:
        raise AssertionError("cleared remote credentials must not call a model")

    monkeypatch.setattr("reader_api.llm.OpenAICompatibleChatProvider.chat", fail)
    generated = client.post(f"/events/{fixture['event_uid']}/synthesis", json={})

    assert generated.status_code == 400
    assert "API Key" in generated.json()["detail"]
    with sessionmaker(bind=engine)() as session:
        assert session.query(EvidenceSnapshot).count() == 0
        assert session.query(SynthesisVersion).count() == 0


@pytest.mark.parametrize(
    ("provider", "expected_error"),
    [
        ("local", "本地模型服务未连接，请检查 LM Studio"),
        ("openai_compatible", "云端合成服务不可用，请检查地址、模型和密钥"),
    ],
)
def test_synchronous_generation_failure_keeps_snapshot_and_audit(
    monkeypatch, provider: str, expected_error: str
) -> None:
    fixture = seed_multi_source_event()
    client = TestClient(app)
    with sessionmaker(bind=engine)() as session:
        interaction_count = session.query(InteractionEvent).count()
        user_state_count = session.query(EventUserState).count()
    if provider == "openai_compatible":
        saved = client.patch(
            "/ai/settings",
            json={
                "synthesis_remote_base_url": "https://api.example.com",
                "synthesis_remote_model": "remote-model",
                "synthesis_remote_api_key": "secret",
            },
        )
        assert saved.status_code == 200

    def fail(*_: object, **__: object) -> None:
        with sessionmaker(bind=engine)() as audit:
            attempt = audit.scalar(select(GenerationAttempt))
            assert attempt is not None and attempt.status == "running"
            assert audit.query(EvidenceSnapshot).count() == 1
            assert audit.query(EvidenceSnapshotMember).count() == 2
        raise RuntimeError("upstream leaked secret-token")

    target = (
        "reader_api.llm.OpenAICompatibleChatProvider.chat"
        if provider == "openai_compatible"
        else "reader_api.main.LocalChatProvider.chat"
    )
    monkeypatch.setattr(target, fail)
    response = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": provider}
    )

    assert response.status_code == 502
    assert response.json()["detail"] == expected_error
    assert "secret-token" not in response.text
    with sessionmaker(bind=engine)() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None and event.current_synthesis_version_id is None
        attempt = session.scalar(select(GenerationAttempt))
        assert attempt is not None and attempt.status == "failed"
        assert attempt.error == expected_error
        assert "secret-token" not in attempt.error
        assert session.query(LLMTask).count() == 0
        assert session.query(EvidenceSnapshot).count() == 1
        assert session.query(SynthesisVersion).count() == 0
        assert session.query(InteractionEvent).count() == interaction_count
        assert session.query(EventUserState).count() == user_state_count

    if provider == "local":
        synthesis = client.get(f"/events/{fixture['event_uid']}").json()[
            "synthesis"
        ]
        assert synthesis["status"] == "missing"
        assert synthesis["task"]["status"] == "failed"
        assert synthesis["task"]["application_status"] == "not_started"
        assert synthesis["task"]["result_uid"] is None
        assert synthesis["task"]["error"] == expected_error


def test_remote_synthesis_selection_does_not_reroute_item_summary(monkeypatch) -> None:
    fixture = seed_multi_source_event()
    client = TestClient(app)
    saved = client.patch(
        "/ai/settings",
        json={
            "synthesis_provider": "openai_compatible",
            "synthesis_remote_base_url": "https://api.example.com",
            "synthesis_remote_model": "remote-model",
            "synthesis_remote_api_key": "secret",
        },
    )
    assert saved.status_code == 200

    def local_chat(*_: object, **__: object) -> dict[str, object]:
        return {"summary": "仍由本地摘要路径生成"}

    def remote_fail(*_: object, **__: object) -> None:
        raise AssertionError("remote synthesis selection must not affect item summaries")

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", local_chat)
    monkeypatch.setattr(
        "reader_api.llm.OpenAICompatibleChatProvider.chat", remote_fail
    )
    response = client.post(f"/items/{fixture['item_id']}/summarize")

    assert response.status_code == 200
    assert response.json()["summary"] == "仍由本地摘要路径生成"


def test_later_event_revision_does_not_rewrite_the_snapshot(monkeypatch) -> None:
    fixture = seed_multi_source_event()

    def chat(_: object, model: str, __: str, input_text: str) -> dict[str, object]:
        return {"blocks": valid_blocks(version_uids_from_input(input_text))}

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", chat)
    client = TestClient(app)
    generated = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    ).json()
    Session = sessionmaker(bind=engine)
    with Session() as session:
        snapshot = session.scalar(select(EvidenceSnapshot))
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert snapshot is not None and event is not None
        old_target = snapshot.target_revision_id
        old_content_fingerprint = snapshot.content_fingerprint
        old_members = list(
            session.scalars(
                select(EvidenceSnapshotMember.evidence_version_id)
                .where(EvidenceSnapshotMember.snapshot_id == snapshot.id)
                .order_by(EvidenceSnapshotMember.position)
            )
        )
        old_revision = session.get(EventRevision, event.current_revision_id)
        assert old_revision is not None
        next_revision = EventRevision(
            uid="aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa",
            event_id=event.id,
            revision_no=old_revision.revision_no + 1,
            evidence_fingerprint="9" * 64,
            title_snapshot="Changed event",
            event_time_snapshot=NOW,
            created_at=NOW,
        )
        session.add(next_revision)
        session.flush()
        first_link = session.scalar(
            select(EventRevisionEvidence)
            .where(EventRevisionEvidence.revision_id == old_revision.id)
            .order_by(EventRevisionEvidence.id)
            .limit(1)
        )
        assert first_link is not None
        session.add(
            EventRevisionEvidence(
                revision_id=next_revision.id,
                evidence_version_id=first_link.evidence_version_id,
                evidence_type=first_link.evidence_type,
                role=first_link.role,
                created_at=NOW,
            )
        )
        event.current_revision_id = next_revision.id
        session.commit()

    with Session() as session:
        snapshot = session.scalar(select(EvidenceSnapshot))
        assert snapshot is not None
        assert snapshot.target_revision_id == old_target
        assert snapshot.content_fingerprint == old_content_fingerprint
        assert (
            list(
                session.scalars(
                    select(EvidenceSnapshotMember.evidence_version_id)
                    .where(EvidenceSnapshotMember.snapshot_id == snapshot.id)
                    .order_by(EvidenceSnapshotMember.position)
                )
            )
            == old_members
        )
    state = client.get(f"/events/{fixture['event_uid']}").json()["synthesis"]
    assert generated["current"]["target_revision_uid"] != state["target_revision_uid"]
    assert state["status"] == "missing"
    assert state["current"] is not None


def test_single_source_event_keeps_old_synthesis_but_defaults_to_source(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    Session = sessionmaker(bind=engine)
    with Session() as session:
        event = session.scalar(select(Event).where(Event.uid == fixture["event_uid"]))
        assert event is not None
        links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == event.current_revision_id)
                .order_by(EventRevisionEvidence.id)
            )
        )
        assert len(links) == 2
        append_event_revision(session, event, [links[0]], [links[0].role])
        session.commit()
        event_uid = event.uid

    state = client.get(f"/events/{event_uid}").json()["synthesis"]
    listed = next(
        row
        for row in client.get("/clusters").json()
        if row["id"] == fixture["cluster_id"]
    )

    assert state["status"] == listed["synthesis_freshness"]["status"] == "missing"
    assert state["current"] is not None
    assert state["source_count"] == 1
    assert state["can_generate"] is False
    assert state["default_view"] == "source"
    assert client.post(f"/events/{event_uid}/synthesis", json={}).status_code == 409


def test_lineage_only_reuses_synthesis_for_stable_event_and_preserves_parent_history(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))

    Session = sessionmaker(bind=engine, expire_on_commit=False)
    with Session() as session:
        parent = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        items = list(session.scalars(select(ContentItem).order_by(ContentItem.id)))
        original_cluster = session.scalar(
            select(Cluster)
            .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
            .where(ClusterItem.content_item_id == items[0].id)
        )
        assert parent is not None and original_cluster is not None
        a_version_id = parent.current_synthesis_version_id
        assert a_version_id is not None
        a_version = session.get(SynthesisVersion, a_version_id)
        assert a_version is not None
        a_version_uid = a_version.uid
        assert len(items) == 2

        original_links = list(
            session.scalars(
                select(EventRevisionEvidence)
                .where(EventRevisionEvidence.revision_id == fixture["revision_id"])
                .order_by(EventRevisionEvidence.id)
            )
        )
        b_roles = [link.role for link in original_links]
        b_roles[0] = "challenge"
        append_event_revision(session, parent, original_links, b_roles)
        session.commit()
        clear_current_synthesis_pointer(parent.uid)
        b_state = client.post(
            f"/events/{parent.uid}/synthesis", json={"provider": "local"}
        )
        assert b_state.status_code == 200
        b_version_uid = b_state.json()["current"]["version_uid"]
        assert b_version_uid != a_version_uid
        session.refresh(parent)
        b_version_id = parent.current_synthesis_version_id
        assert b_version_id is not None and b_version_id != a_version_id

        append_event_revision(
            session,
            parent,
            original_links,
            [link.role for link in original_links],
        )
        session.commit()
        active_a = client.get(f"/events/{parent.uid}")
        assert active_a.status_code == 200
        assert active_a.json()["synthesis"]["current"]["version_uid"] == a_version_uid
        session.refresh(parent)
        assert parent.current_synthesis_version_id == b_version_id

        with clustering_run(
            session,
            scope_type="synthesis-lineage-continuation",
            item_ids=[item.id for item in items],
            rule_version="synthesis-lineage-v1",
        ) as continuation_run_id:
            for link in session.scalars(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == original_cluster.id
                )
            ):
                session.delete(link)
            rebuilt_cluster = Cluster(
                cluster_key="synthesis-lineage-rebuilt",
                title="Rebuilt synthesis cluster",
                first_seen_at=NOW,
                last_seen_at=NOW,
            )
            session.add(rebuilt_cluster)
            session.flush()
            session.add_all(
                ClusterItem(
                    cluster_id=rebuilt_cluster.id,
                    content_item_id=item.id,
                )
                for item in items
            )
        session.commit()

        continuation = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == continuation_run_id
            )
        )
        session.refresh(parent)
        assert continuation is not None
        assert continuation.reconciliation_kind == "continued"
        assert continuation.event_id == parent.id
        assert parent.current_synthesis_version_id == b_version_id
        continued_state = client.get(f"/events/{parent.uid}").json()["synthesis"]
        assert continued_state["status"] == "current"
        assert continued_state["current"]["version_uid"] == a_version_uid
        continued_list_row = next(
            row
            for row in client.get("/clusters").json()
            if row["id"] == rebuilt_cluster.id
        )
        assert continued_list_row["synthesis_freshness"]["status"] == "current"

        with monkeypatch.context() as policy_patch:
            policy_patch.setattr(
                "reader_api.event_synthesis.SYNTHESIS_POLICY_VERSION",
                "event-synthesis-policy-v3-test",
            )
            active_after_policy_change = client.get(f"/events/{parent.uid}")
            assert active_after_policy_change.status_code == 200
            assert (
                active_after_policy_change.json()["synthesis"]["status"]
                == "missing"
            )

        with clustering_run(
            session,
            scope_type="synthesis-lineage-split",
            item_ids=[item.id for item in items],
            rule_version="synthesis-lineage-v1",
        ) as split_run_id:
            second_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == rebuilt_cluster.id,
                    ClusterItem.content_item_id == items[1].id,
                )
            )
            assert second_link is not None
            session.delete(second_link)
            split_cluster = Cluster(
                cluster_key="synthesis-lineage-split",
                title="Split synthesis cluster",
                first_seen_at=NOW,
                last_seen_at=NOW,
            )
            session.add(split_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=split_cluster.id,
                    content_item_id=items[1].id,
                )
            )
        session.commit()

        children = list(
            session.scalars(
                select(Event)
                .join(
                    ClusterEventProjection,
                    ClusterEventProjection.event_id == Event.id,
                )
                .where(
                    ClusterEventProjection.clustering_run_id == split_run_id
                )
                .order_by(Event.id)
            )
        )
        session.refresh(parent)
        assert len(children) == 2
        assert parent.status == "superseded"
        assert parent.current_synthesis_version_id == b_version_id
        assert all(child.current_synthesis_version_id is None for child in children)
        assert all(
            client.get(f"/events/{child.uid}").json()["synthesis"]["status"]
            == "missing"
            for child in children
        )

        with monkeypatch.context() as policy_patch:
            policy_patch.setattr(
                "reader_api.event_synthesis.SYNTHESIS_POLICY_VERSION",
                "event-synthesis-policy-v3-test",
            )
            frozen_fallback = client.get(f"/events/{parent.uid}")
            assert frozen_fallback.status_code == 200
            assert frozen_fallback.json()["synthesis"]["status"] == "current"
            assert frozen_fallback.json()["synthesis"]["current"][
                "version_uid"
            ] == b_version_uid
            assert frozen_fallback.json()["synthesis"]["current"]["blocks"][0][
                "citations"
            ]

        before_delete = client.get(f"/events/{parent.uid}")
        assert before_delete.status_code == 200
        assert before_delete.json()["synthesis"]["status"] == "current"
        assert before_delete.json()["synthesis"]["current"][
            "version_uid"
        ] == a_version_uid
        assert before_delete.json()["synthesis"]["current"]["blocks"][0][
            "citations"
        ]
        revision_uid = before_delete.json()["current_revision"]["revision_uid"]

        cluster_ids = [
            original_cluster.id,
            rebuilt_cluster.id,
            split_cluster.id,
        ]
        for link in session.scalars(
            select(ClusterItem).where(ClusterItem.cluster_id.in_(cluster_ids))
        ):
            session.delete(link)
        session.flush()
        for cluster_id in cluster_ids:
            cluster = session.get(Cluster, cluster_id)
            assert cluster is not None
            session.delete(cluster)
        session.commit()

        assert session.get(SynthesisVersion, a_version_id) is not None
        after_delete = client.get(f"/events/{parent.uid}")
        assert after_delete.status_code == 200
        assert after_delete.json()["synthesis"]["status"] == "current"
        assert after_delete.json()["synthesis"]["current"][
            "version_uid"
        ] == a_version_uid
        assert after_delete.json()["synthesis"]["current"]["blocks"][0][
            "citations"
        ]
        revision = client.get(f"/events/{parent.uid}/revisions/{revision_uid}")
        assert revision.status_code == 200
        assert len(revision.json()["evidence"]) == 2


def test_merge_successor_does_not_copy_a_real_parent_synthesis(monkeypatch) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))

    Session = sessionmaker(bind=engine, expire_on_commit=False)
    with Session() as session:
        parent = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        items = list(session.scalars(select(ContentItem).order_by(ContentItem.id)))
        parent_cluster = session.scalar(
            select(Cluster)
            .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
            .where(ClusterItem.content_item_id == items[0].id)
        )
        assert parent is not None and parent_cluster is not None
        parent_version_id = parent.current_synthesis_version_id
        assert parent_version_id is not None

        third = add_source_item(session, 3)
        with clustering_run(
            session,
            scope_type="synthesis-lineage-merge-parent",
            item_ids=[third.id],
            rule_version="synthesis-lineage-v1",
        ):
            other_cluster = Cluster(
                cluster_key="synthesis-lineage-merge-parent",
                title="Other merge parent",
                first_seen_at=NOW,
                last_seen_at=NOW,
            )
            session.add(other_cluster)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=other_cluster.id,
                    content_item_id=third.id,
                )
            )
        session.commit()

        with clustering_run(
            session,
            scope_type="synthesis-lineage-merge",
            item_ids=[*(item.id for item in items), third.id],
            rule_version="synthesis-lineage-v1",
        ) as merge_run_id:
            other_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == other_cluster.id,
                    ClusterItem.content_item_id == third.id,
                )
            )
            assert other_link is not None
            session.delete(other_link)
            session.add(
                ClusterItem(
                    cluster_id=parent_cluster.id,
                    content_item_id=third.id,
                )
            )
        session.commit()

        successor_projection = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == merge_run_id
            )
        )
        assert successor_projection is not None
        assert successor_projection.reconciliation_kind == "merged"
        successor = session.get(Event, successor_projection.event_id)
        session.refresh(parent)
        assert successor is not None
        assert successor.id != parent.id
        assert successor.current_synthesis_version_id is None
        assert parent.status == "superseded"
        assert parent.current_synthesis_version_id == parent_version_id

        successor_detail = client.get(f"/events/{successor.uid}")
        parent_detail = client.get(f"/events/{parent.uid}")
        assert successor_detail.status_code == 200
        assert successor_detail.json()["synthesis"]["status"] == "missing"
        assert parent_detail.status_code == 200
        assert parent_detail.json()["synthesis"]["status"] == "current"
        assert parent_detail.json()["synthesis"]["current"]["blocks"][0][
            "citations"
        ]


def test_material_review_reenters_unread_until_the_target_revision_is_seen(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    initial = client.get(f"/events/{fixture['event_uid']}").json()
    seen_revision_uid = initial["current_revision"]["revision_uid"]
    mark_event_seen(client, fixture, seen_revision_uid, "issue45-seen-initial")
    order_before = [row["id"] for row in client.get("/clusters").json()]
    first_seen_before = client.get(f"/clusters/{fixture['cluster_id']}").json()[
        "first_seen_at"
    ]
    target_revision_uid = add_new_source_evidence(
        fixture, index=3, scope="event-material-update-projection"
    )

    before_review = client.get(f"/events/{fixture['event_uid']}").json()
    assert before_review["current_revision_differs_from_seen"] is True
    assert before_review["has_material_update"] is False
    assert before_review["material_update_revision_uid"] is None
    assert client.get(
        "/clusters", params={"read_status": "unread"}
    ).json() == []

    def material_without_rewrite(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        return {
            "result": "material",
            "reason": "新增证据改变了事件的核心事实。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr(
        "reader_api.main.LocalChatProvider.chat", material_without_rewrite
    )
    reviewed = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert reviewed.status_code == 502

    with sessionmaker(bind=engine)() as session:
        target_revision = session.scalar(
            select(EventRevision).where(EventRevision.uid == target_revision_uid)
        )
        assert target_revision is not None
        target_revision.created_at = datetime(
            2026, 7, 17, 8, 0, tzinfo=timezone.utc
        )
        session.commit()
        selected = report_clusters(
            session,
            datetime(2026, 7, 17, tzinfo=timezone.utc),
            datetime(2026, 7, 18, tzinfo=timezone.utc),
        )
        assert [cluster.id for cluster in selected] == [fixture["cluster_id"]]

    event_detail = client.get(f"/events/{fixture['event_uid']}").json()
    cluster_detail = client.get(f"/clusters/{fixture['cluster_id']}").json()
    listed = next(
        row
        for row in client.get("/clusters").json()
        if row["id"] == fixture["cluster_id"]
    )
    for projection in (event_detail, cluster_detail, listed):
        assert projection["has_material_update"] is True
        assert projection["material_update_revision_uid"] == target_revision_uid
    assert event_detail["user_state"]["read_status"] == "summary_seen"
    assert cluster_detail["read_status"] == "summary_seen"
    assert event_detail["synthesis"]["status"] == "stale"
    assert [
        row["id"]
        for row in client.get(
            "/clusters", params={"read_status": "unread"}
        ).json()
    ] == [fixture["cluster_id"]]
    assert client.get(
        "/clusters/count", params={"read_status": "unread"}
    ).json() == {"count": 1}
    assert {row["all_unread_count"] for row in client.get("/sources").json()} == {
        1
    }
    assert [row["id"] for row in client.get("/clusters").json()] == order_before
    assert cluster_detail["first_seen_at"] == first_seen_before

    stale_seen = mark_event_seen(
        client, fixture, seen_revision_uid, "issue45-seen-stale-synthesis"
    )
    assert stale_seen["seen_revision_uid"] == seen_revision_uid
    assert stale_seen["has_material_update"] is True
    assert stale_seen["material_update_revision_uid"] == target_revision_uid
    still_updated = client.get(f"/events/{fixture['event_uid']}").json()
    assert still_updated["has_material_update"] is True
    assert still_updated["material_update_revision_uid"] == target_revision_uid

    current_seen = mark_event_seen(
        client, fixture, target_revision_uid, "issue45-seen-current-source"
    )
    assert current_seen["seen_revision_uid"] == target_revision_uid
    assert current_seen["has_material_update"] is False
    assert current_seen["material_update_revision_uid"] is None
    cleared = client.get(f"/events/{fixture['event_uid']}").json()
    cleared_cluster = client.get(f"/clusters/{fixture['cluster_id']}").json()
    for projection in (cleared, cleared_cluster):
        assert projection["has_material_update"] is False
        assert projection["material_update_revision_uid"] is None
    assert client.get(
        "/clusters", params={"read_status": "unread"}
    ).json() == []
    assert client.get(
        "/clusters/count", params={"read_status": "unread"}
    ).json() == {"count": 0}
    assert [row["id"] for row in client.get("/clusters").json()] == order_before

    with sessionmaker(bind=engine)() as session:
        interactions = list(
            session.scalars(
                select(InteractionEvent).where(
                    InteractionEvent.operation_id.like("issue45-%")
                )
            )
        )
        assert {interaction.operation_id for interaction in interactions} == {
            "issue45-seen-initial",
            "issue45-seen-stale-synthesis",
            "issue45-seen-current-source",
        }
        revision_ids = {
            session.scalar(
                select(EventRevision.id).where(EventRevision.uid == revision_uid)
            )
            for revision_uid in (seen_revision_uid, target_revision_uid)
        }
        assert {
            interaction.observed_revision_id for interaction in interactions
        } == revision_ids
        state = session.scalar(
            select(EventUserState).where(
                EventUserState.event_id == fixture["event_id"]
            )
        )
        assert state is not None
        session.delete(state)
        session.commit()

    no_seen_state = client.get(f"/events/{fixture['event_uid']}").json()
    assert no_seen_state["has_material_update"] is False
    assert no_seen_state["material_update_revision_uid"] is None
    assert [
        row["id"]
        for row in client.get(
            "/clusters", params={"read_status": "unread"}
        ).json()
    ] == [fixture["cluster_id"]]


def test_report_period_keeps_an_earlier_material_update_after_a_later_one(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))

    def material_without_rewrite(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        return {
            "result": "material",
            "reason": "新增证据改变了事件的核心事实。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr(
        "reader_api.main.LocalChatProvider.chat", material_without_rewrite
    )
    for index, created_at in (
        (3, datetime(2026, 7, 17, 8, 0, tzinfo=timezone.utc)),
        (4, datetime(2026, 7, 19, 8, 0, tzinfo=timezone.utc)),
    ):
        revision_uid = add_new_source_evidence(
            fixture,
            index=index,
            scope=f"report-material-window-{index}",
        )
        assert client.post(
            f"/events/{fixture['event_uid']}/synthesis",
            json={"provider": "local"},
        ).status_code == 502
        with sessionmaker(bind=engine)() as session:
            revision = session.scalar(
                select(EventRevision).where(EventRevision.uid == revision_uid)
            )
            assert revision is not None
            revision.created_at = created_at
            session.commit()

    with sessionmaker(bind=engine)() as session:
        selected = report_clusters(
            session,
            datetime(2026, 7, 17, tzinfo=timezone.utc),
            datetime(2026, 7, 18, tzinfo=timezone.utc),
        )

    assert [cluster.id for cluster in selected] == [fixture["cluster_id"]]


@pytest.mark.parametrize("review_result", ["ordinary", "uncertain"])
def test_non_material_reviews_never_project_a_seen_event_as_updated(
    monkeypatch,
    review_result: str,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    seen_revision_uid = client.get(f"/events/{fixture['event_uid']}").json()[
        "current_revision"
    ]["revision_uid"]
    mark_event_seen(
        client,
        fixture,
        seen_revision_uid,
        f"issue45-{review_result}-seen",
    )
    add_new_source_evidence(
        fixture,
        index=3,
        scope=f"event-{review_result}-update-counterexample",
    )

    def review(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        return {
            "result": review_result,
            "reason": "这不是已确认的实质变化。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
        }

    monkeypatch.setattr("reader_api.main.LocalChatProvider.chat", review)
    assert client.post(
        f"/events/{fixture['event_uid']}/synthesis",
        json={"provider": "local"},
    ).status_code == 200

    event_detail = client.get(f"/events/{fixture['event_uid']}").json()
    cluster_detail = client.get(f"/clusters/{fixture['cluster_id']}").json()
    for projection in (event_detail, cluster_detail):
        assert projection["has_material_update"] is False
        assert projection["material_update_revision_uid"] is None
    assert client.get(
        "/clusters", params={"read_status": "unread"}
    ).json() == []
    assert client.get(
        "/clusters/count", params={"read_status": "unread"}
    ).json() == {"count": 0}


def test_reading_a_new_synthesis_covering_the_material_target_clears_the_update(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    seen_revision_uid = client.get(f"/events/{fixture['event_uid']}").json()[
        "current_revision"
    ]["revision_uid"]
    mark_event_seen(
        client, fixture, seen_revision_uid, "issue45-new-synthesis-baseline"
    )
    target_revision_uid = add_new_source_evidence(
        fixture,
        index=3,
        scope="event-material-update-new-synthesis",
    )

    def material_with_rewrite(
        _: object, model: str, __: str, input_text: str
    ) -> dict[str, object]:
        comparison = json.loads(input_text)
        target_evidence = comparison["target_evidence"]
        return {
            "result": "material",
            "reason": "新增证据改变了事件的核心事实。",
            "citations": [
                {
                    "evidence_version_uid": comparison["new_evidence"][0][
                        "evidence_version_uid"
                    ]
                }
            ],
            "synthesis": {
                "blocks": valid_blocks(
                    [row["evidence_version_uid"] for row in target_evidence]
                )
            },
        }

    monkeypatch.setattr(
        "reader_api.main.LocalChatProvider.chat", material_with_rewrite
    )
    refreshed = client.post(
        f"/events/{fixture['event_uid']}/synthesis", json={"provider": "local"}
    )
    assert refreshed.status_code == 200
    assert refreshed.json()["current"]["target_revision_uid"] == target_revision_uid
    updated = client.get(f"/events/{fixture['event_uid']}").json()
    assert updated["has_material_update"] is True
    assert updated["material_update_revision_uid"] == target_revision_uid

    seen = mark_event_seen(
        client, fixture, target_revision_uid, "issue45-new-synthesis-seen"
    )
    assert seen["has_material_update"] is False
    assert seen["material_update_revision_uid"] is None
    assert client.get(f"/events/{fixture['event_uid']}").json()[
        "has_material_update"
    ] is False


def test_ambiguous_successor_does_not_copy_a_real_parent_synthesis(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    client = generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))

    Session = sessionmaker(bind=engine, expire_on_commit=False)
    with Session() as session:
        parent = session.scalar(
            select(Event).where(Event.uid == fixture["event_uid"])
        )
        items = list(session.scalars(select(ContentItem).order_by(ContentItem.id)))
        parent_cluster = session.scalar(
            select(Cluster)
            .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
            .where(ClusterItem.content_item_id == items[0].id)
        )
        assert parent is not None and parent_cluster is not None
        parent_version_id = parent.current_synthesis_version_id
        assert parent_version_id is not None
        third = add_source_item(session, 3)

        with clustering_run(
            session,
            scope_type="synthesis-lineage-ambiguous",
            item_ids=[*(item.id for item in items), third.id],
            rule_version="synthesis-lineage-v1",
        ) as ambiguous_run_id:
            replaced_link = session.scalar(
                select(ClusterItem).where(
                    ClusterItem.cluster_id == parent_cluster.id,
                    ClusterItem.content_item_id == items[1].id,
                )
            )
            assert replaced_link is not None
            session.delete(replaced_link)
            session.add(
                ClusterItem(
                    cluster_id=parent_cluster.id,
                    content_item_id=third.id,
                )
            )
        session.commit()

        successor_projection = session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.clustering_run_id == ambiguous_run_id
            )
        )
        assert successor_projection is not None
        assert successor_projection.reconciliation_kind == "ambiguous"
        successor = session.get(Event, successor_projection.event_id)
        session.refresh(parent)
        assert successor is not None
        assert successor.id != parent.id
        assert successor.current_synthesis_version_id is None
        assert parent.status == "superseded"
        assert parent.current_synthesis_version_id == parent_version_id

        successor_detail = client.get(f"/events/{successor.uid}")
        parent_detail = client.get(f"/events/{parent.uid}")
        assert successor_detail.status_code == 200
        assert successor_detail.json()["synthesis"]["status"] == "missing"
        assert parent_detail.status_code == 200
        assert parent_detail.json()["synthesis"]["status"] == "current"
        assert parent_detail.json()["synthesis"]["current"]["blocks"][0][
            "citations"
        ]
