from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from reader_api.ai_runtime import save_ai_settings
from reader_api.db import engine
from reader_api.main import app
from reader_api.maintenance import GENERATION_RETENTION, run_explicit_maintenance
from reader_api.models import (
    Cluster,
    ClusterItem,
    ContentItem,
    Event,
    EventRevision,
    GenerationApplication,
    GenerationAttempt,
    GenerationRequest,
    GenerationRequestPayload,
    GenerationResult,
    LLMTask,
    Source,
    SynthesisBlock,
    UserState,
    now_utc,
)
from reader_api.report_generation import (
    ReportValidationError,
    _legacy_report_timestamp,
    freeze_report_input,
    report_clusters,
    validate_report_output,
)
from tests.factories import assign_publishable_cluster
from tests.test_event_synthesis import (
    add_new_source_evidence,
    add_source_item,
    generate_valid_synthesis,
    seed_multi_source_event,
)
def test_legacy_report_timestamp_is_stable_across_sqlite_timezone_reload() -> None:
    aware = datetime(2026, 7, 19, 8, 30, tzinfo=timezone.utc)
    naive = aware.replace(tzinfo=None)

    assert _legacy_report_timestamp(aware) == "2026-07-19T08:30:00+00:00"
    assert _legacy_report_timestamp(naive) == "2026-07-19T08:30:00+00:00"


def _report_input(*numbers: int) -> dict[str, object]:
    return {
        "items": [
            {
                "cluster_id": number,
                "event_uid": f"event-{number}",
                "event_revision_uid": f"revision-{number}",
                "title": f"事件 {number}",
                "first_seen_at": None,
                "last_seen_at": None,
                "documents": [
                    {
                        "source_name": f"来源 {number}",
                        "title": f"文章 {number}",
                        "url": f"https://example.com/{number}",
                        "published_at": None,
                    }
                ],
            }
            for number in numbers
        ]
    }


def test_report_output_returns_only_cited_frozen_items_in_first_use_order() -> None:
    result = validate_report_output(
        {"title": "日报", "body": "先看第二项 [2]，再补第一项 [1]，并再次引用 [2]。"},
        _report_input(1, 2),
    )

    assert result["cluster_ids"] == [2, 1]
    assert [citation["event_uid"] for citation in result["citations"]] == [
        "event-2",
        "event-1",
    ]
    assert [citation["citation_no"] for citation in result["citations"]] == [2, 1]


def test_report_output_rejects_uncited_paragraphs_and_list_items() -> None:
    for body in (
        "已核验事实 [1]\n\n未提供来源的结论",
        "- 有来源 [1]\n- 无来源",
    ):
        with pytest.raises(ReportValidationError, match="每个正文段落"):
            validate_report_output({"title": "日报", "body": body}, _report_input(1))


def test_report_input_uses_current_event_synthesis_instead_of_legacy_cluster_summary(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))

    with sessionmaker(bind=engine)() as session:
        cluster = session.get(Cluster, fixture["cluster_id"])
        event = session.get(Event, fixture["event_id"])
        assert cluster is not None and event is not None
        revision = session.get(EventRevision, event.current_revision_id)
        assert revision is not None
        summary = session.query(SynthesisBlock).filter_by(
            synthesis_version_id=event.current_synthesis_version_id,
            kind="summary",
        ).one()
        cluster.generated_title = "旧 Cluster 标题"
        cluster.generated_summary = "旧 Cluster 摘要"
        revision.title_snapshot = "当前 Event 标题"
        summary.body = "当前 Event 合成摘要"
        session.commit()

        selected = report_clusters(
            session,
            datetime(2026, 7, 16, tzinfo=timezone.utc),
            datetime(2026, 7, 17, tzinfo=timezone.utc),
        )
        frozen = freeze_report_input(
            session,
            period="day",
            start=datetime(2026, 7, 16, tzinfo=timezone.utc),
            end=datetime(2026, 7, 17, tzinfo=timezone.utc),
            clusters=selected,
            source_ids=[source.id for source in session.query(Source).all()],
            provenance_complete=True,
        )
        session.delete(summary)
        session.commit()
        frozen_without_summary = freeze_report_input(
            session,
            period="day",
            start=datetime(2026, 7, 16, tzinfo=timezone.utc),
            end=datetime(2026, 7, 17, tzinfo=timezone.utc),
            clusters=selected,
            source_ids=[source.id for source in session.query(Source).all()],
            provenance_complete=True,
        )

    assert frozen["items"][0]["title"] == "当前 Event 标题"
    assert frozen["items"][0]["summary"] == "当前 Event 合成摘要"
    assert frozen_without_summary["items"][0]["summary"] == "当前 Event 标题"


def test_report_input_never_pairs_a_stale_synthesis_with_the_current_revision(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    generate_valid_synthesis(monkeypatch, str(fixture["event_uid"]))
    current_revision_uid = add_new_source_evidence(
        fixture,
        index=3,
        scope="report-stale-synthesis",
    )

    with sessionmaker(bind=engine)() as session:
        cluster = session.get(Cluster, fixture["cluster_id"])
        event = session.get(Event, fixture["event_id"])
        assert cluster is not None and event is not None
        revision = session.get(EventRevision, event.current_revision_id)
        assert revision is not None
        revision.title_snapshot = "当前证据标题"
        summary = session.query(SynthesisBlock).filter_by(
            synthesis_version_id=event.current_synthesis_version_id,
            kind="summary",
        ).one()
        summary.body = "尚未覆盖当前证据的旧摘要"
        session.commit()

        frozen = freeze_report_input(
            session,
            period="day",
            start=datetime(2026, 7, 16, tzinfo=timezone.utc),
            end=datetime(2026, 7, 17, tzinfo=timezone.utc),
            clusters=[cluster],
            source_ids=[source.id for source in session.query(Source).all()],
            provenance_complete=True,
        )

    assert frozen["items"][0]["event_revision_uid"] == current_revision_uid
    assert frozen["items"][0]["summary"] == "当前证据标题"


def test_report_selection_excludes_clusters_with_only_uninterested_items() -> None:
    fixture = seed_multi_source_event()

    with sessionmaker(bind=engine)() as session:
        item_ids = [
            item_id
            for (item_id,) in session.query(ClusterItem.content_item_id)
            .filter_by(cluster_id=fixture["cluster_id"])
            .all()
        ]
        session.add_all(
            UserState(
                object_type="item",
                object_id=item_id,
                read_status="unread",
                uninterested=True,
                uninterested_at=now_utc(),
            )
            for item_id in item_ids
        )
        session.commit()

        selected = report_clusters(
            session,
            datetime(2026, 7, 16, tzinfo=timezone.utc),
            datetime(2026, 7, 17, tzinfo=timezone.utc),
        )

    assert selected == []


def test_postgres_report_selection_selects_its_distinct_sort_expression() -> None:
    statements = []

    class PostgreSQLSession:
        def get_bind(self):
            return SimpleNamespace(dialect=postgresql.dialect())

        def scalars(self, statement):
            statements.append(statement)
            return SimpleNamespace(all=lambda: [])

    assert report_clusters(
        PostgreSQLSession(),
        datetime(2026, 7, 16, tzinfo=timezone.utc),
        datetime(2026, 7, 17, tzinfo=timezone.utc),
    ) == []
    assert "report_at" in statements[0].selected_columns.keys()


def test_local_daily_weekly_and_monthly_reports_use_generation_lifecycle(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    monkeypatch.setattr("reader_api.main.settings.llm_task_provider", "local")

    class FakeProvider:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def chat(self, *_args, **_kwargs) -> dict[str, object]:
            return {
                "text": '{"title":"周期报告","body":"可核验正文 [1]"}',
                "usage": {"input_tokens": 40, "output_tokens": 8},
            }

    monkeypatch.setattr("reader_api.main.LocalChatProvider", FakeProvider)
    client = TestClient(app)

    for period in ("day", "week", "month"):
        generated = client.post(
            "/reports/generate",
            params={"period": period, "date": "2026-07-16"},
        )
        assert generated.status_code == 200, generated.text
        assert generated.json()["status"] == "ready"
        assert generated.json()["body"] == "可核验正文 [1]"
        assert generated.json()["items"] == [fixture["cluster_id"]]
        assert generated.json()["citations"][0]["sources"]

    tasks = client.get("/generation/tasks").json()
    assert {task["task_type"] for task in tasks} == {
        "report:day",
        "report:week",
        "report:month",
    }
    assert {task["status"] for task in tasks} == {"complete"}
    assert {task["artifact_type"] for task in tasks} == {"report"}
    assert {task["artifact_uid"] for task in tasks} == {
        "report:day:20260716",
        "report:week:20260713",
        "report:month:20260701",
    }
    assert {(task["input_tokens"], task["output_tokens"]) for task in tasks} == {
        (40, 8)
    }

    with sessionmaker(bind=engine)() as session:
        requests = session.query(GenerationRequest).order_by(GenerationRequest.id).all()
        assert len(requests) == 3
        assert {request.schema_version for request in requests} == {"report-schema-v4"}
        assert session.query(GenerationRequestPayload).count() == 3
        assert session.query(GenerationAttempt).count() == 3
        assert session.query(GenerationResult).count() == 3
        assert session.query(GenerationApplication).count() == 3
        assert (
            session.query(LLMTask)
            .filter(LLMTask.task_type.in_(("report:day", "report:week", "report:month")))
            .count()
            == 0
        )
        frozen = session.get(GenerationRequestPayload, requests[0].id)
        assert frozen is not None
        input_data = frozen.payload_json["input_data"]
        assert input_data["selection"]["rule_version"] == "event-material-update-report-selection-v3"
        assert input_data["items"][0]["event_uid"]
        assert input_data["items"][0]["event_revision_uid"]
        assert input_data["items"][0]["documents"][0]["document_id"]
        assert input_data["items"][0]["documents"][0]["source_id"]



def test_remote_report_uses_same_lifecycle_without_local_fallback(monkeypatch) -> None:
    fixture = seed_multi_source_event()
    with sessionmaker(bind=engine)() as session:
        save_ai_settings(
            session,
            {
                "synthesis_provider": "openai_compatible",
                "synthesis_remote_base_url": "https://api.example.com/v1",
                "synthesis_remote_model": "remote-report-model",
                "synthesis_remote_api_key": "test-remote-report-key",
            },
        )
        session.commit()

    calls: list[tuple[str, str]] = []

    class FakeRemoteProvider:
        def chat(
            self, model: str, _system_prompt: str, input_text: str
        ) -> dict[str, object]:
            calls.append((model, input_text))
            return {
                "text": '{"title":"远端月报","body":"远端正文 [1]"}',
                "usage": {"prompt_tokens": 55, "completion_tokens": 11},
            }

    monkeypatch.setattr(
        "reader_api.main.synthesis_remote_provider",
        lambda _settings: FakeRemoteProvider(),
    )

    response = TestClient(app).post(
        "/reports/generate", params={"period": "month", "date": "2026-07-16"}
    )

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "ready"
    assert response.json()["title"] == "远端月报"
    assert response.json()["items"] == [fixture["cluster_id"]]
    assert len(calls) == 1
    assert calls[0][0] == "remote-report-model"
    assert "Claim 1" in calls[0][1]
    task = TestClient(app).get("/generation/tasks").json()[0]
    assert task["provider"] == "openai_compatible"
    assert task["status"] == "complete"
    assert (task["input_tokens"], task["output_tokens"]) == (55, 11)
    assert len(task["sources"]) == 2

    with sessionmaker(bind=engine)() as session:
        assert session.query(GenerationRequest).one().provider == "openai_compatible"
        assert session.query(GenerationAttempt).one().status == "complete"
        assert session.query(GenerationResult).one().payload_json["body"] == "远端正文 [1]"
        assert (
            session.query(LLMTask).filter(LLMTask.task_type == "report:month").count()
            == 0
        )


def test_remote_report_rechecks_source_policy_before_persisting_result(
    monkeypatch,
) -> None:
    fixture = seed_multi_source_event()
    with sessionmaker(bind=engine)() as session:
        item = session.get(ContentItem, fixture["item_id"])
        assert item is not None
        source_id = item.source_id
        save_ai_settings(
            session,
            {
                "synthesis_provider": "openai_compatible",
                "synthesis_remote_base_url": "https://api.example.com/v1",
                "synthesis_remote_model": "remote-report-model",
                "synthesis_remote_api_key": "test-remote-report-key",
            },
        )
        session.commit()

    class TighteningProvider:
        def chat(self, *_args, **_kwargs) -> dict[str, object]:
            with sessionmaker(bind=engine)() as policy_session:
                source = policy_session.get(Source, source_id)
                assert source is not None
                source.external_generation_allowed = False
                policy_session.commit()
            return {
                "text": '{"title":"不应保存","body":"正文 [1]"}',
                "usage": {"input_tokens": 50, "output_tokens": 10},
            }

    monkeypatch.setattr(
        "reader_api.main.synthesis_remote_provider",
        lambda _settings: TighteningProvider(),
    )
    response = TestClient(app).post(
        "/reports/generate", params={"period": "day", "date": "2026-07-16"}
    )

    assert response.status_code == 409, response.text
    assert "来源" in response.json()["detail"]
    with sessionmaker(bind=engine)() as session:
        assert session.query(GenerationRequest).count() == 1
        attempt = session.query(GenerationAttempt).one()
        assert attempt.status == "failed"
        assert (attempt.input_tokens_actual, attempt.output_tokens_actual) == (50, 10)
        assert session.query(GenerationResult).count() == 0
        assert session.query(GenerationApplication).count() == 0


def test_report_rejects_cluster_without_event_revision_mapping() -> None:
    seed_multi_source_event()
    with sessionmaker(bind=engine)() as session:
        orphan = add_source_item(session, 99, embedding_vector="[0.0,1.0]")
        assign_publishable_cluster(session, orphan)
        session.commit()

    response = TestClient(app).post(
        "/reports/generate", params={"period": "day", "date": "2026-07-16"}
    )

    assert response.status_code == 409, response.text
    assert response.json()["detail"] == "报告入选 Cluster 缺少 Event/Revision 映射"
    with sessionmaker(bind=engine)() as session:
        assert session.query(GenerationRequest).count() == 0
        assert session.query(GenerationResult).count() == 0
        assert session.query(GenerationApplication).count() == 0


def test_report_apply_failure_reuses_result_without_second_model_call(
    monkeypatch,
) -> None:
    from reader_api import generation_results

    seed_multi_source_event()
    monkeypatch.setattr("reader_api.main.settings.llm_task_provider", "local")
    calls = 0

    class FakeProvider:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def chat(self, *_args, **_kwargs) -> dict[str, object]:
            nonlocal calls
            calls += 1
            return {
                "text": '{"title":"可重放日报","body":"正文 [1]"}',
                "usage": {"input_tokens": 44, "output_tokens": 9},
            }

    monkeypatch.setattr("reader_api.main.LocalChatProvider", FakeProvider)
    original_apply = generation_results.apply_report_result

    def fail_apply(*_args, **_kwargs) -> None:
        raise SQLAlchemyError("forced report apply failure")

    monkeypatch.setattr(
        generation_results,
        "apply_report_result",
        fail_apply,
    )
    client = TestClient(app)

    failed = client.post(
        "/reports/generate", params={"period": "day", "date": "2026-07-16"}
    )

    assert failed.status_code == 503, failed.text
    assert calls == 1
    with sessionmaker(bind=engine)() as session:
        assert session.query(GenerationResult).count() == 1
        assert session.query(GenerationApplication).one().status == "failed"
        frozen = session.query(GenerationRequestPayload).one()
        assert frozen.application_context_json is not None
        frozen.payload_json = None
        frozen.purged_at = now_utc()
        session.commit()

    monkeypatch.setattr(
        generation_results, "apply_report_result", original_apply
    )
    replayed = client.post(
        "/reports/generate", params={"period": "day", "date": "2026-07-16"}
    )

    assert replayed.status_code == 200, replayed.text
    assert replayed.json()["status"] == "ready"
    assert replayed.json()["title"] == "可重放日报"
    assert calls == 1
    with sessionmaker(bind=engine)() as session:
        assert session.query(GenerationResult).count() == 1
        application = session.query(GenerationApplication).one()
        assert application.status == "applied"
        assert application.apply_attempt_count == 2
        assert application.last_error == "生成结果保存失败，请重试"
