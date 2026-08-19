import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import sessionmaker

from reader_api.db import Base, engine
from reader_api.generation_lifecycle import (
    complete_generation_attempt,
    get_or_create_generation_request,
    start_generation_attempt,
)
from reader_api.main import app


def test_generation_control_defaults_to_safe_zero_model_state() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    response = TestClient(app).get("/generation/control")

    assert response.status_code == 200
    assert response.json() == {
        "global_pause": True,
        "auto_run": False,
        "daily_budget_tokens": None,
        "input_estimator": "unicode-codepoints-v1",
        "output_reserve_tokens": 0,
        "day_timezone": "Asia/Shanghai",
        "used_tokens": 0,
        "reserved_tokens": 0,
        "remaining_tokens": None,
        "requires_usage_review": False,
    }


def test_generation_control_can_be_configured_and_persists() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    updated = client.patch(
        "/generation/control",
        json={
            "global_pause": False,
            "auto_run": True,
            "daily_budget_tokens": 1200,
            "input_estimator": "utf8-bytes-v1",
            "output_reserve_tokens": 200,
            "day_timezone": "UTC",
        },
    )

    assert updated.status_code == 200
    assert updated.json() == {
        "global_pause": False,
        "auto_run": True,
        "daily_budget_tokens": 1200,
        "input_estimator": "utf8-bytes-v1",
        "output_reserve_tokens": 200,
        "day_timezone": "UTC",
        "used_tokens": 0,
        "reserved_tokens": 0,
        "remaining_tokens": 1200,
        "requires_usage_review": False,
    }
    assert client.get("/generation/control").json() == updated.json()


@pytest.mark.parametrize(
    "change",
    [
        {"daily_budget_tokens": -1},
        {"daily_budget_tokens": 2_147_483_648},
        {"output_reserve_tokens": -1},
        {"output_reserve_tokens": 2_147_483_648},
        {"output_reserve_tokens": None},
        {"global_pause": None},
        {"auto_run": None},
        {"input_estimator": None},
        {"day_timezone": ""},
        {"day_timezone": None},
        {"day_timezone": "Mars/Olympus"},
    ],
)
def test_generation_control_rejects_invalid_budget_and_day_boundary(
    change: dict[str, object],
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    response = client.patch("/generation/control", json=change)

    assert response.status_code == 400
    assert client.get("/generation/control").json()["daily_budget_tokens"] is None


def test_generation_control_accepts_zero_as_a_configured_budget() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    response = client.patch(
        "/generation/control",
        json={"global_pause": False, "daily_budget_tokens": 0},
    )

    assert response.status_code == 200
    assert response.json()["daily_budget_tokens"] == 0
    assert response.json()["remaining_tokens"] == 0


def test_generation_control_rejects_an_unknown_input_estimator() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    response = TestClient(app).patch(
        "/generation/control",
        json={"input_estimator": "word-count-v1"},
    )

    assert response.status_code == 422


def test_generation_request_uses_the_configured_deterministic_estimator() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    assert client.patch(
        "/generation/control",
        json={"input_estimator": "utf8-bytes-v1"},
    ).status_code == 200
    payload = {"input": "中文"}
    canonical = '{"input":"中文"}'
    with sessionmaker(bind=engine)() as session:
        get_or_create_generation_request(
            session,
            task_type="event_synthesis",
            reason="automatic",
            target_type="event",
            target_id=1,
            target_uid="event-estimator",
            provider="local",
            model="local-model",
            prompt_version="prompt-v1",
            schema_version="schema-v1",
            input_fingerprint="c" * 64,
            payload=payload,
        )
        session.commit()

    task = client.get("/generation/tasks").json()[0]
    assert task["input_tokens_estimated"] == len(canonical.encode("utf-8"))


def test_generation_task_waits_for_public_one_time_approval() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    assert client.patch(
        "/generation/control",
        json={"output_reserve_tokens": 200},
    ).status_code == 200
    with sessionmaker(bind=engine)() as session:
        request, _ = get_or_create_generation_request(
            session,
            task_type="event_synthesis",
            reason="automatic",
            target_type="event",
            target_id=1,
            target_uid="event-1",
            provider="local",
            model="local-model",
            prompt_version="prompt-v1",
            schema_version="schema-v1",
            input_fingerprint="a" * 64,
            payload={"input": "frozen evidence"},
        )
        request_uid = request.uid
        session.commit()

    tasks = client.get("/generation/tasks")

    assert tasks.status_code == 200
    assert tasks.json()[0]["request_uid"] == request_uid
    assert tasks.json()[0]["status"] == "pending"
    assert tasks.json()[0]["approval_status"] == "awaiting"
    assert tasks.json()[0]["admission_status"] == "awaiting"
    assert tasks.json()[0]["input_tokens_estimated"] == len(
        '{"input":"frozen evidence"}'
    )
    assert tasks.json()[0]["output_tokens_reserved"] == 200

    approved = client.post(f"/generation/requests/{request_uid}/approve")

    assert approved.status_code == 200
    assert approved.json()["approval_status"] == "approved"
    assert approved.json()["admission_status"] == "awaiting"
    assert client.get("/generation/tasks").json()[0] == approved.json()


def test_generation_request_with_an_existing_result_cannot_be_approved() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with sessionmaker(bind=engine)() as session:
        request, _ = get_or_create_generation_request(
            session,
            task_type="event_synthesis",
            reason="legacy-complete",
            target_type="event",
            target_id=1,
            target_uid="event-complete",
            provider="local",
            model="local-model",
            prompt_version="prompt-v1",
            schema_version="schema-v1",
            input_fingerprint="b" * 64,
            payload={"input": "completed evidence"},
        )
        attempt = start_generation_attempt(session, request)
        complete_generation_attempt(
            session,
            attempt=attempt,
            payload={"blocks": []},
            input_tokens=12,
            output_tokens=7,
        )
        request_uid = request.uid
        session.commit()

    client = TestClient(app)
    task = client.get("/generation/tasks").json()[0]
    response = client.post(f"/generation/requests/{request_uid}/approve")

    assert task["result_uid"] is not None
    assert task["approval_status"] == "awaiting"
    assert response.status_code == 409
    assert response.json()["detail"] == "已有生成结果，无需再次批准"


def test_missing_actual_usage_applies_but_disables_auto_run_for_review() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    assert client.patch(
        "/generation/control",
        json={
            "global_pause": False,
            "auto_run": True,
            "daily_budget_tokens": 1000,
        },
    ).status_code == 200
    with sessionmaker(bind=engine)() as session:
        request, _ = get_or_create_generation_request(
            session,
            task_type="event_synthesis",
            reason="usage-unknown",
            target_type="event",
            target_id=1,
            target_uid="event-usage-unknown",
            provider="local",
            model="local-model",
            prompt_version="prompt-v1",
            schema_version="schema-v1",
            input_fingerprint="d" * 64,
            payload={"input": "completed evidence"},
        )
        attempt = start_generation_attempt(session, request)
        result, _application = complete_generation_attempt(
            session,
            attempt=attempt,
            payload={"blocks": []},
            input_tokens=None,
            output_tokens=7,
        )
        assert result.output_fingerprint
        assert result.schema_version == "schema-v1"
        session.commit()

    control = client.get("/generation/control").json()

    assert control["auto_run"] is False
    assert control["requires_usage_review"] is True
