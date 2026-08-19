from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import Engine

import reader_api.db as db_module
import reader_api.main as main_module
import reader_api.migrations.runtime as runtime_module
import reader_api.worker as worker_module
from reader_api.migrations.alembic_config import BASELINE_REVISION, code_head_revisions
from reader_api.migrations.runtime import (
    SchemaRevisionError,
    assert_connection_at_head,
    assert_database_at_head,
)
from reader_api.models import Cluster, ContentItem, Document, RawEntry, Source, UserState
from tests.factories import make_raw_entry


def make_revision_database(revision: str | None = None) -> Engine:
    engine = create_engine("sqlite:///:memory:")
    if revision is None:
        return engine
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        )
        connection.execute(
            text("INSERT INTO alembic_version (version_num) VALUES (:revision)"),
            {"revision": revision},
        )
    return engine


def test_runtime_gate_accepts_exact_code_head_without_writing() -> None:
    code_head = code_head_revisions()[0]
    engine = make_revision_database(code_head)

    with engine.connect() as connection:
        assert_connection_at_head(connection)
        assert (
            connection.scalar(text("SELECT version_num FROM alembic_version"))
            == code_head
        )
        assert connection.scalar(text("SELECT count(*) FROM alembic_version")) == 1

    engine.dispose()


def test_runtime_gate_rejects_unregistered_database() -> None:
    engine = make_revision_database()

    with engine.connect() as connection:
        with pytest.raises(SchemaRevisionError, match="未登记"):
            assert_connection_at_head(connection)

    engine.dispose()


def test_runtime_gate_rejects_behind_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = make_revision_database(BASELINE_REVISION)
    monkeypatch.setattr(
        runtime_module,
        "code_head_revisions",
        lambda: ("0002_future_revision",),
    )

    with engine.connect() as connection:
        with pytest.raises(SchemaRevisionError, match=BASELINE_REVISION):
            assert_connection_at_head(connection)

    engine.dispose()


def test_runtime_gate_rejects_unknown_revision() -> None:
    engine = make_revision_database("unknown_revision")

    with engine.connect() as connection:
        with pytest.raises(SchemaRevisionError, match="unknown_revision"):
            assert_connection_at_head(connection)

    engine.dispose()


def test_runtime_gate_rejects_unreachable_database() -> None:
    class UnreachableEngine:
        def connect(self):
            raise OSError("connection refused")

    with pytest.raises(RuntimeError, match="无法验证数据库"):
        assert_database_at_head(UnreachableEngine())  # type: ignore[arg-type]


def test_postgres_init_only_delegates_to_read_only_head_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []
    fake_engine = type(
        "FakeEngine",
        (),
        {"dialect": type("FakeDialect", (), {"name": "postgresql"})()},
    )()

    monkeypatch.setattr(db_module, "engine", fake_engine)
    monkeypatch.setattr(
        db_module,
        "assert_database_at_head",
        lambda selected_engine: calls.append(selected_engine),
    )
    monkeypatch.setattr(
        db_module.Base.metadata,
        "create_all",
        lambda **_: pytest.fail("PostgreSQL runtime 不得调用 metadata.create_all"),
    )

    db_module.prepare_runtime_database()

    assert calls == [fake_engine]


def test_postgres_init_discards_inherited_pool_before_head_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []

    class FakeEngine:
        dialect = type("FakeDialect", (), {"name": "postgresql"})()

        def dispose(self, *, close: bool = True) -> None:
            calls.append(("dispose", close))

    fake_engine = FakeEngine()
    monkeypatch.setattr(db_module, "engine", fake_engine)
    monkeypatch.setattr(db_module, "_engine_process_id", 100)
    monkeypatch.setattr(db_module.os, "getpid", lambda: 200)
    monkeypatch.setattr(
        db_module,
        "assert_database_at_head",
        lambda selected_engine: calls.append(("gate", selected_engine)),
    )

    db_module.prepare_runtime_database()
    db_module.prepare_runtime_database()

    assert calls == [
        ("dispose", False),
        ("gate", fake_engine),
        ("gate", fake_engine),
    ]


def test_prepare_runtime_database_rejects_persistent_sqlite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persistent_engine = create_engine(f"sqlite:///{tmp_path / 'reader.db'}")
    monkeypatch.setattr(db_module, "engine", persistent_engine)

    with pytest.raises(RuntimeError, match="内存测试"):
        db_module.prepare_runtime_database()

    persistent_engine.dispose()


def test_api_startup_stops_before_business_repairs_when_gate_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def reject_schema() -> None:
        calls.append("gate")
        raise SchemaRevisionError("数据库未到 head")

    monkeypatch.setattr(main_module, "prepare_runtime_database", reject_schema)

    with pytest.raises(SchemaRevisionError, match="未到 head"):
        with TestClient(main_module.app):
            pass

    assert calls == ["gate"]


def test_api_startup_does_not_run_business_repairs_after_gate_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        main_module,
        "prepare_runtime_database",
        lambda: calls.append("gate"),
    )

    def reject_repair(_: object) -> int:
        raise AssertionError("API 启动不得执行历史业务修复")

    monkeypatch.setattr(
        main_module,
        "repair_over_split_documents",
        reject_repair,
        raising=False,
    )
    monkeypatch.setattr(
        main_module,
        "repair_title_only_clusters",
        reject_repair,
        raising=False,
    )
    monkeypatch.setattr(
        main_module,
        "repair_windowed_clusters",
        reject_repair,
        raising=False,
    )

    with TestClient(main_module.app):
        pass

    assert calls == ["gate"]


def test_repeated_api_and_worker_startup_do_not_mutate_business_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_module.Base.metadata.drop_all(db_module.engine)
    db_module.Base.metadata.create_all(db_module.engine)
    tracked_models = (Source, RawEntry, Document, ContentItem, Cluster, UserState)

    with db_module.SessionLocal() as session:
        source = Source(name="startup invariant", url="https://example.com/startup.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="entry-1",
            title="Startup invariant",
            content_hash="raw-hash",
        )
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title=raw.title)
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_hash="item-hash",
        )
        cluster = Cluster(cluster_key="startup-cluster", title=raw.title)
        session.add_all(
            [
                item,
                cluster,
                UserState(object_type="item", object_id=1, starred=True),
            ]
        )
        session.commit()

    def snapshot() -> tuple[int, ...]:
        with db_module.SessionLocal() as session:
            return tuple(
                session.scalar(select(func.count()).select_from(model)) or 0
                for model in tracked_models
            )

    class StubRedis:
        @classmethod
        def from_url(cls, _: str) -> object:
            return object()

    class StubWorker:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def work(self) -> None:
            pass

    monkeypatch.setattr(worker_module, "start_fetch_scheduler", lambda: None)
    monkeypatch.setattr(worker_module, "Redis", StubRedis)
    monkeypatch.setattr(worker_module, "Worker", StubWorker)
    before = snapshot()

    for _ in range(2):
        with TestClient(main_module.app):
            pass
        worker_module.run_worker("fetch")
        worker_module.run_worker("llm")

    assert snapshot() == before == (1, 1, 1, 1, 1, 1)


@pytest.mark.parametrize("role", ["fetch", "llm"])
def test_worker_startup_stops_before_scheduler_and_redis_when_gate_fails(
    role: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def reject_schema() -> None:
        calls.append("gate")
        raise SchemaRevisionError("数据库未到 head")

    monkeypatch.setattr(worker_module, "prepare_runtime_database", reject_schema)
    monkeypatch.setattr(
        worker_module,
        "start_fetch_scheduler",
        lambda: calls.append("scheduler"),
    )
    monkeypatch.setattr(
        worker_module.Redis,
        "from_url",
        lambda _: calls.append("redis") or object(),
    )

    with pytest.raises(SchemaRevisionError, match="未到 head"):
        worker_module.run_worker(role)

    assert calls == ["gate"]
