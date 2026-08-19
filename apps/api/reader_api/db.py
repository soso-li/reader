import os
from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import StaticPool

from .config import settings
from .migrations.runtime import assert_database_at_head


class Base(DeclarativeBase):
    pass


connect_args = (
    {"check_same_thread": False}
    if settings.database_url.startswith("sqlite")
    else {}
)
if settings.database_url.startswith("postgresql+psycopg"):
    connect_args = {"prepare_threshold": None}
engine_kwargs = {"connect_args": connect_args, "hide_parameters": True}
if settings.database_url == "sqlite:///:memory:":
    engine_kwargs["poolclass"] = StaticPool
engine = create_engine(settings.database_url, **engine_kwargs)
_engine_process_id = os.getpid()
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
)


def prepare_runtime_database() -> None:
    """Create isolated SQLite test schema or verify persistent PostgreSQL head."""
    global _engine_process_id

    process_id = os.getpid()
    if process_id != _engine_process_id:
        engine.dispose(close=False)
        _engine_process_id = process_id

    from . import models  # noqa: F401

    if engine.dialect.name == "sqlite":
        if engine.url.database not in {None, ":memory:"}:
            raise RuntimeError("SQLite 仅允许用于隔离的内存测试")
        Base.metadata.create_all(bind=engine)
        return
    if engine.dialect.name != "postgresql":
        raise RuntimeError(f"不支持的数据库类型：{engine.dialect.name}")
    assert_database_at_head(engine)


def get_session() -> Generator[Session, None, None]:
    with SessionLocal() as session:
        yield session
