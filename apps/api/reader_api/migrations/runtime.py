from __future__ import annotations

from alembic.migration import MigrationContext
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from .alembic_config import code_head_revisions


class SchemaRevisionError(RuntimeError):
    """Raised when a runtime process is not connected to the code schema head."""


def current_revision_heads(connection: Connection) -> tuple[str, ...]:
    return tuple(sorted(MigrationContext.configure(connection).get_current_heads()))


def assert_connection_at_head(connection: Connection) -> None:
    expected = code_head_revisions()
    current = current_revision_heads(connection)
    if not current:
        raise SchemaRevisionError(
            "数据库尚未登记 Alembic revision；请先运行独立 migrate job"
        )
    if current != expected:
        raise SchemaRevisionError(
            "数据库 Alembic revision 与代码不一致："
            f"current={','.join(current)} expected={','.join(expected)}；"
            "请先运行独立 migrate job"
        )


def assert_database_at_head(engine: Engine) -> None:
    try:
        with engine.connect() as connection:
            with connection.begin():
                if connection.dialect.name == "postgresql":
                    connection.execute(text("SET TRANSACTION READ ONLY"))
                assert_connection_at_head(connection)
    except SchemaRevisionError:
        raise
    except Exception as exc:
        raise RuntimeError("无法验证数据库 Alembic revision") from exc
