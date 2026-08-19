from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import Connection


config = context.config
target_metadata = None


def run_migrations_offline() -> None:
    database_url = config.get_main_option("sqlalchemy.url")
    if not database_url:
        raise RuntimeError("Alembic database URL 未设置")
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_with_connection(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    supplied_connection = config.attributes.get("connection")
    if supplied_connection is not None:
        run_migrations_with_connection(supplied_connection)
        return

    section = config.get_section(config.config_ini_section, {})
    if not section.get("sqlalchemy.url"):
        raise RuntimeError("Alembic database URL 未设置")
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args={"prepare_threshold": None},
    )
    with connectable.connect() as connection:
        run_migrations_with_connection(connection)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
