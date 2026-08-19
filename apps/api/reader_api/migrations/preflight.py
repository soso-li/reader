from __future__ import annotations

from dataclasses import dataclass

from alembic import command
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection

from .alembic_config import (
    BASELINE_REVISION,
    PRODUCTION_LEGACY_REVISION,
    STRICT_LEGACY_REVISION,
    make_alembic_config,
)
from .schema_contract import (
    compare_legacy_schema,
    compare_production_legacy_schema,
    compare_strict_legacy_schema,
    read_postgres_schema,
)


PREFLIGHT_LOCK_ID = 780020263


@dataclass(frozen=True)
class PreflightReport:
    ok: bool
    errors: tuple[str, ...]
    current_revision: str | None
    matched_revision: str | None


def migration_engine(database_url: str):
    make_alembic_config(database_url)
    return create_engine(database_url, connect_args={"prepare_threshold": None})


def current_alembic_revision(connection: Connection) -> str | None:
    inspector = inspect(connection)
    if "alembic_version" not in inspector.get_table_names():
        return None
    columns = {column["name"] for column in inspector.get_columns("alembic_version")}
    if "version_num" not in columns:
        return "<invalid>"
    revisions = connection.scalars(text("SELECT version_num FROM alembic_version")).all()
    if len(revisions) != 1:
        return "<invalid>"
    return str(revisions[0])


def preflight_connection(connection: Connection) -> PreflightReport:
    snapshot = read_postgres_schema(connection)
    baseline_errors = compare_legacy_schema(snapshot)
    production_errors = compare_production_legacy_schema(snapshot)
    strict_errors = compare_strict_legacy_schema(snapshot)
    if not baseline_errors:
        matched_revision = BASELINE_REVISION
        errors: list[str] = []
    elif not strict_errors:
        matched_revision = STRICT_LEGACY_REVISION
        errors = []
    elif not production_errors:
        matched_revision = PRODUCTION_LEGACY_REVISION
        errors = []
    else:
        matched_revision = None
        errors = baseline_errors
    revision = current_alembic_revision(connection)
    allowed_revisions = {None, matched_revision}
    if revision not in allowed_revisions:
        expected_revision = matched_revision or "与 legacy schema 匹配的 revision"
        errors.append(
            f"Alembic revision 应为空或 {expected_revision}，实际为 {revision}"
        )
    return PreflightReport(
        ok=not errors,
        errors=tuple(errors),
        current_revision=revision,
        matched_revision=matched_revision,
    )


def preflight_legacy_database(database_url: str) -> PreflightReport:
    engine = migration_engine(database_url)
    try:
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text("SET TRANSACTION READ ONLY"))
                return preflight_connection(connection)
    finally:
        engine.dispose()


def stamp_legacy_database(database_url: str) -> None:
    engine = migration_engine(database_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": PREFLIGHT_LOCK_ID},
            )
            report = preflight_connection(connection)
            if not report.ok:
                details = "\n".join(f"- {error}" for error in report.errors)
                raise RuntimeError(f"legacy schema preflight 失败：\n{details}")
            config = make_alembic_config(database_url)
            config.attributes["connection"] = connection
            if report.matched_revision is None:
                raise RuntimeError("legacy schema 未匹配可登记 revision")
            command.stamp(config, report.matched_revision)
    finally:
        engine.dispose()
