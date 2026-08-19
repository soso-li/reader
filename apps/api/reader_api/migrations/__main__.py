from __future__ import annotations

import argparse
import json
import os

from alembic import command
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import text

from ..deployment_validation import database_target
from ..production_target import production_target_identity
from .alembic_config import make_alembic_config
from .ambiguous_audit_fast_path import (
    AMBIGUOUS_AFTER_FAST_PATH_SQL,
    AMBIGUOUS_AUDIT_STAGE_REVISION,
    AMBIGUOUS_MEMBERSHIP_INDEX_SQL,
)
from .preflight import migration_engine, preflight_legacy_database, stamp_legacy_database
from .runtime import assert_database_at_head
from .folder_type_prepare import prepare_folder_media_types


MIGRATION_URL_ENV = "READER_MIGRATION_DATABASE_URL"
PRODUCTION_OVERRIDE_ENV = "READER_MIGRATION_ALLOW_PRODUCTION"


def _revision_ancestors(
    scripts: ScriptDirectory,
    revision_id: str,
) -> set[str]:
    ancestors: set[str] = set()
    pending = list(scripts.get_revisions(revision_id))
    while pending:
        revision = pending.pop()
        if revision.revision in ancestors:
            continue
        ancestors.add(revision.revision)
        down_revisions = revision.down_revision
        if down_revisions is None:
            continue
        if isinstance(down_revisions, str):
            down_revisions = (down_revisions,)
        pending.extend(scripts.get_revisions(down_revisions))
    return ancestors


def _current_revisions(database_url: str) -> tuple[str, ...]:
    engine = migration_engine(database_url)
    try:
        with engine.connect() as connection:
            return tuple(
                sorted(MigrationContext.configure(connection).get_current_heads())
            )
    finally:
        engine.dispose()


def _install_ambiguous_audit_fast_path(database_url: str) -> None:
    engine = migration_engine(database_url)
    try:
        with engine.begin() as connection:
            connection.execute(text(AMBIGUOUS_MEMBERSHIP_INDEX_SQL))
            connection.execute(text(AMBIGUOUS_AFTER_FAST_PATH_SQL))
            connection.execute(
                text(
                    "ANALYZE clustering_runs, clustering_run_memberships, "
                    "clustering_run_projection_predecessors, "
                    "cluster_event_projections"
                )
            )
    finally:
        engine.dispose()


def upgrade_database(database_url: str) -> None:
    config = make_alembic_config(database_url)
    current_revisions = _current_revisions(database_url)
    stage_ancestors = _revision_ancestors(
        ScriptDirectory.from_config(config),
        AMBIGUOUS_AUDIT_STAGE_REVISION,
    )
    if not current_revisions or (
        len(current_revisions) == 1
        and current_revisions[0] in stage_ancestors
    ):
        if current_revisions != (AMBIGUOUS_AUDIT_STAGE_REVISION,):
            command.upgrade(config, AMBIGUOUS_AUDIT_STAGE_REVISION)
        _install_ambiguous_audit_fast_path(database_url)
    command.upgrade(config, "head")


def database_url_from_environment(
    *,
    require_production_override: bool = True,
) -> str:
    database_url = os.environ.get(MIGRATION_URL_ENV, "").strip()
    if not database_url:
        raise RuntimeError(
            f"必须显式设置 {MIGRATION_URL_ENV}；不会回退到应用 DATABASE_URL"
        )
    identity = production_target_identity(database_url)
    make_alembic_config(database_url)
    if (
        require_production_override
        and identity.looks_like_production
        and os.environ.get(PRODUCTION_OVERRIDE_ENV) != "1"
    ):
        raise RuntimeError(
            "拒绝迁移疑似生产数据库；恢复副本必须使用独立数据库名。"
            f"确需生产维护时显式设置 {PRODUCTION_OVERRIDE_ENV}=1"
        )
    return database_url


def main() -> int:
    parser = argparse.ArgumentParser(description="Reader 数据库迁移工具")
    parser.add_argument(
        "command",
        choices=(
            "check-head",
            "preflight",
            "stamp-legacy",
            "upgrade",
            "prepare-folder-media-types",
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="执行 prepare-folder-media-types；默认仅输出只读计划",
    )
    parser.add_argument(
        "--production-maintenance",
        action="store_true",
        help="确认本次为已批准的生产维护窗口",
    )
    parser.add_argument(
        "--maintenance-id",
        default="",
        help="生产维护工单或变更编号",
    )
    args = parser.parse_args()
    if args.command != "prepare-folder-media-types" and (
        args.apply or args.production_maintenance or args.maintenance_id
    ):
        parser.error("--apply 和生产维护参数只适用于 prepare-folder-media-types")
    database_url = database_url_from_environment(
        require_production_override=args.command not in {
            "check-head",
            "prepare-folder-media-types",
        }
    )

    if args.command == "check-head":
        engine = migration_engine(database_url)
        try:
            assert_database_at_head(engine)
        finally:
            engine.dispose()
        print("数据库已位于 Alembic head")
        return 0

    if args.command == "preflight":
        report = preflight_legacy_database(database_url)
        if report.ok:
            print("legacy schema preflight 通过")
            return 0
        print("legacy schema preflight 失败")
        for error in report.errors:
            print(f"- {error}")
        return 1
    if args.command == "stamp-legacy":
        stamp_legacy_database(database_url)
        print("legacy schema 已登记 baseline revision")
        return 0

    if args.command == "prepare-folder-media-types":
        # Reuse deployment target safety rather than treating --apply as a
        # sufficient authorization. Production requires all three signals:
        # flag, non-empty maintenance id, and READER_DEPLOYMENT_ALLOW_PRODUCTION.
        target = database_target(
            database_url,
            production_maintenance=args.production_maintenance,
            maintenance_id=args.maintenance_id,
            environ=os.environ,
        )
        report = prepare_folder_media_types(
            database_url,
            apply=args.apply,
            target=target.public_identity(),
        )
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    upgrade_database(database_url)
    print("数据库已升级到 Alembic head")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
