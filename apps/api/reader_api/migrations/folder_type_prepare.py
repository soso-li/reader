"""Prepare the frozen 0069 Folder media-type migration without running DDL."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import importlib
from typing import Mapping

from alembic.migration import MigrationContext
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from ..cluster import decluster_source_items


EXPECTED_REVISION = "0068_cluster_current_projection"
FOLDER_TYPE_PREPARE_LOCK_KEY = 4_472_058_401_441_449_069


@dataclass(frozen=True)
class FolderTypePrepareAction:
    source_id: int
    folder_id: int
    folder_name: str
    source_status: str
    from_media_type: str
    to_media_type: str
    from_folder_id: int | None
    to_folder_id: int | None
    cluster_membership_count: int

    @property
    def requires_decluster(self) -> bool:
        return (
            self.from_media_type == "article"
            and self.to_media_type != "article"
            and self.cluster_membership_count > 0
        )

    def public_dict(self) -> dict[str, object]:
        result = asdict(self)
        result["requires_decluster"] = self.requires_decluster
        return result


def _migration_module():
    # The migration is deliberately the owner of its frozen legacy mapping.
    # Runtime code may consume it, but Alembic never imports runtime code.
    return importlib.import_module(
        "reader_api.alembic.versions.0069_folder_media_types"
    )


def _assert_expected_revision(connection: Connection) -> None:
    heads = tuple(sorted(MigrationContext.configure(connection).get_current_heads()))
    if heads != (EXPECTED_REVISION,):
        rendered = ", ".join(heads) if heads else "<none>"
        raise RuntimeError(
            "folder_media_type_prepare_requires_revision_0068: "
            f"current={rendered}; expected={EXPECTED_REVISION}"
        )


def _acquire_prepare_lock(connection: Connection) -> None:
    if connection.dialect.name == "postgresql":
        connection.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": FOLDER_TYPE_PREPARE_LOCK_KEY},
        )


def _membership_counts(connection: Connection) -> dict[int, int]:
    rows = connection.execute(
        text(
            """
            SELECT content_items.source_id, count(cluster_items.id)
            FROM cluster_items
            JOIN content_items ON content_items.id = cluster_items.content_item_id
            GROUP BY content_items.source_id
            """
        )
    )
    return {int(source_id): int(count) for source_id, count in rows}


def _folder_type_prepare_actions(connection: Connection) -> list[FolderTypePrepareAction]:
    migration = _migration_module()
    frozen_plan = migration._folder_type_plan(connection)
    current_sources = {
        int(row.id): row
        for row in connection.execute(
            text("SELECT id, folder_id, media_type, status FROM sources")
        ).mappings()
    }
    memberships = _membership_counts(connection)
    actions: list[FolderTypePrepareAction] = []
    for folder_id, target_media_type, planned_sources in frozen_plan:
        folder_name = str(
            connection.scalar(
                text("SELECT name FROM folders WHERE id = :folder_id"),
                {"folder_id": folder_id},
            )
            or ""
        )
        for source_id, effective_media_type, source_status in planned_sources:
            current = current_sources.get(source_id)
            if current is None:
                raise RuntimeError(
                    "folder_media_type_prepare_source_missing: "
                    f"source_id={source_id} folder_id={folder_id}"
                )
            desired_folder_id = (
                folder_id
                if source_status != "deleted"
                and effective_media_type == target_media_type
                else None
            )
            action = FolderTypePrepareAction(
                source_id=source_id,
                folder_id=folder_id,
                folder_name=folder_name,
                source_status=source_status,
                from_media_type=str(current.media_type),
                to_media_type=effective_media_type,
                from_folder_id=(
                    int(current.folder_id) if current.folder_id is not None else None
                ),
                to_folder_id=desired_folder_id,
                cluster_membership_count=memberships.get(source_id, 0),
            )
            if (
                action.from_media_type != action.to_media_type
                or action.from_folder_id != action.to_folder_id
            ):
                actions.append(action)
    return actions


def _apply_actions(session: Session, actions: list[FolderTypePrepareAction]) -> None:
    for action in actions:
        if action.requires_decluster:
            # Keep the source as article until the normal audited decluster
            # workflow has captured and removed its current membership.
            decluster_source_items(
                session,
                action.source_id,
                force=True,
                rollback_on_failure=True,
            )
        if action.from_media_type == action.to_media_type:
            continue
        session.execute(
            text("UPDATE sources SET media_type = :media_type WHERE id = :source_id"),
            {"source_id": action.source_id, "media_type": action.to_media_type},
        )
        session.flush()

    for action in actions:
        if action.from_folder_id == action.to_folder_id:
            continue
        session.execute(
            text("UPDATE sources SET folder_id = :folder_id WHERE id = :source_id"),
            {"source_id": action.source_id, "folder_id": action.to_folder_id},
        )


def _report(
    actions: list[FolderTypePrepareAction],
    *,
    apply: bool,
    target: Mapping[str, object] | None,
) -> dict[str, object]:
    return {
        "operation": "prepare-folder-media-types",
        "expected_revision": EXPECTED_REVISION,
        "applied": apply,
        "target": dict(target or {}),
        "action_count": len(actions),
        "decluster_source_count": sum(
            action.requires_decluster for action in actions
        ),
        "actions": [action.public_dict() for action in actions],
    }


def prepare_folder_media_types(
    database_url: str,
    *,
    apply: bool = False,
    target: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Plan or atomically apply all source changes that frozen 0069 will make.

    The URL is supplied by the migration CLI only after it has been explicitly
    read from ``READER_MIGRATION_DATABASE_URL`` and target-authorized.
    """
    engine = create_engine(database_url)
    try:
        if not apply:
            with engine.connect() as connection:
                with connection.begin():
                    if connection.dialect.name == "sqlite":
                        connection.exec_driver_sql("PRAGMA query_only = ON")
                    elif connection.dialect.name == "postgresql":
                        connection.execute(text("SET TRANSACTION READ ONLY"))
                    _assert_expected_revision(connection)
                    actions = _folder_type_prepare_actions(connection)
                    return _report(actions, apply=False, target=target)

        with Session(engine) as session:
            with session.begin():
                connection = session.connection()
                _acquire_prepare_lock(connection)
                _assert_expected_revision(connection)
                actions = _folder_type_prepare_actions(connection)
                _apply_actions(session, actions)
            return _report(actions, apply=True, target=target)
    finally:
        engine.dispose()
