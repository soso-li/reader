"""Make Folder media types explicit and enforce Source-folder compatibility."""

from __future__ import annotations

from collections import Counter

from alembic import op
import sqlalchemy as sa


revision: str = "0069_folder_media_types"
down_revision: str | None = "0068_cluster_current_projection"
branch_labels: str | None = None
depends_on: str | None = None


MEDIA_TYPES = ("article", "social", "image", "video", "podcast", "notification")
LEGACY_MEDIA_FOLDER_NAMES = {
    "social": {"social", "socialmedia", "social media"},
    "image": {"image", "images", "picture", "pictures", "photo", "photos"},
    "video": {"video", "videos"},
    "podcast": {"audio", "audios", "podcast", "podcasts"},
    "notification": {"notification", "notifications"},
}

# No exact mixed-folder mapping has been confirmed for this frozen revision.
# Any historic database with a mixed folder must be audited separately and
# mapped here before its migration can proceed. Keys include the historical
# name to avoid an accidental ID reuse or stale operator note changing behavior.
CONFIRMED_MIXED_FOLDER_TYPES: dict[tuple[int, str], str] = {}


def legacy_folder_media_type(name: str) -> str:
    parts = {part.strip().lower() for part in name.split(" / ")}
    for media_type, names in LEGACY_MEDIA_FOLDER_NAMES.items():
        if parts & names:
            return media_type
    return "article"


def effective_legacy_source_media_type(source_media_type: str, folder_name: str) -> str:
    if source_media_type in MEDIA_TYPES and source_media_type != "article":
        return source_media_type
    return legacy_folder_media_type(folder_name)


def upgrade() -> None:
    connection = op.get_bind()
    folder_type_plan = _folder_type_plan(connection)
    _assert_article_sources_are_declustered(connection, folder_type_plan)
    op.add_column("folders", sa.Column("media_type", sa.String(length=32), nullable=True))
    _apply_folder_type_plan(connection, folder_type_plan)
    if connection.dialect.name == "sqlite":
        _upgrade_sqlite_constraints()
    else:
        _upgrade_postgresql_constraints()


def _folder_type_plan(
    connection: sa.Connection,
) -> list[tuple[int, str, list[tuple[int, str, str]]]]:
    rows = connection.execute(
        sa.text(
            """
            SELECT folders.id, folders.name, sources.id, sources.media_type, sources.status
            FROM folders
            LEFT JOIN sources
              ON sources.folder_id = folders.id
            ORDER BY folders.id, sources.id
            """
        )
    )
    folders: dict[int, tuple[str, Counter[str], list[tuple[int, str, str]]]] = {}
    for folder_id, folder_name, source_id, source_media_type, source_status in rows:
        folder_id = int(folder_id)
        name = str(folder_name)
        if folder_id not in folders:
            folders[folder_id] = (name, Counter(), [])
        if source_media_type is not None:
            media_type = str(source_media_type)
            if media_type not in MEDIA_TYPES:
                raise RuntimeError(
                    "source_media_type_invalid: "
                    f"folder_id={folder_id} source_id={source_id} media_type={media_type!r}"
                )
            effective_media_type = effective_legacy_source_media_type(media_type, name)
            folders[folder_id][2].append(
                (int(source_id), effective_media_type, str(source_status or ""))
            )
            if source_status != "deleted":
                folders[folder_id][1][effective_media_type] += 1

    plan: list[tuple[int, str, list[tuple[int, str, str]]]] = []
    for folder_id, (name, counts, sources) in folders.items():
        active_types = [media_type for media_type in MEDIA_TYPES if counts[media_type]]
        if not active_types:
            target_media_type = legacy_folder_media_type(name)
        elif len(active_types) == 1:
            target_media_type = active_types[0]
        else:
            target_media_type = CONFIRMED_MIXED_FOLDER_TYPES.get((folder_id, name), "")
            if not target_media_type:
                summary = ", ".join(
                    f"{media_type}={counts[media_type]}" for media_type in active_types
                )
                raise RuntimeError(
                    "folder_media_type_mixed_unmapped: "
                    f"folder_id={folder_id} name={name!r} counts={summary}; "
                    "请先运行 folder_type_audit 并把确认映射固定到 0069"
                )
            if target_media_type not in MEDIA_TYPES:
                raise RuntimeError(
                    "folder_media_type_mixed_mapping_invalid: "
                    f"folder_id={folder_id} name={name!r} media_type={target_media_type!r}"
                )
            if target_media_type not in active_types:
                raise RuntimeError(
                    "folder_media_type_mixed_mapping_not_present: "
                    f"folder_id={folder_id} name={name!r} media_type={target_media_type!r}"
                )
        plan.append((folder_id, target_media_type, sources))
    return plan


def _assert_article_sources_are_declustered(
    connection: sa.Connection,
    plan: list[tuple[int, str, list[tuple[int, str, str]]]],
) -> None:
    """Fail before DDL if 0069 would make a clustered source non-article.

    Alembic deliberately owns this frozen guard instead of importing the
    runtime prepare command. The command creates the audit run and removes
    membership; this migration only verifies that prerequisite.
    """
    blocked: list[str] = []
    for folder_id, target_media_type, sources in plan:
        for source_id, effective_media_type, _source_status in sources:
            if effective_media_type == "article":
                continue
            current_media_type = connection.scalar(
                sa.text("SELECT media_type FROM sources WHERE id = :source_id"),
                {"source_id": source_id},
            )
            if current_media_type != "article":
                continue
            has_membership = connection.scalar(
                sa.text(
                    """
                    SELECT 1
                    FROM cluster_items
                    JOIN content_items
                      ON content_items.id = cluster_items.content_item_id
                    WHERE content_items.source_id = :source_id
                    LIMIT 1
                    """
                ),
                {"source_id": source_id},
            )
            if has_membership:
                blocked.append(
                    f"source_id={source_id} folder_id={folder_id} "
                    f"target={effective_media_type}"
                )
    if blocked:
        raise RuntimeError(
            "folder_media_type_prepare_required: "
            + "; ".join(blocked)
            + "; 先运行 prepare-folder-media-types --apply"
        )


def _apply_folder_type_plan(
    connection: sa.Connection,
    plan: list[tuple[int, str, list[tuple[int, str, str]]]],
) -> None:
    for folder_id, target_media_type, sources in plan:
        for source_id, effective_media_type, source_status in sources:
            connection.execute(
                sa.text(
                    """
                    UPDATE sources
                    SET media_type = :media_type, folder_id = :folder_id
                    WHERE id = :source_id
                    """
                ),
                {
                    "source_id": source_id,
                    "media_type": effective_media_type,
                    "folder_id": (
                        folder_id
                        if source_status != "deleted"
                        and effective_media_type == target_media_type
                        else None
                    ),
                },
            )
        connection.execute(
            sa.text("UPDATE folders SET media_type = :media_type WHERE id = :folder_id"),
            {"folder_id": folder_id, "media_type": target_media_type},
        )


def _upgrade_postgresql_constraints() -> None:
    op.alter_column("folders", "media_type", existing_type=sa.String(length=32), nullable=False)
    op.drop_constraint("folders_name_key", "folders", type_="unique")
    op.create_unique_constraint("uq_folder_media_type_name", "folders", ["media_type", "name"])
    op.create_unique_constraint("uq_folder_id_media_type", "folders", ["id", "media_type"])
    op.create_check_constraint(
        "ck_folder_media_type", "folders", "media_type IN ('article', 'social', 'image', 'video', 'podcast', 'notification')"
    )
    op.create_check_constraint(
        "ck_source_media_type", "sources", "media_type IN ('article', 'social', 'image', 'video', 'podcast', 'notification')"
    )
    op.drop_constraint("sources_folder_id_fkey", "sources", type_="foreignkey")
    op.create_foreign_key(
        "fk_source_folder_media_type",
        "sources",
        "folders",
        ["folder_id", "media_type"],
        ["id", "media_type"],
    )


def _upgrade_sqlite_constraints() -> None:
    naming_convention = {
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
    }
    with op.batch_alter_table(
        "folders", recreate="always", naming_convention=naming_convention
    ) as batch:
        batch.alter_column(
            "media_type", existing_type=sa.String(length=32), nullable=False
        )
        batch.drop_constraint("folders_name_key", type_="unique")
        batch.create_unique_constraint("uq_folder_media_type_name", ["media_type", "name"])
        batch.create_unique_constraint("uq_folder_id_media_type", ["id", "media_type"])
        batch.create_check_constraint(
            "ck_folder_media_type",
            "media_type IN ('article', 'social', 'image', 'video', 'podcast', 'notification')",
        )
    with op.batch_alter_table(
        "sources", recreate="always", naming_convention=naming_convention
    ) as batch:
        batch.drop_constraint("fk_sources_folder_id_folders", type_="foreignkey")
        batch.create_check_constraint(
            "ck_source_media_type",
            "media_type IN ('article', 'social', 'image', 'video', 'podcast', 'notification')",
        )
        batch.create_foreign_key(
            "fk_source_folder_media_type",
            "folders",
            ["folder_id", "media_type"],
            ["id", "media_type"],
        )


def downgrade() -> None:
    raise RuntimeError(
        "文件夹类型迁移不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
