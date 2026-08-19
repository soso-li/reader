from __future__ import annotations

import importlib

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError

from reader_api.media_types import effective_legacy_source_media_type


MIGRATION = importlib.import_module("reader_api.alembic.versions.0069_folder_media_types")


def create_pre_0069_schema(engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE folders (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT '',
                    CONSTRAINT folders_name_key UNIQUE (name)
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE content_items (
                    id INTEGER PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id)
                )
                """
            )
        )
        connection.execute(
            text("CREATE TABLE clusters (id INTEGER PRIMARY KEY)"),
        )
        connection.execute(
            text(
                """
                CREATE TABLE cluster_items (
                    id INTEGER PRIMARY KEY,
                    cluster_id INTEGER NOT NULL REFERENCES clusters(id),
                    content_item_id INTEGER NOT NULL REFERENCES content_items(id)
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE sources (
                    id INTEGER PRIMARY KEY,
                    folder_id INTEGER REFERENCES folders(id),
                    media_type TEXT NOT NULL DEFAULT 'article',
                    status TEXT NOT NULL DEFAULT 'active'
                )
                """
            )
        )


def upgrade_0069(connection, monkeypatch) -> None:
    monkeypatch.setattr(MIGRATION, "op", Operations(MigrationContext.configure(connection)))
    MIGRATION.upgrade()


def test_0069_frozen_legacy_type_rules_match_the_current_compatibility_rule() -> None:
    folder_names = (
        "科技",
        "SocialMedia",
        "SocialMedia / Twitter",
        "Pictures",
        "videos",
        "audio",
        "notifications",
    )
    for source_media_type in (*MIGRATION.MEDIA_TYPES, "unexpected"):
        for folder_name in folder_names:
            assert MIGRATION.effective_legacy_source_media_type(
                source_media_type, folder_name
            ) == effective_legacy_source_media_type(source_media_type, folder_name)


def test_0069_normalizes_legacy_types_moves_mixed_non_targets_and_replaces_simple_fk(tmp_path, monkeypatch) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / '0069.sqlite'}")
    create_pre_0069_schema(engine)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO folders (id, name) VALUES "
                "(1, 'video'), (2, 'mixed'), (3, 'SocialMedia / Twitter'), (4, 'SocialMedia')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO sources (id, folder_id, media_type, status) VALUES "
                "(1, 1, 'article', 'active'), "
                "(2, 1, 'video', 'active'), "
                "(3, 2, 'social', 'active'), "
                "(4, 2, 'article', 'active'), "
                "(5, 2, 'video', 'active'), "
                "(6, 3, 'article', 'deleted')"
            )
        )
        monkeypatch.setattr(MIGRATION, "CONFIRMED_MIXED_FOLDER_TYPES", {(2, "mixed"): "social"})
        upgrade_0069(connection, monkeypatch)

    with engine.connect() as connection:
        folders = connection.execute(text("SELECT id, media_type FROM folders ORDER BY id")).all()
        sources = connection.execute(
            text("SELECT id, folder_id, media_type FROM sources ORDER BY id")
        ).all()
        assert folders == [(1, "video"), (2, "social"), (3, "social"), (4, "social")]
        assert sources == [
            (1, 1, "video"),
            (2, 1, "video"),
            (3, 2, "social"),
            (4, None, "article"),
            (5, None, "video"),
            (6, None, "social"),
        ]
        foreign_keys = inspect(connection).get_foreign_keys("sources")
        assert len(foreign_keys) == 1
        assert foreign_keys[0]["constrained_columns"] == ["folder_id", "media_type"]
        connection.exec_driver_sql("PRAGMA foreign_keys = ON")
        with pytest.raises(IntegrityError):
            connection.execute(
                text("INSERT INTO sources (id, folder_id, media_type, status) VALUES (7, 1, 'article', 'active')")
            )
        with pytest.raises(IntegrityError):
            connection.execute(
                text("INSERT INTO folders (id, name, media_type) VALUES (5, 'bad', 'book')")
            )
        connection.execute(
            text("INSERT INTO folders (id, name, media_type) VALUES (5, 'same', 'article')")
        )
        connection.execute(
            text("INSERT INTO folders (id, name, media_type) VALUES (6, 'same', 'video')")
        )
        with pytest.raises(IntegrityError):
            connection.execute(
                text("INSERT INTO folders (id, name, media_type) VALUES (7, 'same', 'video')")
            )
    engine.dispose()


def test_0069_unknown_mixed_folder_fails_closed_and_rolls_back(tmp_path, monkeypatch) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / '0069-mixed.sqlite'}")
    create_pre_0069_schema(engine)
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO folders (id, name) VALUES (1, 'mixed')"))
        connection.execute(
            text(
                "INSERT INTO sources (id, folder_id, media_type, status) VALUES "
                "(1, 1, 'article', 'active'), (2, 1, 'social', 'active')"
            )
        )
    with pytest.raises(RuntimeError, match="folder_media_type_mixed_unmapped"):
        with engine.begin() as connection:
            monkeypatch.setattr(MIGRATION, "CONFIRMED_MIXED_FOLDER_TYPES", {})
            upgrade_0069(connection, monkeypatch)

    with engine.connect() as connection:
        assert "media_type" not in {column["name"] for column in inspect(connection).get_columns("folders")}
        assert connection.execute(text("SELECT id, name FROM folders")).all() == [(1, "mixed")]
        assert connection.execute(
            text("SELECT id, folder_id, media_type, status FROM sources ORDER BY id")
        ).all() == [(1, 1, "article", "active"), (2, 1, "social", "active")]
    engine.dispose()


def test_0069_refuses_clustered_article_type_change_before_any_ddl(
    tmp_path, monkeypatch
) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / '0069-decluster-required.sqlite'}")
    create_pre_0069_schema(engine)
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO folders (id, name) VALUES (1, 'video')"))
        connection.execute(
            text(
                "INSERT INTO sources (id, folder_id, media_type, status) "
                "VALUES (1, 1, 'article', 'active')"
            )
        )
        connection.execute(text("INSERT INTO content_items (id, source_id) VALUES (1, 1)"))
        connection.execute(text("INSERT INTO clusters (id) VALUES (1)"))
        connection.execute(
            text(
                "INSERT INTO cluster_items (id, cluster_id, content_item_id) "
                "VALUES (1, 1, 1)"
            )
        )

    with pytest.raises(RuntimeError, match="folder_media_type_prepare_required"):
        with engine.begin() as connection:
            monkeypatch.setattr(
                MIGRATION,
                "op",
                Operations(MigrationContext.configure(connection)),
            )
            monkeypatch.setattr(
                MIGRATION.op,
                "add_column",
                lambda *_args, **_kwargs: pytest.fail("0069 performed DDL before guard"),
            )
            MIGRATION.upgrade()

    with engine.connect() as connection:
        assert "media_type" not in {
            column["name"] for column in inspect(connection).get_columns("folders")
        }
        assert connection.execute(
            text("SELECT media_type FROM sources WHERE id = 1")
        ).scalar_one() == "article"
        assert connection.scalar(text("SELECT count(*) FROM cluster_items")) == 1
    engine.dispose()


def test_0069_rejects_a_mixed_mapping_that_is_not_an_observed_type(tmp_path, monkeypatch) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / '0069-invalid-map.sqlite'}")
    create_pre_0069_schema(engine)
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO folders (id, name) VALUES (1, 'mixed')"))
        connection.execute(
            text(
                "INSERT INTO sources (id, folder_id, media_type, status) VALUES "
                "(1, 1, 'article', 'active'), (2, 1, 'social', 'active')"
            )
        )
    with pytest.raises(RuntimeError, match="folder_media_type_mixed_mapping_not_present"):
        with engine.begin() as connection:
            monkeypatch.setattr(MIGRATION, "CONFIRMED_MIXED_FOLDER_TYPES", {(1, "mixed"): "video"})
            upgrade_0069(connection, monkeypatch)
    engine.dispose()
