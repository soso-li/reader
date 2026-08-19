from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from reader_api.migrations import folder_type_prepare


def create_0068_prepare_schema(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE alembic_version (version_num TEXT NOT NULL)"))
        connection.execute(
            text(
                "INSERT INTO alembic_version (version_num) "
                "VALUES ('0068_cluster_current_projection')"
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE folders (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    CONSTRAINT folders_name_key UNIQUE (name)
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE sources (
                    id INTEGER PRIMARY KEY,
                    folder_id INTEGER,
                    media_type TEXT NOT NULL,
                    status TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            text("CREATE TABLE content_items (id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL)"))
        connection.execute(text("CREATE TABLE clusters (id INTEGER PRIMARY KEY)"))
        connection.execute(
            text(
                """
                CREATE TABLE cluster_items (
                    id INTEGER PRIMARY KEY,
                    cluster_id INTEGER NOT NULL,
                    content_item_id INTEGER NOT NULL
                )
                """
            )
        )


def test_prepare_requires_exactly_0068_and_is_read_only_by_default(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'prepare.sqlite'}"
    engine = create_engine(database_url)
    create_0068_prepare_schema(engine)
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO folders (id, name) VALUES (1, 'Videos')"))
        connection.execute(
            text(
                "INSERT INTO sources (id, folder_id, media_type, status) "
                "VALUES (1, 1, 'article', 'active')"
            )
        )

    report = folder_type_prepare.prepare_folder_media_types(database_url)
    assert report["applied"] is False
    assert report["action_count"] == 1
    assert report["actions"] == [
        {
            "source_id": 1,
            "folder_id": 1,
            "folder_name": "Videos",
            "source_status": "active",
            "from_media_type": "article",
            "to_media_type": "video",
            "from_folder_id": 1,
            "to_folder_id": 1,
            "cluster_membership_count": 0,
            "requires_decluster": False,
        }
    ]
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT folder_id, media_type FROM sources WHERE id = 1")
        ).one() == (1, "article")

    with engine.begin() as connection:
        connection.execute(text("UPDATE alembic_version SET version_num = '0069_folder_media_types'"))
    with pytest.raises(RuntimeError, match="requires_revision_0068"):
        folder_type_prepare.prepare_folder_media_types(database_url)
    engine.dispose()


def test_prepare_apply_rolls_back_every_source_when_decluster_fails(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_url = f"sqlite:///{tmp_path / 'prepare-atomic.sqlite'}"
    engine = create_engine(database_url)
    create_0068_prepare_schema(engine)
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO folders (id, name) VALUES (1, 'Videos')"))
        connection.execute(
            text(
                "INSERT INTO sources (id, folder_id, media_type, status) VALUES "
                "(1, 1, 'article', 'active'), (2, 1, 'article', 'active')"
            )
        )
        connection.execute(text("INSERT INTO content_items (id, source_id) VALUES (1, 2)"))
        connection.execute(text("INSERT INTO clusters (id) VALUES (1)"))
        connection.execute(
            text(
                "INSERT INTO cluster_items (id, cluster_id, content_item_id) "
                "VALUES (1, 1, 1)"
            )
        )

    def fail_decluster(session, source_id: int, **kwargs: object) -> None:
        assert source_id == 2
        assert session.scalar(
            text("SELECT media_type FROM sources WHERE id = :source_id"),
            {"source_id": source_id},
        ) == "article"
        assert kwargs == {"force": True, "rollback_on_failure": True}
        raise RuntimeError("decluster write failed")

    monkeypatch.setattr(folder_type_prepare, "decluster_source_items", fail_decluster)
    with pytest.raises(RuntimeError, match="decluster write failed"):
        folder_type_prepare.prepare_folder_media_types(database_url, apply=True)

    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT id, folder_id, media_type FROM sources ORDER BY id")
        ).all() == [(1, 1, "article"), (2, 1, "article")]
        assert connection.scalar(text("SELECT count(*) FROM cluster_items")) == 1
    engine.dispose()
