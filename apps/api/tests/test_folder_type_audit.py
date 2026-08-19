from __future__ import annotations

from sqlalchemy import create_engine, text

from reader_api.migrations.folder_type_audit import folder_type_audit
from reader_api.media_types import effective_legacy_source_media_type


def test_folder_type_audit_is_read_only_and_uses_the_legacy_runtime_rule(tmp_path) -> None:
    database_path = tmp_path / "folder-audit.sqlite"
    database_url = f"sqlite:///{database_path}"
    engine = create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE folders (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"))
        connection.execute(
            text(
                "CREATE TABLE sources (id INTEGER PRIMARY KEY, folder_id INTEGER, media_type TEXT, status TEXT)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO folders (id, name) VALUES "
                "(1, '科技'), (2, 'video / 收藏'), (3, '混合'), (4, 'SocialMedia')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO sources (id, folder_id, media_type, status) VALUES "
                "(1, 1, 'article', 'active'), "
                "(2, 2, 'article', 'active'), "
                "(3, 3, 'social', 'active'), "
                "(4, 3, 'article', 'trial'), "
                "(5, 3, 'video', 'deleted')"
            )
        )
        before = connection.scalar(text("SELECT count(*) FROM sources"))

    assert effective_legacy_source_media_type("article", "video / 收藏") == "video"
    assert effective_legacy_source_media_type("social", "video / 收藏") == "social"

    assert folder_type_audit(database_url) == [
        {
            "folder_id": 1,
            "folder_name": "科技",
            "effective_media_type_counts": {
                "article": 1,
                "social": 0,
                "image": 0,
                "video": 0,
                "podcast": 0,
                "notification": 0,
            },
        },
        {
            "folder_id": 2,
            "folder_name": "video / 收藏",
            "effective_media_type_counts": {
                "article": 0,
                "social": 0,
                "image": 0,
                "video": 1,
                "podcast": 0,
                "notification": 0,
            },
        },
        {
            "folder_id": 3,
            "folder_name": "混合",
            "effective_media_type_counts": {
                "article": 1,
                "social": 1,
                "image": 0,
                "video": 0,
                "podcast": 0,
                "notification": 0,
            },
        },
        {
            "folder_id": 4,
            "folder_name": "SocialMedia",
            "effective_media_type_counts": {
                "article": 0,
                "social": 0,
                "image": 0,
                "video": 0,
                "podcast": 0,
                "notification": 0,
            },
        },
    ]

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM sources")) == before
    engine.dispose()
