"""Read-only audit for the 0069 explicit Folder media-type migration."""

from __future__ import annotations

import argparse
import json
from collections import Counter

from sqlalchemy import create_engine, text

from ..media_types import SOURCE_MEDIA_TYPES, effective_legacy_source_media_type


def folder_type_audit(database_url: str) -> list[dict[str, object]]:
    engine = create_engine(database_url)
    try:
        with engine.connect() as connection:
            with connection.begin():
                if connection.dialect.name == "sqlite":
                    connection.exec_driver_sql("PRAGMA query_only = ON")
                if connection.dialect.name == "postgresql":
                    connection.execute(text("SET TRANSACTION READ ONLY"))
                rows = connection.execute(
                    text(
                        """
                        SELECT folders.id, folders.name, sources.media_type
                        FROM folders
                        LEFT JOIN sources
                          ON sources.folder_id = folders.id
                         AND sources.status <> 'deleted'
                        ORDER BY folders.id, sources.id
                        """
                    )
                )
                result: list[dict[str, object]] = []
                current_id: int | None = None
                current_name = ""
                counts: Counter[str] = Counter()
                for folder_id, folder_name, source_media_type in rows:
                    if current_id is not None and folder_id != current_id:
                        result.append(_audit_row(current_id, current_name, counts))
                        counts = Counter()
                    current_id = int(folder_id)
                    current_name = str(folder_name)
                    if source_media_type is not None:
                        counts[
                            effective_legacy_source_media_type(
                                str(source_media_type), current_name
                            )
                        ] += 1
                if current_id is not None:
                    result.append(_audit_row(current_id, current_name, counts))
                return result
    finally:
        engine.dispose()


def _audit_row(folder_id: int, name: str, counts: Counter[str]) -> dict[str, object]:
    return {
        "folder_id": folder_id,
        "folder_name": name,
        "effective_media_type_counts": {
            media_type: int(counts[media_type]) for media_type in SOURCE_MEDIA_TYPES
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="只读审计 Reader 文件夹的现行有效来源类型")
    parser.add_argument("--database-url", required=True)
    args = parser.parse_args()
    print(json.dumps(folder_type_audit(args.database_url), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
