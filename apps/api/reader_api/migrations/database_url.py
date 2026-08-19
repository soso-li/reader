from __future__ import annotations

from sqlalchemy.engine import URL, make_url


TARGET_OVERRIDE_QUERY_PARAMETERS = frozenset(
    {
        "database",
        "dbname",
        "host",
        "hostaddr",
        "port",
        "service",
        "servicefile",
    }
)


def parse_postgres_database_url(database_url: str) -> URL:
    try:
        url = make_url(database_url)
    except Exception as exc:
        raise ValueError("PostgreSQL 数据库地址无效") from exc
    if url.drivername != "postgresql+psycopg":
        raise ValueError("数据库地址必须使用 postgresql+psycopg")
    if "," in (url.host or ""):
        raise ValueError("数据库地址不允许使用 authority 多主机语法")

    overrides = sorted(
        str(key)
        for key in url.query
        if str(key).lower() in TARGET_OVERRIDE_QUERY_PARAMETERS
    )
    if overrides:
        raise ValueError(
            "数据库地址不允许通过查询参数覆盖连接目标：" + ", ".join(overrides)
        )
    return url
