from __future__ import annotations

from .database_url import parse_postgres_database_url


LOCAL_TEST_HOSTS = {"127.0.0.1", "localhost", "::1"}


def validate_isolated_test_database_url(database_url: str) -> str:
    """Fail closed before a destructive PostgreSQL integration test."""
    try:
        url = parse_postgres_database_url(database_url)
    except Exception as exc:
        raise ValueError(f"隔离测试数据库地址无效：{exc}") from exc
    database = url.database or ""
    if (url.host or "") not in LOCAL_TEST_HOSTS:
        raise ValueError("隔离测试数据库默认只允许本机 PostgreSQL")
    if not database.startswith("reader_test_"):
        raise ValueError("隔离测试数据库名称必须以 reader_test_ 开头")
    return database_url
