import os

import pytest


os.environ["DATABASE_URL"] = "sqlite:///:memory:"


class FakeBulkReadRedis:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}

    def set(
        self,
        key: str,
        value: str | bytes,
        *,
        ex: int,
        nx: bool,
    ) -> bool:
        if nx and key in self.values:
            return False
        self.values[key] = value.encode() if isinstance(value, str) else value
        return True

    def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    def expire(self, key: str, _seconds: int) -> bool:
        return key in self.values


@pytest.fixture(autouse=True)
def disable_api_auth_for_tests(monkeypatch):
    from reader_api.main import app, get_bulk_read_redis

    bulk_read_redis = FakeBulkReadRedis()
    monkeypatch.setattr(
        "reader_api.rss.download_image",
        lambda *_args, **_kwargs: None,
    )
    app.state.api_auth_disabled_for_tests = True
    app.dependency_overrides[get_bulk_read_redis] = lambda: bulk_read_redis
    yield
    app.dependency_overrides.pop(get_bulk_read_redis, None)
    app.state.api_auth_disabled_for_tests = False


@pytest.fixture
def bulk_read_redis():
    from reader_api.main import app, get_bulk_read_redis

    return app.dependency_overrides[get_bulk_read_redis]()
