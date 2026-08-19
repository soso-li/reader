import json
import sys
import types
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError

import pytest

import reader_api.rss as rss_module  # noqa: E402
import reader_api.source_ingest as source_ingest_module  # noqa: E402
from reader_api.article_fetch import ArticleFetchResult  # noqa: E402
from reader_api.article_image_cache import DownloadedImage, cache_key_for_url  # noqa: E402
from reader_api.cluster import assign_cluster  # noqa: E402
from reader_api.db import Base, engine  # noqa: E402
from reader_api.digest import canonical_url, content_hash, lsh_signature, normalize_title  # noqa: E402
from reader_api.models import AppSetting, Cluster, ClusterEventProjection, ClusteringRun, ClusteringRunMembership, ClusteringRunProjectionPredecessor, ClusteringRunScopeEvidence, ClusterItem, ContentEmbedding, ContentItem, Document, EventRevision, RawEntry, Source, SourceEntryIdentity, SourceEntryRelation, UserState  # noqa: E402
from reader_api.clustering_run import ClusteringRule, clustering_run, evidence_anchor_for_item  # noqa: E402
from reader_api.rss import fetch_article_text, fetch_enabled_sources, fetch_source, full_text_for_short_rss, parse_date, repair_duplicate_feed_entries, repair_over_split_documents  # noqa: E402
from reader_api.source_ingest import IngestEntry  # noqa: E402
from reader_api.source_entry_revision import RawEntryRevisionInput, allocate_raw_entry_revision  # noqa: E402
from tests.factories import make_raw_entry  # noqa: E402
from sqlalchemy import event, func, select, update  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402


class FeedResponse:
    def __init__(self, body: bytes, headers: dict[str, str] | None = None):
        self.body = body
        self._headers = headers or {}
        self.headers = self

    def get_content_charset(self) -> str:
        return "utf-8"

    def get(self, name: str, default: str = "") -> str:
        return self._headers.get(name, default)

    def __enter__(self) -> "FeedResponse":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        return self.body if size < 0 else self.body[:size]


def test_parse_date_supports_atom_iso_dates() -> None:
    assert parse_date("2026-06-28T17:42:51-04:00") == datetime(2026, 6, 28, 21, 42, 51, tzinfo=timezone.utc)
    assert parse_date("2026-06-29 14:30:40") == datetime(2026, 6, 29, 6, 30, 40, tzinfo=timezone.utc)
    assert parse_date("2026-06-29 13:20:38  +0800") == datetime(2026, 6, 29, 5, 20, 38, tzinfo=timezone.utc)
    assert parse_date("Wed, 01 Jul 2026 11:19:00 +08") == datetime(2026, 7, 1, 3, 19, 0, tzinfo=timezone.utc)


def test_fetch_source_preserves_source_guid_hint_for_ingest(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Identity RSS</title>
        <item>
          <guid>source-guid-1</guid>
          <title>Entry with GUID</title>
          <link>https://example.com/with-guid</link>
          <description>First body</description>
        </item>
        <item>
          <title>Entry without GUID</title>
          <link>https://example.com/without-guid</link>
          <description>Second body</description>
        </item>
      </channel>
    </rss>
    """
    captured: list[IngestEntry] = []

    monkeypatch.setattr("reader_api.rss.urlopen", lambda *_args, **_kwargs: FeedResponse(feed))

    def capture_entries(
        _session,
        _source,
        entries: list[IngestEntry],
        imported_item_ids=None,
        failed_entry_ids=None,
    ) -> int:
        captured.extend(entries)
        return len(entries)

    monkeypatch.setattr("reader_api.rss.ingest_source_entries", capture_entries)

    with Session() as session:
        source = Source(name="Identity RSS", url="https://example.com/identity.xml")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 2

    assert captured[0].source_guid == "source-guid-1"
    assert captured[0].external_id == "source-guid-1"
    assert captured[1].source_guid == ""
    assert captured[1].external_id


def test_fetch_source_rejects_oversized_feed_before_parsing(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    monkeypatch.setattr(rss_module, "MAX_FEED_BYTES", 8)
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(b"x" * 9),
    )

    with Session() as session:
        source = Source(name="Oversized", url="https://example.com/large.xml")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 0
        session.refresh(source)
        assert source.last_error == "RSS 抓取失败: 响应超过 10 MiB"
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 0


def test_fetch_source_reuses_validators_and_skips_unchanged_payload(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Cached feed</title><item>
      <guid>cached-1</guid><title>Cached entry</title>
      <link>https://example.com/cached</link><description>Cached body</description>
    </item></channel></rss>
    """
    requests: list[object] = []
    responses = iter(
        [
            FeedResponse(
                feed,
                {
                    "ETag": '"feed-v1"',
                    "Last-Modified": "Tue, 28 Jul 2026 12:00:00 GMT",
                },
            ),
            FeedResponse(
                feed,
                {
                    "ETag": '"feed-v1"',
                    "Last-Modified": "Tue, 28 Jul 2026 12:00:00 GMT",
                },
            ),
            HTTPError(
                "https://example.com/cached.xml",
                304,
                "Not Modified",
                {"ETag": '"feed-v1"'},
                None,
            ),
        ]
    )

    def fake_urlopen(request: object, **_kwargs: object) -> FeedResponse:
        requests.append(request)
        response = next(responses)
        if isinstance(response, HTTPError):
            raise response
        return response

    parse_calls = 0
    original_parse = rss_module.feedparser.parse

    def count_parse(*args: object, **kwargs: object):
        nonlocal parse_calls
        parse_calls += 1
        return original_parse(*args, **kwargs)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)
    monkeypatch.setattr("reader_api.rss.feedparser.parse", count_parse)

    with Session() as session:
        source = Source(
            name="Cached feed",
            url="https://example.com/cached.xml",
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        assert fetch_source(session, source) == 0
        assert fetch_source(session, source) == 0
        session.refresh(source)

        assert source.fetch_etag == '"feed-v1"'
        assert source.fetch_last_modified == "Tue, 28 Jul 2026 12:00:00 GMT"
        assert source.last_successful_payload_hash
        assert source.last_error == ""
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 1

    second_headers = {
        name.lower(): value for name, value in requests[1].header_items()
    }
    third_headers = {
        name.lower(): value for name, value in requests[2].header_items()
    }
    assert second_headers["if-none-match"] == '"feed-v1"'
    assert second_headers["if-modified-since"] == (
        "Tue, 28 Jul 2026 12:00:00 GMT"
    )
    assert third_headers["if-none-match"] == '"feed-v1"'
    assert parse_calls == 1


def test_fetch_source_rolls_back_validators_when_ingest_fails(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Failed feed</title><item>
      <guid>failed-1</guid><title>Failed entry</title>
      <link>https://example.com/failed</link><description>Failed body</description>
    </item></channel></rss>
    """
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(
            feed,
            {"ETag": '"must-not-commit"'},
        ),
    )

    def fail_ingest(*_args: object, **_kwargs: object) -> int:
        raise RuntimeError("ingest failed")

    monkeypatch.setattr("reader_api.rss.ingest_parsed_source", fail_ingest)

    with Session() as session:
        source = Source(
            name="Failed feed",
            url="https://example.com/failed.xml",
        )
        session.add(source)
        session.commit()

        with pytest.raises(RuntimeError, match="ingest failed"):
            fetch_source(session, source)
        session.refresh(source)

        assert source.fetch_etag is None
        assert source.fetch_last_modified is None
        assert source.last_successful_payload_hash is None


def test_partial_projection_failure_keeps_feed_retryable(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Retryable feed</title>
      <item><guid>broken</guid><title>Broken</title>
        <link>https://example.com/broken</link><description>Broken body</description>
      </item>
      <item><guid>valid</guid><title>Valid</title>
        <link>https://example.com/valid</link><description>Valid body</description>
      </item>
    </channel></rss>
    """
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(
            feed,
            {"ETag": '"retryable"'},
        ),
    )
    original_create = source_ingest_module.create_initial_projection
    failed_once = False

    def fail_first_projection(
        session,
        source,
        raw,
        prepared,
    ):
        nonlocal failed_once
        if raw.external_id == "broken" and not failed_once:
            failed_once = True
            raise RuntimeError("projection failed")
        return original_create(session, source, raw, prepared)

    monkeypatch.setattr(
        source_ingest_module,
        "create_initial_projection",
        fail_first_projection,
    )

    with Session() as session:
        source = Source(
            name="Retryable feed",
            url="https://example.com/retryable.xml",
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        session.refresh(source)
        assert source.fetch_etag is None
        assert source.last_successful_payload_hash is None
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 1

        assert fetch_source(session, source) == 1
        session.refresh(source)
        assert source.fetch_etag == '"retryable"'
        assert source.last_successful_payload_hash
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 2


def test_fetch_source_rejects_unsolicited_not_modified(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            HTTPError(
                "https://example.com/no-validator.xml",
                304,
                "Not Modified",
                {},
                None,
            )
        ),
    )

    with Session() as session:
        source = Source(
            name="No validator",
            url="https://example.com/no-validator.xml",
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 0
        assert source.last_error == "RSS 抓取失败: HTTP Error 304: Not Modified"
        assert source.last_successful_payload_hash is None


def test_fetch_source_without_full_content_skips_raw_url_existence_query(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>No full text</title><item>
      <guid>no-full-text-1</guid><title>No full text entry</title>
      <link>https://example.com/no-full-text</link><description>Short body</description>
    </item></channel></rss>
    """
    statements: list[str] = []

    def capture_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(feed),
    )
    event.listen(engine, "before_cursor_execute", capture_statement)
    try:
        with Session() as session:
            source = Source(
                name="No full text",
                url="https://example.com/no-full-text.xml",
                fetch_full_content=False,
            )
            session.add(source)
            session.commit()
            assert fetch_source(session, source) == 1
    finally:
        event.remove(engine, "before_cursor_execute", capture_statement)

    assert not any(
        "WHERE raw_entries.source_id =" in statement
        and "raw_entries.url =" in statement
        for statement in statements
    )


@pytest.mark.parametrize(
    "outcome",
    ["success", "request-error", "parse-error"],
)
def test_fetch_source_discards_result_when_source_url_changes(
    monkeypatch,
    outcome: str,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Old endpoint</title><item>
      <guid>old-endpoint-entry</guid><title>Old endpoint entry</title>
      <link>https://old.example/story</link><description>Old body</description>
    </item></channel></rss>
    """

    with Session() as session:
        source = Source(
            name="Changing endpoint",
            url="https://old.example/feed.xml",
        )
        session.add(source)
        session.commit()

        def change_url_during_request(*_args, **_kwargs):
            session.execute(
                update(Source)
                .where(Source.id == source.id)
                .values(url="https://new.example/feed.xml")
                .execution_options(synchronize_session=False)
            )
            session.commit()
            if outcome == "request-error":
                raise URLError("old endpoint failed")
            return FeedResponse(b"not xml" if outcome == "parse-error" else feed)

        monkeypatch.setattr("reader_api.rss.urlopen", change_url_during_request)

        assert fetch_source(session, source) == 0
        session.refresh(source)
        assert source.url == "https://new.example/feed.xml"
        assert source.last_error == ""
        assert source.last_fetched_at is None
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 0


@pytest.mark.parametrize(
    ("source_update", "expected_status", "expected_enabled"),
    [
        ({"status": "archived", "enabled": False}, "archived", False),
        ({"status": "muted"}, "muted", True),
        ({"enabled": False}, "active", False),
    ],
    ids=["archived", "muted", "paused"],
)
@pytest.mark.parametrize(
    "outcome",
    ["success", "request-error", "parse-error"],
)
def test_fetch_source_discards_result_when_source_becomes_ineligible(
    monkeypatch,
    source_update: dict[str, object],
    expected_status: str,
    expected_enabled: bool,
    outcome: str,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Disabled during fetch</title><item>
      <guid>disabled-during-fetch</guid><title>Disabled during fetch</title>
      <link>https://example.com/disabled-story</link><description>Old body</description>
    </item></channel></rss>
    """

    with Session() as session:
        source = Source(
            name="Disabled during fetch",
            url="https://example.com/disabled-feed.xml",
        )
        session.add(source)
        session.commit()

        def disable_source_during_request(*_args, **_kwargs):
            session.execute(
                update(Source)
                .where(Source.id == source.id)
                .values(**source_update)
                .execution_options(synchronize_session=False)
            )
            session.commit()
            if outcome == "request-error":
                raise URLError("disabled endpoint failed")
            return FeedResponse(b"not xml" if outcome == "parse-error" else feed)

        monkeypatch.setattr("reader_api.rss.urlopen", disable_source_during_request)

        assert fetch_source(session, source) == 0
        session.refresh(source)
        assert source.status == expected_status
        assert source.enabled is expected_enabled
        assert source.last_error == ""
        assert source.last_fetched_at is None
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 0


def test_short_rss_body_uses_fetched_article_text(monkeypatch) -> None:
    monkeypatch.setattr("reader_api.rss.fetch_article_text", lambda _url: "完整正文 " * 80)

    assert full_text_for_short_rss("短摘要", "https://example.com/story").startswith("完整正文")
    assert full_text_for_short_rss("已有正文 " * 200, "https://example.com/story") == ""
    assert parse_date("2026-06-26") == datetime(2026, 6, 25, 16, 0, 0, tzinfo=timezone.utc)


def test_short_rss_body_keeps_rss_images_when_fetched_text_replaces(monkeypatch) -> None:
    fetched = "完整正文 " * 80 + "\n![占位](https://example.com/t.png)"
    raw = "短摘要\n![封面](https://cdn.example.com/cover.jpg)"
    monkeypatch.setattr("reader_api.rss.fetch_article_text", lambda _url: fetched)

    text = full_text_for_short_rss(raw, "https://example.com/story")

    assert text.startswith("![封面](https://cdn.example.com/cover.jpg)")
    assert "完整正文" in text


def test_short_rss_body_forwards_manual_selectors(monkeypatch) -> None:
    received: list[tuple[str, str | None, str | None]] = []

    def fake_fetch(
        url: str,
        *,
        article_selector: str | None = None,
        remove_selector: str | None = None,
    ) -> str:
        received.append((url, article_selector, remove_selector))
        return "网页正文 " * 80

    monkeypatch.setattr("reader_api.rss.fetch_article_text", fake_fetch)

    text = full_text_for_short_rss(
        "短摘要",
        "https://example.com/story",
        article_selector="css:article",
        remove_selector="css:.advertisement",
    )

    assert text.startswith("网页正文")
    assert received == [
        (
            "https://example.com/story",
            "css:article",
            "css:.advertisement",
        )
    ]


def test_fetch_source_keeps_short_rss_body_by_default(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Example RSS</title>
        <link>javascript:alert(1)</link>
        <item>
          <guid>short-article</guid>
          <title>Short English story</title>
          <link>https://example.com/story</link>
          <description>Short RSS summary</description>
        </item>
      </channel>
    </rss>
    """
    fetched_body = "Full article paragraph with enough detail for reading. " * 40

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)
    article_fetch_calls: list[str] = []

    def fake_fetch_article(url: str, *_args: object, **_kwargs: object) -> ArticleFetchResult:
        article_fetch_calls.append(url)
        return ArticleFetchResult(
            content_html=f"<p>{fetched_body}</p>",
            content_text=fetched_body,
            body_source="webpage",
            web_fetch_status="succeeded",
            method="test",
            version="test-v1",
            final_url=url,
            diagnostics=(),
        )

    monkeypatch.setattr("reader_api.rss.fetch_article", fake_fetch_article)

    with Session() as session:
        source = Source(name="Example RSS", url="https://example.com/feed.xml")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        raw = session.scalar(select(RawEntry))
        document = session.scalar(select(Document))
        item = session.scalar(select(ContentItem))
        site_url = source.site_url
        reading_body = (
            document.reading_html,
            document.body_source,
            document.web_fetch_status,
        )

    assert raw is not None
    assert document is not None
    assert item is not None
    assert article_fetch_calls == []
    assert raw.raw_summary == "Short RSS summary"
    assert raw.raw_content == ""
    assert document.content_text.strip() == "Short RSS summary"
    assert "Short RSS summary" in reading_body[0]
    assert reading_body[1:] == ("rss", "not_requested")
    assert item.content_text.strip() == "Short RSS summary"
    assert site_url == ""


def test_article_fetch_failures_do_not_log_private_entry_urls(monkeypatch, caplog) -> None:
    private_url = "https://example.com/story?token=secret"
    def fail_fetch(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("reader_api.rss.fetch_article", fail_fetch)
    caplog.set_level(logging.ERROR, logger="reader_api.rss")

    result = rss_module.prepare_entry_reading_body(
        "<p>RSS fallback</p>",
        "RSS fallback",
        private_url,
        fetch_web=True,
        article_selector=None,
        remove_selector=None,
    )

    assert result.content_text == "RSS fallback"
    assert "网页正文抓取失败，采用 RSS" in caplog.text
    assert private_url not in caplog.text
    assert "token=secret" not in caplog.text


def test_fetch_source_normalizes_rss_html_into_shared_projections(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
      <channel>
        <title>Structured RSS</title>
        <item>
          <guid>structured-article</guid>
          <title>Structured article</title>
          <link>https://example.com/story</link>
          <content:encoded><![CDATA[
            <article>
              <h2>Section</h2>
              <p>Body with <strong>important detail</strong>.</p>
              <figure>
                <img src="/cover.jpg" alt="Cover">
                <figcaption>Caption</figcaption>
              </figure>
            </article>
          ]]></content:encoded>
        </item>
      </channel>
    </rss>
    """
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(feed),
    )
    with Session() as session:
        source = Source(
            name="Structured RSS",
            url="https://example.com/feed.xml",
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        document = session.scalars(select(Document)).one()
        item = session.scalars(select(ContentItem)).one()

        assert "<h2" in document.reading_html
        assert "<strong>important detail</strong>" in document.reading_html
        assert 'data-reader-original-src="https://example.com/cover.jpg"' in (
            document.reading_html
        )
        assert document.content_text == (
            "## Section\n\n"
            "Body with **important detail**.\n\n"
            "![Cover](https://example.com/cover.jpg)\n\n"
            "Caption"
        )
        assert item.content_text == document.content_text


def test_fetch_source_predownloads_list_image_omitted_by_webpage_before_projection(
    monkeypatch,
    tmp_path,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    image_url = "https://static.chongdiantou.com/uploads/cover.jpg"
    feed = f"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
      <channel><title>Image RSS</title><item>
        <guid>image-entry</guid><title>Image entry</title>
        <link>https://example.com/story</link>
        <content:encoded><![CDATA[
          <p>Body</p><img src="{image_url}" alt="Cover">
        ]]></content:encoded>
      </item></channel>
    </rss>
    """.encode()
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(feed),
    )
    monkeypatch.setattr(
        "reader_api.rss.fetch_article",
        lambda url, *_args, **_kwargs: ArticleFetchResult(
            content_html=f"<article><p>{'Webpage body ' * 40}</p></article>",
            content_text="",
            body_source="webpage",
            web_fetch_status="succeeded",
            method="test",
            version="test-v1",
            final_url=url,
            diagnostics=(),
        ),
    )
    monkeypatch.setenv("READER_ARTICLE_IMAGE_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("READER_ARTICLE_IMAGE_CACHE_MAX_BYTES", "1024")
    original = b"\x89PNG\r\n\x1a\noriginal"
    transaction_states: list[bool] = []

    with Session() as session:
        monkeypatch.setattr(
            "reader_api.rss.download_image",
            lambda *_args, **_kwargs: (
                transaction_states.append(session.in_transaction())
                or DownloadedImage(original, "image/png")
            ),
        )
        source = Source(
            name="Image RSS",
            url="https://example.com/feed.xml",
            fetch_full_content=True,
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        document = session.scalars(select(Document)).one()
        assert document.body_source == "webpage"
        assert image_url not in document.reading_html

    key = cache_key_for_url(image_url)
    assert (tmp_path / f"{key}.body").read_bytes() == original
    assert transaction_states == [False]


def test_article_network_preparation_runs_outside_database_transaction(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Outside transaction</title><item>
      <guid>outside-transaction</guid><title>Outside transaction</title>
      <link>https://example.com/story</link><description>RSS body</description>
    </item></channel></rss>
    """
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(feed),
    )
    transaction_states: list[bool] = []

    with Session() as session:
        source = Source(
            name="Outside transaction",
            url="https://example.com/feed.xml",
            fetch_full_content=True,
        )
        session.add(source)
        session.commit()

        def fetch_article_outside_transaction(
            url: str,
            *_args: object,
            **_kwargs: object,
        ) -> ArticleFetchResult:
            transaction_states.append(session.in_transaction())
            return ArticleFetchResult.rss_failed(
                "RSS body",
                final_url=url,
                diagnostics=("test",),
            )

        monkeypatch.setattr(
            "reader_api.rss.fetch_article",
            fetch_article_outside_transaction,
        )

        assert fetch_source(session, source) == 1
        assert transaction_states == [False]


def test_local_rsshub_feed_is_allowed_but_private_article_url_is_not(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Local RSSHub</title>
        <item>
          <guid>private-article</guid>
          <title>Private article URL</title>
          <link>http://127.0.0.1/private-article</link>
          <description>RSS body remains readable</description>
        </item>
      </channel>
    </rss>
    """
    feed_urls: list[str] = []

    def fake_urlopen(request: object, **__: object) -> FeedResponse:
        feed_urls.append(request.full_url)  # type: ignore[attr-defined]
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(
            name="Local RSSHub",
            url="http://rsshub:1200/local/feed",
            fetch_full_content=True,
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        document = session.scalar(select(Document))
        reading_state = (
            document.body_source,
            document.web_fetch_status,
        )

    assert feed_urls == ["http://rsshub:1200/local/feed"]
    assert document is not None
    assert document.content_text == "RSS body remains readable"
    assert reading_state == ("rss", "failed")


@pytest.mark.parametrize(
    (
        "fetched_body",
        "expected_text",
        "expected_body_source",
        "expected_web_fetch_status",
    ),
    [
        (
            "Full article paragraph with enough detail for reading. " * 40,
            "Full article paragraph with enough detail for reading. " * 40,
            "webpage",
            "succeeded",
        ),
        ("", "Short RSS summary", "rss", "failed"),
    ],
)
def test_fetch_source_records_full_content_outcome(
    monkeypatch,
    fetched_body: str,
    expected_text: str,
    expected_body_source: str,
    expected_web_fetch_status: str,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Example RSS</title>
        <item>
          <guid>short-article</guid>
          <title>Short English story</title>
          <link>https://example.com/story</link>
          <description>Short RSS summary</description>
        </item>
      </channel>
    </rss>
    """
    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)
    monkeypatch.setattr(
        "reader_api.rss.fetch_article",
        lambda url, *_args, **_kwargs: (
            ArticleFetchResult(
                content_html=f"<p>{fetched_body}</p>",
                content_text=fetched_body,
                body_source="webpage",
                web_fetch_status="succeeded",
                method="test",
                version="test-v1",
                final_url=url,
                diagnostics=(),
            )
            if fetched_body
            else ArticleFetchResult.rss_failed(
                "Short RSS summary",
                final_url=url,
                diagnostics=("test-failure",),
            )
        ),
    )

    with Session() as session:
        source = Source(name="Example RSS", url="https://example.com/feed.xml", fetch_full_content=True)
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        raw = session.scalar(select(RawEntry))
        document = session.scalar(select(Document))
        item = session.scalar(select(ContentItem))
        reading_body = (
            document.reading_html,
            document.body_source,
            document.web_fetch_status,
        )

    assert raw is not None
    assert document is not None
    assert item is not None
    assert raw.raw_summary == "Short RSS summary"
    assert raw.raw_content == ""
    assert document.content_text.strip() == expected_text.strip()
    assert expected_text.strip() in reading_body[0]
    assert reading_body[1:] == (
        expected_body_source,
        expected_web_fetch_status,
    )
    assert item.content_text.strip() == expected_text.strip()


def test_fetch_source_uses_the_activated_public_rule_snapshot(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    commit = "b" * 40
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Example RSS</title><item>
      <guid>active-rule</guid><title>Active rule story</title>
      <link>https://active.example/story</link>
      <description>Short RSS summary</description>
    </item></channel></rss>
    """
    page = (
        "<html><body><main><p>"
        + ("Activated public rule body. " * 40)
        + "</p></main></body></html>"
    ).encode()

    class PageResponse:
        status = 200
        headers = {"Content-Type": "text/html; charset=utf-8"}

        def __init__(self) -> None:
            self.body = page

        def read(self, size: int) -> bytes:
            chunk, self.body = self.body[:size], self.body[size:]
            return chunk

        def set_timeout(self, _seconds: float) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(feed),
    )
    monkeypatch.setattr(
        "reader_api.article_fetch.resolve_addresses",
        lambda *_args: ("93.184.216.34",),
    )
    monkeypatch.setattr(
        "reader_api.article_fetch._request_once",
        lambda **_kwargs: PageResponse(),
    )

    with Session() as session:
        session.add(
            AppSetting(
                key="fivefilters_rules_snapshot",
                value=json.dumps(
                    {
                        "version": f"fivefilters@{commit}",
                        "rules": {
                            "active.example": {
                                "body": ["//main"],
                                "strip": [],
                                "strip_id_or_class": [],
                            }
                        },
                        "skipped": {},
                        "activated_at": "2026-07-30T10:00:00+00:00",
                    }
                ),
            )
        )
        source = Source(
            name="Example RSS",
            url="https://example.com/feed.xml",
            fetch_full_content=True,
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        document = session.scalars(select(Document)).one()

        assert document.body_source == "webpage"
        assert "Activated public rule body" in document.content_text


def test_first_fetch_only_requests_webpages_for_latest_twenty_entries(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    items = "".join(
        (
            f"<item><guid>entry-{index}</guid><title>Entry {index}</title>"
            f"<link>https://example.com/story/{index}</link>"
            f"<pubDate>Wed, {30 - index:02d} Jul 2026 08:00:00 +0000</pubDate>"
            f"<description>RSS body {index}</description></item>"
        )
        for index in range(25)
    )
    feed = (
        '<?xml version="1.0" encoding="utf-8"?>'
        f"<rss version=\"2.0\"><channel><title>Initial RSS</title>{items}</channel></rss>"
    ).encode()
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(feed),
    )
    requested_urls: list[str] = []

    def fake_fetch_article(
        url: str,
        _rss_text: str,
        **_kwargs: object,
    ) -> ArticleFetchResult:
        requested_urls.append(url)
        body = f"<article><p>{'Web body ' * 50}{url}</p></article>"
        return ArticleFetchResult(
            content_html=body,
            content_text="",
            body_source="webpage",
            web_fetch_status="succeeded",
            method="test",
            version="test-v1",
            final_url=url,
            diagnostics=(),
        )

    monkeypatch.setattr("reader_api.rss.fetch_article", fake_fetch_article)

    with Session() as session:
        source = Source(
            name="Initial RSS",
            url="https://example.com/feed.xml",
            fetch_full_content=True,
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 25
        states = session.execute(
            select(Document.body_source, Document.web_fetch_status)
            .join(RawEntry, Document.raw_entry_id == RawEntry.id)
            .order_by(RawEntry.published_at.desc())
        ).all()

    assert requested_urls == [
        f"https://example.com/story/{index}" for index in range(20)
    ]
    assert states[:20] == [("webpage", "succeeded")] * 20
    assert states[20:] == [("rss", "not_requested")] * 5


def test_first_fetch_retry_keeps_entries_after_twenty_on_rss(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    items = "".join(
        (
            f"<item><guid>entry-{index}</guid><title>Entry {index}</title>"
            f"<link>https://example.com/story/{index}</link>"
            f"<pubDate>Wed, {30 - index:02d} Jul 2026 08:00:00 +0000</pubDate>"
            f"<description>RSS body {index}</description></item>"
        )
        for index in range(21)
    )
    feed = (
        '<?xml version="1.0" encoding="utf-8"?>'
        f"<rss version=\"2.0\"><channel><title>Initial retry RSS</title>{items}</channel></rss>"
    ).encode()
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(feed),
    )
    requested_urls: list[str] = []

    def fake_fetch_article(
        url: str,
        _rss_text: str,
        **_kwargs: object,
    ) -> ArticleFetchResult:
        requested_urls.append(url)
        return ArticleFetchResult(
            content_html=f"<p>{'Web body ' * 50}{url}</p>",
            content_text="",
            body_source="webpage",
            web_fetch_status="succeeded",
            method="test",
            version="test-v1",
            final_url=url,
            diagnostics=(),
        )

    monkeypatch.setattr("reader_api.rss.fetch_article", fake_fetch_article)
    original_create = source_ingest_module.create_initial_projection
    failed_once = False

    def fail_oldest_projection(session, source, raw, prepared):
        nonlocal failed_once
        if raw.external_id == "entry-20" and not failed_once:
            failed_once = True
            raise RuntimeError("projection failed")
        return original_create(session, source, raw, prepared)

    monkeypatch.setattr(
        source_ingest_module,
        "create_initial_projection",
        fail_oldest_projection,
    )

    with Session() as session:
        source = Source(
            name="Initial retry RSS",
            url="https://example.com/feed.xml",
            fetch_full_content=True,
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 20
        assert fetch_source(session, source) == 1
        oldest_state = session.execute(
            select(Document.body_source, Document.web_fetch_status)
            .join(RawEntry, Document.raw_entry_id == RawEntry.id)
            .where(RawEntry.external_id == "entry-20")
        ).one()

    assert requested_urls == [
        f"https://example.com/story/{index}" for index in range(20)
    ]
    assert oldest_state == ("rss", "not_requested")


def test_later_fetch_processes_more_than_twenty_new_entries_without_refetching_known_payloads(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    def feed_with(indices: list[int]) -> bytes:
        items = "".join(
            (
                f"<item><guid>entry-{index}</guid><title>Entry {index}</title>"
                f"<link>https://example.com/story/{index}</link>"
                f"<description>RSS body {index}</description></item>"
            )
            for index in indices
        )
        return (
            '<?xml version="1.0" encoding="utf-8"?>'
            f"<rss version=\"2.0\"><channel><title>Updates RSS</title>{items}</channel></rss>"
        ).encode()

    feeds = iter((feed_with([0]), feed_with(list(range(26)))))
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(next(feeds)),
    )
    requested_urls: list[str] = []

    def fake_fetch_article(
        url: str,
        _rss_text: str,
        **_kwargs: object,
    ) -> ArticleFetchResult:
        requested_urls.append(url)
        return ArticleFetchResult(
            content_html=f"<p>{'Web body ' * 50}{url}</p>",
            content_text="",
            body_source="webpage",
            web_fetch_status="succeeded",
            method="test",
            version="test-v1",
            final_url=url,
            diagnostics=(),
        )

    monkeypatch.setattr("reader_api.rss.fetch_article", fake_fetch_article)

    with Session() as session:
        source = Source(
            name="Updates RSS",
            url="https://example.com/feed.xml",
            fetch_full_content=True,
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        assert fetch_source(session, source) == 25
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 26

    assert requested_urls == [
        "https://example.com/story/0",
        *(f"https://example.com/story/{index}" for index in range(1, 26)),
    ]


def test_distinct_source_identities_with_same_payload_are_both_prepared(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Same payload</title>
      <item><guid>first-guid</guid><title>Same title</title>
        <link>https://example.com/same</link><description>Same body</description>
      </item>
      <item><guid>second-guid</guid><title>Same title</title>
        <link>https://example.com/same</link><description>Same body</description>
      </item>
    </channel></rss>
    """
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(feed),
    )
    calls: list[str] = []
    monkeypatch.setattr(
        "reader_api.rss.fetch_article",
        lambda url, *_args, **_kwargs: (
            calls.append(url)
            or ArticleFetchResult(
                content_html="<p>Prepared webpage body.</p>",
                content_text="Prepared webpage body.",
                body_source="webpage",
                web_fetch_status="succeeded",
                method="test",
                version="test-v1",
                final_url=url,
                diagnostics=(),
            )
        ),
    )

    with Session() as session:
        source = Source(
            name="Same payload",
            url="https://example.com/same.xml",
            fetch_full_content=True,
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 2
        states = session.scalars(
            select(Document.body_source).order_by(Document.id)
        ).all()

    assert calls == [
        "https://example.com/same",
        "https://example.com/same",
    ]
    assert states == ["webpage", "webpage"]


def test_new_revision_web_failure_uses_new_rss_body_instead_of_old_webpage(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    def feed(body: str) -> bytes:
        return (
            '<?xml version="1.0" encoding="utf-8"?>'
            "<rss version=\"2.0\"><channel><title>Revision RSS</title>"
            "<item><guid>revision-entry</guid><title>Revision entry</title>"
            "<link>https://example.com/revision</link>"
            f"<description>{body}</description></item></channel></rss>"
        ).encode()

    feeds = iter((feed("Old RSS body"), feed("New RSS body")))
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(next(feeds)),
    )
    results = iter(
        (
            ArticleFetchResult(
                content_html=f"<p>{'Old webpage body ' * 40}</p>",
                content_text="",
                body_source="webpage",
                web_fetch_status="succeeded",
                method="test",
                version="test-v1",
                final_url="https://example.com/revision",
                diagnostics=(),
            ),
            ArticleFetchResult.rss_failed(
                "New RSS body",
                final_url="https://example.com/revision",
                diagnostics=("timeout",),
            ),
        )
    )
    monkeypatch.setattr(
        "reader_api.rss.fetch_article",
        lambda *_args, **_kwargs: next(results),
    )

    with Session() as session:
        source = Source(
            name="Revision RSS",
            url="https://example.com/feed.xml",
            fetch_full_content=True,
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        first_document = session.scalars(select(Document)).one()
        assert "Old webpage body" in first_document.content_text

        assert fetch_source(session, source) == 0
        document = session.scalars(select(Document)).one()
        item = session.scalars(select(ContentItem)).one()

        assert session.scalar(select(func.count()).select_from(RawEntry)) == 2
        assert document.content_text == "New RSS body"
        assert item.content_text == "New RSS body"
        assert (document.body_source, document.web_fetch_status) == (
            "rss",
            "failed",
        )
        assert "Old webpage body" not in document.content_text


def test_fetch_source_does_not_refresh_existing_short_item_on_scheduled_fetch(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Example RSS</title>
        <item>
          <guid>short-existing</guid>
          <title>Existing short English story</title>
          <link>https://example.com/existing</link>
          <description>Short RSS summary</description>
        </item>
      </channel>
    </rss>
    """
    article_fetch_calls: list[str] = []

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    def fake_fetch_article_text(url: str) -> str:
        article_fetch_calls.append(url)
        return ""

    monkeypatch.setattr("reader_api.rss.fetch_article_text", fake_fetch_article_text)

    with Session() as session:
        source = Source(name="Example RSS", url="https://example.com/feed.xml")
        session.add(source)
        session.commit()

        first_seen_ids: list[int] = []
        assert fetch_source(session, source, imported_item_ids=first_seen_ids) == 1
        item = session.scalar(select(ContentItem))
        assert item is not None
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "old-model"
        session.add(ContentEmbedding(content_item_id=item.id, representation="zh_canonical", model="old-model", vector="[0.0,1.0]"))
        session.commit()

        second_seen_ids: list[int] = []
        assert fetch_source(session, source, imported_item_ids=second_seen_ids) == 0
        session.refresh(item)
        document = session.get(Document, item.document_id)
        raw = session.scalar(select(RawEntry))
        embedding_count = session.scalar(select(func.count()).select_from(ContentEmbedding).where(ContentEmbedding.content_item_id == item.id))

    assert first_seen_ids == [item.id]
    assert second_seen_ids == []
    assert article_fetch_calls == []
    assert raw is not None
    assert raw.raw_summary == "Short RSS summary"
    assert document is not None
    assert document.content_text == "Short RSS summary"
    assert item.content_text == "Short RSS summary"
    assert item.embedding_vector == "[1.0,0.0]"
    assert item.embedding_model == "old-model"
    assert embedding_count == 1


def test_repeating_unchanged_clustered_feed_does_not_create_clustering_history(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Stable RSS</title><item>
      <guid>stable-entry</guid><title>Stable entry</title>
      <link>https://example.com/stable-entry</link><description>Stable body</description>
    </item></channel></rss>
    """
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(feed),
    )

    with Session() as session:
        source = Source(name="Stable RSS", url="https://example.com/stable.xml")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        item = session.scalar(select(ContentItem))
        assert item is not None
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "embedding-model"
        with clustering_run(
            session,
            scope_type="stable-feed-test",
            item_ids=[item.id],
            rule_version=ClusteringRule("stable-feed-test-v1").version,
        ):
            assign_cluster(session, item)

        models = (
            ClusteringRun,
            ClusteringRunScopeEvidence,
            ClusteringRunMembership,
            ClusteringRunProjectionPredecessor,
            ClusterEventProjection,
        )
        before = tuple(
            session.scalar(select(func.count()).select_from(model))
            for model in models
        )
        assert before[0] == 1
        previous_fetched_at = source.last_fetched_at

        assert fetch_source(session, source) == 0

        after = tuple(
            session.scalar(select(func.count()).select_from(model))
            for model in models
        )
        session.refresh(source)

    assert after == before
    assert source.last_fetched_at is not None
    assert previous_fetched_at is not None
    assert source.last_fetched_at >= previous_fetched_at


def test_unclustered_revision_and_new_article_do_not_create_clustering_run(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feeds = iter(
        (
            b"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel><title>Pending RSS</title>
              <item><guid>pending-entry</guid><title>Pending entry</title>
                <link>https://example.com/pending-entry</link>
                <description>Original pending body</description></item>
            </channel></rss>""",
            b"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel><title>Pending RSS</title>
              <item><guid>pending-entry</guid><title>Pending entry</title>
                <link>https://example.com/pending-entry</link>
                <description>Updated pending body</description></item>
              <item><guid>new-entry</guid><title>New entry</title>
                <link>https://example.com/new-entry</link>
                <description>New body</description></item>
            </channel></rss>""",
        )
    )
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(next(feeds)),
    )

    with Session() as session:
        source = Source(name="Pending RSS", url="https://example.com/pending.xml")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        assert fetch_source(session, source) == 1
        content_texts = session.scalars(
            select(ContentItem.content_text).order_by(ContentItem.id)
        ).all()

        assert session.scalar(select(func.count()).select_from(ClusteringRun)) == 0
        assert content_texts == ["Updated pending body", "New body"]


def test_clustered_article_revision_creates_one_run_but_known_old_payload_does_not(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    bodies = iter(("Original body", "Updated body", "Original body"))

    def fake_urlopen(*_args, **_kwargs) -> FeedResponse:
        body = next(bodies)
        return FeedResponse(
            f"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel><title>Revision RSS</title><item>
              <guid>revision-entry</guid><title>Revision entry</title>
              <link>https://example.com/revision-entry</link>
              <description>{body}</description>
            </item></channel></rss>
            """.encode()
        )

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(
            name="Revision RSS",
            url="https://example.com/revision.xml",
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        item = session.scalar(select(ContentItem))
        assert item is not None
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "embedding-model"
        with clustering_run(
            session,
            scope_type="revision-feed-test",
            item_ids=[item.id],
            rule_version=ClusteringRule("revision-feed-test-v1").version,
        ):
            assign_cluster(session, item)

        run_count = session.scalar(select(func.count()).select_from(ClusteringRun))
        projection_count = session.scalar(
            select(func.count()).select_from(ClusterEventProjection)
        )
        revision_count = session.scalar(
            select(func.count()).select_from(EventRevision)
        )

        assert fetch_source(session, source) == 0
        assert session.scalar(select(func.count()).select_from(ClusteringRun)) == run_count + 1
        assert session.scalar(
            select(func.count()).select_from(ClusterEventProjection)
        ) == projection_count + 1
        assert session.scalar(
            select(func.count()).select_from(EventRevision)
        ) == revision_count + 1
        after_update = (
            session.scalar(select(func.count()).select_from(ClusteringRun)),
            session.scalar(
                select(func.count()).select_from(ClusterEventProjection)
            ),
            session.scalar(select(func.count()).select_from(EventRevision)),
        )

        assert fetch_source(session, source) == 0
        after_old_replay = (
            session.scalar(select(func.count()).select_from(ClusteringRun)),
            session.scalar(
                select(func.count()).select_from(ClusterEventProjection)
            ),
            session.scalar(select(func.count()).select_from(EventRevision)),
        )
        session.refresh(item)

    assert after_old_replay == after_update
    assert item.content_text == "Updated body"


def test_same_final_text_revision_does_not_create_clustering_run(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feeds = iter(
        (
            b"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel><title>Stable webpage</title><item>
              <guid>stable-webpage</guid><title>Stable webpage</title>
              <link>https://example.com/stable-webpage</link>
              <description>RSS metadata one</description>
            </item></channel></rss>""",
            b"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel><title>Stable webpage</title><item>
              <guid>stable-webpage</guid><title>Stable webpage</title>
              <link>https://example.com/stable-webpage</link>
              <description>RSS metadata two</description>
            </item></channel></rss>""",
        )
    )
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(next(feeds)),
    )
    monkeypatch.setattr(
        "reader_api.rss.fetch_article",
        lambda url, *_args, **_kwargs: ArticleFetchResult(
            content_html="<article><p>Stable final webpage body.</p></article>",
            content_text="Stable final webpage body.",
            body_source="webpage",
            web_fetch_status="succeeded",
            method="test",
            version="test-v1",
            final_url=url,
            diagnostics=(),
        ),
    )

    with Session() as session:
        source = Source(
            name="Stable webpage",
            url="https://example.com/stable-webpage.xml",
            fetch_full_content=True,
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        item = session.scalars(select(ContentItem)).one()
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "stable-model"
        with clustering_run(
            session,
            scope_type="stable-webpage-test",
            item_ids=[item.id],
            rule_version=ClusteringRule("stable-webpage-test-v1").version,
        ):
            assign_cluster(session, item)
        before = session.scalar(
            select(func.count()).select_from(ClusteringRun)
        )

        assert fetch_source(session, source) == 0
        session.refresh(item)

        assert (
            session.scalar(select(func.count()).select_from(ClusteringRun))
            == before
        )
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 2
        assert item.content_text == "Stable final webpage body."
        assert item.embedding_vector == "[1.0,0.0]"
        assert item.embedding_model == "stable-model"


def test_clustered_article_revision_snapshots_only_affected_cluster(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    target_bodies = iter(("Original target body", "Updated target body"))

    def fake_urlopen(*_args, **_kwargs) -> FeedResponse:
        target_body = next(target_bodies)
        return FeedResponse(
            f"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel><title>Scoped RSS</title>
              <item><guid>target-entry</guid><title>Target entry</title>
                <link>https://example.com/target-entry</link>
                <description>{target_body}</description></item>
              <item><guid>unrelated-entry</guid><title>Unrelated entry</title>
                <link>https://example.com/unrelated-entry</link>
                <description>Stable unrelated body</description></item>
            </channel></rss>
            """.encode()
        )

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(name="Scoped RSS", url="https://example.com/scoped.xml")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 2
        target, unrelated = session.scalars(
            select(ContentItem).order_by(ContentItem.id)
        ).all()
        for index, item in enumerate((target, unrelated), start=1):
            item.embedding_vector = f"[{index}.0,0.0]"
            item.embedding_model = "embedding-model"
            with clustering_run(
                session,
                scope_type=f"scoped-feed-test-{index}",
                item_ids=[item.id],
                rule_version=ClusteringRule("scoped-feed-test-v1").version,
            ):
                assign_cluster(session, item)

        target_cluster_id = session.scalar(
            select(ClusterItem.cluster_id).where(
                ClusterItem.content_item_id == target.id
            )
        )
        unrelated_anchor = evidence_anchor_for_item(session, unrelated)

        assert fetch_source(session, source) == 0
        rss_run = session.scalar(
            select(ClusteringRun)
            .where(ClusteringRun.scope_type == "rss-source-ingest")
            .order_by(ClusteringRun.started_at.desc())
        )
        assert rss_run is not None
        membership_anchors = set(
            session.scalars(
                select(ClusteringRunMembership.evidence_anchor).where(
                    ClusteringRunMembership.run_id == rss_run.id
                )
            ).all()
        )
        projected_cluster_ids = session.scalars(
            select(ClusterEventProjection.cluster_id_snapshot).where(
                ClusterEventProjection.clustering_run_id == rss_run.id
            )
        ).all()

    assert unrelated_anchor not in membership_anchors
    assert projected_cluster_ids == [target_cluster_id]


def test_incomplete_normal_article_projection_conservatively_creates_source_run(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    bodies = iter(("Original body", "Updated body"))

    def fake_urlopen(*_args, **_kwargs) -> FeedResponse:
        body = next(bodies)
        return FeedResponse(
            f"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel><title>Incomplete RSS</title><item>
              <guid>incomplete-entry</guid><title>Incomplete entry</title>
              <link>https://example.com/incomplete-entry</link>
              <description>{body}</description>
            </item></channel></rss>
            """.encode()
        )

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(
            name="Incomplete RSS",
            url="https://example.com/incomplete.xml",
        )
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        item = session.scalar(select(ContentItem))
        identity = session.scalar(select(SourceEntryIdentity))
        assert item is not None
        assert identity is not None
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "embedding-model"
        with clustering_run(
            session,
            scope_type="incomplete-feed-test",
            item_ids=[item.id],
            rule_version=ClusteringRule("incomplete-feed-test-v1").version,
        ):
            assign_cluster(session, item)

        allocate_raw_entry_revision(
            session,
            identity.id,
            RawEntryRevisionInput(
                external_id="incomplete-entry",
                title="Incomplete entry",
                url="https://example.com/incomplete-entry",
                raw_summary="Updated body",
                content_hash=content_hash(
                    "Incomplete entry",
                    "Updated body",
                    "https://example.com/incomplete-entry",
                ),
            ),
        )
        identity.projection_pending = True
        session.commit()
        before = session.scalar(select(func.count()).select_from(ClusteringRun))

        assert fetch_source(session, source) == 0
        session.refresh(item)
        session.refresh(identity)

        assert session.scalar(select(func.count()).select_from(ClusteringRun)) == before + 1
        assert item.content_text == "Updated body"
        assert identity.projection_pending is False


def test_media_only_article_update_does_not_create_clustering_run(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    media_urls = iter(
        (
            "https://cdn.example.com/first.jpg",
            "https://cdn.example.com/second.jpg",
        )
    )

    def fake_urlopen(*_args, **_kwargs) -> FeedResponse:
        media_url = next(media_urls)
        return FeedResponse(
            f"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel><title>Media RSS</title><item>
              <guid>media-entry</guid><title>Media entry</title>
              <link>https://example.com/media-entry</link>
              <description>Stable body</description>
              <enclosure url="{media_url}" type="image/jpeg" />
            </item></channel></rss>
            """.encode()
        )

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(name="Media RSS", url="https://example.com/media.xml")
        session.add(source)
        session.commit()
        assert fetch_source(session, source) == 1
        item = session.scalar(select(ContentItem))
        assert item is not None
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "embedding-model"
        with clustering_run(
            session,
            scope_type="media-feed-test",
            item_ids=[item.id],
            rule_version=ClusteringRule("media-feed-test-v1").version,
        ):
            assign_cluster(session, item)
        before = session.scalar(select(func.count()).select_from(ClusteringRun))

        assert fetch_source(session, source) == 0
        session.refresh(item)

        assert session.scalar(select(func.count()).select_from(ClusteringRun)) == before
        assert item.media_url == "https://cdn.example.com/second.jpg"


def test_digest_revision_stays_pending_without_source_clustering_run(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    revisions = iter(("首次", "更新"))

    def fake_urlopen(*_args, **_kwargs) -> FeedResponse:
        revision = next(revisions)
        rows = "".join(
            f"<p>{index}. {revision}新闻内容 {index} https://example.com/{index}</p>"
            for index in range(1, 8)
        )
        return FeedResponse(
            f"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel><title>Digest RSS</title><item>
              <guid>digest-entry</guid>
              <title>AI 简报：OpenAI / Nvidia / Anthropic / Microsoft / Google / Apple</title>
              <link>https://example.com/digest-entry</link>
              <description><![CDATA[{rows}]]></description>
            </item></channel></rss>
            """.encode()
        )

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(name="Digest RSS", url="https://example.com/digest.xml")
        session.add(source)
        session.commit()
        assert fetch_source(session, source) == 1
        document = session.scalar(select(Document))
        assert document is not None
        assert document.document_type in {"digest", "mixed"}
        item = session.scalar(
            select(ContentItem).where(ContentItem.document_id == document.id)
        )
        assert item is not None
        original_content = item.content_text
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "embedding-model"
        with clustering_run(
            session,
            scope_type="digest-feed-test",
            item_ids=[item.id],
            rule_version=ClusteringRule("digest-feed-test-v1").version,
        ):
            assign_cluster(session, item)
        before = session.scalar(select(func.count()).select_from(ClusteringRun))

        assert fetch_source(session, source) == 0
        identity = session.scalar(select(SourceEntryIdentity))
        session.refresh(item)

        assert identity is not None
        assert identity.current_revision_no == 2
        assert identity.projection_pending is True
        assert item.content_text == original_content
        assert session.scalar(select(func.count()).select_from(ClusteringRun)) == before


def test_new_article_does_not_create_run_when_source_has_legacy_history(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Legacy RSS</title><item>
      <guid>new-entry</guid><title>New entry</title>
      <link>https://example.com/new-entry</link><description>New body</description>
    </item></channel></rss>
    """
    monkeypatch.setattr(
        "reader_api.rss.urlopen",
        lambda *_args, **_kwargs: FeedResponse(feed),
    )

    with Session() as session:
        source = Source(name="Legacy RSS", url="https://example.com/legacy.xml")
        session.add(
            make_raw_entry(
                source=source,
                external_id="legacy-entry",
                title="Legacy entry",
                url="https://example.com/legacy-entry",
            )
        )
        session.commit()

        assert fetch_source(session, source) == 1

        assert session.scalar(select(func.count()).select_from(ClusteringRun)) == 0


def test_fetch_article_text_extracts_post_content(monkeypatch) -> None:
    from reader_api.article_fetch import PageFetchResult

    html = f"""
    <html><body>
      <nav>Navigation should be ignored</nav>
      <div class="container med post-content">
        <h1>Story title</h1>
        <p>First paragraph.</p>
        <p>Second paragraph with details. {"detail " * 180}</p>
        <img src="/image.jpg" alt="Hero">
      </div>
      <article><p>Related card should be ignored.</p></article>
    </body></html>
    """

    monkeypatch.setattr(
        "reader_api.rss.fetch_public_html",
        lambda url: PageFetchResult(True, html, url, "", 1, 0),
    )

    text = fetch_article_text(
        "https://example.com/story",
        article_selector="css:.post-content",
    )

    assert "First paragraph" in text
    assert "Second paragraph" in text
    assert "Navigation should be ignored" not in text
    assert "Related card should be ignored" not in text
    assert "https://example.com/image.jpg" in text


def test_fetch_article_text_prefers_trafilatura_when_available(monkeypatch) -> None:
    from reader_api.article_fetch import PageFetchResult

    html = "<html><body><main><p>fallback only</p></main></body></html>"
    extracted = "Full article paragraph. " * 60

    def fake_extract(*_: object, **__: object) -> str:
        return extracted

    monkeypatch.setitem(sys.modules, "trafilatura", types.SimpleNamespace(extract=fake_extract))
    monkeypatch.setattr(
        "reader_api.rss.fetch_public_html",
        lambda url: PageFetchResult(True, html, url, "", 1, 0),
    )

    assert fetch_article_text("https://example.com/story") == extracted.strip()


def test_fetch_source_replaces_url_placeholder_name_with_feed_title(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Example RSS</title>
        <item><guid>1</guid><title>One</title><description>Body</description></item>
      </channel>
    </rss>
    """

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)
    monkeypatch.setattr("reader_api.rss.fetch_article_text", lambda _url: "")

    with Session() as session:
        source = Source(name="https://example.com/feed.xml", url="https://example.com/feed.xml")
        session.add(source)
        session.commit()

        item_ids: list[int] = []
        assert fetch_source(session, source, imported_item_ids=item_ids) == 1
        assert source.name == "Example RSS"
        assert item_ids == session.scalars(select(ContentItem.id)).all()


def test_fetch_source_appends_time_correction_without_overwriting_raw_entry(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Example RSS</title>
        <item>
          <guid>entry-1</guid>
          <title>Local time item</title>
          <link>https://example.com/item</link>
          <pubDate>2026-06-29 14:30:40</pubDate>
          <description>Example summary</description>
        </item>
      </channel>
    </rss>
    """

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        wrong_utc_time = datetime(2026, 6, 29, 14, 30, 40, tzinfo=timezone.utc)
        source = Source(name="Example RSS", url="https://example.com/feed.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="entry-1",
            title="Local time item",
            published_at=wrong_utc_time,
            content_hash=content_hash("Local time item"),
        )
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title="Local time item", content_text="Example summary")
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title="Local time item",
            summary="Example summary",
            content_text="Example summary",
            published_at=wrong_utc_time,
            content_hash=content_hash("Local time item"),
            normalized_title=normalize_title("Local time item"),
            embedding_vector="[1.0,0.0]",
            embedding_model="embedding-model",
        )
        session.add(item)
        session.flush()
        assign_cluster(session, item)
        session.commit()
        raw_id = raw.id
        item_id = item.id
        source_id = source.id
        session.expunge_all()
        source = session.get(Source, source_id)
        assert source is not None

        assert fetch_source(session, source) == 0

        expected = datetime(2026, 6, 29, 6, 30, 40, tzinfo=timezone.utc)
        raw = session.get(RawEntry, raw_id)
        item = session.get(ContentItem, item_id)
        cluster = session.scalar(select(Cluster))

        assert raw is not None
        assert item is not None
        assert raw.published_at is not None
        assert item.published_at is not None
        assert raw.published_at.replace(tzinfo=timezone.utc) == wrong_utc_time
        assert item.published_at.replace(tzinfo=timezone.utc) == expected
        assert cluster is not None
        assert cluster.first_seen_at is not None
        assert cluster.first_seen_at.replace(tzinfo=timezone.utc) == expected
        assert cluster.last_seen_at is not None
        assert cluster.last_seen_at.replace(tzinfo=timezone.utc) == expected
        assert session.scalar(select(func.count()).select_from(RawEntry).where(RawEntry.source_id == source.id)) == 2
        assert item.document.raw_entry_id != raw.id


def test_fetch_source_preserves_original_digest_item(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = """<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Example RSS</title>
        <item>
          <guid>digest-1</guid>
          <title>AI 早报：OpenAI / Nvidia / Anthropic / Microsoft / Google / Apple</title>
          <link>https://example.com/digest</link>
          <description><![CDATA[
            <p>1. OpenAI 发布新模型 https://example.com/a</p>
            <p>2. Nvidia 推出新芯片 https://example.com/b</p>
            <p>3. Microsoft 建设 AI 数据中心 https://example.com/c</p>
            <p>4. Anthropic 更新 Claude https://example.com/d</p>
            <p>5. Google 发布开发者平台 https://example.com/e</p>
            <p>6. Apple 推出系统更新 https://example.com/f</p>
          ]]></description>
        </item>
      </channel>
    </rss>
    """.encode()

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(name="Example RSS", url="https://example.com/feed.xml")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1

        document = session.scalar(select(Document))
        titles = session.scalars(select(ContentItem.title).order_by(ContentItem.id)).all()

        assert document is not None
        assert document.document_type == "digest"
        assert titles[0] == "AI 早报：OpenAI / Nvidia / Anthropic / Microsoft / Google / Apple"
        assert len(titles) == 7
        assert all(session.scalars(select(ContentItem.lsh_signature)).all())


def test_repair_over_split_documents_collapses_false_digest() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    full_text = """
    育碧（Ubisoft）旗下开放世界合作射击游戏《全境封锁2》目前在 Steam 平台已不再锁国区，可正常购买。
    战术战斗 - 在紧张刺激的掩体枪战中迎战敌人。
    打造终极特工 - 搜刮并打造顶尖装备，学习强力技能。
    团队协作方能挽救生命 - 与最多三位其他特工组队执行战术合作任务。
    极致终局体验 - 达到最高等级仅是征程的开始。
    """
    title = "《全境封锁2》Steam 不再锁国区"
    url = "https://example.com/the-division-2"

    with Session() as session:
        source = Source(name="超能网", url="https://example.com/feed.xml")
        raw = make_raw_entry(source=source, external_id="bad-split", title=title, url=url, content_hash=content_hash(title, full_text, url))
        document = Document(raw_entry=raw, document_type="digest", title=title, summary=full_text[:120], content_text=full_text, digest_score=0.80)
        session.add(document)
        session.flush()
        pieces = [
            (title, full_text),
            ("团队协作方能挽救生命 - 与最多三位其他特工组队执行战术合作任务。", "团队协作方能挽救生命 - 与最多三位其他特工组队执行战术合作任务。"),
            ("极致终局体验 - 达到最高等级仅是征程的开始。", "极致终局体验 - 达到最高等级仅是征程的开始。"),
        ]
        item_ids: list[int] = []
        for item_title, item_text in pieces:
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=item_title,
                summary=item_text[:120],
                content_text=item_text,
                url=url,
                content_hash=content_hash(item_title, item_text, url),
                canonical_url=canonical_url(url),
                normalized_title=normalize_title(item_title),
                lsh_signature=lsh_signature(item_title, item_text),
                embedding_vector="[1.0,0.0]",
                embedding_model="embedding-model",
            )
            session.add(item)
            session.flush()
            assign_cluster(session, item)
            item_ids.append(item.id)
        session.add(UserState(object_type="item", object_id=item_ids[-1], read_status="summary_seen"))
        session.commit()

        assert session.scalar(select(func.count()).select_from(ContentItem)) == 3
        assert repair_over_split_documents(session) == 1

        items = session.scalars(select(ContentItem)).all()
        assert len(items) == 1
        assert items[0].title == title
        assert "打造终极特工" in items[0].content_text
        assert session.scalar(select(func.count()).select_from(UserState).where(UserState.object_type == "item")) == 0
        assert session.scalar(select(func.count()).select_from(ClusterItem)) == 1


def test_repair_over_split_documents_keeps_current_valid_digest() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    title = "AI 简报：OpenAI / Nvidia / Anthropic / Microsoft / Google / Apple"
    url = "https://example.com/brief"
    body = """
    1. OpenAI 发布新模型，面向开发者开放新的推理和工具调用能力
    2. Nvidia 推出新芯片，主打数据中心训练和推理场景
    3. Anthropic 更新 Claude，新增企业团队管理和安全能力
    4. Microsoft 建设 AI 数据中心，扩大云端算力供给
    5. Google 发布开发者平台，整合搜索广告和模型工具
    6. Apple 推出系统更新，强化端侧模型和隐私保护
    """
    raw_html = """
    <p>1. <a href="https://example.com/a">OpenAI 发布新模型，面向开发者开放新的推理和工具调用能力</a></p>
    <p>2. <a href="https://example.com/b">Nvidia 推出新芯片，主打数据中心训练和推理场景</a></p>
    <p>3. <a href="https://example.com/c">Anthropic 更新 Claude，新增企业团队管理和安全能力</a></p>
    <p>4. <a href="https://example.com/d">Microsoft 建设 AI 数据中心，扩大云端算力供给</a></p>
    <p>5. <a href="https://example.com/e">Google 发布开发者平台，整合搜索广告和模型工具</a></p>
    <p>6. <a href="https://example.com/f">Apple 推出系统更新，强化端侧模型和隐私保护</a></p>
    """

    with Session() as session:
        source = Source(name="AI 简报", url="https://example.com/feed.xml")
        raw = make_raw_entry(
            source=source,
            external_id="valid-digest",
            title=title,
            url=url,
            raw_content=raw_html,
            content_hash=content_hash(title, body, url),
        )
        document = Document(raw_entry=raw, document_type="mixed", title=title, summary=body[:120], content_text=body, digest_score=0.70)
        session.add(document)
        session.flush()
        for item_title, item_text in [(title, body), ("OpenAI 发布新模型", "OpenAI 发布新模型"), ("Nvidia 推出新芯片", "Nvidia 推出新芯片")]:
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=item_title,
                summary=item_text[:120],
                content_text=item_text,
                url=url,
                content_hash=content_hash(item_title, item_text, url),
                canonical_url=canonical_url(url),
                normalized_title=normalize_title(item_title),
                lsh_signature=lsh_signature(item_title, item_text),
                embedding_vector="[1.0,0.0]",
                embedding_model="embedding-model",
            )
            session.add(item)
            session.flush()
            assign_cluster(session, item)
        session.commit()

        assert repair_over_split_documents(session) == 0
        session.refresh(document)

        assert session.scalar(select(func.count()).select_from(ContentItem).where(ContentItem.document_id == document.id)) == 3
        assert document.document_type == "digest"
        assert document.digest_score >= 0.90


def test_fetch_source_imports_item_without_published_time(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Example RSS</title>
        <item>
          <guid>entry-no-date</guid>
          <title>No date item</title>
          <link>https://example.com/no-date</link>
          <description>Body without published time</description>
        </item>
      </channel>
    </rss>
    """

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(name="Example RSS", url="https://example.com/feed.xml")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        raw = session.scalar(select(RawEntry))
        document = session.scalar(select(Document))
        item = session.scalar(select(ContentItem))

    assert raw is not None
    assert raw.published_at is None
    assert document is not None
    assert document.title == "No date item"
    assert item is not None
    assert item.published_at is None
    assert item.title == "No date item"


def test_fetch_source_keeps_distinct_entries_with_reused_guid(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Example RSS</title>
        <item>
          <guid>same-guid</guid>
          <title>First reused guid item</title>
          <link>https://example.com/first</link>
          <description>First body</description>
        </item>
        <item>
          <guid>same-guid</guid>
          <title>Second reused guid item</title>
          <link>https://example.com/second</link>
          <description>Second body</description>
        </item>
      </channel>
    </rss>
    """

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(name="Example RSS", url="https://example.com/feed.xml")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 2
        assert fetch_source(session, source) == 0
        titles = session.scalars(select(RawEntry.title).order_by(RawEntry.id)).all()
        external_ids = session.scalars(select(RawEntry.external_id).order_by(RawEntry.id)).all()
        identity_count = session.scalar(select(func.count()).select_from(SourceEntryIdentity))

    assert titles == ["First reused guid item", "Second reused guid item"]
    assert external_ids == ["same-guid", "same-guid"]
    assert identity_count == 2


def test_fetch_source_keeps_distinct_entries_with_same_url_different_titles(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Example RSS</title>
        <item>
          <title>First title for same URL</title>
          <link>https://example.com/shared</link>
          <description>First body</description>
        </item>
        <item>
          <title>Second title for same URL</title>
          <link>https://example.com/shared</link>
          <description>Second body</description>
        </item>
      </channel>
    </rss>
    """

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(name="Example RSS", url="https://example.com/feed.xml")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        assert fetch_source(session, source) == 0
        titles = session.scalars(select(RawEntry.title).order_by(RawEntry.id)).all()
        item_titles = session.scalars(select(ContentItem.title).order_by(ContentItem.id)).all()
        identity_count = session.scalar(select(func.count()).select_from(SourceEntryIdentity))

    assert titles == ["First title for same URL", "Second title for same URL"]
    assert item_titles == ["Second title for same URL"]
    assert identity_count == 1


def test_fetch_source_dedupes_no_guid_same_url_same_title_changed_body(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    bodies = iter(["First body", "Edited body"])

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        body = next(bodies)
        feed = f"""<?xml version="1.0" encoding="utf-8"?>
        <rss version="2.0">
          <channel>
            <title>Example RSS</title>
            <item>
              <title>Stable title</title>
              <link>https://example.com/stable</link>
              <description>{body}</description>
            </item>
          </channel>
        </rss>
        """.encode()
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)
    monkeypatch.setattr("reader_api.rss.fetch_article_text", lambda _url: "")

    with Session() as session:
        source = Source(name="Example RSS", url="https://example.com/feed.xml")
        session.add(source)
        session.commit()

        first_seen_ids: list[int] = []
        assert fetch_source(session, source, imported_item_ids=first_seen_ids) == 1
        item_id = session.scalar(select(ContentItem.id))
        second_seen_ids: list[int] = []
        assert fetch_source(session, source, imported_item_ids=second_seen_ids) == 0
        assert session.scalar(select(func.count()).select_from(RawEntry).where(RawEntry.source_id == source.id)) == 2
        assert session.scalar(select(func.count()).select_from(ContentItem).where(ContentItem.source_id == source.id)) == 1
        assert session.scalar(select(func.count()).select_from(ClusteringRun)) == 0
        document = session.scalar(select(Document))
        latest_raw = session.scalar(select(RawEntry).order_by(RawEntry.revision_no.desc()))

    assert first_seen_ids == [item_id]
    assert second_seen_ids == [item_id]
    assert document is not None
    assert latest_raw is not None
    assert document.raw_entry_id == latest_raw.id
    assert latest_raw.revision_no == 2


def test_fetch_source_updates_same_guid_same_url_without_duplicate(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feeds = iter(
        [
            b"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel><title>Example RSS</title>
              <item>
                <guid>https://example.com/story</guid>
                <title>Original title</title>
                <link>https://example.com/story</link>
                <description>Original body</description>
              </item>
            </channel></rss>
            """,
            b"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel><title>Example RSS</title>
              <item>
                <guid>https://example.com/story</guid>
                <title>Original title (Updated)</title>
                <link>https://example.com/story</link>
                <description>Original body with update details</description>
              </item>
            </channel></rss>
            """,
        ]
    )

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(next(feeds))

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)
    monkeypatch.setattr("reader_api.rss.fetch_article_text", lambda _url: "")

    with Session() as session:
        source = Source(name="Example RSS", url="https://example.com/feed.xml")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        item = session.scalar(select(ContentItem))
        assert item is not None
        item.embedding_vector = "[1.0,0.0]"
        item.embedding_model = "old-model"
        session.add(ContentEmbedding(content_item_id=item.id, representation="zh_canonical", model="old-model", vector="[1.0,0.0]"))
        session.commit()

        assert fetch_source(session, source) == 0
        raws = session.scalars(select(RawEntry).order_by(RawEntry.revision_no)).all()
        document = session.scalar(select(Document))
        item = session.scalar(select(ContentItem))
        embedding_count = session.scalar(select(func.count()).select_from(ContentEmbedding))

    assert len(raws) == 2
    assert document is not None
    assert item is not None
    assert raws[0].title == "Original title"
    assert raws[1].title == "Original title (Updated)"
    assert document.raw_entry_id == raws[1].id
    assert item.title == "Original title (Updated)"
    assert item.content_text == "Original body with update details"
    assert item.embedding_vector is None
    assert item.embedding_model == ""
    assert embedding_count == 0


def test_duplicate_feed_repair_records_relation_without_mutation() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="Example RSS", url="https://example.com/feed.xml")
        session.add(source)
        session.flush()
        cluster = Cluster(cluster_key="duplicate", title="Duplicate")
        session.add(cluster)
        session.flush()
        for external_id, title, published_at in [
            ("https://example.com/story", "Original title", datetime(2026, 7, 1, tzinfo=timezone.utc)),
            ("https://example.com/story:abcdef123456", "Original title (Updated)", datetime(2026, 7, 2, tzinfo=timezone.utc)),
        ]:
            raw = make_raw_entry(source_id=source.id, external_id=external_id, title=title, url="https://example.com/story", published_at=published_at, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title, document_type="normal_article")
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                url="https://example.com/story",
                published_at=published_at,
                canonical_url=canonical_url("https://example.com/story"),
                content_hash=content_hash(title),
                normalized_title=normalize_title(title),
                embedding_vector="[1.0,0.0]",
                embedding_model="old-model",
            )
            session.add(item)
            session.flush()
            session.add(ClusterItem(cluster_id=cluster.id, content_item_id=item.id, duplicate_score=1.0))
        session.commit()

        before = tuple(
            session.scalar(select(func.count()).select_from(model))
            for model in (RawEntry, Document, ContentItem, Cluster, ClusterItem)
        )
        assert repair_duplicate_feed_entries(session) == 1
        session.commit()
        raw_count = session.scalar(select(func.count()).select_from(RawEntry))
        raw_titles = session.scalars(select(RawEntry.title).order_by(RawEntry.id)).all()
        after = tuple(
            session.scalar(select(func.count()).select_from(model))
            for model in (RawEntry, Document, ContentItem, Cluster, ClusterItem)
        )
        cluster_item_count = session.scalar(select(func.count()).select_from(ClusterItem))
        relation = session.scalars(select(SourceEntryRelation)).one()

    assert before == after == (2, 2, 2, 1, 2)
    assert raw_count == 2
    assert raw_titles == ["Original title", "Original title (Updated)"]
    assert cluster_item_count == 2
    assert relation.active is True


def test_fetch_source_discards_when_refreshed_status_is_ineligible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><title>Stale source</title><item>
      <guid>stale-1</guid><title>Stale item</title>
      <link>https://example.com/stale-item</link><description>Body</description>
    </item></channel></rss>
    """
    monkeypatch.setattr(
        rss_module,
        "urlopen",
        lambda *_args, **_kwargs: FeedResponse(feed),
    )
    original_lock = rss_module.clustering_run_execution_lock
    lock_active = False

    @contextmanager
    def tracked_lock(session):
        nonlocal lock_active
        with original_lock(session):
            lock_active = True
            try:
                yield
            finally:
                lock_active = False

    monkeypatch.setattr(rss_module, "clustering_run_execution_lock", tracked_lock)

    with Session() as session:
        source = Source(
            name="Stale source",
            url="https://example.com/stale.xml",
            status="active",
            media_type="article",
        )
        session.add(source)
        session.commit()
        original_refresh = session.refresh

        def archive_during_refresh(instance, *args, **kwargs):
            assert lock_active is True
            original_refresh(instance, *args, **kwargs)
            instance.status = "archived"

        monkeypatch.setattr(session, "refresh", archive_during_refresh)

        assert fetch_source(session, source) == 0
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 0
        assert session.scalar(select(func.count()).select_from(ClusteringRun)) == 0
        assert session.scalar(select(func.count()).select_from(ClusterItem)) == 0


def test_fetch_source_archives_non_article_without_clustering(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Video RSS</title>
        <item>
          <guid>video-1</guid>
          <title>Video item</title>
          <link>https://example.com/video</link>
          <description>Video body</description>
        </item>
      </channel>
    </rss>
    """

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(name="Video RSS", url="https://example.com/feed.xml", media_type="video")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        item = session.scalar(select(ContentItem).where(ContentItem.source_id == source.id))
        assert item is not None
        assert item.media_url == ""
        assert item.media_kind == ""
        assert item.media_duration == 0
        assert session.scalar(select(func.count()).select_from(ClusterItem)) == 0


def test_fetch_source_extracts_enclosure_media_and_duration(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
      <channel>
        <title>Media RSS</title>
        <item>
          <guid>video-1</guid>
          <title>Video item</title>
          <link>https://example.com/video</link>
          <description>Video body</description>
          <media:content url="https://cdn.example.com/video.mp4" medium="video" type="video/mp4" duration="125" />
        </item>
        <item>
          <guid>podcast-1</guid>
          <title>Podcast item</title>
          <link>https://example.com/podcast</link>
          <description>Podcast body</description>
          <enclosure url="https://cdn.example.com/episode.mp3" type="audio/mpeg" length="1200" />
          <itunes:duration>01:02:03</itunes:duration>
        </item>
        <item>
          <guid>image-1</guid>
          <title>Image item</title>
          <link>https://example.com/image</link>
          <description>Image body</description>
          <media:content url="https://cdn.example.com/photo.jpg" medium="image" type="image/jpeg" />
        </item>
      </channel>
    </rss>
    """

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(name="Media RSS", url="https://example.com/feed.xml", media_type="video")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 3
        items = {item.title: item for item in session.scalars(select(ContentItem)).all()}

    assert items["Video item"].media_url == "https://cdn.example.com/video.mp4"
    assert items["Video item"].media_kind == "video"
    assert items["Video item"].media_duration == 125
    assert items["Podcast item"].media_url == "https://cdn.example.com/episode.mp3"
    assert items["Podcast item"].media_kind == "audio"
    assert items["Podcast item"].media_duration == 3723
    assert items["Image item"].media_url == "https://cdn.example.com/photo.jpg"
    assert items["Image item"].media_kind == "image"


def test_fetch_source_extracts_youtube_page_duration(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/">
      <channel>
        <title>YouTube RSS</title>
        <item>
          <guid>yt:video:abc123XYZ_0</guid>
          <title>YouTube item</title>
          <link>https://www.youtube.com/watch?v=abc123XYZ_0</link>
          <description>YouTube body</description>
          <media:content url="https://www.youtube.com/v/abc123XYZ_0?version=3" type="application/x-shockwave-flash" />
        </item>
      </channel>
    </rss>
    """
    watch_urls: list[str] = []

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    def fake_fetch_article_html(url: str) -> str:
        watch_urls.append(url)
        return '<html><script>{"lengthSeconds":"1195"}</script></html>'

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)
    monkeypatch.setattr(
        "reader_api.rss.fetch_article_html",
        fake_fetch_article_html,
    )

    with Session() as session:
        source = Source(name="YouTube RSS", url="https://example.com/youtube.xml", media_type="video")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        item = session.scalar(select(ContentItem))

    assert item is not None
    assert item.media_url == "https://www.youtube.com/v/abc123XYZ_0?version=3"
    assert item.media_kind == "video"
    assert item.media_duration == 1195
    assert watch_urls == ["https://www.youtube.com/watch?v=abc123XYZ_0"]


def test_fetch_source_extracts_bilibili_duration_from_rss_metadata(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Bilibili RSS</title>
        <item>
          <guid>bili-rss-1</guid>
          <title>Bilibili RSS item</title>
          <link>https://www.bilibili.com/video/BV1rssDuration</link>
          <description><![CDATA[{"@context":"https://schema.org","@type":"VideoObject","duration":"PT5M37S"}]]></description>
        </item>
      </channel>
    </rss>
    """

    def fake_urlopen(request: object, **__: object) -> FeedResponse:
        url = request.full_url if hasattr(request, "full_url") else str(request)
        assert "bilibili.com/video" not in url
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(name="Bilibili RSS", url="https://example.com/bilibili.xml", media_type="video")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        item = session.scalar(select(ContentItem))

    assert item is not None
    assert item.media_url == ""
    assert item.media_kind == ""
    assert item.media_duration == 337


def test_fetch_source_extracts_bilibili_page_duration(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Bilibili RSS</title>
        <item>
          <guid>bili-page-1</guid>
          <title>Bilibili page item</title>
          <link>https://www.bilibili.com/video/BV1pageDuration</link>
          <description>Video body</description>
        </item>
      </channel>
    </rss>
    """
    page_urls: list[str] = []

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    def fake_fetch_article_html(url: str) -> str:
        page_urls.append(url)
        return '<html><script>window.__INITIAL_STATE__={"videoData":{"duration":421}}</script></html>'

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)
    monkeypatch.setattr(
        "reader_api.rss.fetch_article_html",
        fake_fetch_article_html,
    )

    with Session() as session:
        source = Source(name="Bilibili RSS", url="https://example.com/bilibili.xml", media_type="video")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        item = session.scalar(select(ContentItem))

    assert item is not None
    assert item.media_duration == 421
    assert page_urls == ["https://www.bilibili.com/video/BV1pageDuration"]


def test_fetch_source_extracts_bilibili_duration_when_feed_has_thumbnail_media(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/">
      <channel>
        <title>Bilibili RSS</title>
        <item>
          <guid>bili-thumb-1</guid>
          <title>Bilibili thumbnail item</title>
          <link>https://www.bilibili.com/video/BV1thumbDuration</link>
          <description>Video body</description>
          <media:content url="https://i0.hdslb.com/bfs/archive/thumb.jpg" medium="image" type="image/jpeg" />
        </item>
      </channel>
    </rss>
    """

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)
    monkeypatch.setattr(
        "reader_api.rss.fetch_article_html",
        lambda _url: '<html><script>window.__INITIAL_STATE__={"videoData":{"duration":93}}</script></html>',
    )

    with Session() as session:
        source = Source(name="Bilibili RSS", url="https://example.com/bilibili.xml", media_type="video")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        item = session.scalar(select(ContentItem))

    assert item is not None
    assert item.media_url == "https://i0.hdslb.com/bfs/archive/thumb.jpg"
    assert item.media_kind == "image"
    assert item.media_duration == 93


def test_fetch_source_backfills_existing_bilibili_duration(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Bilibili RSS</title>
        <item>
          <guid>bili-existing-1</guid>
          <title>Existing Bilibili item</title>
          <link>https://www.bilibili.com/video/BV1existingDuration</link>
          <description>Video body</description>
        </item>
      </channel>
    </rss>
    """
    pages = iter(
        [
            b"<html><script>window.__INITIAL_STATE__={}</script></html>",
            b'<html><script>window.__INITIAL_STATE__={"videoData":{"timelength":188000}}</script></html>',
        ]
    )
    feeds = iter(
        [
            feed,
            feed,
            feed.replace(b"</channel>", b"<!-- refreshed --> </channel>"),
        ]
    )
    page_urls: list[str] = []

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(next(feeds))

    def fake_fetch_article_html(url: str) -> str:
        page_urls.append(url)
        return next(pages).decode()

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)
    monkeypatch.setattr(
        "reader_api.rss.fetch_article_html",
        fake_fetch_article_html,
    )

    with Session() as session:
        source = Source(name="Bilibili RSS", url="https://example.com/bilibili.xml", media_type="video")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 1
        item = session.scalar(select(ContentItem))
        assert item is not None
        assert item.media_duration == 0

        assert fetch_source(session, source) == 0
        assert fetch_source(session, source) == 0
        item = session.scalar(select(ContentItem))
        raw_count = session.scalar(select(func.count()).select_from(RawEntry))

    assert item is not None
    assert item.media_duration == 188
    assert raw_count == 1
    assert page_urls == [
        "https://www.bilibili.com/video/BV1existingDuration",
        "https://www.bilibili.com/video/BV1existingDuration",
    ]


def test_fetch_enabled_sources_records_error_and_keeps_fetching(
    monkeypatch,
    caplog,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    feed = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0">
      <channel>
        <title>Good RSS</title>
        <item>
          <guid>ok-1</guid>
          <title>Good item</title>
          <link>https://example.com/good/item</link>
          <description>Good body</description>
        </item>
      </channel>
    </rss>
    """

    def fake_urlopen(request: object, **__: object) -> FeedResponse:
        if "bad.example" in request.full_url:  # type: ignore[attr-defined]
            raise URLError("bad feed")
        return FeedResponse(feed)

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)
    caplog.set_level(logging.INFO, logger="reader_api.rss")

    with Session() as session:
        bad = Source(name="Bad RSS", url="https://bad.example/feed.xml")
        good = Source(name="Good RSS", url="https://good.example/feed.xml")
        session.add_all([bad, good])
        session.commit()

        run_result: dict[str, int] = {}
        assert fetch_enabled_sources(session, run_result=run_result) == 1
        assert run_result == {
            "attempted_sources": 2,
            "successful_sources": 1,
        }
        session.refresh(bad)
        session.refresh(good)

        assert bad.last_error.startswith("RSS 抓取失败")
        assert "bad feed" in bad.last_error
        assert bad.last_fetched_at is not None
        assert good.last_error == ""
        assert session.scalar(select(func.count()).select_from(RawEntry).where(RawEntry.source_id == good.id)) == 1

    assert (
        "RSS 抓取完成：imported=1 item_ids=0 clustering_runs=0 skipped_runs=1"
        in caplog.messages
    )


def test_fetch_enabled_sources_never_fetches_internal_smoke_fixtures(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    requested: list[str] = []

    def fail_if_requested(request: object, **__: object) -> FeedResponse:
        requested.append(request.full_url)  # type: ignore[attr-defined]
        raise AssertionError("internal smoke fixture must not reach the network")

    monkeypatch.setattr("reader_api.rss.urlopen", fail_if_requested)
    with Session() as session:
        session.add_all(
            [
                Source(
                    name="Deployment smoke",
                    url="https://deployment-smoke.invalid/run/feed.xml",
                    enabled=True,
                ),
                Source(
                    name="P0.3 smoke",
                    url="https://p03-smoke.invalid/run/feed.xml",
                    enabled=True,
                ),
            ]
        )
        session.commit()

        assert fetch_enabled_sources(session) == 0
        assert requested == []


def test_fetch_enabled_sources_fetches_bibigpt_once_after_22_beijing_until_success(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    class FrozenDateTime(datetime):
        current = datetime(2026, 7, 31, 13, 59, tzinfo=timezone.utc)

        @classmethod
        def now(cls, tz=None):
            return cls.current.astimezone(tz) if tz else cls.current.replace(tzinfo=None)

    attempts: list[str] = []
    bibigpt_attempts = 0

    def fake_fetch_source(
        session: Session,
        source: Source,
        imported_item_ids: list[int] | None = None,
    ) -> int:
        nonlocal bibigpt_attempts
        attempts.append(source.name)
        source.last_fetched_at = FrozenDateTime.current
        if source.url == rss_module.BIBIGPT_DAILY_FEED_URL:
            bibigpt_attempts += 1
            source.last_error = "RSS 抓取失败: timeout" if bibigpt_attempts == 1 else ""
        else:
            source.last_error = ""
        session.commit()
        return 0

    monkeypatch.setattr(rss_module, "datetime", FrozenDateTime)
    monkeypatch.setattr(rss_module, "fetch_source", fake_fetch_source)

    with Session() as session:
        session.add_all(
            [
                Source(
                    name="BibiGPT",
                    url=rss_module.BIBIGPT_DAILY_FEED_URL,
                ),
                Source(name="Normal RSS", url="https://example.com/feed.xml"),
            ]
        )
        session.commit()

        def assert_run(
            current: datetime,
            expected_attempts: list[str],
            successful_sources: int,
        ) -> None:
            FrozenDateTime.current = current
            attempts.clear()
            run_result: dict[str, int] = {}
            assert fetch_enabled_sources(session, run_result=run_result) == 0
            assert sorted(attempts) == sorted(expected_attempts)
            assert run_result == {
                "attempted_sources": len(expected_attempts),
                "successful_sources": successful_sources,
            }

        assert_run(datetime(2026, 7, 31, 13, 59, tzinfo=timezone.utc), ["Normal RSS"], 1)
        assert_run(datetime(2026, 7, 31, 14, 0, tzinfo=timezone.utc), ["BibiGPT", "Normal RSS"], 1)
        assert_run(datetime(2026, 7, 31, 14, 20, tzinfo=timezone.utc), ["BibiGPT", "Normal RSS"], 2)
        assert_run(datetime(2026, 7, 31, 14, 40, tzinfo=timezone.utc), ["Normal RSS"], 1)
        assert_run(datetime(2026, 8, 1, 14, 0, tzinfo=timezone.utc), ["BibiGPT", "Normal RSS"], 2)


def test_fetch_enabled_sources_continues_after_unexpected_source_error(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    class TrackingSession:
        def __init__(self, inner: object):
            self.inner = inner
            self.rollback_called = False

        def rollback(self) -> None:
            self.rollback_called = True
            self.inner.rollback()  # type: ignore[attr-defined]

        def __getattr__(self, name: str) -> object:
            return getattr(self.inner, name)

    def fake_fetch_source(_session: object, source: Source, imported_item_ids: list[int] | None = None) -> int:
        if source.name == "Bad RSS":
            raise RuntimeError("unexpected parser error")
        if imported_item_ids is not None:
            imported_item_ids.extend([1, 2])
        return 2

    monkeypatch.setattr("reader_api.rss.fetch_source", fake_fetch_source)

    with Session() as session:
        bad = Source(name="Bad RSS", url="https://bad.example/feed.xml")
        good = Source(name="Good RSS", url="https://good.example/feed.xml")
        session.add_all([bad, good])
        session.commit()

        tracking_session = TrackingSession(session)

        assert fetch_enabled_sources(tracking_session) == 2
        assert tracking_session.rollback_called is True
        session.refresh(bad)
        session.refresh(good)

        assert bad.last_error == "RSS 抓取失败: unexpected parser error"
        assert bad.last_fetched_at is not None
        assert good.last_error == ""


def test_fetch_enabled_sources_continues_when_recording_source_error_also_fails(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    def fake_fetch_source(
        _session: object,
        source: Source,
        imported_item_ids: list[int] | None = None,
    ) -> int:
        if source.name == "Bad RSS":
            raise RuntimeError("parser failed")
        if imported_item_ids is not None:
            imported_item_ids.append(7)
        return 1

    @contextmanager
    def fake_locked_source(
        _session: object,
        source: Source,
        _attempted_url: str,
    ):
        if source.name == "Bad RSS":
            raise RuntimeError("connection result was closed")
        yield True

    monkeypatch.setattr("reader_api.rss.fetch_source", fake_fetch_source)
    monkeypatch.setattr(
        "reader_api.rss.locked_current_fetch_source",
        fake_locked_source,
    )

    with Session() as session:
        session.add_all(
            [
                Source(name="Bad RSS", url="https://bad.example/feed.xml"),
                Source(name="Good RSS", url="https://good.example/feed.xml"),
            ]
        )
        session.commit()
        imported_item_ids: list[int] = []

        assert fetch_enabled_sources(session, imported_item_ids) == 1
        assert imported_item_ids == [7]


def test_fetch_enabled_sources_discards_unexpected_error_after_source_is_archived(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    def fake_fetch_source(
        session: Session,
        source: Source,
        imported_item_ids: list[int] | None = None,
    ) -> int:
        if source.name == "Archiving RSS":
            source.status = "archived"
            source.enabled = False
            session.commit()
            raise RuntimeError("unexpected error after archive")
        return 1

    monkeypatch.setattr("reader_api.rss.fetch_source", fake_fetch_source)

    with Session() as session:
        archiving = Source(
            name="Archiving RSS",
            url="https://archive.example/feed.xml",
        )
        good = Source(name="Good RSS", url="https://good.example/feed.xml")
        session.add_all([archiving, good])
        session.commit()

        assert fetch_enabled_sources(session) == 1
        session.refresh(archiving)

        assert archiving.status == "archived"
        assert archiving.enabled is False
        assert archiving.last_error == ""
        assert archiving.last_fetched_at is None


def test_fetch_enabled_sources_discards_unexpected_error_after_source_url_changes(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    def fake_fetch_source(
        session: Session,
        source: Source,
        imported_item_ids: list[int] | None = None,
    ) -> int:
        source.url = "https://new.example/feed.xml"
        session.commit()
        raise RuntimeError("old response exploded")

    monkeypatch.setattr("reader_api.rss.fetch_source", fake_fetch_source)

    with Session() as session:
        source = Source(
            name="Changing RSS",
            url="https://old.example/feed.xml",
        )
        session.add(source)
        session.commit()

        assert fetch_enabled_sources(session) == 0
        session.refresh(source)

        assert source.url == "https://new.example/feed.xml"
        assert source.last_error == ""
        assert source.last_fetched_at is None


def test_fetch_source_records_invalid_feed_attempt(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    def fake_urlopen(*_: object, **__: object) -> FeedResponse:
        return FeedResponse(b"not xml at all")

    monkeypatch.setattr("reader_api.rss.urlopen", fake_urlopen)

    with Session() as session:
        source = Source(name="Broken RSS", url="https://broken.example/feed.xml")
        session.add(source)
        session.commit()

        assert fetch_source(session, source) == 0
        session.refresh(source)

        assert source.last_error.startswith("RSS 解析失败")
        assert source.last_fetched_at is not None
        assert session.scalar(select(func.count()).select_from(RawEntry)) == 0
