import pytest
from sqlalchemy.orm import sessionmaker

from reader_api.db import Base, engine
from reader_api.models import Document, Source
from tests.factories import make_raw_entry


@pytest.mark.parametrize(
    "body",
    [
        {},
        {
            "reading_html": "<p>RSS 正文</p>",
            "body_source": "rss",
            "web_fetch_status": "not_requested",
        },
        {
            "reading_html": "<p>RSS 回退正文</p>",
            "body_source": "rss",
            "web_fetch_status": "failed",
        },
        {
            "reading_html": "<p>网页正文</p>",
            "body_source": "webpage",
            "web_fetch_status": "succeeded",
        },
    ],
)
def test_document_model_accepts_legacy_and_new_body_states(
    body: dict[str, str]
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with sessionmaker(bind=engine)() as session:
        source = Source(name="Body source", url="https://example.com/body.xml")
        raw = make_raw_entry(source=source)
        session.add(
            Document(raw_entry=raw, title="Body contract", **body)
        )
        session.commit()


@pytest.mark.parametrize(
    "body",
    [
        {
            "reading_html": "<p>缺状态</p>",
            "body_source": None,
            "web_fetch_status": None,
        },
        {
            "reading_html": None,
            "body_source": "rss",
            "web_fetch_status": "not_requested",
        },
        {
            "reading_html": "<p>错配</p>",
            "body_source": "webpage",
            "web_fetch_status": "failed",
        },
        {
            "reading_html": "<p>未知来源</p>",
            "body_source": "reader",
            "web_fetch_status": "succeeded",
        },
    ],
)
def test_document_model_rejects_invalid_body_states(
    body: dict[str, str | None]
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with sessionmaker(bind=engine)() as session:
        source = Source(name="Invalid body", url="https://example.com/invalid.xml")
        raw = make_raw_entry(source=source)
        session.add(
            Document(raw_entry=raw, title="Invalid body contract", **body)
        )

        with pytest.raises(ValueError, match="正文状态"):
            session.commit()
