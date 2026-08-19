import json
import tarfile
from datetime import datetime, timedelta, timezone
from io import BytesIO

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from reader_api.db import Base, engine
from reader_api.main import app
from reader_api.models import AppSetting, ContentItem, Document, RawEntry, Source
from tests.factories import make_raw_entry


@pytest.fixture(autouse=True)
def reset_database() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def fivefilters_archive(files: dict[str, str]) -> bytes:
    buffer = BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, text in files.items():
            data = text.encode()
            member = tarfile.TarInfo(f"ftr-site-config-test/{name}")
            member.size = len(data)
            archive.addfile(member, BytesIO(data))
    return buffer.getvalue()


def patch_article_html(monkeypatch, html: str) -> None:
    class Response:
        status = 200
        headers = {"Content-Type": "text/html; charset=utf-8"}

        def __init__(self) -> None:
            self.body = html.encode()

        def read(self, size: int) -> bytes:
            chunk, self.body = self.body[:size], self.body[size:]
            return chunk

        def set_timeout(self, _seconds: float) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "reader_api.article_fetch.resolve_addresses",
        lambda *_args: ("93.184.216.34",),
    )
    monkeypatch.setattr(
        "reader_api.article_fetch._request_once",
        lambda **_kwargs: Response(),
    )


def test_article_preview_lists_only_the_source_latest_six_entries() -> None:
    with sessionmaker(bind=engine)() as session:
        source = Source(name="Preview", url="https://example.com/feed.xml")
        session.add(source)
        session.flush()
        now = datetime.now(timezone.utc)
        for index in range(7):
            session.add(
                make_raw_entry(
                    source_id=source.id,
                    external_id=f"entry-{index}",
                    title=f"文章 {index}",
                    url=f"https://example.com/articles/{index}",
                    published_at=now - timedelta(hours=index),
                    raw_content=f"<p>RSS 正文 {index}</p>",
                )
            )
        session.commit()
        source_id = source.id

    response = TestClient(app).get(f"/sources/{source_id}/article-preview")

    assert response.status_code == 200
    assert [entry["title"] for entry in response.json()["entries"]] == [
        f"文章 {index}" for index in range(6)
    ]


def test_article_preview_rejects_an_entry_outside_the_latest_six() -> None:
    with sessionmaker(bind=engine)() as session:
        source = Source(name="Preview", url="https://example.com/feed.xml")
        session.add(source)
        session.flush()
        now = datetime.now(timezone.utc)
        entries = [
            make_raw_entry(
                source_id=source.id,
                external_id=f"entry-{index}",
                title=f"文章 {index}",
                url=f"https://example.com/articles/{index}",
                published_at=now - timedelta(hours=index),
                raw_content=f"<p>RSS 正文 {index}</p>",
            )
            for index in range(7)
        ]
        session.add_all(entries)
        session.commit()
        source_id = source.id
        excluded_id = entries[-1].id

    response = TestClient(app).post(
        f"/sources/{source_id}/article-preview",
        json={
            "raw_entry_id": excluded_id,
            "fetch_full_content": True,
            "article_selector": None,
            "remove_selector": None,
        },
    )

    assert response.status_code == 404


def test_article_preview_rejects_an_arbitrary_url_field() -> None:
    response = TestClient(app).post(
        "/sources/1/article-preview",
        json={
            "raw_entry_id": 1,
            "fetch_full_content": True,
            "article_selector": None,
            "remove_selector": None,
            "url": "https://attacker.example/arbitrary",
        },
    )

    assert response.status_code == 422


def test_article_preview_returns_only_sanitized_output_without_writes(
    monkeypatch,
) -> None:
    with sessionmaker(bind=engine)() as session:
        source = Source(name="Preview", url="https://example.com/feed.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="entry",
            title="预览文章",
            url="https://example.com/articles/preview",
            raw_content="<p>RSS 摘要很短。</p>",
        )
        session.add(raw)
        session.commit()
        source_id = source.id
        raw_entry_id = raw.id

    html = (
        "<html><body><article><p>"
        + ("安全网页正文。" * 80)
        + "</p><div class='ad'>必须删除的广告</div>"
        + "<script>window.UNSAFE_PREVIEW = true</script></article></body></html>"
    )

    patch_article_html(monkeypatch, html)

    response = TestClient(app).post(
        f"/sources/{source_id}/article-preview",
        json={
            "raw_entry_id": raw_entry_id,
            "fetch_full_content": True,
            "article_selector": "css:article",
            "remove_selector": "css:.ad",
        },
    )

    assert response.status_code == 200
    preview = response.json()
    assert preview["body_source"] == "webpage"
    assert preview["method"] == "manual"
    assert preview["matched_elements"] == 1
    assert preview["removed_elements"] == 1
    assert preview["webpage_characters"] > preview["rss_characters"]
    assert "必须删除的广告" not in preview["reading_html"]
    assert "UNSAFE_PREVIEW" not in preview["reading_html"]
    assert "raw_html" not in preview
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count(RawEntry.id))) == 1
        assert session.scalar(select(func.count(Document.id))) == 0
        assert session.scalar(select(func.count(ContentItem.id))) == 0


def test_article_preview_explains_a_valid_manual_rule_quality_fallback(
    monkeypatch,
) -> None:
    with sessionmaker(bind=engine)() as session:
        source = Source(name="Preview", url="https://example.com/feed.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="entry",
            title="预览文章",
            url="https://example.com/articles/preview",
            raw_content="<p>" + ("RSS 正文。" * 90) + "</p>",
        )
        session.add(raw)
        session.commit()
        source_id = source.id
        raw_entry_id = raw.id

    html = (
        "<html><body><article><p>"
        + ("网页短文。" * 20)
        + "</p><div class='ad'>广告</div></article></body></html>"
    )

    patch_article_html(monkeypatch, html)

    response = TestClient(app).post(
        f"/sources/{source_id}/article-preview",
        json={
            "raw_entry_id": raw_entry_id,
            "fetch_full_content": True,
            "article_selector": "css:article",
            "remove_selector": "css:.ad",
        },
    )

    assert response.status_code == 200
    preview = response.json()
    assert preview["body_source"] == "rss"
    assert preview["diagnostics"] == ["manual_quality_rejected"]
    assert preview["webpage_characters"] > 0
    assert preview["matched_elements"] == 1
    assert preview["removed_elements"] == 1
    assert preview["fallback_reason"] == "手工正文未通过质量检查"


def test_article_preview_applies_a_remove_only_rule_to_public_extraction(
    monkeypatch,
) -> None:
    with sessionmaker(bind=engine)() as session:
        source = Source(name="Preview", url="https://example.com/feed.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="entry",
            title="预览文章",
            url="https://news.about.com/articles/preview",
            raw_content="<p>RSS 摘要。</p>",
        )
        session.add(raw)
        session.commit()
        source_id = source.id
        raw_entry_id = raw.id

    html = (
        "<html><body><div id='articlebody'><p>"
        + ("公共规则正文。" * 80)
        + "</p><div class='ad'>REMOVE_ONLY_MARKER</div></div></body></html>"
    )

    patch_article_html(monkeypatch, html)

    response = TestClient(app).post(
        f"/sources/{source_id}/article-preview",
        json={
            "raw_entry_id": raw_entry_id,
            "fetch_full_content": True,
            "article_selector": None,
            "remove_selector": "css:.ad",
        },
    )

    assert response.status_code == 200
    preview = response.json()
    assert preview["method"] == "fivefilters"
    assert preview["removed_elements"] == 1
    assert "REMOVE_ONLY_MARKER" not in preview["reading_html"]


def test_article_preview_reports_the_active_public_rule_snapshot() -> None:
    activated_at = "2026-07-30T10:00:00+00:00"
    commit = "a" * 40
    with sessionmaker(bind=engine)() as session:
        source = Source(name="Preview", url="https://example.com/feed.xml")
        session.add_all(
            [
                source,
                AppSetting(
                    key="fivefilters_rules_snapshot",
                    value=json.dumps(
                        {
                            "version": f"fivefilters@{commit}",
                            "rules": {},
                            "skipped": {},
                            "activated_at": activated_at,
                        }
                    ),
                ),
            ]
        )
        session.commit()
        source_id = source.id

    response = TestClient(app).get(f"/sources/{source_id}/article-preview")

    assert response.status_code == 200
    assert response.json()["public_rules"] == {
        "version": f"fivefilters@{commit}",
        "commit": commit,
        "activated_at": "2026-07-30T10:00:00Z",
        "bundled": False,
    }


def test_article_preview_uses_the_active_public_rule_snapshot(monkeypatch) -> None:
    commit = "b" * 40
    with sessionmaker(bind=engine)() as session:
        source = Source(name="Preview", url="https://example.com/feed.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="entry",
            title="预览文章",
            url="https://preview.example/articles/preview",
            raw_content="<p>RSS 摘要。</p>",
        )
        session.add_all(
            [
                raw,
                AppSetting(
                    key="fivefilters_rules_snapshot",
                    value=json.dumps(
                        {
                            "version": f"fivefilters@{commit}",
                            "rules": {
                                "preview.example": {
                                    "body": ["//main"],
                                    "strip": [],
                                    "strip_id_or_class": [],
                                }
                            },
                            "skipped": {},
                            "activated_at": "2026-07-30T10:00:00+00:00",
                        }
                    ),
                ),
            ]
        )
        session.commit()
        source_id = source.id
        raw_entry_id = raw.id

    html = "<html><body><main><p>" + ("激活规则正文。" * 80) + "</p></main></body></html>"

    patch_article_html(monkeypatch, html)

    response = TestClient(app).post(
        f"/sources/{source_id}/article-preview",
        json={
            "raw_entry_id": raw_entry_id,
            "fetch_full_content": True,
            "article_selector": None,
            "remove_selector": None,
        },
    )

    assert response.status_code == 200
    assert response.json()["method"] == "fivefilters"
    assert response.json()["version"] == f"fivefilters@{commit}"


def test_public_rule_update_checks_then_activates_an_explicit_commit(
    monkeypatch,
) -> None:
    old_commit = "a" * 40
    new_commit = "c" * 40
    with sessionmaker(bind=engine)() as session:
        source = Source(name="Preview", url="https://example.com/feed.xml")
        session.add(source)
        session.flush()
        session.add_all(
            [
                make_raw_entry(
                    source_id=source.id,
                    external_id="entry",
                    title="预览文章",
                    url="https://candidate.example/articles/preview",
                    raw_content="<p>RSS 摘要。</p>",
                ),
                AppSetting(
                    key="fivefilters_rules_snapshot",
                    value=json.dumps(
                        {
                            "version": f"fivefilters@{old_commit}",
                            "rules": {
                                "candidate.example": {
                                    "body": ["//article"],
                                    "strip": [],
                                    "strip_id_or_class": [],
                                }
                            },
                            "skipped": {},
                            "activated_at": "2026-07-30T10:00:00+00:00",
                        }
                    ),
                ),
            ]
        )
        session.commit()

    candidate_archive = fivefilters_archive(
        {
            "candidate.example.txt": (
                "title: //h1\n"
                "body: //main\n"
                "strip: //aside\n"
                "strip_id_or_class: advertisement\n"
            ),
            "blocked.example.txt": (
                "body: //article\n"
                "http_header(user-agent): Example\n"
            ),
        }
    )

    def read_url(url: str, _max_bytes: int) -> bytes:
        if "/commits/master" in url:
            return json.dumps({"sha": new_commit}).encode()
        assert url.endswith(new_commit)
        return candidate_archive

    html = (
        "<html><body><main><p>"
        + ("候选公共规则正文。" * 80)
        + "</p><aside>必须删除</aside>"
        + "<script>window.UNSAFE_CANDIDATE = true</script></main></body></html>"
    )

    monkeypatch.setattr("reader_api.public_rules._read_url", read_url)
    patch_article_html(monkeypatch, html)
    client = TestClient(app)

    checked = client.post("/article-rules/check")

    assert checked.status_code == 200
    check_payload = checked.json()
    preview = check_payload.pop("preview")
    assert check_payload == {
        "current_version": f"fivefilters@{old_commit}",
        "current_commit": old_commit,
        "candidate_version": f"fivefilters@{new_commit}",
        "candidate_commit": new_commit,
        "rules_count": 1,
        "skipped_count": 1,
        "subscribed_domains": 1,
        "covered_subscribed_domains": 1,
        "changed_subscribed_domains": 1,
        "tested_subscribed_domains": 1,
        "invalid_subscribed_domains": [],
        "failed_subscribed_domains": [],
        "passed": True,
        "can_activate": True,
    }
    assert preview["hostname"] == "candidate.example"
    assert preview["method"] == "fivefilters"
    assert preview["passed"] is True
    assert preview["adopted_webpage"] is True
    assert preview["matched_elements"] == 1
    assert preview["removed_elements"] == 1
    assert "必须删除" not in preview["reading_html"]
    assert "UNSAFE_CANDIDATE" not in preview["reading_html"]
    with sessionmaker(bind=engine)() as session:
        assert (
            json.loads(session.get(AppSetting, "fivefilters_rules_snapshot").value)[
                "version"
            ]
            == f"fivefilters@{old_commit}"
        )

    activated = client.post(
        "/article-rules/activate",
        json={"commit": new_commit},
    )

    assert activated.status_code == 200
    assert activated.json()["commit"] == new_commit
    assert activated.json()["bundled"] is False
    with sessionmaker(bind=engine)() as session:
        saved = json.loads(
            session.get(AppSetting, "fivefilters_rules_snapshot").value
        )
        assert saved["version"] == f"fivefilters@{new_commit}"
        assert saved["activated_at"]


def test_public_rule_extraction_failure_keeps_the_last_known_good_snapshot(
    monkeypatch,
) -> None:
    old_commit = "a" * 40
    new_commit = "e" * 40
    original = json.dumps(
        {
            "version": f"fivefilters@{old_commit}",
            "rules": {
                "candidate.example": {
                    "body": ["//article"],
                    "strip": [],
                    "strip_id_or_class": [],
                }
            },
            "skipped": {},
            "activated_at": "2026-07-30T10:00:00+00:00",
        }
    )
    with sessionmaker(bind=engine)() as session:
        source = Source(name="Preview", url="https://example.com/feed.xml")
        session.add(source)
        session.flush()
        session.add_all(
            [
                make_raw_entry(
                    source_id=source.id,
                    external_id="entry",
                    title="预览文章",
                    url="https://candidate.example/articles/preview",
                    raw_content="<p>RSS 摘要。</p>",
                ),
                AppSetting(
                    key="fivefilters_rules_snapshot",
                    value=original,
                ),
            ]
        )
        session.commit()

    archive = fivefilters_archive(
        {"candidate.example.txt": "body: //missing\n"}
    )

    def read_url(url: str, _max_bytes: int) -> bytes:
        if "/commits/master" in url:
            return json.dumps({"sha": new_commit}).encode()
        return archive

    html = (
        "<html><body><article><p>"
        + ("通用提取正文。" * 80)
        + "</p></article></body></html>"
    )

    monkeypatch.setattr("reader_api.public_rules._read_url", read_url)
    patch_article_html(monkeypatch, html)
    client = TestClient(app)

    checked = client.post("/article-rules/check")

    assert checked.status_code == 200
    assert checked.json()["passed"] is False
    assert checked.json()["can_activate"] is False
    assert checked.json()["failed_subscribed_domains"] == [
        "candidate.example"
    ]
    assert checked.json()["preview"]["method"] != "fivefilters"
    assert checked.json()["preview"]["reading_html"]

    activated = client.post(
        "/article-rules/activate",
        json={"commit": new_commit},
    )

    assert activated.status_code == 502
    with sessionmaker(bind=engine)() as session:
        assert session.get(AppSetting, "fivefilters_rules_snapshot").value == original


def test_public_rule_update_failure_keeps_the_last_known_good_snapshot(
    monkeypatch,
) -> None:
    old_commit = "a" * 40
    original = json.dumps(
        {
            "version": f"fivefilters@{old_commit}",
            "rules": {},
            "skipped": {},
            "activated_at": "2026-07-30T10:00:00+00:00",
        }
    )
    with sessionmaker(bind=engine)() as session:
        session.add(
            AppSetting(
                key="fivefilters_rules_snapshot",
                value=original,
            )
        )
        session.commit()

    monkeypatch.setattr(
        "reader_api.public_rules.urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("offline")),
    )

    response = TestClient(app).post(
        "/article-rules/activate",
        json={"commit": "d" * 40},
    )

    assert response.status_code == 502
    with sessionmaker(bind=engine)() as session:
        assert session.get(AppSetting, "fivefilters_rules_snapshot").value == original
