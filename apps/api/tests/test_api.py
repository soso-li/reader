import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from threading import Barrier, BrokenBarrierError
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient  # noqa: E402

from reader_api.db import Base, engine, prepare_runtime_database  # noqa: E402
import reader_api.main as main_module  # noqa: E402
from reader_api.main import app, fetch_favicon_bytes, fetch_icon_candidate, indexed_cluster_search_query, indexed_item_search_query, llm_text, local_llm, report_bounds, report_key, report_state_key, search_clause  # noqa: E402
from reader_api.ai_runtime import RuntimeAISettings, runtime_ai_settings  # noqa: E402
from reader_api.models import AppSetting, Cluster, ClusterItem, ClusteringRun, ContentEmbedding, ContentItem, Document, FeedMetric, FilterMatch, FilterRule, Folder, GenerationAdmission, GenerationApplication, GenerationAttempt, GenerationControl, GenerationRequest, GenerationRequestPayload, GenerationResult, GenerationRunnerPresence, InteractionEvent, LLMTask, RawEntry, Source, SourceEntryRelation, UserState  # noqa: E402
from reader_api.source_entry_relations import record_duplicate_feed_relations  # noqa: E402
from reader_api.digest import canonical_url, content_hash, lsh_signature, normalize_title  # noqa: E402
from reader_api.clustering_run import clustering_run  # noqa: E402
from reader_api.cluster import repair_exact_content_duplicates  # noqa: E402
from reader_api.discover import preview_json_entry  # noqa: E402
from reader_api.public_fetch import PublicFetchResult  # noqa: E402
from reader_api.config import Settings, settings  # noqa: E402
from reader_api.translations import TRANSLATION_CHUNK_CHAR_LIMIT, TRANSLATION_TASK_TYPE  # noqa: E402
from tests.factories import assign_publishable_cluster as assign_cluster, make_raw_entry  # noqa: E402
from sqlalchemy import create_engine, event, func, inspect, select, text  # noqa: E402
from sqlalchemy import insert  # noqa: E402
from sqlalchemy.dialects import postgresql  # noqa: E402
from sqlalchemy.exc import IntegrityError, OperationalError  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402
from sqlalchemy.pool import QueuePool  # noqa: E402


def user_state_snapshot() -> list[tuple[int, str, int, str, bool, bool, str]]:
    with sessionmaker(bind=engine)() as session:
        return [
            (
                state.id,
                state.object_type,
                state.object_id,
                state.read_status,
                state.read_later,
                state.starred,
                state.updated_at.isoformat(),
            )
            for state in session.scalars(select(UserState).order_by(UserState.id)).all()
        ]


def public_fetch_result(
    body: bytes,
    content_type: str,
    url: str,
) -> PublicFetchResult:
    return PublicFetchResult(True, body, content_type, url, "")


def set_cluster_event_state(
    client: TestClient,
    cluster: dict[str, object],
    *,
    operation_id: str,
    action: str,
    value: bool | str,
) -> dict[str, object]:
    response = client.post(
        "/event-user-state",
        json={
            "event_uid": cluster["event_uid"],
            "observed_revision_uid": cluster["current_revision_uid"],
            "operation_id": operation_id,
            "action": action,
            "value": value,
        },
    )
    assert response.status_code == 200
    return response.json()


def set_object_user_state(
    client: TestClient,
    object_type: str,
    object_id: int,
    *,
    operation_id: str,
    **set_value: object,
) -> dict[str, object]:
    response = client.patch(
        f"/user-state/{object_type}/{object_id}",
        json={"operation_id": operation_id, **set_value},
    )
    assert response.status_code == 200
    return response.json()


def reject_inline_reclustering(*_: object, **__: object) -> int:
    raise AssertionError("Cluster 生成/读取路径不得同步重聚类")


def enable_generation(session) -> None:
    session.add(
        GenerationControl(
            id=1,
            global_pause=False,
            auto_run=False,
            daily_budget_tokens=10_000_000,
            input_estimator="unicode-codepoints-v1",
            output_reserve_tokens=0,
            day_timezone="Asia/Shanghai",
        )
    )


def test_report_openapi_keeps_report_source_component_name() -> None:
    schemas = TestClient(app).get("/openapi.json").json()["components"]["schemas"]

    assert schemas["ReportCitationOut"]["properties"]["sources"]["items"]["$ref"] == "#/components/schemas/ReportSourceOut"
    assert "ReportSourceOut" in schemas


def create_short_cluster_fixture(*, item_status: str, generated: bool = False) -> tuple[int, int, int]:
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="Short RSS Feed", url="https://example.com/rss.xml", fetch_full_content=True)
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="1", title="Short RSS", content_hash=content_hash("Short RSS"))
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title="Short RSS", content_text="One sentence summary.")
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title="Short RSS",
            summary="One sentence summary.",
            content_text="One sentence summary.",
            url="https://example.com/full",
            content_hash=content_hash("Short RSS"),
            embedding_vector="[1.0,0.0]",
            embedding_model="old-model",
        )
        session.add(item)
        session.flush()
        cluster = assign_cluster(session, item)
        if generated:
            cluster.generated_summary = "旧摘要"
            cluster.generated_content = "旧短合成"
        fixed_updated_at = datetime(2026, 6, 1, tzinfo=timezone.utc)
        session.add_all(
            [
                UserState(
                    object_type="item",
                    object_id=item.id,
                    read_status=item_status,
                    read_later=True,
                    updated_at=fixed_updated_at,
                ),
            ]
        )
        session.commit()
        return cluster.id, item.id, document.id


def test_folder_source_and_state_api() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    folder = client.post("/folders", json={"name": "Tech", "media_type": "article"}).json()
    same_article_folder = client.post(
        "/folders", json={"name": "Tech", "media_type": "article"}
    ).json()
    same_name_video_folder = client.post(
        "/folders", json={"name": "Tech", "media_type": "video"}
    ).json()
    normalized_article_folder = client.post(
        "/folders", json={"name": "Articles / Science", "media_type": "article"}
    ).json()
    same_normalized_article_folder = client.post(
        "/folders", json={"name": "Science", "media_type": "article"}
    ).json()
    assert same_article_folder["id"] == folder["id"]
    assert same_name_video_folder["id"] != folder["id"]
    assert normalized_article_folder["name"] == "Science"
    assert same_normalized_article_folder["id"] == normalized_article_folder["id"]
    assert client.patch(
        f"/folders/{folder['id']}", json={"media_type": "video"}
    ).status_code == 422
    source = client.post(
        "/sources",
        json={"name": "Example", "url": "https://example.com/rss.xml", "folder_id": folder["id"]},
    ).json()
    created_trial = client.post(
        "/sources",
        json={"name": "Trial New", "url": "https://example.com/trial-new.xml", "status": "trial"},
    ).json()
    video_source = client.post(
        "/sources",
        json={"name": "Video Feed", "url": "https://example.com/video.xml", "media_type": "video"},
    ).json()

    assert source["folder_id"] == folder["id"]
    assert folder["media_type"] == "article"
    assert source["status_changed_at"]
    assert source["fetch_full_content"] is False
    assert created_trial["status"] == "trial"
    assert video_source["media_type"] == "video"
    same_type_patch = client.patch(
        f"/sources/{source['id']}", json={"media_type": "article"}
    ).json()
    assert same_type_patch["folder_id"] == folder["id"]
    assert client.patch(f"/sources/{video_source['id']}", json={"media_type": "image"}).json()["media_type"] == "image"
    assert client.patch(f"/sources/{source['id']}", json={"fetch_full_content": True}).json()["fetch_full_content"] is True
    assert client.post("/sources", json={"name": "Bad Media", "url": "https://example.com/bad-media.xml", "media_type": "book"}).status_code == 400
    assert client.patch(f"/sources/{video_source['id']}", json={"media_type": "book"}).status_code == 400
    assert client.post("/sources", json={"name": "Wrong Folder", "url": "https://example.com/wrong-folder.xml", "media_type": "video", "folder_id": folder["id"]}).status_code == 400
    changed_type = client.patch(f"/sources/{source['id']}", json={"media_type": "video"}).json()
    assert changed_type["media_type"] == "video"
    assert changed_type["folder_id"] is None
    assert client.patch(f"/sources/{source['id']}", json={"media_type": "article", "folder_id": folder["id"]}).status_code == 200
    assert any(item["name"] == "Tech" for item in client.get("/folders").json())
    assert client.get("/sources").json()[0]["name"] == "Example"
    renamed_folder = client.patch(f"/folders/{folder['id']}", json={"name": "Articles / Tech Renamed"}).json()
    assert renamed_folder["name"] == "Tech Renamed"

    moved_folder = client.post("/folders", json={"name": "AI", "media_type": "article"}).json()
    with sessionmaker(bind=engine)() as session:
        cached_source = session.get(Source, source["id"])
        assert cached_source is not None
        cached_source.fetch_etag = '"old-endpoint"'
        cached_source.fetch_last_modified = "Tue, 28 Jul 2026 12:00:00 GMT"
        cached_source.last_successful_payload_hash = "a" * 64
        session.commit()
    edited = client.patch(
        f"/sources/{source['id']}",
        json={"name": "Edited", "url": "https://example.com/edited.xml", "folder_id": moved_folder["id"]},
    ).json()
    assert edited["name"] == "Edited"
    assert edited["url"] == "https://example.com/edited.xml"
    assert edited["folder_id"] == moved_folder["id"]
    with sessionmaker(bind=engine)() as session:
        cached_source = session.get(Source, source["id"])
        assert cached_source is not None
        assert cached_source.fetch_etag is None
        assert cached_source.fetch_last_modified is None
        assert cached_source.last_successful_payload_hash is None
    duplicate = client.post("/sources", json={"name": "Duplicate", "url": "https://example.com/duplicate.xml"}).json()
    assert client.patch(f"/sources/{duplicate['id']}", json={"url": "https://example.com/edited.xml"}).status_code == 400
    assert client.post("/sources", json={"name": "Duplicate Case", "url": "HTTPS://EXAMPLE.COM/edited.xml#rss"}).json()["id"] == source["id"]
    assert client.post("/sources", json={"name": "Blank", "url": "   "}).status_code == 400
    assert client.post("/sources", json={"name": "Local File", "url": "file:///tmp/feed.xml"}).status_code == 400
    assert client.post("/sources", json={"name": "Credentials", "url": "https://reader:secret@example.com/feed.xml"}).status_code == 400
    assert client.post("/sources", json={"name": "Invalid port", "url": "https://example.com:not-a-port/feed.xml"}).status_code == 400
    assert client.patch(f"/sources/{source['id']}", json={"url": "javascript:alert(1)"}).status_code == 400
    assert client.post("/sources", json={"name": "Missing Folder", "url": "https://example.com/missing.xml", "folder_id": 9999}).status_code == 400
    assert client.patch(f"/sources/{source['id']}", json={"folder_id": 9999}).status_code == 400
    assert client.post("/sources", json={"name": "Duplicate Spaced", "url": " https://example.com/edited.xml "}).json()["id"] == source["id"]
    with sessionmaker(bind=engine)() as session:
        legacy = Source(name="Legacy", url="HTTPS://LEGACY.EXAMPLE.COM/feed.xml#old")
        session.add(legacy)
        session.commit()
        legacy_id = legacy.id
    legacy_response = client.post("/sources", json={"name": "Legacy Duplicate", "url": "https://legacy.example.com/feed.xml"}).json()
    assert legacy_response["id"] == legacy_id
    assert legacy_response["url"] == "https://legacy.example.com/feed.xml"

    assert client.delete(f"/folders/{moved_folder['id']}").status_code == 409
    cleared = client.patch(f"/sources/{source['id']}", json={"folder_id": None}).json()
    assert cleared["folder_id"] is None
    assert client.delete(f"/folders/{moved_folder['id']}").status_code == 204
    deleted_folder = client.post(
        "/folders", json={"name": "Delete after source", "media_type": "article"}
    ).json()
    deleted_folder_source = client.post(
        "/sources",
        json={
            "name": "Delete folder source",
            "url": "https://example.com/delete-folder-source.xml",
            "folder_id": deleted_folder["id"],
        },
    ).json()
    assert client.delete(f"/sources/{deleted_folder_source['id']}").status_code == 204
    assert client.delete(f"/folders/{deleted_folder['id']}").status_code == 204
    paused = client.patch(f"/sources/{source['id']}", json={"enabled": False}).json()
    assert paused["enabled"] is False
    trial = client.patch(f"/sources/{source['id']}", json={"status": "trial"}).json()
    assert trial["status"] == "trial"
    archived_by_status = client.patch(f"/sources/{source['id']}", json={"status": "archived"}).json()
    assert archived_by_status["status"] == "archived"
    assert archived_by_status["enabled"] is False
    archived_stays_paused = client.patch(f"/sources/{source['id']}", json={"status": "archived", "enabled": True}).json()
    assert archived_stays_paused["enabled"] is False
    patched_active = client.patch(f"/sources/{source['id']}", json={"status": "active"}).json()
    assert patched_active["status"] == "active"
    assert patched_active["enabled"] is True
    patched_archived = client.patch(f"/sources/{source['id']}", json={"status": "archived"}).json()
    assert patched_archived["enabled"] is False
    assert client.delete(f"/sources/{source['id']}").status_code == 204
    replacement = client.post("/sources", json={"name": "Readd", "url": "https://example.com/edited.xml", "status": "active"}).json()
    assert replacement["id"] != source["id"]
    assert replacement["name"] == "Readd"
    assert replacement["status"] == "active"
    assert replacement["enabled"] is True
    created_archived = client.post("/sources", json={"name": "Archived New", "url": "https://example.com/archived-new.xml", "status": "archived"}).json()
    assert created_archived["status"] == "archived"
    assert created_archived["enabled"] is False


def test_source_api_persists_article_selectors() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    source = client.post(
        "/sources",
        json={
            "name": "Selector source",
            "url": "https://example.com/selector.xml",
            "article_selector": "article.content",
            "remove_selector": "css:.advertisement",
        },
    ).json()

    assert source["article_selector"] == "article.content"
    assert source["remove_selector"] == "css:.advertisement"
    edited = client.patch(
        f"/sources/{source['id']}",
        json={"article_selector": "xpath://main/article"},
    ).json()
    assert edited["article_selector"] == "xpath://main/article"
    invalid = client.patch(
        f"/sources/{source['id']}",
        json={"article_selector": "xpath://article/@href"},
    )
    assert invalid.status_code == 422


@pytest.mark.parametrize(
    ("field", "selector"),
    [
        ("article_selector", "css:article["),
        ("remove_selector", "xpath://article["),
        ("article_selector", "xpath:string(//article)"),
        ("article_selector", "xpath://article/text()"),
        ("article_selector", "xpath://article/@href"),
        ("article_selector", "xpath://article/@href/."),
        ("article_selector", "xpath://comment()"),
        (
            "remove_selector",
            'xpath://article[re:test(string(.), "正文")]',
        ),
    ],
)
def test_source_api_rejects_invalid_article_selectors(
    field: str, selector: str
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    response = client.post(
        "/sources",
        json={
            "name": "Invalid selector",
            "url": f"https://example.com/{field}.xml",
            field: selector,
        },
    )

    assert response.status_code == 422


def test_source_privacy_policy_defaults_and_atomic_updates() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    first = client.post(
        "/sources",
        json={"name": "Public candidate", "url": "https://example.com/public.xml"},
    ).json()
    second = client.post(
        "/sources",
        json={"name": "Private candidate", "url": "https://example.com/private.xml"},
    ).json()

    assert first["privacy_class"] == "unclassified"
    assert first["external_generation_allowed"] is False

    allowed = client.patch(
        f"/sources/{first['id']}",
        json={"privacy_class": "public", "external_generation_allowed": True},
    )
    assert allowed.status_code == 200
    assert allowed.json()["privacy_class"] == "public"
    assert allowed.json()["external_generation_allowed"] is True

    tightened = client.patch(
        f"/sources/{first['id']}", json={"privacy_class": "private"}
    )
    assert tightened.status_code == 200
    assert tightened.json()["privacy_class"] == "private"
    assert tightened.json()["external_generation_allowed"] is False

    invalid = client.patch(
        f"/sources/{first['id']}", json={"external_generation_allowed": True}
    )
    assert invalid.status_code == 400
    assert invalid.json()["detail"] == "只有公开来源才能允许发送给外部生成服务"

    bulk = client.post(
        "/sources/bulk",
        json={
            "ids": [first["id"], second["id"]],
            "set": {
                "privacy_class": "public",
                "external_generation_allowed": True,
            },
        },
    )
    assert bulk.status_code == 200
    assert bulk.json() == {"updated": 2}
    rows = {row["id"]: row for row in client.get("/sources").json()}
    assert {
        (rows[first["id"]]["privacy_class"], rows[first["id"]]["external_generation_allowed"]),
        (rows[second["id"]]["privacy_class"], rows[second["id"]]["external_generation_allowed"]),
    } == {("public", True)}

    tightened_bulk = client.post(
        "/sources/bulk",
        json={
            "ids": [first["id"], second["id"]],
            "set": {"privacy_class": "private"},
        },
    )
    assert tightened_bulk.status_code == 200
    rows = {row["id"]: row for row in client.get("/sources").json()}
    assert all(row["privacy_class"] == "private" for row in rows.values())
    assert not any(row["external_generation_allowed"] for row in rows.values())

    assert client.delete(f"/sources/{first['id']}").status_code == 204
    replacement = client.post(
        "/sources",
        json={
            "name": "Public candidate restored",
            "url": "https://example.com/public.xml",
            "status": "active",
        },
    )
    assert replacement.status_code == 200
    assert replacement.json()["id"] != first["id"]
    assert replacement.json()["privacy_class"] == "unclassified"
    assert replacement.json()["external_generation_allowed"] is False

    with sessionmaker(bind=engine)() as session:
        assert session.query(LLMTask).count() == 0
        assert session.query(GenerationRequest).count() == 0



def test_sources_bulk_updates_management_fields_and_status_side_effects(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    calls: list[tuple[str, int]] = []
    monkeypatch.setattr("reader_api.main.cluster_source_items", lambda session, source_id: calls.append(("cluster", source_id)))
    monkeypatch.setattr("reader_api.main.decluster_source_items", lambda session, source_id: calls.append(("decluster", source_id)))
    client = TestClient(app)
    folder = client.post("/folders", json={"name": "Bulk", "media_type": "article"}).json()
    video_folder = client.post(
        "/folders", json={"name": "Bulk video", "media_type": "video"}
    ).json()
    first = client.post("/sources", json={"name": "One", "url": "https://example.com/one.xml"}).json()
    second = client.post("/sources", json={"name": "Two", "url": "https://example.com/two.xml"}).json()

    assert client.post("/sources/bulk", json={"ids": [first["id"], second["id"]], "set": {"folder_id": folder["id"], "media_type": "article", "enabled": False}}).json() == {"updated": 2}
    rows = {source["id"]: source for source in client.get("/sources").json()}
    assert rows[first["id"]]["folder_id"] == folder["id"]
    assert rows[second["id"]]["enabled"] is False
    assert client.post(
        "/sources/bulk",
        json={
            "ids": [first["id"], second["id"]],
            "set": {"folder_id": folder["id"], "media_type": "video"},
        },
    ).status_code == 400
    unchanged = {source["id"]: source for source in client.get("/sources").json()}
    assert all(unchanged[source_id]["media_type"] == "article" for source_id in (first["id"], second["id"]))
    assert all(unchanged[source_id]["folder_id"] == folder["id"] for source_id in (first["id"], second["id"]))
    assert client.post(
        "/sources/bulk",
        json={"ids": [first["id"], second["id"]], "set": {"media_type": "video"}},
    ).json() == {"updated": 2}
    video_rows = {source["id"]: source for source in client.get("/sources").json()}
    assert all(video_rows[source_id]["media_type"] == "video" for source_id in (first["id"], second["id"]))
    assert all(video_rows[source_id]["folder_id"] is None for source_id in (first["id"], second["id"]))
    assert client.post(
        "/sources/bulk",
        json={
            "ids": [first["id"], second["id"]],
            "set": {"folder_id": video_folder["id"], "media_type": "video"},
        },
    ).json() == {"updated": 2}
    assert client.post(
        "/sources/bulk",
        json={
            "ids": [first["id"], second["id"]],
            "set": {"folder_id": folder["id"], "media_type": "article"},
        },
    ).json() == {"updated": 2}

    assert client.post("/sources/bulk", json={"ids": [first["id"]], "set": {"status": "trial"}}).json() == {"updated": 1}
    trial = next(source for source in client.get("/sources").json() if source["id"] == first["id"])
    assert trial["status"] == "trial"
    assert ("decluster", first["id"]) in calls

    assert client.post("/sources/bulk", json={"ids": [first["id"]], "set": {"status": "archived", "enabled": True}}).json() == {"updated": 1}
    archived = next(source for source in client.get("/sources").json() if source["id"] == first["id"])
    assert archived["enabled"] is False
    assert client.post("/sources/bulk", json={"ids": [first["id"]], "set": {"status": "active"}}).json() == {"updated": 1}
    assert ("cluster", first["id"]) in calls
    assert client.post("/sources/bulk", json={"ids": [first["id"]], "set": {"status": "bad"}}).status_code == 400
    assert client.post("/sources/bulk", json={"ids": [9999], "set": {"enabled": False}}).status_code == 404

    social = client.post(
        "/sources",
        json={
            "name": "Social transition",
            "url": "https://example.com/social-transition.xml",
            "media_type": "social",
            "status": "active",
        },
    ).json()
    calls.clear()

    transitioned = client.patch(
        f"/sources/{social['id']}",
        json={"media_type": "article", "status": "muted"},
    ).json()

    assert transitioned["media_type"] == "article"
    assert transitioned["status"] == "muted"
    assert calls == [("decluster", social["id"])]


def test_bulk_source_updates_reject_oversized_sql_parameter_lists() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    response = TestClient(app).post(
        "/sources/bulk",
        json={"ids": list(range(1, 10_002)), "set": {"enabled": False}},
    )

    assert response.status_code == 422


def test_combined_source_patch_does_not_persist_intermediate_clustering_run() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(
            name="Combined transition",
            url="https://example.com/combined-transition.xml",
            media_type="social",
            status="active",
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="combined-transition-item",
            title="Combined transition item",
            content_hash=content_hash("Combined transition item"),
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            title=raw.title,
            content_text="Combined transition body",
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_text=document.content_text,
            content_hash=content_hash(raw.title, document.content_text),
        )
        session.add(item)
        session.commit()
        source_id = source.id
        item_id = item.id

    response = TestClient(app).patch(
        f"/sources/{source_id}",
        json={"media_type": "article", "status": "muted"},
    )

    assert response.status_code == 200
    with Session() as session:
        assert session.scalar(
            select(func.count()).select_from(ClusterItem).where(
                ClusterItem.content_item_id == item_id
            )
        ) == 0
        assert session.scalar(select(func.count()).select_from(ClusteringRun)) == 0


def test_source_patch_acquires_execution_lock_before_url_uniqueness_query(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    source = TestClient(app).post(
        "/sources",
        json={
            "name": "Lock order",
            "url": "https://example.com/lock-order.xml",
            "media_type": "social",
        },
    ).json()
    lock_active = False

    @contextmanager
    def tracked_lock(_session):
        nonlocal lock_active
        assert lock_active is False
        lock_active = True
        try:
            yield
        finally:
            lock_active = False

    def source_by_url_inside_lock(*_args, **_kwargs):
        assert lock_active is True
        return None

    def defer_inside_lock(_session):
        assert lock_active is True

    monkeypatch.setattr(
        "reader_api.main.clustering_run_execution_lock",
        tracked_lock,
    )
    monkeypatch.setattr(
        "reader_api.main.defer_clustering_run_lock_until_transaction_end",
        defer_inside_lock,
    )
    monkeypatch.setattr("reader_api.main.source_by_url", source_by_url_inside_lock)
    monkeypatch.setattr("reader_api.main.cluster_source_items", lambda *_args: None)

    response = TestClient(app).patch(
        f"/sources/{source['id']}",
        json={
            "url": "https://example.com/lock-order-new.xml",
            "media_type": "article",
        },
    )

    assert response.status_code == 200


def test_source_pause_skips_clustering_execution_lock_for_single_and_bulk(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    sources = [
        client.post(
            "/sources",
            json={
                "name": f"Pause {index}",
                "url": f"https://example.com/pause-{index}.xml",
            },
        ).json()
        for index in (1, 2)
    ]

    @contextmanager
    def fail_if_locked(_session):
        raise AssertionError("纯暂停不应申请聚类执行锁")
        yield

    monkeypatch.setattr(
        "reader_api.main.clustering_run_execution_lock",
        fail_if_locked,
    )

    assert client.patch(
        f"/sources/{sources[0]['id']}", json={"enabled": False}
    ).status_code == 200
    assert client.post(
        "/sources/bulk",
        json={"ids": [sources[1]["id"]], "set": {"enabled": False}},
    ).status_code == 200


def test_sources_discover_reads_a_direct_feed_and_returns_six_read_only_previews(monkeypatch) -> None:
    entries = "".join(
        f"""
        <item>
          <title>Entry {index}</title>
          <link>https://example.com/posts/{index}</link>
          <description><![CDATA[<p>Summary {index}</p>]]></description>
          <pubDate>Mon, 01 Jun 2026 10:0{index}:00 +0000</pubDate>
          <enclosure url="https://example.com/media/{index}.mp3" type="audio/mpeg" />
        </item>
        """
        for index in range(7)
    ).encode()

    def fake_fetch(url: str, **_kwargs: object) -> PublicFetchResult:
        if url == "https://example.com/":
            return public_fetch_result(
                b"""<!doctype html><html><head><title>Example Site</title><link rel="alternate" type="application/rss+xml" title="Main Feed" href="/feed.xml"></head></html>""",
                "text/html",
                url,
            )
        if url == "https://example.com/feed.xml":
            return public_fetch_result(
                b"<?xml version='1.0'?><rss><channel><title>Main Feed</title><link>javascript:alert(1)</link>"
                + entries
                + b"</channel></rss>",
                "application/rss+xml",
                url,
            )
        return public_fetch_result(
            b"<html><head><title>No Feed</title></head></html>",
            "text/html",
            url,
        )

    monkeypatch.setattr("reader_api.discover.fetch_public_bytes", fake_fetch)
    client = TestClient(app)

    direct = client.post("/sources/discover", json={"url": "https://example.com/feed.xml"}).json()
    assert direct["title"] == "Main Feed"
    assert direct["site_url"] == "https://example.com/feed.xml"
    assert direct["candidates"] == [{"title": "Main Feed", "url": "https://example.com/feed.xml"}]
    assert len(direct["entries"]) == 6
    assert [entry["title"] for entry in direct["entries"]] == [
        "Entry 6",
        "Entry 5",
        "Entry 4",
        "Entry 3",
        "Entry 2",
        "Entry 1",
    ]
    assert direct["entries"][0] == {
        "title": "Entry 6",
        "summary": "Summary 6",
        "image_url": "",
        "media_url": "https://example.com/media/6.mp3",
        "media_kind": "audio",
        "media_duration": 0,
        "url": "https://example.com/posts/6",
        "published_at": "2026-06-01T10:06:00Z",
    }
    rejected_page = client.post("/sources/discover", json={"url": "https://example.com/"})
    assert rejected_page.status_code == 400
    assert rejected_page.json()["detail"] == "请输入直接可访问的 RSS、Atom、JSON Feed 或 Newsletter Feed 地址"
    assert client.post("/sources/discover", json={"url": "https://example.com/no-feed"}).status_code == 400


def test_sources_discover_rejects_private_targets_before_request(monkeypatch) -> None:
    monkeypatch.setattr(settings, "rsshub_base_url", "")
    response = TestClient(app).post(
        "/sources/discover",
        json={"url": "http://127.0.0.1/feed.xml"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "无法读取 Feed，请检查地址和网络连接"


def test_sources_discover_passes_the_configured_rsshub_origin(monkeypatch) -> None:
    calls: list[tuple[str, str, int]] = []

    def fake_fetch(url: str, **kwargs: object) -> PublicFetchResult:
        calls.append((url, str(kwargs["trusted_origin"]), int(kwargs["max_bytes"])))
        return public_fetch_result(
            b"<?xml version='1.0'?><rss><channel><title>Claude Blog</title></channel></rss>",
            "application/rss+xml",
            url,
        )

    monkeypatch.setattr(settings, "rsshub_base_url", "http://192.0.2.6:1200")
    monkeypatch.setattr("reader_api.discover.fetch_public_bytes", fake_fetch)

    response = TestClient(app).post(
        "/sources/discover",
        json={"url": "http://192.0.2.6:1200/claude/blog"},
    )

    assert response.status_code == 200
    assert calls == [
        (
            "http://192.0.2.6:1200/claude/blog",
            "http://192.0.2.6:1200",
            10 * 1024 * 1024,
        )
    ]


def test_sources_discover_supports_atom_and_json_feed_without_followup_requests(monkeypatch) -> None:
    calls: list[str] = []
    atom = b"""<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"><title>Atom Feed</title><link href="https://example.com"/><entry><title>Old dated entry</title><updated>2025-01-01T00:00:00Z</updated></entry><entry><title>Undated entry</title></entry><entry><title>Newest dated entry</title><link href="https://example.com/posts/newest"/><summary type="html">&lt;p&gt;Newest&lt;img src="/images/newest.jpg" /&gt;&lt;/p&gt;</summary><content type="html">&lt;img src="https://example.com/images/second.jpg" /&gt;</content><updated>2026-01-01T00:00:00Z</updated></entry></feed>"""
    json_feed = json.dumps(
        {
            "version": "https://jsonfeed.org/version/1.1",
            "title": "JSON Feed",
            "home_page_url": "https://example.org",
            "items": [None] * 6
            + [
                {
                    "id": "1",
                    "title": "JSON entry",
                    "content_html": "<p>JSON summary <img src=\"https://example.org/images/html.jpg\"></p>",
                    "content_text": "![later image](https://example.org/images/text.jpg)",
                    "url": "https://example.org/entry",
                    "attachments": [
                        {
                            "url": "https://example.org/audio.mp3",
                            "mime_type": "audio/mpeg",
                            "duration_in_seconds": 12,
                        }
                    ],
                }
            ],
        }
    ).encode()

    def fake_fetch(url: str, **_kwargs: object) -> PublicFetchResult:
        calls.append(url)
        if url.endswith("atom.xml"):
            return public_fetch_result(atom, "application/atom+xml", url)
        if url.endswith("timeout.xml"):
            return PublicFetchResult(False, b"", "", url, "request_failed")
        if url.endswith("large.xml"):
            return PublicFetchResult(False, b"", "", url, "body_too_large")
        if url.endswith("not-a-feed.json"):
            return public_fetch_result(b'{"items": []}', "application/json", url)
        return public_fetch_result(json_feed, "application/feed+json", url)

    monkeypatch.setattr("reader_api.discover.fetch_public_bytes", fake_fetch)
    client = TestClient(app)
    atom_body = client.post("/sources/discover", json={"url": "https://example.com/atom.xml"}).json()
    json_body = client.post("/sources/discover", json={"url": "https://example.org/feed.json"}).json()

    assert [entry["title"] for entry in atom_body["entries"]] == [
        "Newest dated entry",
        "Old dated entry",
        "Undated entry",
    ]
    assert atom_body["entries"][0]["media_url"] == ""
    assert atom_body["entries"][0]["media_kind"] == ""
    assert atom_body["entries"][0]["image_url"] == "https://example.com/images/newest.jpg"
    assert json_body["entries"][0]["title"] == "JSON entry"
    assert json_body["entries"][0]["image_url"] == "https://example.org/images/html.jpg"
    assert json_body["entries"][0]["media_url"] == "https://example.org/audio.mp3"
    assert json_body["entries"][0]["media_duration"] == 12
    assert calls == ["https://example.com/atom.xml", "https://example.org/feed.json"]
    invalid_json = client.post(
        "/sources/discover", json={"url": "https://example.org/not-a-feed.json"}
    )
    assert invalid_json.status_code == 400
    assert invalid_json.json()["detail"] == "Feed 内容无法解析，请确认这是有效的 Feed 地址"
    timeout = client.post(
        "/sources/discover", json={"url": "https://example.org/timeout.xml"}
    )
    assert timeout.status_code == 400
    assert timeout.json()["detail"] == "无法读取 Feed，请检查地址和网络连接"
    too_large = client.post(
        "/sources/discover", json={"url": "https://example.org/large.xml"}
    )
    assert too_large.status_code == 400
    assert too_large.json()["detail"] == "Feed 响应超过 10 MiB 限制"
    assert client.post("/sources/discover", json={"url": "rsshub:/twitter/user/DIYgod"}).status_code == 400


def test_json_feed_explicit_image_stays_ahead_of_content_image_fallback() -> None:
    preview = preview_json_entry(
        {
            "title": "Explicit image wins",
            "url": "https://example.org/entry",
            "image": "https://example.org/images/explicit.jpg",
            "content_html": "<img src=\"https://example.org/images/content.jpg\">",
            "content_text": "![later](https://example.org/images/text.jpg)",
        }
    )

    assert preview.image_url == "https://example.org/images/explicit.jpg"


def test_sources_discover_does_not_write_reader_or_task_records(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    tables = (
        Source,
        RawEntry,
        Document,
        ContentItem,
        LLMTask,
        GenerationRequest,
        GenerationRequestPayload,
        GenerationAttempt,
        GenerationResult,
        GenerationApplication,
    )
    with sessionmaker(bind=engine)() as session:
        before = {model.__tablename__: session.scalar(select(func.count()).select_from(model)) for model in tables}

    monkeypatch.setattr(
        "reader_api.discover.fetch_public_bytes",
        lambda url, **_kwargs: public_fetch_result(
            b"<?xml version='1.0'?><rss><channel><title>Read only</title><item><title>Preview</title></item></channel></rss>",
            "application/rss+xml",
            url,
        ),
    )
    response = TestClient(app).post(
        "/sources/discover", json={"url": "https://example.com/read-only.xml"}
    )
    assert response.status_code == 200

    with sessionmaker(bind=engine)() as session:
        after = {model.__tablename__: session.scalar(select(func.count()).select_from(model)) for model in tables}
    assert after == before


def test_resource_names_reject_values_larger_than_postgres_columns() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    folder = client.post(
        "/folders", json={"name": "Folder", "media_type": "article"}
    ).json()
    source = client.post(
        "/sources", json={"name": "Source", "url": "https://example.com/name.xml"}
    ).json()
    topic = client.post(
        "/topics", json={"name": "Topic", "query": "topic"}
    ).json()

    assert client.post(
        "/folders", json={"name": "f" * 241, "media_type": "article"}
    ).status_code == 422
    assert client.patch(
        f"/folders/{folder['id']}", json={"name": "f" * 241}
    ).status_code == 422
    assert client.post(
        "/sources",
        json={"name": "s" * 321, "url": "https://example.com/too-long.xml"},
    ).status_code == 422
    assert client.patch(
        f"/sources/{source['id']}", json={"name": "s" * 321}
    ).status_code == 422
    assert client.post(
        "/topics", json={"name": "t" * 241, "query": "topic"}
    ).status_code == 422
    assert client.patch(
        f"/topics/{topic['id']}", json={"name": "t" * 241}
    ).status_code == 422


def test_sources_and_folders_use_natural_ascii_first_sort() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    for index, name in enumerate(["中文", "10 Feed", "2 Feed", "Beta", "alpha"], 1):
        client.post("/folders", json={"name": name, "media_type": "article"})
        client.post("/sources", json={"name": name, "url": f"https://example.com/{index}.xml"})

    assert [folder["name"] for folder in client.get("/folders").json()] == ["2 Feed", "10 Feed", "alpha", "Beta", "中文"]
    assert [source["name"] for source in client.get("/sources").json()] == ["2 Feed", "10 Feed", "alpha", "Beta", "中文"]


def test_item_image_url_skips_placeholder_and_falls_back_to_raw_rss() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    with sessionmaker(bind=engine)() as session:
        source = Source(name="图片源", url="https://example.com/feed.xml", site_url="https://example.com/site")
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="image-1",
            title="图片测试",
            raw_content='<p><img src="https://cdn.example.com/rss.jpg" alt="RSS 图"></p>',
            content_hash=content_hash("图片测试"),
        )
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title="图片测试", content_text="正文\n![占位](https://example.com/t.png)")
        session.add(document)
        session.flush()
        session.add(
            ContentItem(
                document_id=document.id,
                source_id=source.id,
                title="图片测试",
                summary="图片测试",
                content_text=document.content_text,
                content_hash=content_hash("图片测试"),
            )
        )
        session.commit()

    item = client.get("/items", params={"include_content": False}).json()[0]

    assert item["image_url"] == "https://cdn.example.com/rss.jpg"
    assert item["source_site_url"] == "https://example.com/site"


def test_item_out_includes_media_fields_and_image_media_fallback() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    with sessionmaker(bind=engine)() as session:
        source = Source(name="图片源", url="https://example.com/feed.xml", media_type="image")
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="image-media", title="图片媒体", content_hash=content_hash("图片媒体"))
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title="图片媒体", content_text="图片媒体")
        session.add(document)
        session.flush()
        session.add(
            ContentItem(
                document_id=document.id,
                source_id=source.id,
                title="图片媒体",
                summary="图片媒体",
                content_text="图片媒体",
                content_hash=content_hash("图片媒体"),
                media_url="https://cdn.example.com/photo.jpg",
                media_kind="image",
                media_duration=0,
            )
        )
        session.commit()

    item = client.get("/items", params={"media_type": "image", "include_content": False}).json()[0]

    assert item["image_url"] == "https://cdn.example.com/photo.jpg"
    assert item["media_url"] == "https://cdn.example.com/photo.jpg"
    assert item["media_kind"] == "image"
    assert item["media_duration"] == 0


def test_video_item_image_url_uses_youtube_thumbnail_fallback() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    with sessionmaker(bind=engine)() as session:
        source = Source(name="视频源", url="https://example.com/video.xml", media_type="video")
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="youtube-video", title="YouTube 视频", content_hash=content_hash("YouTube 视频"))
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title="YouTube 视频", content_text="YouTube 视频")
        session.add(document)
        session.flush()
        session.add(
            ContentItem(
                document_id=document.id,
                source_id=source.id,
                title="YouTube 视频",
                summary="YouTube 视频",
                content_text="YouTube 视频",
                content_hash=content_hash("YouTube 视频"),
                url="https://www.youtube.com/watch?v=cE_5j_Wtj88",
                media_url="https://www.youtube.com/v/cE_5j_Wtj88?version=3",
                media_kind="video",
            )
        )
        session.commit()

    item = client.get("/items", params={"media_type": "video", "include_content": False}).json()[0]

    assert item["image_url"] == "https://i.ytimg.com/vi/cE_5j_Wtj88/hqdefault.jpg"


def test_item_detail_does_not_refresh_short_body_without_source_opt_in(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fetched: list[str] = []
    monkeypatch.setattr("reader_api.rss.fetch_article_text", lambda url: fetched.append(url) or ("Full article " * 80))

    with sessionmaker(bind=engine)() as session:
        source = Source(name="短源", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="1", title="短文", url="https://example.com/story", content_hash=content_hash("短文"))
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title="短文", content_text="短摘要")
        session.add(document)
        session.flush()
        item = ContentItem(document_id=document.id, source_id=source.id, title="短文", summary="短摘要", content_text="短摘要", url=raw.url, content_hash=content_hash("短文"))
        session.add(item)
        session.commit()
        item_id = item.id

    body = TestClient(app).get(f"/items/{item_id}").json()

    assert fetched == []
    assert body["content_text"] == "短摘要"


def test_bulk_mark_read_updates_current_item_and_cluster_scope() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    folder = client.post("/folders", json={"name": "AI", "media_type": "article"}).json()
    source_a = client.post("/sources", json={"name": "A", "url": "https://example.com/a.xml", "folder_id": folder["id"]}).json()
    source_b = client.post("/sources", json={"name": "B", "url": "https://example.com/b.xml"}).json()

    with sessionmaker(bind=engine)() as session:
        ids: list[int] = []
        for source_id, title in [(source_a["id"], "one"), (source_a["id"], "two"), (source_b["id"], "three")]:
            raw = make_raw_entry(source_id=source_id, external_id=title, title=title, raw_content=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            item = ContentItem(document_id=doc.id, source_id=source_id, title=title, summary=title, content_text=title, content_hash=content_hash(title))
            session.add(item)
            session.flush()
            ids.append(item.id)
        with clustering_run(
            session,
            scope_type="bulk-read-test",
            item_ids=[ids[2]],
            rule_version="bulk-read-test-v1",
        ):
            cluster = assign_cluster(session, session.get(ContentItem, ids[2]))
        session.commit()
        cluster_id = cluster.id

    prepared_items = client.post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "item", "folder_id": folder["id"]},
    ).json()
    assert prepared_items["target_count"] == 2
    assert client.post(
        "/user-state/bulk-read",
        json={"batch_id": prepared_items["batch_id"]},
    ).json() == {"updated": 2}
    assert client.get("/sources").json()[0]["unread_count"] == 0
    assert client.get("/sources").json()[1]["unread_count"] == 1

    prepared_clusters = client.post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "event", "source_id": source_b["id"]},
    ).json()
    assert prepared_clusters["target_count"] == 1
    assert client.post(
        "/user-state/bulk-read",
        json={"batch_id": prepared_clusters["batch_id"]},
    ).json() == {"updated": 1}
    assert client.get(f"/clusters/{cluster_id}").json()["read_status"] == "summary_seen"


def test_bulk_mark_read_media_type_scope_is_item_level() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        article_source = Source(name="Article Feed", url="https://example.com/article.xml", media_type="article")
        social_source = Source(name="Social Feed", url="https://example.com/social.xml", media_type="social")
        notification_source = Source(name="GitHub Releases", url="https://example.com/releases.xml", media_type="notification")
        session.add_all([article_source, social_source, notification_source])
        session.flush()
        for source, titles in [(article_source, ["Article item"]), (social_source, ["Social one", "Social two"]), (notification_source, ["Release one"])]:
            for title in titles:
                raw = make_raw_entry(source_id=source.id, external_id=title, title=title, raw_content=title, content_hash=content_hash(title))
                session.add(raw)
                session.flush()
                doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
                session.add(doc)
                session.flush()
                item = ContentItem(document_id=doc.id, source_id=source.id, title=title, summary=title, content_text=title, content_hash=content_hash(title))
                session.add(item)
                session.flush()
                if source.media_type == "article":
                    assign_cluster(session, item)
        session.commit()

    client = TestClient(app)
    prepared = client.post(
        "/user-state/bulk-read/prepare",
        json={"object_type": "item", "media_type": "social"},
    ).json()
    assert client.post(
        "/user-state/bulk-read",
        json={"batch_id": prepared["batch_id"]},
    ).json() == {"updated": 2}
    assert client.get("/items", params={"media_type": "social", "read_status": "unread"}).json() == []
    assert [item["title"] for item in client.get("/items", params={"media_type": "notification", "read_status": "unread"}).json()] == ["Release one"]
    assert [cluster["title"] for cluster in client.get("/clusters", params={"read_status": "unread"}).json()] == ["Article item"]


def test_source_counts_use_unread_clusters_and_dedupe_folder_totals() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    folder = client.post("/folders", json={"name": "Tech", "media_type": "article"}).json()
    source_a = client.post("/sources", json={"name": "A", "url": "https://example.com/a.xml", "folder_id": folder["id"]}).json()
    source_b = client.post("/sources", json={"name": "B", "url": "https://example.com/b.xml", "folder_id": folder["id"]}).json()

    with sessionmaker(bind=engine)() as session:
        item_ids: list[int] = []
        for source_id, title in [(source_a["id"], "same story A"), (source_b["id"], "same story B")]:
            raw = make_raw_entry(source_id=source_id, external_id=title, title=title, raw_content=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            item = ContentItem(document_id=doc.id, source_id=source_id, title=title, summary=title, content_text=title, content_hash=content_hash(title))
            session.add(item)
            session.flush()
            item_ids.append(item.id)
        cluster = Cluster(cluster_key="same-story", title="same story")
        session.add(cluster)
        session.flush()
        with clustering_run(
            session,
            scope_type="source-count-event-state-test",
            item_ids=item_ids,
            rule_version="source-count-event-state-test-v1",
        ):
            session.add_all([ClusterItem(cluster_id=cluster.id, content_item_id=item_id) for item_id in item_ids])
            session.flush()
        session.commit()
        cluster_id = cluster.id

    sources = client.get("/sources").json()
    assert [(source["name"], source["unread_count"], source["folder_unread_count"], source["all_unread_count"]) for source in sources] == [("A", 1, 1, 1), ("B", 1, 1, 1)]

    selects: list[str] = []

    def capture_selects(_connection, _cursor, statement, _parameters, _context, _executemany) -> None:
        if statement.lstrip().upper().startswith(("SELECT", "WITH")):
            selects.append(statement)

    event.listen(engine, "before_cursor_execute", capture_selects)
    try:
        response = client.get("/sources/navigation")
        assert response.status_code == 200, response.text
        lightweight = response.json()
    finally:
        event.remove(engine, "before_cursor_execute", capture_selects)

    assert len(selects) == 1
    assert [(source["name"], source["unread_count"], source["folder_unread_count"], source["all_unread_count"]) for source in lightweight] == [("A", 1, 1, 1), ("B", 1, 1, 1)]
    assert set(lightweight[0]) == {
        "id", "folder_id", "name", "url", "site_url", "media_type", "status",
        "enabled", "unread_count", "folder_unread_count", "all_unread_count",
        "starred_count", "last_fetched_at", "last_error",
    }

    cluster = client.get(f"/clusters/{cluster_id}").json()
    set_cluster_event_state(
        client,
        cluster,
        operation_id="90120000-0000-4000-8000-000000000000",
        action="read_status_set",
        value="summary_seen",
    )
    sources = client.get("/sources").json()
    assert [(source["name"], source["unread_count"], source["folder_unread_count"], source["all_unread_count"]) for source in sources] == [("A", 0, 0, 0), ("B", 0, 0, 0)]


def test_source_unread_counts_match_explicit_article_and_media_views() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        article_source = Source(
            name="Filtered Article",
            url="https://example.com/filtered.xml",
            media_type="article",
        )
        video_source = Source(
            name="Video Queue",
            url="https://example.com/video.xml",
            media_type="video",
        )
        session.add_all([article_source, video_source])
        session.flush()

        def add_item(source: Source, title: str) -> ContentItem:
            raw = make_raw_entry(
                source_id=source.id,
                external_id=title,
                title=title,
                raw_content=title,
                content_hash=content_hash(title),
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                title=title,
                content_text=title,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                content_hash=content_hash(title),
            )
            session.add(item)
            session.flush()
            return item

        article_item = add_item(article_source, "Filtered article")
        with clustering_run(
            session,
            scope_type="source-unread-count-test",
            item_ids=[article_item.id],
            rule_version="source-unread-count-test-v1",
        ):
            article_cluster = assign_cluster(session, article_item)
        video_filtered = add_item(video_source, "Filtered video")
        video_seen = add_item(video_source, "Seen video")
        video_uninterested = add_item(video_source, "Uninteresting video")
        article_rule = FilterRule(
            source_id=article_source.id,
            match_type="literal",
            pattern="Filtered article",
            enabled=True,
        )
        video_rule = FilterRule(
            source_id=video_source.id,
            match_type="literal",
            pattern="Filtered video",
            enabled=True,
        )
        session.add_all([article_rule, video_rule])
        session.flush()
        session.add_all([
            FilterMatch(rule_id=article_rule.id, content_item_id=article_item.id),
            FilterMatch(rule_id=video_rule.id, content_item_id=video_filtered.id),
            UserState(
                object_type="item",
                object_id=video_seen.id,
                read_status="summary_seen",
            ),
        ])
        session.commit()
        article_source_id = article_source.id
        video_source_id = video_source.id
        article_cluster_id = article_cluster.id
        video_uninterested_id = video_uninterested.id

    client = TestClient(app)
    assert client.post(
        "/uninterested",
        json={
            "operation_id": "90600000-0000-4000-8000-000000000000",
            "target_type": "item",
            "item_id": video_uninterested_id,
            "value": True,
            "reason": "topic",
        },
    ).status_code == 200

    sources = {source["id"]: source for source in client.get("/sources/navigation").json()}
    assert sources[article_source_id]["unread_count"] == 1
    assert sources[article_source_id]["folder_unread_count"] == 0
    assert sources[article_source_id]["all_unread_count"] == 0
    assert sources[video_source_id]["unread_count"] == 1
    assert client.get(
        "/clusters/count",
        params={"source_id": article_source_id, "read_status": "unread"},
    ).json() == {"count": 1}
    assert len(client.get(
        "/items",
        params={"source_id": video_source_id, "read_status": "unread"},
    ).json()) == 1

    cluster = client.get(f"/clusters/{article_cluster_id}").json()
    assert client.post(
        "/uninterested",
        json={
            "operation_id": "90610000-0000-4000-8000-000000000000",
            "target_type": "event",
            "event_uid": cluster["event_uid"],
            "observed_revision_uid": cluster["current_revision_uid"],
            "value": True,
            "reason": "topic",
        },
    ).status_code == 200
    sources = {source["id"]: source for source in client.get("/sources/navigation").json()}
    assert sources[article_source_id]["unread_count"] == 0


def test_cluster_detail_and_preview_items_are_oldest_first() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    with sessionmaker(bind=engine)() as session:
        source = Source(name="Source", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        cluster = Cluster(cluster_key="ordered-cluster", title="Cluster")
        session.add(cluster)
        session.flush()
        for title, published_at in [
            ("newer", datetime(2026, 7, 1, 10, tzinfo=timezone.utc)),
            ("older", datetime(2026, 7, 1, 8, tzinfo=timezone.utc)),
        ]:
            raw = make_raw_entry(source_id=source.id, external_id=title, title=title, raw_content=title, published_at=published_at, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            document = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(document)
            session.flush()
            item = ContentItem(document_id=document.id, source_id=source.id, title=title, summary=title, content_text=title, published_at=published_at, content_hash=content_hash(title))
            session.add(item)
            session.flush()
            session.add(ClusterItem(cluster_id=cluster.id, content_item_id=item.id))
        cluster.first_seen_at = datetime(2026, 7, 1, 8, tzinfo=timezone.utc)
        session.commit()
        cluster_id = cluster.id

    assert [item["title"] for item in client.get(f"/clusters/{cluster_id}").json()["items"]] == ["older", "newer"]
    assert [item["title"] for item in client.get("/clusters").json()[0]["items"]] == ["older", "newer"]


def test_prepare_runtime_database_creates_complete_isolated_sqlite_schema() -> None:
    Base.metadata.drop_all(engine)

    prepare_runtime_database()

    with engine.begin() as connection:
        assert set(inspect(connection).get_table_names()) == set(Base.metadata.tables)


def test_postgres_content_item_insert_does_not_cast_halfvec_null_to_varchar() -> None:
    stmt = insert(ContentItem).values(
        document_id=1,
        source_id=1,
        title="title",
        summary="",
        content_text="body",
        url="",
        content_hash="hash",
        canonical_url="",
        normalized_title="title",
        lsh_signature="",
        embedding_vector=None,
        embedding_model="",
        cluster_score=0.0,
    )

    compiled = str(stmt.compile(dialect=postgresql.dialect()))

    assert "embedding_vector)s::VARCHAR" not in compiled


def test_llm_text_prefers_message_output() -> None:
    assert (
        llm_text(
            {
                "output": [
                    {"type": "reasoning", "content": "non-message output"},
                    {"type": "message", "content": '{"answer":"可展示回答"}'},
                ]
            }
        )
        == '{"answer":"可展示回答"}'
    )


def test_ai_settings_exposes_local_provider_timeout() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    body = TestClient(app).get("/ai/settings").json()

    assert body["translation_provider"] == "local"
    assert body["translation_api_key_configured"] is False
    assert "translation_api_key" not in body
    assert body["endpoint"] == "http://127.0.0.1:1234/api/v1/chat"
    assert body["translation_endpoint"] == "http://127.0.0.1:1234/api/v1/chat"
    assert body["embedding_endpoint"] == "http://127.0.0.1:1234/v1/embeddings"
    assert body["llm_model"] == "qwen/qwen3.5-9b"
    assert body["translation_model"] == "hy-mt2-1.8b"
    assert body["embedding_model"] == "text-embedding-qwen3-embedding-4b"
    assert body["timeout_seconds"] == 240.0


def test_deployment_settings_reject_model_names_longer_than_storage_boundary() -> None:
    values = {
        "DATABASE_URL": "sqlite:///./reader-test.db",
        "LLM_MODEL": "m" * 121,
    }

    with pytest.raises(ValueError):
        Settings(**values)

    values["LLM_MODEL"] = "m" * 120
    configured = Settings(**values)
    assert len(configured.llm_model) == 120


def test_runtime_ai_settings_ignores_historical_oversized_model_names(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    monkeypatch.setattr("reader_api.ai_runtime.settings.llm_model", "safe-fallback")
    with sessionmaker(bind=engine)() as session:
        session.add_all(
            [
                AppSetting(key="llm_model", value="l" * 121),
                AppSetting(key="synthesis_remote_model", value="s" * 121),
            ]
        )
        session.commit()

        configured = runtime_ai_settings(session)

    assert configured.llm_model == "safe-fallback"
    assert configured.synthesis_remote_model == ""


def test_ai_settings_does_not_accept_translation_key_from_environment(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    monkeypatch.setenv("TRANSLATION_API_KEY", "environment-secret")

    body = TestClient(app).get("/ai/settings").json()

    assert "translation_api_key" not in Settings.model_fields
    assert body["translation_api_key_configured"] is False
    assert "environment-secret" not in str(body)


def test_runtime_ai_settings_fails_closed_when_setting_store_is_unavailable() -> None:
    class FailingSession:
        info: dict[str, object] = {}

        @staticmethod
        def scalars(*_: object) -> None:
            raise OperationalError("SELECT app_settings", {}, RuntimeError("database unavailable"))

    with pytest.raises(OperationalError):
        runtime_ai_settings(FailingSession())  # type: ignore[arg-type]


def test_database_errors_hide_secret_setting_parameters() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    secret = "database-error-secret"
    with engine.begin() as connection:
        connection.execute(insert(AppSetting), {"key": "translation_api_key", "value": "first"})
        with pytest.raises(IntegrityError) as error:
            connection.execute(insert(AppSetting), {"key": "translation_api_key", "value": secret})

    assert secret not in str(error.value)
    assert "SQL parameters hidden" in str(error.value)


def test_ai_settings_cloud_translation_key_is_write_only_and_can_be_cleared() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    saved = client.patch(
        "/ai/settings",
        json={
            "translation_provider": "openai_compatible",
            "translation_base_url": "https://cloud.example/v1",
            "translation_model": "cloud-model",
            "translation_api_key": "cloud-secret",
        },
    )

    assert saved.status_code == 200
    assert saved.json()["translation_provider"] == "openai_compatible"
    assert saved.json()["translation_endpoint"] == "https://cloud.example/v1/chat/completions"
    assert saved.json()["translation_api_key_configured"] is True
    assert "translation_api_key" not in saved.json()

    preserved = client.patch("/ai/settings", json={"translation_model": "cloud-model-2", "translation_api_key": ""})
    assert preserved.status_code == 200
    assert preserved.json()["translation_api_key_configured"] is True

    fetched = client.get("/ai/settings").json()
    assert fetched["translation_api_key_configured"] is True
    assert "cloud-secret" not in str(fetched)

    cleared = client.patch("/ai/settings", json={"translation_provider": "local", "clear_translation_api_key": True})
    assert cleared.status_code == 200
    assert cleared.json()["translation_api_key_configured"] is False


def test_ai_settings_keeps_remote_synthesis_separate_and_write_only() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    saved = client.patch(
        "/ai/settings",
        json={
            "task_provider": "openai_compatible",
            "synthesis_provider": " OpenAI_Compatible ",
            "synthesis_remote_base_url": "https://synthesis.example/v1",
            "synthesis_remote_model": "synthesis-model",
            "synthesis_remote_api_key": "synthesis-secret",
        },
    )

    assert saved.status_code == 200
    body = saved.json()
    assert body["task_provider"] == "openai_compatible"
    assert body["synthesis_provider"] == "openai_compatible"
    assert (
        body["synthesis_remote_endpoint"]
        == "https://synthesis.example/v1/chat/completions"
    )
    assert body["synthesis_remote_model"] == "synthesis-model"
    assert body["synthesis_remote_api_key_configured"] is True
    assert body["translation_provider"] == "local"
    assert "synthesis_remote_api_key" not in body
    assert "synthesis-secret" not in str(body)
    with sessionmaker(bind=engine)() as session:
        task_provider = session.scalar(
            select(AppSetting.value).where(AppSetting.key == "llm_task_provider")
        )
        synthesis_provider = session.scalar(
            select(AppSetting.value).where(AppSetting.key == "synthesis_provider")
        )
    assert task_provider == "openai_compatible"
    assert synthesis_provider == "openai_compatible"

    cleared = client.patch(
        "/ai/settings", json={"clear_synthesis_remote_api_key": True}
    )
    assert cleared.status_code == 200
    assert cleared.json()["task_provider"] == "openai_compatible"
    assert cleared.json()["synthesis_provider"] == "openai_compatible"
    assert cleared.json()["synthesis_remote_api_key_configured"] is False


def test_cleared_remote_synthesis_key_does_not_block_local_provider() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    remote_settings = {
        "synthesis_remote_base_url": "https://synthesis.example/v1",
        "synthesis_remote_model": "synthesis-model",
    }
    saved = client.patch(
        "/ai/settings",
        json={
            "synthesis_provider": "openai_compatible",
            **remote_settings,
            "synthesis_remote_api_key": "synthesis-secret",
        },
    )
    assert saved.status_code == 200
    cleared = client.patch(
        "/ai/settings", json={"clear_synthesis_remote_api_key": True}
    )
    assert cleared.status_code == 200

    switched = client.patch(
        "/ai/settings",
        json={
            "synthesis_provider": "local",
            **remote_settings,
            "synthesis_remote_api_key": "",
        },
    )

    assert switched.status_code == 200
    assert switched.json()["synthesis_provider"] == "local"
    assert switched.json()["synthesis_remote_api_key_configured"] is False


@pytest.mark.parametrize(
    ("payload", "field"),
    [
        ({"llm_model": "m" * 121}, "llm_model"),
        ({"translation_model": "m" * 121}, "translation_model"),
        ({"embedding_model": "m" * 121}, "embedding_model"),
        (
            {
                "synthesis_provider": "openai_compatible",
                "synthesis_remote_base_url": "https://synthesis.example/v1",
                "synthesis_remote_model": "m" * 121,
                "synthesis_remote_api_key": "synthesis-secret",
            },
            "synthesis_remote_model",
        ),
    ],
)
def test_ai_settings_rejects_model_names_longer_than_storage_boundary(
    payload: dict[str, str], field: str
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    response = TestClient(app).patch("/ai/settings", json=payload)

    assert response.status_code == 400
    assert response.json()["detail"] == f"{field} 不能超过 120 个字符"


def test_ai_settings_rejects_retired_legacy_provider() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    response = TestClient(app).patch(
        "/ai/settings", json={"task_provider": "legacy"}
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "生成任务只支持本地模型或远端兼容接口"


def test_synthesis_provider_can_change_to_local_without_remote_settings() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    response = TestClient(app).patch(
        "/ai/settings",
        json={
            "synthesis_provider": "local",
            "synthesis_remote_base_url": "",
            "synthesis_remote_model": "",
            "synthesis_remote_api_key": "",
        },
    )

    assert response.status_code == 200
    assert response.json()["synthesis_provider"] == "local"


def test_clearing_synthesis_key_still_rejects_an_insecure_remote_address() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    saved = client.patch(
        "/ai/settings",
        json={
            "synthesis_provider": "openai_compatible",
            "synthesis_remote_base_url": "https://synthesis.example/v1",
            "synthesis_remote_model": "synthesis-model",
            "synthesis_remote_api_key": "synthesis-secret",
        },
    )
    assert saved.status_code == 200

    response = client.patch(
        "/ai/settings",
        json={
            "clear_synthesis_remote_api_key": True,
            "synthesis_remote_base_url": "http://synthesis.example/v1",
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "云端合成地址必须使用 https://"


def test_synthesis_provider_defaults_to_the_existing_task_provider(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(
        "reader_api.ai_runtime.settings.llm_task_provider", "openai_compatible"
    )

    body = TestClient(app).get("/ai/settings").json()

    assert body["task_provider"] == "openai_compatible"
    assert body["synthesis_provider"] == "openai_compatible"


def test_ai_settings_rejects_header_unsafe_cloud_key_without_echoing_it() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    unsafe_key = "cloud-secret\nAuthorization: Bearer leaked-secret"

    response = client.patch(
        "/ai/settings",
        json={
            "translation_provider": "openai_compatible",
            "translation_base_url": "https://cloud.example/v1",
            "translation_model": "cloud-model",
            "translation_api_key": unsafe_key,
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "云端 API Key 格式无效"
    assert unsafe_key not in response.text
    assert "leaked-secret" not in response.text


def test_ai_settings_restores_saved_local_profile_after_cloud_translation() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    local = client.patch(
        "/ai/settings",
        json={
            "translation_provider": "local",
            "translation_base_url": "http://translate.local:1234",
            "translation_model": "local-translation-model",
        },
    )
    cloud = client.patch(
        "/ai/settings",
        json={
            "translation_provider": "openai_compatible",
            "translation_base_url": "https://cloud.example/v1",
            "translation_model": "cloud-model",
            "translation_api_key": "cloud-secret",
        },
    )
    restored = client.patch(
        "/ai/settings",
        json={"translation_provider": "local", "clear_translation_api_key": True},
    )

    assert local.status_code == 200
    assert cloud.status_code == 200
    assert restored.status_code == 200
    assert restored.json()["translation_provider"] == "local"
    assert restored.json()["translation_base_url"] == "http://translate.local:1234"
    assert restored.json()["translation_model"] == "local-translation-model"
    assert restored.json()["translation_endpoint"] == "http://translate.local:1234/api/v1/chat"
    assert restored.json()["translation_api_key_configured"] is False


def test_ai_settings_cloud_translation_requires_https_and_key() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    missing_key = client.patch(
        "/ai/settings",
        json={"translation_provider": "openai_compatible", "translation_base_url": "https://cloud.example", "translation_model": "cloud-model"},
    )
    insecure = client.patch(
        "/ai/settings",
        json={
            "translation_provider": "openai_compatible",
            "translation_base_url": "http://cloud.example",
            "translation_model": "cloud-model",
            "translation_api_key": "secret",
        },
    )

    assert missing_key.status_code == 400
    assert missing_key.json()["detail"] == "云端翻译需要 API Key"
    assert insecure.status_code == 400
    assert insecure.json()["detail"] == "云端翻译地址必须使用 https://"


@pytest.mark.parametrize(
    "payload",
    [
        {"base_url": "http://user:pass@model.example"},
        {"embedding_base_url": "http://model.example?token=secret"},
        {
            "translation_provider": "openai_compatible",
            "translation_base_url": "https://user:pass@translate.example",
            "translation_model": "cloud-model",
            "translation_api_key": "secret",
        },
        {
            "synthesis_remote_base_url": "https://model.example/v1#secret",
            "synthesis_remote_model": "cloud-model",
            "synthesis_remote_api_key": "secret",
        },
    ],
)
def test_ai_settings_reject_model_urls_that_can_leak_credentials(
    payload: dict[str, str],
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    response = TestClient(app).patch("/ai/settings", json=payload)

    assert response.status_code == 400
    assert response.json()["detail"] == (
        "模型地址必须是有效且不含用户名、密码、查询参数或片段的 URL"
    )


def test_translation_ai_chat_uses_selected_cloud_provider(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    captured: dict[str, object] = {}

    class CloudResponse:
        def __enter__(self) -> "CloudResponse":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        @staticmethod
        def read(size: int = -1) -> bytes:
            body = b'{"choices":[{"message":{"content":"cloud translation"}}]}'
            return body if size < 0 else body[:size]

    def fake_urlopen(request, timeout: float) -> CloudResponse:
        captured.update(
            url=request.full_url,
            timeout=timeout,
            api_key=request.get_header("Authorization"),
            body=json.loads(request.data.decode("utf-8")),
        )
        return CloudResponse()

    monkeypatch.setattr("reader_api.llm.urlopen", fake_urlopen)
    client = TestClient(app)
    assert client.patch(
        "/ai/settings",
        json={
            "translation_provider": "openai_compatible",
            "translation_base_url": "https://cloud.example/v1",
            "translation_model": "cloud-model",
            "translation_api_key": "cloud-secret",
            "timeout_seconds": 19,
        },
    ).status_code == 200

    response = client.post("/ai/chat", json={"model_type": "translation", "system_prompt": "translate", "input": "hello"})

    assert response.status_code == 200
    assert response.json()["model"] == "cloud-model"
    assert response.json()["provider"] == "openai_compatible"
    assert response.json()["endpoint"] == "https://cloud.example/v1/chat/completions"
    assert captured == {
        "url": "https://cloud.example/v1/chat/completions",
        "timeout": 19.0,
        "api_key": "Bearer cloud-secret",
        "body": {
            "model": "cloud-model",
            "messages": [
                {"role": "system", "content": "translate"},
                {"role": "user", "content": "hello"},
            ],
        },
    }


def test_translation_cache_isolated_by_provider_and_source_privacy(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    calls: list[str] = []

    class TranslationResponse:
        def __init__(self, payload: bytes) -> None:
            self.payload = payload

        def __enter__(self) -> "TranslationResponse":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def read(self, size: int = -1) -> bytes:
            return self.payload if size < 0 else self.payload[:size]

    def fake_urlopen(request, timeout: float) -> TranslationResponse:
        if request.full_url.endswith("/api/v1/chat"):
            calls.append("local")
            return TranslationResponse(b'{"text":"local translation"}')
        calls.append("openai_compatible")
        return TranslationResponse(b'{"choices":[{"message":{"content":"cloud translation"}}]}')

    monkeypatch.setattr("reader_api.llm.urlopen", fake_urlopen)
    client = TestClient(app)
    text_value = "Apple announced a new product for developers and customers worldwide."
    private_text = "Private research contains confidential details for internal review."
    with sessionmaker(bind=engine)() as session:
        public_source = Source(
            name="Public Feed",
            url="https://example.com/public.xml",
            privacy_class="public",
            external_generation_allowed=True,
        )
        private_source = Source(
            name="Private Feed",
            url="https://example.com/private.xml",
            privacy_class="private",
        )
        session.add_all([public_source, private_source])
        session.flush()
        public_source_id = public_source.id
        private_source_id = private_source.id
        session.commit()

    local = client.post("/translations", json={"text": text_value})
    assert local.json()["translation"] == "local translation"
    assert client.patch(
        "/ai/settings",
        json={
            "translation_provider": "openai_compatible",
            "translation_base_url": "https://cloud.example",
            "translation_model": "hy-mt2-1.8b",
            "translation_api_key": "cloud-secret",
        },
    ).status_code == 200

    cloud = client.post(
        "/translations", json={"text": text_value, "source_id": public_source_id}
    )
    private = client.post(
        "/translations", json={"text": private_text, "source_id": private_source_id}
    )

    assert cloud.json()["translation"] == "cloud translation"
    assert private.json()["translation"] == "local translation"
    assert calls == ["local", "openai_compatible", "local"]
    with sessionmaker(bind=engine)() as session:
        providers = session.scalars(
            select(LLMTask.provider).where(LLMTask.task_type == TRANSLATION_TASK_TYPE).order_by(LLMTask.id)
        ).all()
    assert providers == ["local", "openai_compatible", "local"]


def test_ai_settings_patch_persists_separate_model_channels(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    captured: dict[str, object] = {}

    class FakeProvider:
        def __init__(self, base_url: str, timeout: float) -> None:
            captured["base_url"] = base_url
            captured["timeout"] = timeout

        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            captured["model"] = model
            return {"text": "ok"}

    client = TestClient(app)
    response = client.patch(
        "/ai/settings",
        json={
            "task_provider": "local",
            "base_url": "http://llm.local:1234",
            "translation_base_url": "http://translate.local:1234",
            "embedding_base_url": "http://embed.local:1234",
            "llm_model": "llm-model",
            "translation_model": "translation-model",
            "embedding_model": "embedding-model",
            "timeout_seconds": 12,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["endpoint"] == "http://llm.local:1234/api/v1/chat"
    assert body["translation_endpoint"] == "http://translate.local:1234/api/v1/chat"
    assert body["embedding_endpoint"] == "http://embed.local:1234/v1/embeddings"

    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", FakeProvider)
    chat = client.post("/ai/chat", json={"model_type": "translation", "input": "blue"}).json()

    with sessionmaker(bind=engine)() as session:
        session.query(AppSetting).delete()
        session.commit()

    assert chat["model"] == "translation-model"
    assert captured == {"base_url": "http://translate.local:1234", "timeout": 12.0, "model": "translation-model"}


@pytest.mark.parametrize(
    "text",
    [
        "这是一段中文正文，不需要翻译。",
        "2026 世界电容大会・全球演讲嘉宾招募中",
        "三星半导体负责人 구자흠 表示，新产品将于下月发布。",
        "Apple、Google 与 OpenAI 发布更新，详情见 https://example.com/news",
        "example.com/news",
    ],
)
def test_translation_skips_chinese_without_model_call(monkeypatch, text: str) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", lambda *_: (_ for _ in ()).throw(AssertionError("should not call model")))

    response = TestClient(app).post("/translations", json={"text": text})

    assert response.status_code == 200
    assert response.json()["status"] == "skipped"


def test_translation_uses_translation_model_and_cache(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    calls: list[tuple[str, str, str]] = []

    class FakeProvider:
        def __init__(self, base_url: str, timeout: float) -> None:
            calls.append(("init", base_url, str(timeout)))

        @staticmethod
        def chat(model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            calls.append((model, system_prompt, input_text))
            return {"text": "苹果与 Epic 的案件将由最高法院审理。"}

    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", FakeProvider)
    client = TestClient(app)
    text_value = "Apple filed an appeal after being found in contempt of an order related to App Store fees."

    first = client.post("/translations", json={"text": text_value}).json()
    second = client.post("/translations", json={"text": text_value}).json()

    assert first["status"] == "ready"
    assert first["translation"] == "苹果与 Epic 的案件将由最高法院审理。"
    assert second["translation"] == first["translation"]
    assert [call[0] for call in calls].count("hy-mt2-1.8b") == 1
    assert "{{title_prompt}}{{summary_prompt}}{{terms_prompt}}" in calls[1][1]
    assert calls[1][2] == (
        "Translate to Simplified Chinese (output translation only):\n\n"
        f"{text_value}"
    )

    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="English Feed", url="https://example.com/english.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="english-1", title=text_value, content_hash=content_hash(text_value))
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title=text_value, summary=text_value, content_text=text_value)
        session.add(document)
        session.flush()
        session.add(
            ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=text_value,
                summary=text_value,
                content_text=text_value,
                content_hash=content_hash(text_value, "item"),
                normalized_title=normalize_title(text_value),
            )
        )
        session.commit()

    item = client.get("/items").json()[0]
    assert item["reading_translation_needed"] is True
    assert item["title_translation"] == "苹果与 Epic 的案件将由最高法院审理。"
    assert item["summary_translation"] == "苹果与 Epic 的案件将由最高法院审理。"
    assert item["content_translation"] == "苹果与 Epic 的案件将由最高法院审理。"


def test_translation_maps_rich_text_blocks_and_reuses_the_cache(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    calls: list[tuple[str, str]] = []
    blocks = [
        {
            "id": "block-aaaaaaaaaaaaaaaa",
            "text": "OpenAI released a new report.",
        },
        {
            "id": "block-bbbbbbbbbbbbbbbb",
            "text": "Revenue grew this quarter.",
        },
    ]

    class FakeProvider:
        def __init__(self, base_url: str, timeout: float) -> None:
            pass

        @staticmethod
        def chat(
            model: str, system_prompt: str, input_text: str
        ) -> dict[str, object]:
            calls.append((system_prompt, input_text))
            assert system_prompt == """You are a professional Simplified Chinese native translator who needs to fluently translate text into Simplified Chinese.

## Translation Rules
1. Output only the translated content, without explanations or additional content (such as \"Here's the translation:\" or \"Translation as follows:\")
2. The returned translation must maintain exactly the same number of paragraphs and format as the original text
3. If the text contains HTML tags, consider where the tags should be placed in the translation while maintaining fluency
4. For content that should not be translated (such as proper nouns, code, etc.), keep the original text.
5. If input contains %%, use %% in your output, if input has no %%, don't use %% in your output{{title_prompt}}{{summary_prompt}}{{terms_prompt}}

## OUTPUT FORMAT:
- **Single paragraph input** → Output translation directly (no separators, no extra text)
- **Multi-paragraph input** → Use %% as paragraph separator between translations

## Examples
### Multi-paragraph Input:
Paragraph A

%%

Paragraph B

%%

Paragraph C

%%

Paragraph D

### Multi-paragraph Output:
Translation A

%%

Translation B

%%

Translation C

%%

Translation D

### Single paragraph Input:
Single paragraph content

### Single paragraph Output:
Direct translation without separators

{{imt_style_guide}}"""
            assert input_text == """Translate to Simplified Chinese:

OpenAI released a new report.

%%

Revenue grew this quarter."""
            return {"text": "OpenAI 发布了一份新报告。\n\n%%\n\n本季度收入增长。"}

    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", FakeProvider)
    client = TestClient(app)

    first = client.post("/translations", json={"blocks": blocks})
    second = client.post("/translations", json={"blocks": blocks})

    assert first.status_code == 200
    assert first.json()["blocks"] == [
        {
            "id": "block-aaaaaaaaaaaaaaaa",
            "text": "OpenAI 发布了一份新报告。",
        },
        {
            "id": "block-bbbbbbbbbbbbbbbb",
            "text": "本季度收入增长。",
        },
    ]
    assert first.json()["translation"] == "OpenAI 发布了一份新报告。\n\n本季度收入增长。"
    assert second.json() == first.json()
    assert len(calls) == 1


def test_translation_repairs_invalid_multi_output_with_bounded_single_retries(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    calls: list[str] = []
    first_block_attempts = 0

    class FakeProvider:
        def __init__(self, base_url: str, timeout: float) -> None:
            pass

        @staticmethod
        def chat(
            model: str, system_prompt: str, input_text: str
        ) -> dict[str, object]:
            nonlocal first_block_attempts
            calls.append(input_text)
            if input_text.startswith("Translate to Simplified Chinese:\n\n"):
                return {"text": "被错误合并的译文。"}
            if input_text.endswith("First source block."):
                first_block_attempts += 1
                if first_block_attempts == 1:
                    return {"text": ""}
                if first_block_attempts == 2:
                    return {"text": "かなかなかなかなかなかなかなかな"}
                return {"text": "第一个来源块。"}
            if input_text.endswith("Second source block."):
                return {"text": "第二个来源块。"}
            raise AssertionError(input_text)

    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", FakeProvider)

    payload = {
        "blocks": [
            {
                "id": "block-aaaaaaaaaaaaaaaa",
                "text": "First source block.",
            },
            {
                "id": "block-bbbbbbbbbbbbbbbb",
                "text": "Second source block.",
            },
        ]
    }
    client = TestClient(app)

    first = client.post("/translations", json=payload)
    second = client.post("/translations", json=payload)

    assert first.status_code == 200
    assert first.json()["blocks"] == [
        {
            "id": "block-aaaaaaaaaaaaaaaa",
            "text": "第一个来源块。",
        },
        {
            "id": "block-bbbbbbbbbbbbbbbb",
            "text": "第二个来源块。",
        },
    ]
    assert first.json()["translation"] == "第一个来源块。\n\n第二个来源块。"
    assert second.json() == first.json()
    assert len(calls) == 5
    assert calls[1] == calls[2] == calls[3] == (
        "Translate to Simplified Chinese (output translation only):\n\n"
        "First source block."
    )


def test_translation_does_not_cache_after_single_retries_are_exhausted(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    calls: list[str] = []

    class FakeProvider:
        def __init__(self, base_url: str, timeout: float) -> None:
            pass

        @staticmethod
        def chat(
            model: str, system_prompt: str, input_text: str
        ) -> dict[str, object]:
            calls.append(input_text)
            if input_text.startswith("Translate to Simplified Chinese:\n\n"):
                return {"text": "被错误合并的译文。"}
            if input_text.endswith("First source block."):
                return {"text": "かなかなかなかなかなかなかなかな"}
            return {"text": "第二个来源块。"}

    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", FakeProvider)
    payload = {
        "blocks": [
            {"id": "block-aaaaaaaaaaaaaaaa", "text": "First source block."},
            {"id": "block-bbbbbbbbbbbbbbbb", "text": "Second source block."},
        ]
    }
    client = TestClient(app)

    assert client.post("/translations", json=payload).status_code == 502
    assert client.post("/translations", json=payload).status_code == 502
    assert len(calls) == 10
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count()).select_from(LLMTask)) == 0


def test_translation_chunks_long_foreign_text_and_caches(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    calls: list[str] = []
    expected: list[str] = []

    class FakeProvider:
        def __init__(self, base_url: str, timeout: float) -> None:
            pass

        @staticmethod
        def chat(model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            assert len(input_text) <= TRANSLATION_CHUNK_CHAR_LIMIT
            calls.append(input_text)
            translated = [
                f"译文片段 {len(calls)}-{index}"
                for index in range(1, input_text.count("%%") + 2)
            ]
            expected.extend(translated)
            return {"text": "\n\n%%\n\n".join(translated)}

    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", FakeProvider)
    paragraph = "Apple and Epic are still fighting about App Store fees, links, contempt orders, and developer rules. "
    text_value = "\n\n".join(paragraph * 35 for _ in range(4))
    client = TestClient(app)

    first = client.post("/translations", json={"text": text_value}).json()
    second = client.post("/translations", json={"text": text_value}).json()

    assert first["status"] == "ready"
    assert first["translation"] == "\n\n".join(expected)
    assert second["translation"] == first["translation"]
    assert len(calls) > 1


def test_detail_endpoints_return_cached_foreign_translations(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    calls: list[str] = []

    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        @staticmethod
        def chat(model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            calls.append(input_text)
            body = input_text.split("\n\n", 1)[-1]
            return {
                "text": "\n\n%%\n\n".join(
                    f"中文译文：{paragraph[:24]}"
                    for paragraph in body.split("\n\n%%\n\n")
                )
            }

    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", FakeProvider)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="English Feed", url="https://example.com/english.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="english-1", title="Apple Epic appeal", content_hash=content_hash("Apple Epic appeal"))
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            title="Apple Epic appeal",
            summary="Apple asks the Supreme Court to review Epic app store fees.",
            content_text="Apple asks the Supreme Court to review Epic app store fees.\n\nDevelopers are watching the case closely.",
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title="Apple Epic appeal",
            summary="Apple asks the Supreme Court to review Epic app store fees.",
            content_text=document.content_text,
            content_hash=content_hash("Apple Epic appeal item"),
            normalized_title=normalize_title("Apple Epic appeal"),
        )
        session.add(item)
        session.flush()
        cluster = assign_cluster(session, item)
        cluster.generated_title = "Apple Epic case reaches Supreme Court"
        item_id = item.id
        cluster_id = cluster.id
        session.commit()

    client = TestClient(app)
    for text_value in (
        "Apple Epic appeal",
        "Apple asks the Supreme Court to review Epic app store fees.",
        "Apple asks the Supreme Court to review Epic app store fees.\n\nDevelopers are watching the case closely.",
        "Apple Epic case reaches Supreme Court",
    ):
        assert client.post("/translations", json={"text": text_value}).json()["status"] == "ready"

    item_detail = client.get(f"/items/{item_id}").json()
    cluster_detail = client.get(f"/clusters/{cluster_id}").json()

    assert item_detail["title_translation"].startswith("中文译文：")
    assert item_detail["reading_translation_needed"] is True
    assert item_detail["content_translation"].startswith("中文译文：")
    assert cluster_detail["generated_title_translation"].startswith("中文译文：")
    assert cluster_detail["items"][0]["title_translation"].startswith("中文译文：")
    assert cluster_detail["items"][0]["reading_translation_needed"] is True
    assert cluster_detail["items"][0]["content_translation"].startswith("中文译文：")
    assert any(call.endswith("Apple Epic appeal") for call in calls)
    assert any(call.endswith("Apple Epic case reaches Supreme Court") for call in calls)


def test_reading_detail_survives_unavailable_translation_model(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    class BrokenProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        @staticmethod
        def chat(model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            raise RuntimeError("model offline")

    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", BrokenProvider)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="English Feed", url="https://example.com/english.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="english-offline", title="Apple Epic appeal", content_hash=content_hash("Apple Epic appeal"))
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            title="Apple Epic appeal",
            summary="Apple asks the Supreme Court to review Epic app store fees.",
            content_text="Apple asks the Supreme Court to review Epic app store fees.",
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=document.title,
            summary=document.summary,
            content_text=document.content_text,
            content_hash=content_hash("Apple Epic appeal offline item"),
            normalized_title=normalize_title(document.title),
        )
        session.add(item)
        session.flush()
        item_id = item.id
        session.commit()

    body = TestClient(app).get(f"/items/{item_id}").json()

    assert body["title"] == "Apple Epic appeal"
    assert body["content_text"] == "Apple asks the Supreme Court to review Epic app store fees."
    assert body["title_translation"] == ""
    assert body["content_translation"] == ""


def test_list_endpoints_return_bilingual_foreign_titles(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    calls: list[str] = []

    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        @staticmethod
        def chat(model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            calls.append(input_text)
            return {"text": "中文译文：" + input_text.partition("\n\n")[2]}

    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", FakeProvider)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="English Feed", url="https://example.com/rss.xml", fetch_full_content=True)
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="story-1", title="Apple Epic appeal", content_hash=content_hash("Apple Epic appeal"))
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            title="Apple Epic appeal",
            summary="Apple asks the Supreme Court to review Epic app store fees.",
            content_text="Apple asks the Supreme Court to review Epic app store fees.",
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title="Apple Epic appeal",
            summary=document.summary,
            content_text=document.content_text,
            content_hash=content_hash("Apple Epic appeal item"),
            normalized_title=normalize_title("Apple Epic appeal"),
        )
        session.add(item)
        session.flush()
        cluster = assign_cluster(session, item)
        cluster.generated_title = "Apple Epic case reaches Supreme Court"
        session.commit()

    client = TestClient(app)
    assert client.post("/translations", json={"text": "Apple Epic appeal"}).json()["status"] == "ready"
    assert client.post("/translations", json={"text": "Apple Epic case reaches Supreme Court"}).json()["status"] == "ready"
    calls.clear()

    item_row = client.get("/items", params={"include_content": "false"}).json()[0]
    search_row = client.get("/search", params={"q": "Apple", "include_content": "false"}).json()[0]
    cluster_row = client.get("/clusters").json()[0]

    assert item_row["title_translation"] == "中文译文：Apple Epic appeal"
    assert search_row["title_translation"] == "中文译文：Apple Epic appeal"
    assert item_row["content_text"] == ""
    assert item_row["content_translation"] == ""
    assert cluster_row["generated_title_translation"] == "中文译文：Apple Epic case reaches Supreme Court"
    assert cluster_row["items"][0]["title_translation"] == "中文译文：Apple Epic appeal"
    assert calls == []


def test_item_detail_get_is_read_only_even_when_full_content_is_enabled(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fetched: list[str] = []
    fetched_body = "Apple asks the Supreme Court to review the Epic App Store fee dispute. " * 20

    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            raise AssertionError("GET 详情不得创建模型 Provider")

    monkeypatch.setattr("reader_api.rss.fetch_article_text", lambda url: fetched.append(url) or fetched_body)
    monkeypatch.setattr("reader_api.main.embed_items_by_ids", reject_inline_reclustering)
    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", FakeProvider)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="English Feed", url="https://example.com/rss.xml", fetch_full_content=True)
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="short-1",
            title="Apple Epic appeal",
            url="https://example.com/story",
            raw_summary="Short RSS summary",
            raw_content="",
            content_hash=content_hash("Apple Epic appeal"),
        )
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title=raw.title, summary="Short RSS summary", content_text="Short RSS summary")
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            summary=document.summary,
            content_text=document.content_text,
            url=raw.url,
            content_hash=content_hash(raw.title, document.content_text),
            normalized_title=normalize_title(raw.title),
            embedding_vector="[1.0,0.0]",
            embedding_model="old-model",
        )
        session.add(item)
        session.flush()
        assign_cluster(session, item)
        item_id = item.id
        raw_id = raw.id
        document_id = document.id
        original_content_hash = item.content_hash
        original_lsh_signature = item.lsh_signature
        original_cluster_score = item.cluster_score
        fixed_updated_at = datetime(2026, 6, 1, tzinfo=timezone.utc)
        session.add(
            UserState(object_type="item", object_id=item_id, read_status="original_opened", read_later=True, updated_at=fixed_updated_at)
        )
        session.commit()

    state_before_read = user_state_snapshot()
    client = TestClient(app)
    body = client.get(f"/items/{item_id}").json()

    assert body["content_text"] == "Short RSS summary"
    assert body["title_translation"] == ""
    assert body["content_translation"] == ""
    assert fetched == []
    assert user_state_snapshot() == state_before_read

    with Session() as session:
        raw = session.get(RawEntry, raw_id)
        item = session.get(ContentItem, item_id)
        document = session.get(Document, document_id)
        assert raw is not None
        assert item is not None
        assert document is not None
        assert raw.raw_summary == "Short RSS summary"
        assert item.content_text == "Short RSS summary"
        assert document.content_text == "Short RSS summary"
        assert item.content_hash == original_content_hash
        assert item.lsh_signature == original_lsh_signature
        assert item.cluster_score == original_cluster_score
        assert item.embedding_vector == "[1.0,0.0]"
        assert item.embedding_model == "old-model"


def test_cluster_detail_get_does_not_fetch_or_translate_short_source_body(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fetched: list[str] = []
    fetched_body = "The PlayStation story has new context from the original article. " * 20

    class FailingProvider:
        def __init__(self, *_: object, **__: object) -> None:
            raise AssertionError(
                "opening cluster detail must not create a model provider"
            )

    monkeypatch.setattr("reader_api.rss.fetch_article_text", lambda url: fetched.append(url) or fetched_body)
    monkeypatch.setattr(
        "reader_api.main.embed_items_by_ids", reject_inline_reclustering
    )
    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", FailingProvider)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="English Feed", url="https://example.com/rss.xml", fetch_full_content=True)
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="short-1", title="Sony PlayStation update", url="https://example.com/story", content_hash=content_hash("Sony PlayStation update"))
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title=raw.title, summary="Short RSS summary", content_text="Short RSS summary")
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            summary=document.summary,
            content_text=document.content_text,
            url=raw.url,
            content_hash=content_hash(raw.title, document.content_text),
            normalized_title=normalize_title(raw.title),
            embedding_vector="[1.0,0.0]",
            embedding_model="old-model",
        )
        session.add(item)
        session.flush()
        cluster = assign_cluster(session, item)
        cluster_id = cluster.id
        session.commit()

    body = TestClient(app).get(f"/clusters/{cluster_id}").json()

    assert body["items"][0]["content_text"] == "Short RSS summary"
    assert body["items"][0]["content_translation"] == ""
    assert fetched == []


def test_embed_items_translates_foreign_text_and_stores_zh_canonical_embedding() -> None:
    from reader_api.cluster import ZH_CANONICAL_REPRESENTATION, embed_items_by_ids, load_content_embedding

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    embedding_inputs: list[object] = []
    translation_inputs: list[str] = []

    class FakeEmbeddingProvider:
        @staticmethod
        def embed_many(model: str, input_texts: list[str]) -> dict[str, object]:
            embedding_inputs.append(input_texts)
            return {"data": [{"embedding": [1.0, 0.0]} for _ in input_texts]}

        @staticmethod
        def embed(model: str, input_text: str) -> dict[str, object]:
            embedding_inputs.append(input_text)
            return {"embedding": [0.0, 1.0]}

    class FakeTranslationProvider:
        @staticmethod
        def chat(model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            translation_inputs.append(input_text)
            return {"text": "中文 " + input_text.partition("\n\n")[2]}

    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="English Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="1", title="Apple Epic appeal", content_hash=content_hash("Apple Epic appeal"))
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            title="Apple Epic appeal",
            summary="Apple asks the Supreme Court to review the Epic app store case.",
            content_text="Apple asks the Supreme Court to review the Epic app store case. Developers are watching the fee dispute closely.",
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title="Apple Epic appeal",
            summary=document.summary,
            content_text=document.content_text,
            content_hash=content_hash("Apple Epic appeal item"),
            normalized_title=normalize_title("Apple Epic appeal"),
        )
        session.add(item)
        session.commit()
        item_id = item.id

        processed = embed_items_by_ids(
            session,
            FakeEmbeddingProvider(),
            "embedding-model",
            [item_id],
            translation_provider=FakeTranslationProvider(),
            translation_model="hy-mt2-1.8b",
        )
        item = session.get(ContentItem, item_id)

        assert processed == 1
        assert item.embedding_model == "embedding-model"
        assert item.embedding_vector == "[1.0,0.0]"
        assert load_content_embedding(session, item_id, ZH_CANONICAL_REPRESENTATION, "embedding-model") == [0.0, 1.0]

    assert any("Apple Epic appeal" in text for text in translation_inputs)
    assert any("Supreme Court" in text for text in translation_inputs)
    assert any(isinstance(value, str) and value.startswith("中文 Apple Epic appeal") for value in embedding_inputs)


def test_local_llm_tasks_do_not_enable_reasoning(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeProvider:
        def __init__(self, base_url: str, timeout: float, reasoning: str | None = None) -> None:
            captured["base_url"] = base_url
            captured["timeout"] = timeout
            captured["reasoning"] = reasoning

    monkeypatch.setattr("reader_api.main.LocalChatProvider", FakeProvider)

    local_llm(
        RuntimeAISettings(
            task_provider="local",
            llm_base_url="http://example.com",
            translation_provider="local",
            translation_base_url="http://example.com",
            translation_api_key="",
            embedding_base_url="http://example.com",
            llm_model="llm",
            translation_model="mt",
            embedding_model="embed",
            timeout_seconds=12,
        )
    )

    assert captured["reasoning"] is None


def test_embedding_job_uses_configured_rq_timeout(monkeypatch) -> None:
    captured: list[tuple[str, str, int]] = []

    class FakeRedis:
        @staticmethod
        def from_url(url: str) -> str:
            return url

    class FakeQueue:
        def __init__(self, name: str, connection: str) -> None:
            self.name = name
            self.connection = connection

        def enqueue(self, job_name: str, **kwargs: object) -> SimpleNamespace:
            captured.append((self.name, job_name, int(kwargs["job_timeout"])))
            return SimpleNamespace(id=f"job-{len(captured)}")

    monkeypatch.setattr("reader_api.main.Redis", FakeRedis)
    monkeypatch.setattr("reader_api.main.Queue", FakeQueue)
    monkeypatch.setattr("reader_api.main.settings.rq_job_timeout_seconds", 7200)

    client = TestClient(app)

    assert client.post("/jobs/embeddings").json()["job_id"] == "job-1"
    assert captured == [
        ("reader-llm", "reader_api.worker.embed_all", 7200),
    ]


def test_fetch_endpoint_reuses_scheduler_deduplication(monkeypatch) -> None:
    outcomes = iter([(False, "existing-job"), (True, "queued-job")])
    calls: list[str] = []

    def enqueue_once(_session: object) -> tuple[bool, str]:
        calls.append("enqueue")
        return next(outcomes)

    monkeypatch.setattr(
        "reader_api.main.begin_fetch_refresh",
        enqueue_once,
        raising=False,
    )
    client = TestClient(app)

    assert client.post("/jobs/fetch").json() == {
        "mode": "existing",
        "imported": None,
        "embedded": None,
        "reclustered": None,
        "job_id": "existing-job",
    }
    assert client.post("/jobs/fetch").json()["job_id"] == "queued-job"
    assert calls == ["enqueue", "enqueue"]


def test_source_fetch_endpoint_queues_only_the_requested_source(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    source = client.post(
        "/sources",
        json={"name": "One", "url": "https://example.com/one.xml"},
    ).json()
    other = client.post(
        "/sources",
        json={"name": "Two", "url": "https://example.com/two.xml"},
    ).json()
    queued: list[int] = []
    monkeypatch.setattr(
        "reader_api.main.enqueue_source_fetch_job",
        lambda source_id: queued.append(source_id) or "source-job",
    )

    assert client.post(f"/sources/{source['id']}/fetch").json() == {
        "mode": "queued",
        "imported": None,
        "embedded": None,
        "reclustered": None,
        "job_id": "source-job",
    }
    assert queued == [source["id"]]
    assert other["id"] not in queued

    client.patch(f"/sources/{source['id']}", json={"enabled": False})
    assert client.post(f"/sources/{source['id']}/fetch").status_code == 409
    assert client.post("/sources/999999/fetch").status_code == 404


def test_fetch_queue_failure_does_not_run_inline_work(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        "reader_api.main.begin_fetch_refresh",
        lambda _session: (_ for _ in ()).throw(RuntimeError("redis down")),
    )
    monkeypatch.setattr("reader_api.worker.fetch_enabled_sources", lambda *_args, **_kwargs: calls.append("fetch"))

    response = TestClient(app).post("/jobs/fetch")

    assert response.status_code == 503
    assert calls == []


def test_fetch_status_exposes_only_the_refresh_state(monkeypatch) -> None:
    statuses = iter(["running", "complete", "failed"])
    monkeypatch.setattr(
        "reader_api.main.fetch_refresh_status",
        lambda _session, job_id: next(statuses) if job_id == "fetch-job" else "failed",
    )
    client = TestClient(app)

    assert client.get("/jobs/fetch/fetch-job").json() == {"status": "running"}
    assert client.get("/jobs/fetch/fetch-job").json() == {"status": "complete"}
    assert client.get("/jobs/fetch/fetch-job").json() == {"status": "failed"}


def test_inline_embeddings_job_does_not_run_historical_repairs(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    calls: list[str] = []

    class BrokenQueue:
        def __init__(self, *_: object, **__: object) -> None:
            raise RuntimeError("redis down")

    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            calls.append("provider")

    monkeypatch.setattr("reader_api.main.Queue", BrokenQueue)
    monkeypatch.setattr("reader_api.main.repair_title_only_clusters", reject_inline_reclustering, raising=False)
    monkeypatch.setattr("reader_api.main.repair_windowed_clusters", reject_inline_reclustering, raising=False)
    monkeypatch.setattr("reader_api.main.repair_embedding_clusters", reject_inline_reclustering, raising=False)
    monkeypatch.setattr("reader_api.main.LocalEmbeddingProvider", FakeProvider)
    monkeypatch.setattr("reader_api.main.embed_pending_items", lambda session, provider, model, **kwargs: calls.append("embed") or 4)

    response = TestClient(app).post("/jobs/embeddings").json()

    assert response["mode"] == "inline"
    assert response["embedded"] == 4
    assert response["reclustered"] == 0
    assert calls == ["provider", "embed"]


def test_ai_chat_translation_uses_translation_model(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    captured: dict[str, object] = {}

    class FakeProvider:
        def __init__(self, base_url: str, timeout: float, reasoning: str | None = None) -> None:
            captured["base_url"] = base_url
            captured["timeout"] = timeout
            captured["reasoning"] = reasoning

        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            captured["model"] = model
            captured["system_prompt"] = system_prompt
            captured["input"] = input_text
            return {"text": "blue hue"}

    monkeypatch.setattr("reader_api.ai_runtime.LocalChatProvider", FakeProvider)

    body = TestClient(app).post("/ai/chat", json={"model_type": "translation", "system_prompt": "translate", "input": "蓝色"}).json()

    assert body["model"] == "hy-mt2-1.8b"
    assert body["result"] == {"text": "blue hue"}
    assert captured["model"] == "hy-mt2-1.8b"
    assert captured["reasoning"] is None


def test_reports_placeholder_periods() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    assert client.get("/reports", params={"period": "day"}).json()["title"] == "日报"
    assert client.get("/reports", params={"period": "day"}).json()["status"] == "empty"
    assert client.get("/reports", params={"period": "week"}).json()["title"] == "周报"
    assert client.get("/reports", params={"period": "month"}).json()["title"] == "月报"
    assert client.get("/reports", params={"period": "year"}).status_code == 400


def test_report_bounds_use_local_calendar_days() -> None:
    assert report_bounds("day", "2026-06-29") == (
        datetime(2026, 6, 28, 16, tzinfo=timezone.utc),
        datetime(2026, 6, 29, 16, tzinfo=timezone.utc),
    )
    assert report_bounds("week", "2026-06-29") == (
        datetime(2026, 6, 28, 16, tzinfo=timezone.utc),
        datetime(2026, 7, 5, 16, tzinfo=timezone.utc),
    )
    assert report_bounds("month", "2026-06-29") == (
        datetime(2026, 5, 31, 16, tzinfo=timezone.utc),
        datetime(2026, 6, 30, 16, tzinfo=timezone.utc),
    )
    assert report_state_key("day", datetime(2026, 6, 28, 16, tzinfo=timezone.utc)) == 120260629


def test_user_state_rejects_missing_real_objects() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    for index, object_type in enumerate(("item", "topic"), 1):
        payload: dict[str, object] = {"read_status": "summary_seen"}
        payload["operation_id"] = f"91{index:02d}0000-0000-4000-8000-000000000000"
        assert client.patch(f"/user-state/{object_type}/999", json=payload).status_code == 404
    assert client.patch(
        "/user-state/cluster/999", json={"read_status": "summary_seen"}
    ).status_code == 400
    set_object_user_state(
        client,
        "report",
        20260629,
        operation_id="91040000-0000-4000-8000-000000000000",
        read_status="summary_seen",
    )
    with sessionmaker(bind=engine)() as session:
        assert session.scalar(select(func.count()).select_from(UserState)) == 1


def test_api_error_details_are_user_facing_chinese() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    assert client.patch("/folders/999", json={"name": "X"}).json()["detail"] == "文件夹不存在"
    assert client.get("/items/999").json()["detail"] == "条目不存在"
    assert client.get("/items", params={"media_type": "book"}).json()["detail"] == "不支持的媒体类型"
    assert client.get("/reports", params={"period": "year"}).json()["detail"] == "不支持的报告周期"


def test_import_opml_rejects_invalid_xml_with_chinese_error() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    response = TestClient(app).post("/imports/opml", files={"file": ("bad.opml", b"<opml><body>", "text/xml")})

    assert response.status_code == 400
    assert response.json()["detail"] == "OPML 格式无效"


def test_import_opml_rejects_files_over_two_megabytes() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    response = TestClient(app).post(
        "/imports/opml",
        files={
            "file": (
                "large.opml",
                b"x" * (2 * 1024 * 1024 + 1),
                "text/xml",
            )
        },
    )

    assert response.status_code == 413
    assert response.json()["detail"] == "OPML 文件超过 2MB 限制"


def test_import_opml_stops_chunked_multipart_before_spooling_the_full_file(
    monkeypatch,
) -> None:
    from starlette.datastructures import UploadFile as StarletteUploadFile

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    written = 0
    original_write = StarletteUploadFile.write

    async def recording_write(self, data: bytes) -> None:
        nonlocal written
        written += len(data)
        await original_write(self, data)

    monkeypatch.setattr(StarletteUploadFile, "write", recording_write)
    boundary = "reader-opml-boundary"
    prefix = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="large.opml"\r\n'
        "Content-Type: text/xml\r\n\r\n"
    ).encode()
    suffix = f"\r\n--{boundary}--\r\n".encode()

    def oversized_body():
        yield prefix
        for _ in range(48):
            yield b"x" * (64 * 1024)
        yield suffix

    response = TestClient(app).post(
        "/imports/opml",
        content=oversized_body(),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )

    assert response.request.headers.get("content-length") is None
    assert response.status_code == 413
    assert written <= 2 * 1024 * 1024 + 64 * 1024


def test_export_opml_downloads_attachment() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    response = client.get("/exports/opml")

    assert response.headers["content-disposition"] == 'attachment; filename="reader.opml"'
    assert "Reader 订阅源" in response.text


def test_trial_source_is_out_of_default_flow_but_readable_by_source() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        active_source = Source(name="Active Feed", url="https://example.com/active.xml", status="active")
        trial_source = Source(name="Trial Feed", url="https://example.com/trial.xml", status="trial")
        session.add_all([active_source, trial_source])
        session.flush()
        for source, title in [(active_source, "Active item"), (trial_source, "Trial item")]:
            raw = make_raw_entry(source_id=source.id, external_id=title, title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            item = ContentItem(document_id=doc.id, source_id=source.id, title=title, summary=title, content_text=title, content_hash=content_hash(title))
            session.add(item)
            session.flush()
            if source.status == "active":
                assign_cluster(session, item)
            if source.status == "trial":
                trial_item_id = item.id
        trial_source_id = trial_source.id
        session.commit()

    client = TestClient(app)
    assert [item["title"] for item in client.get("/items").json()] == ["Active item"]
    assert [item["title"] for item in client.get("/items", params={"source_id": trial_source_id}).json()] == ["Trial item"]
    assert client.get(f"/items/{trial_item_id}").json()["title"] == "Trial item"
    assert client.get("/clusters").json()[0]["title"] == "Active item"


def test_item_media_type_filter_uses_source_media_type() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        picture_folder = Folder(name="Pictures", media_type="image")
        session.add(picture_folder)
        session.flush()
        article_source = Source(name="Article Feed", url="https://example.com/article.xml", media_type="article")
        video_source = Source(name="Video Feed", url="https://example.com/video.xml", media_type="video")
        legacy_image_source = Source(name="Image Folder Feed", url="https://example.com/legacy-image.xml", folder_id=picture_folder.id, media_type="image")
        session.add_all([article_source, video_source, legacy_image_source])
        session.flush()
        for source, title in [(article_source, "Article item"), (video_source, "Video item"), (legacy_image_source, "Legacy image item")]:
            raw = make_raw_entry(source_id=source.id, external_id=title, title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            body = f"{title}\n\n![Cover](https://example.com/{source.media_type}.jpg)" if source.media_type == "article" else title
            doc = Document(raw_entry_id=raw.id, title=title, content_text=body)
            session.add(doc)
            session.flush()
            session.add(ContentItem(document_id=doc.id, source_id=source.id, title=title, summary=title, content_text=body, content_hash=content_hash(title)))
        session.commit()

    client = TestClient(app)
    assert [item["title"] for item in client.get("/items", params={"media_type": "article"}).json()] == ["Article item"]
    assert [item["title"] for item in client.get("/items", params={"media_type": "image"}).json()] == ["Legacy image item"]
    assert [item["title"] for item in client.get("/search", params={"q": "item", "media_type": "video"}).json()] == ["Video item"]
    light_item = client.get("/items", params={"media_type": "article", "include_content": False}).json()[0]
    assert light_item["summary"] == "Article item"
    assert light_item["content_text"] == ""
    assert light_item["image_url"] == "https://example.com/article.jpg"
    assert client.get(f"/items/{light_item['id']}").json()["content_text"] == "Article item\n\n![Cover](https://example.com/article.jpg)"
    assert client.get("/items", params={"media_type": "book"}).status_code == 400


def test_lightweight_item_list_eager_loads_raw_image_evidence() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="Podcast Feed", url="https://example.com/podcast.xml", media_type="podcast")
        session.add(source)
        session.flush()
        for index in range(3):
            title = f"Podcast item {index}"
            image_url = f"https://example.com/cover-{index}.jpg"
            raw = make_raw_entry(
                source_id=source.id,
                external_id=title,
                title=title,
                raw_content=f'<img src="{image_url}">',
                content_hash=content_hash(title),
            )
            session.add(raw)
            session.flush()
            document = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(document)
            session.flush()
            session.add(
                ContentItem(
                    document_id=document.id,
                    source_id=source.id,
                    title=title,
                    summary=title,
                    content_text=title,
                    published_at=datetime(2026, 7, 1, index, tzinfo=timezone.utc),
                    content_hash=content_hash(title),
                )
            )
        session.commit()

    select_statements: list[str] = []

    def capture_selects(_connection, _cursor, statement, _parameters, _context, _executemany) -> None:
        if statement.lstrip().upper().startswith("SELECT"):
            select_statements.append(statement)

    event.listen(engine, "before_cursor_execute", capture_selects)
    try:
        response = TestClient(app).get(
            "/items",
            params={"media_type": "podcast", "include_content": False},
        )
    finally:
        event.remove(engine, "before_cursor_execute", capture_selects)

    assert response.status_code == 200
    assert [item["image_url"] for item in response.json()] == [
        "https://example.com/cover-2.jpg",
        "https://example.com/cover-1.jpg",
        "https://example.com/cover-0.jpg",
    ]
    assert len(select_statements) <= 3


def test_browse_items_social_notification_filter_read_status_and_pagination() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        social_source = Source(name="Social Feed", url="https://example.com/social.xml", media_type="social")
        notification_source = Source(name="GitHub Releases", url="https://example.com/releases.xml", media_type="notification")
        session.add_all([social_source, notification_source])
        session.flush()
        seen_social_id = None
        for source, titles in [(social_source, ["Social item 0", "Social item 1", "Social item 2"]), (notification_source, ["Release item"])]:
            for index, title in enumerate(titles):
                raw = make_raw_entry(source_id=source.id, external_id=title, title=title, raw_content=title, content_hash=content_hash(title))
                session.add(raw)
                session.flush()
                doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
                session.add(doc)
                session.flush()
                item = ContentItem(
                    document_id=doc.id,
                    source_id=source.id,
                    title=title,
                    summary=title,
                    content_text=title,
                    published_at=datetime(2026, 7, 1, index, tzinfo=timezone.utc),
                    content_hash=content_hash(title),
                )
                session.add(item)
                session.flush()
                if title == "Social item 1":
                    seen_social_id = item.id
        session.add(UserState(object_type="item", object_id=seen_social_id, read_status="summary_seen"))
        session.commit()

    client = TestClient(app)
    assert [item["title"] for item in client.get("/items", params={"media_type": "social", "limit": 2}).json()] == ["Social item 2", "Social item 1"]
    assert [item["title"] for item in client.get("/items", params={"media_type": "social", "limit": 1, "offset": 1}).json()] == ["Social item 1"]
    assert [item["title"] for item in client.get("/items", params={"media_type": "social", "read_status": "unread"}).json()] == ["Social item 2", "Social item 0"]
    assert [item["title"] for item in client.get("/items", params={"media_type": "notification"}).json()] == ["Release item"]


def test_browse_summary_counts_item_unread_by_media_and_source() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        notification_folder = Folder(name="Notifications", media_type="notification")
        session.add(notification_folder)
        session.flush()
        social_source = Source(name="Social Feed", url="https://example.com/social.xml", media_type="social")
        empty_social_source = Source(name="Empty Social", url="https://example.com/empty-social.xml", media_type="social")
        notification_source = Source(name="Releases", url="https://example.com/releases.xml", folder_id=notification_folder.id, media_type="notification")
        article_source = Source(name="Article Feed", url="https://example.com/article.xml", media_type="article")
        muted_social_source = Source(name="Muted Social", url="https://example.com/muted-social.xml", media_type="social", status="muted")
        session.add_all([social_source, empty_social_source, notification_source, article_source, muted_social_source])
        session.flush()
        seen_social_id = None
        opened_notification_id = None
        for source, titles in [(social_source, ["Social unread", "Social seen"]), (notification_source, ["Release opened"]), (article_source, ["Article unread"]), (muted_social_source, ["Muted ignored"])]:
            for title in titles:
                raw = make_raw_entry(source_id=source.id, external_id=title, title=title, raw_content=title, content_hash=content_hash(title))
                session.add(raw)
                session.flush()
                doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
                session.add(doc)
                session.flush()
                item = ContentItem(document_id=doc.id, source_id=source.id, title=title, summary=title, content_text=title, content_hash=content_hash(title))
                session.add(item)
                session.flush()
                if title == "Social seen":
                    seen_social_id = item.id
                if title == "Release opened":
                    opened_notification_id = item.id
        session.add(UserState(object_type="item", object_id=seen_social_id, read_status="summary_seen"))
        session.add(UserState(object_type="item", object_id=opened_notification_id, read_status="original_opened"))
        session.commit()

    selects: list[str] = []

    def capture_selects(_connection, _cursor, statement, _parameters, _context, _executemany) -> None:
        if statement.lstrip().upper().startswith("SELECT"):
            selects.append(statement)

    event.listen(engine, "before_cursor_execute", capture_selects)
    try:
        summary = {row["media_type"]: row for row in TestClient(app).get("/browse/summary").json()}
    finally:
        event.remove(engine, "before_cursor_execute", capture_selects)

    assert len(selects) <= 2
    assert summary["social"]["total_count"] == 2
    assert summary["social"]["unread_count"] == 1
    assert [(source["name"], source["total_count"], source["unread_count"]) for source in summary["social"]["sources"]] == [
        ("Empty Social", 0, 0),
        ("Social Feed", 2, 1),
    ]
    assert summary["notification"]["total_count"] == 1
    assert summary["notification"]["unread_count"] == 0
    assert summary["notification"]["sources"][0]["name"] == "Releases"
    assert summary["article"]["total_count"] == 1
    assert summary["image"]["sources"] == []


def test_promoting_trial_source_clusters_existing_embedded_items() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="Trial Feed", url="https://example.com/trial.xml", status="trial")
        video_source = Source(name="Trial Video Feed", url="https://example.com/trial-video.xml", status="trial", media_type="video")
        session.add_all([source, video_source])
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="1", title="Trial item", content_hash=content_hash("Trial item"))
        session.add(raw)
        session.flush()
        doc = Document(raw_entry_id=raw.id, title="Trial item", content_text="Trial item body")
        session.add(doc)
        session.flush()
        session.add(
            ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title="Trial item",
                summary="Trial item body",
                content_text="Trial item body",
                content_hash=content_hash("Trial item body"),
                normalized_title=normalize_title("Trial item"),
                embedding_vector="[1.0,0.0]",
                embedding_model="embedding-model",
            )
        )
        source_id = source.id
        video_raw = make_raw_entry(source_id=video_source.id, external_id="video-1", title="Trial video item", content_hash=content_hash("Trial video item"))
        session.add(video_raw)
        session.flush()
        video_doc = Document(raw_entry_id=video_raw.id, title="Trial video item", content_text="Trial video item body")
        session.add(video_doc)
        session.flush()
        session.add(
            ContentItem(
                document_id=video_doc.id,
                source_id=video_source.id,
                title="Trial video item",
                summary="Trial video item body",
                content_text="Trial video item body",
                content_hash=content_hash("Trial video item body"),
                normalized_title=normalize_title("Trial video item"),
            )
        )
        video_source_id = video_source.id
        session.commit()

    client = TestClient(app)
    assert client.get("/clusters").json() == []

    video_promoted = client.patch(f"/sources/{video_source_id}", json={"status": "active"}).json()
    assert video_promoted["status"] == "active"
    assert client.get("/clusters").json() == []

    promoted = client.patch(f"/sources/{source_id}", json={"status": "active"}).json()
    clusters = client.get("/clusters").json()

    assert promoted["status"] == "active"
    assert clusters[0]["title"] == "Trial item"
    invalid_mixed_update = client.patch(
        f"/sources/{source_id}",
        json={"media_type": "video", "status": "bad"},
    )
    assert invalid_mixed_update.status_code == 400
    unchanged_source = client.get("/sources").json()
    unchanged_source = next(row for row in unchanged_source if row["id"] == source_id)
    assert unchanged_source["media_type"] == "article"
    assert unchanged_source["status"] == "active"
    assert client.get("/clusters").json()[0]["title"] == "Trial item"
    cluster_id = clusters[0]["id"]
    assert (
        set_cluster_event_state(
            client,
            clusters[0],
            operation_id="90000000-0000-4000-8000-000000000000",
            action="read_status_set",
            value="summary_seen",
        )["read_status"]
        == "summary_seen"
    )
    set_cluster_event_state(
        client,
        clusters[0],
        operation_id="90111111-1111-4111-8111-111111111111",
        action="starred_set",
        value=True,
    )
    set_cluster_event_state(
        client,
        clusters[0],
        operation_id="90222222-2222-4222-8222-222222222222",
        action="read_later_set",
        value=True,
    )
    muted = client.patch(f"/sources/{source_id}", json={"status": "muted"}).json()
    assert muted["status"] == "muted"
    assert client.get("/clusters").json() == []
    with Session() as session:
        assert session.scalar(select(func.count()).select_from(UserState).where(UserState.object_type == "cluster", UserState.object_id == cluster_id)) == 0
    assert client.get("/items", params={"source_id": source_id}).json()[0]["title"] == "Trial item"
    restored = client.patch(f"/sources/{source_id}", json={"status": "active"}).json()
    assert restored["status"] == "active"
    assert client.get("/clusters").json()[0]["title"] == "Trial item"


def test_delete_source_is_irreversible_without_removing_immutable_history() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(
            name="Archive Feed",
            url="https://example.com/archive.xml",
            privacy_class="public",
            external_generation_allowed=True,
        )
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="1", title="Archived item", content_hash=content_hash("Archived item"))
        session.add(raw)
        session.flush()
        doc = Document(raw_entry_id=raw.id, title="Archived item", content_text="Archived item body")
        session.add(doc)
        session.flush()
        item = ContentItem(
            document_id=doc.id,
            source_id=source.id,
            title="Archived item",
            summary="Archived item body",
            content_text="Archived item body",
            content_hash=content_hash("Archived item body"),
            normalized_title=normalize_title("Archived item"),
        )
        session.add(item)
        session.flush()
        assign_cluster(session, item)
        rule = FilterRule(
            source_id=source.id,
            match_type="literal",
            pattern="Archived",
            enabled=False,
        )
        session.add(rule)
        session.flush()
        session.add(FilterMatch(rule_id=rule.id, content_item_id=item.id))
        global_rule = FilterRule(
            match_type="literal",
            pattern="Archived",
            enabled=False,
        )
        session.add(global_rule)
        session.flush()
        session.add(FilterMatch(rule_id=global_rule.id, content_item_id=item.id))
        source_id = source.id
        item_id = item.id
        session.commit()

    client = TestClient(app)
    assert client.get("/clusters").json()[0]["title"] == "Archived item"
    assert client.post(
        "/uninterested",
        json={
            "operation_id": "90500000-0000-4000-8000-000000000000",
            "target_type": "item",
            "item_id": item_id,
            "value": True,
        },
    ).status_code == 200
    assert client.get("/uninterested-targets").json()["count"] == 1
    assert client.delete(f"/sources/{source_id}").status_code == 204

    assert all(source["id"] != source_id for source in client.get("/sources").json())
    assert client.patch(f"/sources/{source_id}", json={"enabled": True}).status_code == 404
    assert client.delete(f"/sources/{source_id}").status_code == 404
    assert client.get("/items").json() == []
    assert client.get("/items", params={"source_id": source_id}).json() == []
    assert client.get(f"/items/{item_id}").status_code == 404
    assert client.get(f"/items/{item_id}/summary").status_code == 404
    assert client.post(f"/items/{item_id}/summarize").status_code == 404
    assert client.post(
        "/uninterested",
        json={
            "operation_id": "90640000-0000-4000-8000-000000000000",
            "target_type": "item",
            "item_id": item_id,
            "value": True,
        },
    ).status_code == 404
    assert client.patch(
        f"/user-state/item/{item_id}",
        json={
            "operation_id": "90650000-0000-4000-8000-000000000000",
            "starred": True,
        },
    ).status_code == 404
    assert client.get(
        "/assistant",
        params={
            "q": "摘要",
            "item_id": item_id,
            "operation_id": "90600000-0000-4000-8000-000000000000",
        },
    ).status_code == 404
    assert client.get("/clusters").json() == []
    assert client.get("/uninterested-targets").json()["count"] == 0
    assert client.post(
        "/filter-rules/preview",
        json={
            "source_id": None,
            "match_type": "literal",
            "pattern": "Archived",
        },
    ).json() == {"count": 0, "items": []}
    assert client.get("/filter-rules").json()[0]["match_count"] == 0
    replacement = client.post(
        "/sources",
        json={
            "name": "Replacement Feed",
            "url": "https://example.com/archive.xml",
            "status": "active",
        },
    ).json()
    assert replacement["id"] != source_id
    assert replacement["status"] == "active"
    assert client.get("/clusters").json() == []
    with Session() as session:
        deleted = session.get(Source, source_id)
        assert deleted is not None
        assert deleted.status == "deleted"
        assert deleted.enabled is False
        assert deleted.external_generation_allowed is False
        assert deleted.generation_policy_version == 2
        assert session.scalar(select(func.count()).select_from(RawEntry).where(RawEntry.source_id == source_id)) == 1
        assert session.scalar(select(func.count()).select_from(ContentItem).where(ContentItem.source_id == source_id)) == 1
        assert session.scalar(select(func.count()).select_from(FilterRule).where(FilterRule.source_id == source_id)) == 0
        assert session.scalar(select(func.count()).select_from(FilterMatch).where(FilterMatch.content_item_id == item_id)) == 0


def test_source_metrics_include_cluster_and_duplicate_counts() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        session.add(FeedMetric(source_id=source.id, fetched_count=10, read_count=4))
        for index in range(2):
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title="Nvidia chip", content_hash=content_hash("Nvidia chip", str(index)))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title="Nvidia chip", content_text="Nvidia announces a new AI chip")
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title="Nvidia announces a new AI chip",
                summary="New chip",
                content_text=f"Nvidia announces a new AI chip for datacenters. {index}",
                url="https://example.com/nvidia-chip",
                canonical_url=canonical_url("https://example.com/nvidia-chip"),
                content_hash=content_hash("Nvidia chip item", str(index)),
                normalized_title=normalize_title("Nvidia announces a new AI chip"),
            )
            session.add(item)
            session.flush()
            assign_cluster(session, item)
        session.commit()

    source = TestClient(app).get("/sources").json()[0]

    assert source["cluster_count"] == 1
    assert source["duplicate_count"] == 2
    assert source["feed_trust_score"] == 30.0


def test_feed_trust_score_persists_with_cluster_counts() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        session.add(FeedMetric(source_id=source.id, fetched_count=10))
        raw = make_raw_entry(source_id=source.id, external_id="1", title="Nvidia chip", content_hash=content_hash("Nvidia chip"))
        session.add(raw)
        session.flush()
        doc = Document(raw_entry_id=raw.id, title="Nvidia chip", content_text="Nvidia announces a new AI chip")
        session.add(doc)
        session.flush()
        item = ContentItem(
            document_id=doc.id,
            source_id=source.id,
            title="Nvidia announces a new AI chip",
            summary="New chip",
            content_text="Nvidia announces a new AI chip for datacenters.",
            content_hash=content_hash("Nvidia chip item"),
            normalized_title=normalize_title("Nvidia announces a new AI chip"),
        )
        session.add(item)
        session.flush()
        assign_cluster(session, item)
        item_id = item.id
        source_id = source.id
        session.commit()

    client = TestClient(app)
    set_object_user_state(
        client,
        "item",
        item_id,
        operation_id="92000000-0000-4000-8000-000000000000",
        read_status="summary_seen",
    )

    with Session() as session:
        source = session.get(Source, source_id)
        assert source is not None
        assert source.feed_trust_score == 20.0
    assert client.get("/sources").json()[0]["feed_trust_score"] == 20.0


def test_exact_cluster_merges_same_url_with_changed_title_inside_window() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    now = datetime(2026, 6, 29, tzinfo=timezone.utc)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        cluster_ids = []
        for index, (title, offset) in enumerate(
            [
                ("Nvidia announces a new AI chip", timedelta(hours=1)),
                ("Nvidia updates launch details", timedelta(hours=2)),
                ("Nvidia updates launch details again", timedelta(days=40)),
            ],
            1,
        ):
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                url="https://example.com/story?utm_source=rss",
                canonical_url=canonical_url("https://example.com/story?utm_source=rss"),
                published_at=now + offset,
                content_hash=content_hash(title, str(index)),
                normalized_title=normalize_title(title),
            )
            session.add(item)
            session.flush()
            cluster_ids.append(assign_cluster(session, item).id)

    assert cluster_ids[0] == cluster_ids[1]
    assert cluster_ids[2] != cluster_ids[0]


def test_exact_cluster_does_not_merge_title_only_match() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    now = datetime(2026, 6, 29, tzinfo=timezone.utc)
    with Session() as session:
        source = Source(name="Blog", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        cluster_ids = []
        for index in range(2):
            title = "Not much happened today"
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title=title, content_hash=content_hash(title, str(index)))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=f"Different diary entry {index}")
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=f"Different diary entry {index}",
                url=f"https://example.com/story/{index}",
                canonical_url=f"https://example.com/story/{index}",
                published_at=now + timedelta(hours=index),
                content_hash=content_hash(title, str(index)),
                normalized_title=normalize_title(title),
            )
            session.add(item)
            session.flush()
            cluster_ids.append(assign_cluster(session, item).id)

    assert cluster_ids[0] != cluster_ids[1]


def test_exact_cluster_merges_same_source_content_and_time_across_urls() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    published_at = datetime(2026, 6, 24, 16, 17, tzinfo=timezone.utc)
    with Session() as session:
        source = Source(name="Dpreview", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        cluster_ids = []
        for index, (url, offset) in enumerate([
            ("https://www.dpreview.com/news/7189727602/story", timedelta()),
            ("https://m.dpreview.com/news/7189727602/story", timedelta()),
            ("https://www.dpreview.com/news/7189727602/repost", timedelta(minutes=1)),
        ]):
            title = "Amazon Prime Day 2026"
            body = "The exact same complete article body."
            raw = make_raw_entry(
                source_id=source.id,
                external_id=str(index),
                title=title,
                raw_content=body,
                url=url,
                content_hash=content_hash(title, body, url),
            )
            session.add(raw)
            session.flush()
            document = Document(raw_entry_id=raw.id, title=title, content_text=body)
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=title,
                summary=body,
                content_text=body,
                url=url,
                canonical_url=url,
                published_at=published_at + offset,
                content_hash=content_hash(title, body, url),
                normalized_title=normalize_title(title),
            )
            session.add(item)
            session.flush()
            cluster_ids.append(assign_cluster(session, item).id)

    assert cluster_ids[0] == cluster_ids[1]
    assert cluster_ids[2] != cluster_ids[0]


def test_exact_content_duplicate_maintenance_repairs_existing_clusters() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    published_at = datetime(2026, 6, 24, 16, 17, tzinfo=timezone.utc)
    with Session() as session:
        source = Source(name="Dpreview", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        for index, host in enumerate(("www", "m"), 1):
            title = "Duplicate article"
            body = "The same complete article body."
            url = f"https://{host}.dpreview.com/news/story"
            raw = make_raw_entry(
                source_id=source.id,
                external_id=str(index),
                title=title,
                raw_content=body,
                url=url,
                content_hash=content_hash(title, body, url),
            )
            session.add(raw)
            session.flush()
            document = Document(raw_entry_id=raw.id, title=title, content_text=body)
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=title,
                summary=body,
                content_text=body,
                url=url,
                canonical_url=url,
                published_at=published_at,
                content_hash=content_hash(title, body, url),
                normalized_title=normalize_title(title),
                embedding_vector="[1.0,0.0]",
                embedding_model="test-model",
            )
            session.add(item)
            session.flush()
            cluster = Cluster(
                cluster_key=f"legacy-{index}",
                title=title,
                first_seen_at=published_at,
                last_seen_at=published_at,
            )
            session.add(cluster)
            session.flush()
            session.add(ClusterItem(cluster_id=cluster.id, content_item_id=item.id))
        session.commit()

        assert repair_exact_content_duplicates(session) == 1
        assert session.scalar(select(func.count()).select_from(ClusterItem)) == 2
        assert session.scalar(
            select(func.count(func.distinct(ClusterItem.cluster_id)))
        ) == 1


def test_exact_cluster_ignores_inactive_source_clusters() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        inactive_source = Source(name="Muted Feed", url="https://example.com/muted.xml")
        active_source = Source(name="Active Feed", url="https://example.com/active.xml")
        session.add_all([inactive_source, active_source])
        session.flush()

        clusters = []
        for source in (inactive_source, active_source):
            raw = make_raw_entry(source_id=source.id, external_id=source.name, title="Same story", content_hash=content_hash(source.name))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title="Same story", content_text="Same story text")
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title="Same story",
                summary="Same story",
                content_text="Same story text",
                content_hash=content_hash(source.name, "item"),
                normalized_title=normalize_title("Same story"),
            )
            session.add(item)
            session.flush()
            if source is inactive_source:
                clusters.append(assign_cluster(session, item).id)
                source.status = "muted"
                session.flush()
            else:
                clusters.append(assign_cluster(session, item).id)

    assert clusters[0] != clusters[1]


def test_report_generation_uses_clusters_and_persists(monkeypatch) -> None:
    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            assert model
            assert "报告助手" in system_prompt
            assert "Nvidia announces" in input_text
            return {"text": '{"title":"日报标题","body":"报告正文 [1]"}'}

    monkeypatch.setattr("reader_api.main.LocalChatProvider", FakeProvider)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        followup_source = Source(name="Followup Feed", url="https://example.com/followup.xml")
        session.add_all([source, followup_source])
        session.flush()
        source_ids = (source.id, followup_source.id)
        report_items = []
        for index, (item_source, published_at) in enumerate(
            [
                (source, datetime(2026, 6, 29, 6, tzinfo=timezone.utc)),
                (followup_source, datetime(2026, 6, 29, 7, tzinfo=timezone.utc)),
            ],
            1,
        ):
            raw = make_raw_entry(source_id=item_source.id, external_id=str(index), title="Nvidia chip", content_hash=content_hash("Nvidia chip", str(index)))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title="Nvidia chip", content_text="Nvidia announces a new AI chip")
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=item_source.id,
                title="![cover](https://example.com/cover.jpg) Nvidia announces a new AI chip" if index == 1 else "Nvidia announces a new AI chip",
                summary="New chip",
                content_text="Nvidia announces a new AI chip for datacenters.",
                published_at=published_at,
                content_hash=content_hash("Nvidia chip item", str(index)),
                normalized_title=normalize_title("Nvidia announces a new AI chip"),
                lsh_signature=lsh_signature("Nvidia announces a new AI chip", "Nvidia announces a new AI chip for datacenters."),
                embedding_vector=json.dumps([1, 0]),
            )
            session.add(item)
            session.flush()
            report_items.append(item)
        with clustering_run(
            session,
            scope_type="report-generation-test",
            item_ids=[item.id for item in report_items],
            rule_version="report-generation-test-v1",
        ):
            for item in report_items:
                report_cluster_id = assign_cluster(session, item).id
        fixed_updated_at = datetime(2026, 6, 1, tzinfo=timezone.utc)
        report_starts = {period: report_bounds(period, "2026-06-29")[0] for period in ("day", "week", "month")}
        session.add_all(
            [
                UserState(
                    object_type="report",
                    object_id=report_state_key("day", report_starts["day"]),
                    read_status="unread",
                    starred=True,
                    updated_at=fixed_updated_at,
                ),
                UserState(
                    object_type="report",
                    object_id=report_state_key("week", report_starts["week"]),
                    read_status="original_opened",
                    read_later=True,
                    updated_at=fixed_updated_at,
                ),
                UserState(
                    object_type="report",
                    object_id=report_state_key("month", report_starts["month"]),
                    read_status="summary_seen",
                    read_later=True,
                    starred=True,
                    updated_at=fixed_updated_at,
                ),
            ]
        )
        session.add(
            GenerationControl(
                id=1,
                global_pause=False,
                daily_budget_tokens=10_000_000,
            )
        )
        session.commit()

    client = TestClient(app)
    state_before_generation = user_state_snapshot()
    generated = client.post("/reports/generate", params={"period": "day", "date": "2026-06-29"}).json()
    assert client.post("/reports/generate", params={"period": "week", "date": "2026-06-29"}).status_code == 200
    assert client.post("/reports/generate", params={"period": "month", "date": "2026-06-29"}).status_code == 200
    saved = client.get("/reports", params={"period": "day", "date": "2026-06-29"}).json()

    assert generated["status"] == "ready"
    assert saved["object_id"] == report_state_key("day", datetime(2026, 6, 29, tzinfo=timezone.utc))
    assert saved["start"] == "2026-06-28T16:00:00+00:00"
    assert saved["end"] == "2026-06-29T16:00:00+00:00"
    assert saved["read_status"] == "unread"
    assert saved["starred"] is True
    assert saved["title"] == "日报标题"
    assert saved["body"] == "报告正文 [1]"
    assert saved["model_version"]
    assert saved["prompt_version"] == "report-v3"
    assert saved["citations"][0]["citation_no"] == 1
    assert saved["citations"][0]["title"] == "Nvidia announces a new AI chip"
    event_uid = saved["citations"][0]["event_uid"]
    revision_uid = saved["citations"][0]["event_revision_uid"]
    assert event_uid and revision_uid
    assert [source["source_name"] for source in saved["citations"][0]["sources"]] == ["AI Feed", "Followup Feed"]
    assert saved["citations"][0]["sources"][0]["title"] == "Nvidia announces a new AI chip"
    assert not saved["citations"][0]["sources"][0]["title"].startswith("![")
    assert client.get("/clusters").json()[0]["read_status"] == "unread"
    assert user_state_snapshot() == state_before_generation
    with Session() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0

    set_object_user_state(
        client,
        "report",
        saved["object_id"],
        operation_id="93000000-0000-4000-8000-000000000000",
        read_status="summary_seen",
    )
    set_object_user_state(
        client,
        "report",
        saved["object_id"],
        operation_id="93010000-0000-4000-8000-000000000000",
        read_later=True,
    )
    set_object_user_state(
        client,
        "report",
        saved["object_id"],
        operation_id="93020000-0000-4000-8000-000000000000",
        starred=True,
    )
    saved = client.get("/reports", params={"period": "day", "date": "2026-06-29"}).json()
    assert saved["read_status"] == "summary_seen"
    assert saved["read_later"] is True
    assert saved["starred"] is True

    with Session() as session:
        report_states = session.query(UserState).filter_by(object_type="report").order_by(UserState.object_id).all()
        states_by_id = {state.object_id: state for state in report_states}
        assert set(states_by_id) == {report_state_key(period, start) for period, start in report_starts.items()}
        assert states_by_id[report_state_key("day", report_starts["day"])].read_status == "summary_seen"
        assert states_by_id[report_state_key("week", report_starts["week"])].read_status == "original_opened"
        assert states_by_id[report_state_key("month", report_starts["month"])].read_status == "summary_seen"
        assert session.query(UserState).filter_by(object_type="cluster").count() == 0
        day_result = session.scalar(
            select(GenerationResult)
            .join(GenerationRequest, GenerationRequest.id == GenerationResult.request_id)
            .where(GenerationRequest.task_type == "report:day")
        )
        assert day_result is not None
        stored_payload = json.loads(json.dumps(day_result.payload_json))
        for citation in stored_payload["citations"]:
            citation.pop("event_uid", None)
            citation.pop("event_revision_uid", None)
        day_result.payload_json = stored_payload
        session.commit()

    for source_id in source_ids:
        assert client.delete(f"/sources/{source_id}").status_code == 204
    assert client.get(f"/clusters/{report_cluster_id}").status_code == 404
    historical = client.get("/reports", params={"period": "day", "date": "2026-06-29"}).json()
    assert historical["citations"][0]["event_uid"] == event_uid
    assert historical["citations"][0]["event_revision_uid"] == revision_uid
    assert client.get(f"/events/{event_uid}/revisions/{revision_uid}").status_code == 200
    with Session() as session:
        persisted = session.scalar(
            select(GenerationResult)
            .join(GenerationRequest, GenerationRequest.id == GenerationResult.request_id)
            .where(GenerationRequest.task_type == "report:day")
        )
        assert persisted is not None and persisted.payload_json == stored_payload





def test_report_response_preserves_filtered_citation_numbers() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    start, _end = report_bounds("day", "2026-07-10")
    citations = [
        {
            "citation_no": number,
            "cluster_id": number,
            "title": f"Evidence {number}",
            "first_seen_at": None,
            "last_seen_at": None,
            "sources": [],
        }
        for number in (2, 1)
    ]
    with Session() as session:
        session.add(
            LLMTask(
                task_type="report:day",
                provider="local-chat",
                object_type="report",
                object_id=report_key(start),
                status="complete",
                prompt_version="report-v3",
                model_version="test-model",
                result_json=json.dumps(
                    {
                        "title": "Filtered report",
                        "body": "Second [2], then first [1]",
                        "citations": citations,
                        "cluster_ids": [2, 1],
                    }
                ),
            )
        )
        session.commit()

    report = TestClient(app).get(
        "/reports", params={"period": "day", "date": "2026-07-10"}
    )

    assert report.status_code == 200
    assert [citation["citation_no"] for citation in report.json()["citations"]] == [
        2,
        1,
    ]


def test_report_generation_ignores_inactive_source_clusters(monkeypatch) -> None:
    prompts: list[str] = []

    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            prompts.append(input_text)
            return {"text": '{"title":"日报标题","body":"报告正文 [1]"}'}

    monkeypatch.setattr("reader_api.main.LocalChatProvider", FakeProvider)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        active_source = Source(name="Active Feed", url="https://example.com/active.xml")
        muted_source = Source(name="Muted Feed", url="https://example.com/muted.xml")
        video_source = Source(name="Video Feed", url="https://example.com/video.xml", media_type="video")
        session.add_all([active_source, muted_source, video_source])
        session.flush()
        report_items = []
        for source, title in [(active_source, "Active story"), (muted_source, "Muted story"), (video_source, "Video story")]:
            raw = make_raw_entry(source_id=source.id, external_id=title, title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                published_at=datetime(2026, 6, 29, 6, tzinfo=timezone.utc),
                content_hash=content_hash(title),
                normalized_title=normalize_title(title),
            )
            session.add(item)
            session.flush()
            report_items.append(item)
        with clustering_run(
            session,
            scope_type="report-source-filter-test",
            item_ids=[item.id for item in report_items[:2]],
            rule_version="report-source-filter-test-v1",
        ):
            for item in report_items[:2]:
                assign_cluster(session, item)
        assign_cluster(session, report_items[2])
        muted_source.status = "muted"
        session.add(
            GenerationControl(
                id=1,
                global_pause=False,
                daily_budget_tokens=10_000_000,
            )
        )
        session.commit()

    client = TestClient(app)
    body = client.post("/reports/generate", params={"period": "day", "date": "2026-06-29"}).json()

    assert "Active story" in prompts[0]
    assert "Muted story" not in prompts[0]
    assert "Video story" not in prompts[0]
    assert [citation["title"] for citation in body["citations"]] == ["Active story"]
    assert [cluster["title"] for cluster in client.get("/clusters").json()] == ["Active story"]


def test_search_scopes_folder_source_and_multilingual_keywords() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        tech = Folder(name="科技")
        business = Folder(name="商业")
        session.add_all([tech, business])
        session.flush()
        tech_source = Source(name="AI Feed", url="https://example.com/ai.xml", folder_id=tech.id)
        business_source = Source(name="Market Feed", url="https://example.com/market.xml", folder_id=business.id)
        session.add_all([tech_source, business_source])
        session.flush()
        tech_id = tech.id
        business_source_id = business_source.id
        for source, title, body in [
            (tech_source, "英伟达发布 Nvidia AI 芯片", "英伟达发布面向数据中心的 Nvidia AI 芯片。"),
            (business_source, "Nvidia stock moves after earnings", "Market update about Nvidia revenue."),
        ]:
            raw = make_raw_entry(source_id=source.id, external_id=title, title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=body)
            session.add(doc)
            session.flush()
            session.add(
                ContentItem(
                    document_id=doc.id,
                    source_id=source.id,
                    title=title,
                    summary=body,
                    content_text=body,
                    content_hash=content_hash(title, body),
                    normalized_title=normalize_title(title),
                )
            )
        session.commit()

    client = TestClient(app)
    assert [item["title"] for item in client.get("/search", params={"q": "英伟达"}).json()] == ["英伟达发布 Nvidia AI 芯片"]
    assert {item["title"] for item in client.get("/search", params={"q": "Nvidia"}).json()} == {
        "英伟达发布 Nvidia AI 芯片",
        "Nvidia stock moves after earnings",
    }
    assert [item["title"] for item in client.get("/search", params={"q": "Nvidia", "folder_id": tech_id}).json()] == ["英伟达发布 Nvidia AI 芯片"]
    assert [item["title"] for item in client.get("/search", params={"q": "Nvidia", "source_id": business_source_id}).json()] == [
        "Nvidia stock moves after earnings"
    ]
    assert [item["title"] for item in client.get("/search", params={"q": "Nvidia earnings"}).json()] == ["Nvidia stock moves after earnings"]
    assert [item["title"] for item in client.get("/search", params={"q": "Nvidia 芯片"}).json()] == ["英伟达发布 Nvidia AI 芯片"]


def test_clusters_filter_by_folder_or_source_without_trimming_sources() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        tech = Folder(name="科技")
        business = Folder(name="商业")
        session.add_all([tech, business])
        session.flush()
        tech_source = Source(name="Tech Feed", url="https://example.com/tech.xml", folder_id=tech.id)
        business_source = Source(name="Business Feed", url="https://example.com/business.xml", folder_id=business.id)
        session.add_all([tech_source, business_source])
        session.flush()
        shared = Cluster(
            cluster_key="cluster-shared",
            title="Shared story",
            generated_title="AI 合成标题：Epic Games case",
            first_seen_at=datetime(2026, 6, 30, 10, tzinfo=timezone.utc),
        )
        business_only = Cluster(cluster_key="cluster-business", title="Business only", first_seen_at=datetime(2026, 6, 30, 9, tzinfo=timezone.utc))
        session.add_all([shared, business_only])
        session.flush()

        def add_item(source: Source, cluster: Cluster, title: str, published_at: datetime | None = None) -> int:
            raw = make_raw_entry(source_id=source.id, external_id=f"{source.id}-{title}", title=title, content_hash=content_hash(title, source.url))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                published_at=published_at,
                content_hash=content_hash(title, source.url),
            )
            session.add(item)
            session.flush()
            return item.id

        item_ids = [
            add_item(tech_source, shared, "Shared story from tech", datetime(2026, 6, 30, 10, 5, tzinfo=timezone.utc)),
            add_item(business_source, shared, "Shared story from business", datetime(2026, 6, 30, 10, tzinfo=timezone.utc)),
            add_item(business_source, business_only, "Business only"),
        ]
        session.flush()
        with clustering_run(
            session,
            scope_type="cluster-filter-event-state-test",
            item_ids=item_ids,
            rule_version="cluster-filter-event-state-test-v1",
        ):
            session.add_all(
                [
                    ClusterItem(cluster_id=shared.id, content_item_id=item_ids[0]),
                    ClusterItem(cluster_id=shared.id, content_item_id=item_ids[1]),
                    ClusterItem(cluster_id=business_only.id, content_item_id=item_ids[2]),
                ]
            )
            session.flush()
        tech_id = tech.id
        tech_source_id = tech_source.id
        business_source_id = business_source.id
        session.commit()

    client = TestClient(app)
    initial_clusters = {
        cluster["title"]: cluster for cluster in client.get("/clusters").json()
    }
    set_cluster_event_state(
        client,
        initial_clusters["Shared story"],
        operation_id="92200000-0000-4000-8000-000000000000",
        action="read_status_set",
        value="summary_seen",
    )
    set_cluster_event_state(
        client,
        initial_clusters["Business only"],
        operation_id="92210000-0000-4000-8000-000000000000",
        action="starred_set",
        value=True,
    )
    tech_clusters = client.get("/clusters", params={"folder_id": tech_id}).json()
    tech_source_clusters = client.get("/clusters", params={"source_id": tech_source_id}).json()
    business_source_clusters = client.get("/clusters", params={"source_id": business_source_id}).json()

    assert [cluster["title"] for cluster in tech_clusters] == ["Shared story"]
    assert tech_clusters[0]["item_count"] == 2
    assert [item["source_name"] for item in tech_clusters[0]["items"]] == ["Business Feed", "Tech Feed"]
    assert [cluster["title"] for cluster in tech_source_clusters] == ["Shared story"]
    assert [cluster["title"] for cluster in business_source_clusters] == ["Shared story", "Business only"]
    searched_clusters = client.get("/clusters", params={"q": "Epic Games"}).json()
    assert [cluster["title"] for cluster in searched_clusters] == ["Shared story"]
    assert searched_clusters[0]["item_count"] == 2
    assert [item["source_name"] for item in searched_clusters[0]["items"]] == ["Business Feed", "Tech Feed"]
    assert [item["source_name"] for item in client.get(f"/clusters/{tech_clusters[0]['id']}").json()["items"]] == ["Business Feed", "Tech Feed"]
    assert [cluster["title"] for cluster in client.get("/clusters", params={"q": "Business only"}).json()] == ["Business only"]
    assert client.get("/clusters/count", params={"source_id": business_source_id}).json() == {"count": 2}
    assert client.get("/clusters/count", params={"q": "Epic Games"}).json() == {"count": 1}
    assert client.get("/clusters/count", params={"read_status": "unread"}).json() == {"count": 1}
    assert client.get("/clusters/count", params={"starred": True}).json() == {"count": 1}


def test_pipeline_status_uses_latest_embedding_completion_time() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    with sessionmaker(bind=engine)() as session:
        source = Source(name="Pipeline Feed", url="https://example.com/pipeline.xml", last_fetched_at=datetime(2026, 1, 1, 0, 3, tzinfo=timezone.utc))
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="pipeline", title="Pipeline", content_hash=content_hash("Pipeline"))
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title="Pipeline", content_text="Pipeline")
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title="Pipeline",
            summary="Pipeline",
            content_text="Pipeline",
            content_hash=content_hash("Pipeline"),
            embedding_model="embedding-model",
            created_at=datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc),
        )
        session.add(item)
        session.flush()
        session.add(
            ContentEmbedding(
                content_item_id=item.id,
                representation="zh_canonical",
                model="embedding-model",
                vector="[1.0,0.0]",
                created_at=datetime(2026, 1, 1, 0, 2, tzinfo=timezone.utc),
            )
        )
        session.commit()

    assert client.get("/pipeline/status").json()["completed_at"].startswith("2026-01-01T00:02:00")


def test_sources_include_recent_entry_count_for_frequency_display() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    with sessionmaker(bind=engine)() as session:
        active = Source(name="Frequent Feed", url="https://example.com/frequent.xml")
        empty = Source(name="Empty Feed", url="https://example.com/empty.xml")
        session.add_all([active, empty])
        session.flush()
        session.add_all(
            [
                make_raw_entry(source_id=active.id, external_id="recent-1", title="Recent 1", content_hash=content_hash("Recent 1"), fetched_at=datetime.now(timezone.utc) - timedelta(days=1)),
                make_raw_entry(source_id=active.id, external_id="recent-2", title="Recent 2", content_hash=content_hash("Recent 2"), fetched_at=datetime.now(timezone.utc) - timedelta(days=5)),
                make_raw_entry(source_id=active.id, external_id="old", title="Old", content_hash=content_hash("Old"), fetched_at=datetime.now(timezone.utc) - timedelta(days=31)),
            ]
        )
        session.commit()

    rows = {source["name"]: source for source in client.get("/sources").json()}

    assert rows["Frequent Feed"]["recent_entry_count_30d"] == 2
    assert rows["Empty Feed"]["recent_entry_count_30d"] == 0


def test_pipeline_overview_reports_counts_and_queue_metrics(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    class FakeRedis:
        @staticmethod
        def from_url(url: str) -> "FakeConnection":
            return FakeConnection()

    class FakeConnection:
        def zrange(self, key: str, start: int, end: int) -> list[bytes]:
            if key == "rq:wip:reader-fetch":
                return [b"fetch-running:execution"]
            return []

    class FakeQueue:
        def __init__(self, name: str, connection: FakeConnection) -> None:
            self.name = name
            self.connection = connection
            self.jobs = [object(), object()] if name == "reader-fetch" else [object()]

    class FakeStartedRegistry:
        def __init__(self, name: str, connection: FakeConnection) -> None:
            self.name = name

        def get_job_ids(self) -> list[str]:
            return ["llm-running"] if self.name == "reader-llm" else []

    monkeypatch.setattr("reader_api.main.Redis", FakeRedis)
    monkeypatch.setattr("reader_api.main.Queue", FakeQueue)
    monkeypatch.setattr("reader_api.main.StartedJobRegistry", FakeStartedRegistry)

    with sessionmaker(bind=engine)() as session:
        source_ok = Source(name="OK Feed", url="https://example.com/ok.xml", last_fetched_at=datetime(2026, 1, 1, 0, 3, tzinfo=timezone.utc))
        source_failed = Source(name="Failed Feed", url="https://example.com/failed.xml", last_error="timeout")
        session.add_all([source_ok, source_failed])
        session.flush()
        raw = make_raw_entry(source_id=source_ok.id, external_id="pending", title="Pending", content_hash=content_hash("Pending"))
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title="Pending", content_text="Pending")
        session.add(document)
        session.flush()
        session.add(ContentItem(document_id=document.id, source_id=source_ok.id, title="Pending", content_text="Pending", content_hash=content_hash("Pending")))
        session.add(LLMTask(task_type="translation:text", provider="local-chat", object_type="text", object_id=1, status="complete", created_at=datetime.now(timezone.utc)))
        session.add(LLMTask(task_type="item-summary", provider="local", object_type="item", object_id=1, status="pending"))
        session.add(LLMTask(task_type="cluster-synthesis", provider="local", object_type="cluster", object_id=1, status="running"))
        session.add(LLMTask(task_type="report:day", provider="local", object_type="report", object_id=1, status="failed", result_json=json.dumps({"error": "模型超时，请重试"}, ensure_ascii=False)))
        session.commit()

    overview = client.get("/pipeline/overview").json()

    assert overview["rss"]["source_count"] == 2
    assert overview["rss"]["failed_source_count"] == 1
    assert overview["rss"]["last_completed_at"].startswith("2026-01-01T00:03:00")
    assert overview["rss"]["queue"] == {"available": True, "queued": 2, "running": 1, "error": ""}
    assert overview["embedding"]["pending_items"] == 1
    assert overview["embedding"]["queue"] == {"available": True, "queued": 1, "running": 1, "error": ""}
    assert overview["translation"]["cached_24h"] == 1
    assert overview["generation"] == {
        "pending": 0,
        "running": 0,
        "failed": 0,
        "complete": 0,
        "latest_failed_error": "",
    }


def test_pipeline_overview_degrades_when_redis_is_unavailable(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    class BrokenRedis:
        @staticmethod
        def from_url(url: str) -> object:
            raise RuntimeError("redis://:secret@private.example unavailable")

    monkeypatch.setattr("reader_api.main.Redis", BrokenRedis)

    overview = client.get("/pipeline/overview").json()

    assert overview["rss"]["queue"]["available"] is False
    assert overview["embedding"]["queue"]["available"] is False
    assert overview["rss"]["queue"]["error"] == "Redis 未连接"
    assert "secret@private.example" not in json.dumps(overview)


def test_generation_overview_hides_failure_details(monkeypatch) -> None:
    from reader_api.main import generation_task_overview

    monkeypatch.setattr(
        "reader_api.main.generation_tasks_out",
        lambda _session, _requests: [
            SimpleNamespace(
                status="failed",
                error="RuntimeError: token@private.example",
                last_apply_error="",
            )
        ],
    )

    with sessionmaker(bind=engine)() as session:
        counts, error = generation_task_overview(session)

    assert counts == {"failed": 1}
    assert error == "生成任务失败，请在任务分配中查看详情"


def test_generation_get_routes_are_readable(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(
        "reader_api.main.queue_overview",
        lambda _name: {"available": True, "queued": 0, "running": 0, "error": ""},
    )
    client = TestClient(app)

    assert client.get("/generation/tasks").status_code == 200
    assert client.get("/generation/requests/missing-request").status_code == 404
    assert client.get("/generation/control").status_code == 200
    assert client.get("/pipeline/overview").status_code == 200


def test_generation_tasks_query_count_is_constant_for_many_completed_requests(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with sessionmaker(bind=engine)() as session:
        session.add(
            GenerationControl(
                id=1,
                global_pause=True,
                auto_run=False,
                daily_budget_tokens=None,
                input_estimator="unicode-codepoints-v1",
                output_reserve_tokens=0,
                day_timezone="Asia/Shanghai",
            )
        )
        for index in range(24):
            fingerprint = f"{index + 1:064x}"
            request = GenerationRequest(
                request_fingerprint=fingerprint,
                input_fingerprint=fingerprint,
                task_type="test-pending",
                reason="query-count",
                target_type="item",
                target_id=index + 1,
                target_uid=f"00000000-0000-4000-8000-{index + 1:012d}",
                provider="local-chat",
                model="test-model",
                prompt_version="test-v1",
                schema_version="test-v1",
                privacy_status="local",
            )
            session.add(request)
            session.flush()
            session.add_all(
                [
                    GenerationAdmission(request_id=request.id),
                    GenerationRequestPayload(
                        request_id=request.id,
                        payload_json={"input": f"task {index}"},
                        application_context_json=None,
                        payload_fingerprint=fingerprint,
                    ),
                ]
            )
            session.flush()
            finished_at = datetime.now(timezone.utc)
            attempt = GenerationAttempt(
                request_id=request.id,
                attempt_no=1,
                retry_kind="initial",
                status="complete",
                input_tokens_actual=10,
                output_tokens_actual=5,
                started_at=finished_at,
                finished_at=finished_at,
                error="",
            )
            session.add(attempt)
            session.flush()
            result = GenerationResult(
                request_id=request.id,
                attempt_id=attempt.id,
                payload_json={"summary": "stale"},
                payload_fingerprint=fingerprint,
                input_tokens=10,
                output_tokens=5,
            )
            session.add(result)
            session.flush()
            session.add(
                GenerationApplication(
                    request_id=request.id,
                    result_id=result.id,
                    status="failed",
                    error="生成结果已过期，不能应用",
                    last_error="生成结果已过期，不能应用",
                    apply_attempt_count=1,
                )
            )
        session.commit()

    monkeypatch.setattr(
        "reader_api.generation_lifecycle.generation_result_currency",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("Generation 列表不得逐任务执行深度结果校验")
        ),
    )

    statements: list[str] = []

    def capture_selects(_conn, _cursor, statement, _parameters, _context, _many) -> None:
        if statement.lstrip().upper().startswith(("SELECT", "WITH")):
            statements.append(statement)

    event.listen(engine, "before_cursor_execute", capture_selects)
    try:
        response = TestClient(app).get("/generation/tasks", params={"limit": 100})
    finally:
        event.remove(engine, "before_cursor_execute", capture_selects)

    assert response.status_code == 200
    assert len(response.json()) == 24
    assert {task["status"] for task in response.json()} == {"apply_failed"}
    assert {task["result_currency"] for task in response.json()} == {"unverified"}
    assert all(task["can_reapply"] is False for task in response.json())
    assert len(statements) <= 12, f"Generation 列表仍按任务线性查询：{len(statements)} 次"
    client = TestClient(app)
    all_tasks = client.get("/generation/tasks", params={"limit": 100}).json()
    first_page = client.get("/generation/tasks", params={"limit": 5}).json()
    with sessionmaker(bind=engine)() as session:
        session.add(
            GenerationRequest(
                request_fingerprint="f" * 64,
                input_fingerprint="f" * 64,
                task_type="test-newer",
                reason="query-count",
                target_type="item",
                target_id=999,
                target_uid="00000000-0000-4000-8000-000000000999",
                provider="local-chat",
                model="test-model",
                prompt_version="test-v1",
                schema_version="test-v1",
                privacy_status="local",
                created_at=datetime.now(timezone.utc) + timedelta(days=1),
            )
        )
        session.commit()
    second_page = client.get(
        "/generation/tasks",
        params={
            "limit": 5,
            "before_request_uid": first_page[-1]["request_uid"],
        },
    ).json()
    assert [task["request_uid"] for task in second_page] == [
        task["request_uid"] for task in all_tasks[5:10]
    ]
    assert client.get(
        "/generation/tasks",
        params={"before_request_uid": "missing-request"},
    ).status_code == 400


def test_api_requires_service_token_outside_health(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(app.state, "api_auth_disabled_for_tests", False)
    monkeypatch.setattr(
        "reader_api.main.settings",
        SimpleNamespace(api_token="reader-secret", database_url="postgresql://reader"),
    )
    monkeypatch.setattr("reader_api.main.check_db_health", lambda: {"ok": True, "detail": "SELECT 1"})
    monkeypatch.setattr("reader_api.main.check_redis_health", lambda: {"ok": True, "detail": "PING"})
    monkeypatch.setattr("reader_api.main.check_http_endpoint_health", lambda _label, _endpoint: {"ok": True, "detail": "HTTP 200"})
    client = TestClient(app)

    assert client.get("/health").status_code == 200
    assert client.get("/about").status_code == 401
    assert client.get("/about", headers={"X-Reader-API-Token": "wrong"}).status_code == 401
    assert client.get("/about", headers={"X-Reader-API-Token": "reader-secret"}).status_code == 200

    monkeypatch.setattr(
        "reader_api.main.settings",
        SimpleNamespace(api_token="", database_url="sqlite:///:memory:"),
    )
    assert client.get("/about").status_code == 503


def test_about_reports_version_metadata_and_health(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    monkeypatch.setenv("READER_VERSION", "v-test")
    monkeypatch.setenv("READER_COMMIT", "abc1234")
    monkeypatch.setenv("READER_BUILD_TIME", "2026-01-01T00:00:00Z")
    monkeypatch.setenv("READER_DEPLOY_URL", "http://reader.local")
    monkeypatch.setattr("reader_api.main.check_db_health", lambda: {"ok": True, "detail": "SELECT 1"})
    monkeypatch.setattr("reader_api.main.check_redis_health", lambda: {"ok": False, "detail": "redis down"})
    monkeypatch.setattr("reader_api.main.check_http_endpoint_health", lambda label, endpoint: {"ok": True, "detail": endpoint})

    about = client.get("/about").json()

    assert about["version"] == "v-test"
    assert about["commit"] == "abc1234"
    assert about["build_time"] == "2026-01-01T00:00:00Z"
    assert about["deploy_url"] == "http://reader.local"
    assert {doc["label"] for doc in about["docs"]} == {"产品说明", "架构"}
    assert about["health"]["db"]["ok"] is True
    assert about["health"]["redis"] == {"label": "Redis", "ok": False, "detail": "redis down"}
    assert about["health"]["llm"]["ok"] is True
    assert about["health"]["llm"]["detail"] == "http://127.0.0.1:1234/api/v1/models"
    assert about["health"]["embedding"]["ok"] is True


def test_article_image_proxy_serves_persistent_cache_and_reports_usage(
    monkeypatch,
    tmp_path,
) -> None:
    from reader_api.article_image_cache import (
        ArticleImageCache,
        DownloadedImage,
    )

    monkeypatch.setenv("READER_ARTICLE_IMAGE_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("READER_ARTICLE_IMAGE_CACHE_MAX_BYTES", "100")
    url = "https://cdn.example.com/original.png"
    original = b"\x89PNG\r\n\x1a\noriginal"
    key = ArticleImageCache(tmp_path, max_bytes=100).store(
        url,
        DownloadedImage(original, "image/png"),
    )

    client = TestClient(app)
    response = client.get(
        f"/images/article/{key}",
        params={"src": url},
    )
    about = client.get("/about").json()

    assert response.status_code == 200
    assert response.content == original
    assert response.headers["content-type"] == "image/png"
    assert about["article_image_cache"] == {
        "used_bytes": len(original),
        "max_bytes": 100,
    }


def test_article_image_proxy_offloads_from_shared_api_request_pool(
    monkeypatch,
) -> None:
    calls: list[tuple[object, tuple[object, ...]]] = []

    async def fake_to_thread(function, *args):
        calls.append((function, args))
        return main_module.Response(content=b"image", media_type="image/png")

    monkeypatch.setattr(main_module.asyncio, "to_thread", fake_to_thread)

    response = asyncio.run(
        main_module.article_image("a" * 64, "https://example.com/image.png", None)
    )

    assert response.body == b"image"
    assert calls == [
        (
            main_module._article_image,
            ("a" * 64, "https://example.com/image.png", None),
        )
    ]


def test_article_image_proxy_refetches_only_matching_registered_url(
    monkeypatch,
    tmp_path,
) -> None:
    from reader_api.article_image_cache import (
        DownloadedImage,
        cache_key_for_url,
    )

    monkeypatch.setenv("READER_ARTICLE_IMAGE_CACHE_DIR", str(tmp_path))
    url = "https://cdn.example.com/missing.jpg"
    key = cache_key_for_url(url)
    calls: list[str] = []
    deadlines: list[float] = []
    monkeypatch.setattr("reader_api.main.time.monotonic", lambda: 100.0)
    monkeypatch.setattr(
        "reader_api.main.download_image",
        lambda requested_url, **kwargs: (
            calls.append(requested_url)
            or deadlines.append(kwargs["deadline"])
            or DownloadedImage(b"\xff\xd8\xfforiginal", "image/jpeg")
        ),
    )
    client = TestClient(app)

    rejected = client.get(
        f"/images/article/{key}",
        params={"src": "https://attacker.example/wrong.jpg"},
    )
    fetched = client.get(
        f"/images/article/{key}",
        headers={"X-Reader-Image-Source": url},
    )
    cached = client.get(f"/images/article/{key}")

    assert rejected.status_code == 404
    assert fetched.content == b"\xff\xd8\xfforiginal"
    assert cached.content == fetched.content
    assert calls == [url]
    assert deadlines == [103.0]


def test_article_image_proxy_serializes_concurrent_cache_misses(
    monkeypatch,
    tmp_path,
) -> None:
    from reader_api.article_image_cache import (
        DownloadedImage,
        cache_key_for_url,
    )

    monkeypatch.setenv("READER_ARTICLE_IMAGE_CACHE_DIR", str(tmp_path))
    url = "https://cdn.example.com/concurrent.jpg"
    key = cache_key_for_url(url)
    calls: list[str] = []
    concurrent_downloads = Barrier(2)

    def fake_download(
        requested_url: str,
        **_kwargs: object,
    ) -> DownloadedImage:
        calls.append(requested_url)
        try:
            concurrent_downloads.wait(timeout=0.25)
        except BrokenBarrierError:
            pass
        return DownloadedImage(b"\xff\xd8\xfforiginal", "image/jpeg")

    monkeypatch.setattr("reader_api.main.download_image", fake_download)

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(
            executor.map(
                lambda _index: TestClient(app).get(
                    f"/images/article/{key}",
                    params={"src": url},
                ),
                range(2),
            )
        )

    assert [response.status_code for response in responses] == [200, 200]
    assert calls == [url]


def test_article_image_proxy_persists_failed_first_open_attempt(
    monkeypatch,
    tmp_path,
) -> None:
    from reader_api.article_image_cache import cache_key_for_url

    monkeypatch.setenv("READER_ARTICLE_IMAGE_CACHE_DIR", str(tmp_path))
    url = "https://cdn.example.com/unavailable.jpg"
    key = cache_key_for_url(url)
    calls: list[str] = []
    monkeypatch.setattr(
        "reader_api.main.download_image",
        lambda requested_url, **_kwargs: calls.append(requested_url),
    )
    client = TestClient(app)

    first = client.get(f"/images/article/{key}", params={"src": url})
    second = client.get(f"/images/article/{key}", params={"src": url})

    assert [first.status_code, second.status_code] == [502, 502]
    assert calls == [url]


def test_about_reports_http_404_model_endpoints_as_unhealthy(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    class NotFoundClient:
        def __init__(self, **_: object) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_: object) -> None:
            pass

        def get(self, _endpoint: str) -> SimpleNamespace:
            return SimpleNamespace(status_code=404)

    monkeypatch.setattr("reader_api.main.check_db_health", lambda: {"ok": True, "detail": "SELECT 1"})
    monkeypatch.setattr("reader_api.main.check_redis_health", lambda: {"ok": True, "detail": "PING"})
    monkeypatch.setattr("reader_api.main.httpx.Client", NotFoundClient)

    health = TestClient(app).get("/about").json()["health"]

    assert health["llm"] == {"label": "LM Studio LLM", "ok": False, "detail": "HTTP 404"}
    assert health["embedding"] == {"label": "Embedding", "ok": False, "detail": "HTTP 404"}


def test_about_hides_raw_external_health_errors(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    class FailingClient:
        def __init__(self, **_: object) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_: object) -> None:
            pass

        def get(self, _endpoint: str) -> None:
            raise RuntimeError("connection refused: http://token@private.example/models")

    monkeypatch.setattr("reader_api.main.check_db_health", lambda: {"ok": True, "detail": "SELECT 1"})
    monkeypatch.setattr("reader_api.main.check_redis_health", lambda: {"ok": True, "detail": "PING"})
    monkeypatch.setattr("reader_api.main.httpx.Client", FailingClient)

    response = TestClient(app).get("/about")
    health = response.json()["health"]

    assert health["llm"]["detail"] == "LM Studio LLM 不可达"
    assert health["embedding"]["detail"] == "Embedding 不可达"
    assert "token@private.example" not in response.text


def test_about_releases_database_connection_before_external_health_checks(monkeypatch, tmp_path) -> None:
    isolated_engine = create_engine(
        f"sqlite:///{tmp_path / 'about.db'}",
        poolclass=QueuePool,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.1,
    )
    Base.metadata.create_all(isolated_engine)
    IsolatedSession = sessionmaker(bind=isolated_engine)

    def database_available(_label: str, _endpoint: str) -> dict[str, object]:
        with IsolatedSession() as session:
            session.execute(text("SELECT 1"))
        return {"ok": True, "detail": "HTTP 200"}

    monkeypatch.setattr("reader_api.db.SessionLocal", IsolatedSession)
    monkeypatch.setattr("reader_api.main.SessionLocal", IsolatedSession)
    monkeypatch.setattr("reader_api.main.check_db_health", lambda: {"ok": True, "detail": "SELECT 1"})
    monkeypatch.setattr("reader_api.main.check_redis_health", lambda: {"ok": True, "detail": "PING"})
    monkeypatch.setattr("reader_api.main.check_http_endpoint_health", database_available)

    health = TestClient(app).get("/about").json()["health"]

    assert health["llm"]["ok"] is True
    assert health["embedding"]["ok"] is True


def test_about_health_checks_run_independently(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    monkeypatch.setattr("reader_api.main.check_db_health", lambda: {"ok": True, "detail": "SELECT 1"})
    monkeypatch.setattr("reader_api.main.check_redis_health", lambda: (_ for _ in ()).throw(RuntimeError("redis down")))
    monkeypatch.setattr("reader_api.main.check_http_endpoint_health", lambda label, endpoint: {"ok": label == "Embedding", "detail": endpoint if label == "Embedding" else "connection refused"})

    about = client.get("/about").json()

    assert about["health"]["db"]["ok"] is True
    assert about["health"]["redis"]["ok"] is False
    assert about["health"]["redis"]["detail"] == "Redis 健康检查失败"
    assert about["health"]["llm"]["ok"] is False
    assert about["health"]["embedding"]["ok"] is True


def test_about_isolates_ai_setting_store_failures(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)

    monkeypatch.setattr("reader_api.main.check_db_health", lambda: {"ok": True, "detail": "SELECT 1"})
    monkeypatch.setattr("reader_api.main.check_redis_health", lambda: {"ok": True, "detail": "PING"})
    monkeypatch.setattr(
        "reader_api.main.runtime_ai_settings",
        lambda session: (_ for _ in ()).throw(OperationalError("SELECT app_settings", {"value": "secret"}, RuntimeError("database unavailable"))),
    )
    monkeypatch.setattr(
        "reader_api.main.check_http_endpoint_health",
        lambda label, endpoint: pytest.fail(f"AI endpoint check should not run: {label} {endpoint}"),
    )

    response = client.get("/about")

    assert response.status_code == 200
    about = response.json()
    assert about["health"]["db"]["ok"] is True
    assert about["health"]["redis"]["ok"] is True
    assert about["health"]["llm"] == {"label": "LM Studio LLM", "ok": False, "detail": "AI 设置读取失败"}
    assert about["health"]["embedding"] == {"label": "Embedding", "ok": False, "detail": "AI 设置读取失败"}
    assert "secret" not in response.text


def test_favicon_endpoint_fetches_and_caches_success(monkeypatch, tmp_path) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    calls: list[str] = []

    monkeypatch.setenv("READER_FAVICON_CACHE_DIR", str(tmp_path))
    monkeypatch.setattr("reader_api.main.FAVICON_MEMORY_CACHE", {})

    def fake_fetch(domain: str):
        calls.append(domain)
        return {"body": b"png", "content_type": "image/png"}

    monkeypatch.setattr("reader_api.main.fetch_favicon_bytes", fake_fetch)

    first = client.get("/images/favicon", params={"domain": "Example.com"})
    second = client.get("/images/favicon", params={"domain": "example.com"})

    assert first.status_code == 200
    assert first.content == b"png"
    assert first.headers["x-reader-favicon-cache"] == "miss"
    assert second.content == b"png"
    assert second.headers["x-reader-favicon-cache"] == "hit"
    assert calls == ["example.com"]


def test_favicon_endpoint_negative_cache_avoids_repeated_failure_fetches(monkeypatch, tmp_path) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    calls: list[str] = []

    monkeypatch.setenv("READER_FAVICON_CACHE_DIR", str(tmp_path))
    monkeypatch.setattr("reader_api.main.FAVICON_MEMORY_CACHE", {})

    def fake_fetch(domain: str):
        calls.append(domain)
        return None

    monkeypatch.setattr("reader_api.main.fetch_favicon_bytes", fake_fetch)

    first = client.get("/images/favicon", params={"domain": "missing.example"})
    second = client.get("/images/favicon", params={"domain": "missing.example"})

    assert first.status_code == 404
    assert first.content == b""
    assert first.headers["x-reader-favicon-cache"] == "miss-negative"
    assert second.status_code == 404
    assert second.content == b""
    assert second.headers["x-reader-favicon-cache"] == "hit-negative"
    assert calls == ["missing.example"]


def test_favicon_endpoint_rejects_private_targets_before_request(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("READER_FAVICON_CACHE_DIR", str(tmp_path))
    monkeypatch.setattr("reader_api.main.FAVICON_MEMORY_CACHE", {})

    response = TestClient(app).get(
        "/images/favicon",
        params={"domain": "127.0.0.1"},
    )

    assert response.status_code == 404
    assert response.headers["x-reader-favicon-negative"] == "1"


def test_favicon_endpoint_never_serves_svg_from_cache_or_fetch(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("READER_FAVICON_CACHE_DIR", str(tmp_path))
    monkeypatch.setattr("reader_api.main.FAVICON_MEMORY_CACHE", {})
    monkeypatch.setattr(
        "reader_api.main.fetch_favicon_bytes",
        lambda _domain: {
            "body": b'<svg xmlns="http://www.w3.org/2000/svg"><script/></svg>',
            "content_type": "image/svg+xml",
        },
    )

    first = TestClient(app).get(
        "/images/favicon",
        params={"domain": "active.example"},
    )
    second = TestClient(app).get(
        "/images/favicon",
        params={"domain": "active.example"},
    )

    assert first.status_code == second.status_code == 404
    assert first.content == second.content == b""
    assert first.headers["x-content-type-options"] == "nosniff"
    assert first.headers["content-security-policy"] == "default-src 'none'; sandbox"


def test_fetch_icon_candidate_rejects_empty_images(monkeypatch) -> None:
    monkeypatch.setattr(
        "reader_api.main.fetch_limited",
        lambda *_args, **_kwargs: {"body": b"", "content_type": "image/x-icon"},
    )

    assert fetch_icon_candidate("https://empty.example/favicon.ico") is None


def test_fetch_icon_candidate_rejects_svg_images(monkeypatch) -> None:
    monkeypatch.setattr(
        "reader_api.main.fetch_limited",
        lambda *_args, **_kwargs: {
            "body": b'<svg xmlns="http://www.w3.org/2000/svg"><script/></svg>',
            "content_type": "image/svg+xml",
        },
    )

    assert fetch_icon_candidate("https://active.example/favicon.svg") is None


def test_fetch_favicon_bytes_falls_back_to_google_s2(monkeypatch) -> None:
    candidates: list[str] = []

    def fake_icon_candidate(url: str):
        candidates.append(url)
        if "google.com/s2" in url:
            return {"body": b"google-favicon", "content_type": "image/png"}
        return None

    monkeypatch.setattr("reader_api.main.fetch_icon_candidate", fake_icon_candidate)
    monkeypatch.setattr("reader_api.main.fetch_limited", lambda *_args, **_kwargs: None)

    assert fetch_favicon_bytes("www.ithome.com") == {"body": b"google-favicon", "content_type": "image/png"}
    assert candidates == [
        "https://www.ithome.com/favicon.ico",
        "https://www.google.com/s2/favicons?domain=www.ithome.com&sz=32",
    ]


def test_items_and_search_support_offset_pagination() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="Paged Feed", url="https://example.com/paged.xml")
        session.add(source)
        session.flush()
        for index in range(3):
            title = f"Paged item {index}"
            raw = make_raw_entry(source_id=source.id, external_id=title, title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                published_at=datetime(2026, 6, 29, index, tzinfo=timezone.utc),
                content_hash=content_hash(title),
                normalized_title=normalize_title(title),
            )
            session.add(item)
            session.flush()
            assign_cluster(session, item)
        session.commit()

    client = TestClient(app)
    assert [item["title"] for item in client.get("/items", params={"limit": 2}).json()] == ["Paged item 2", "Paged item 1"]
    assert [item["title"] for item in client.get("/items", params={"limit": 2, "offset": 2}).json()] == ["Paged item 0"]
    assert [item["title"] for item in client.get("/search", params={"q": "Paged", "limit": 1, "offset": 1}).json()] == ["Paged item 1"]
    assert [cluster["title"] for cluster in client.get("/clusters", params={"limit": 2}).json()] == ["Paged item 2", "Paged item 1"]
    assert [cluster["title"] for cluster in client.get("/clusters", params={"limit": 2, "offset": 2}).json()] == ["Paged item 0"]
    assert [cluster["title"] for cluster in client.get("/clusters", params={"q": "Paged", "limit": 1, "offset": 1}).json()] == ["Paged item 1"]
    assert [cluster["title"] for cluster in client.get("/clusters", params={"order": "asc", "limit": 2}).json()] == ["Paged item 0", "Paged item 1"]
    assert [cluster["title"] for cluster in client.get("/clusters", params={"order": "asc", "limit": 2, "offset": 2}).json()] == ["Paged item 2"]
    assert [cluster["title"] for cluster in client.get("/clusters", params={"order": "asc", "q": "Paged", "limit": 1, "offset": 1}).json()] == ["Paged item 1"]
    assert client.get("/clusters", params={"order": "sideways"}).status_code == 400


def test_search_and_user_state_are_not_boolean() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="1", title="Nvidia chip", content_hash=content_hash("Nvidia chip"))
        session.add(raw)
        session.flush()
        doc = Document(raw_entry_id=raw.id, title="Nvidia chip", content_text="Nvidia announces a new AI chip")
        session.add(doc)
        session.flush()
        item = ContentItem(
            document_id=doc.id,
            source_id=source.id,
            title="Nvidia announces a new AI chip",
            summary="New chip for AI workloads",
            content_text="Nvidia announces a new AI chip for datacenters.",
            content_hash=content_hash("Nvidia chip item"),
            normalized_title=normalize_title("Nvidia announces a new AI chip"),
        )
        session.add(item)
        session.flush()
        with clustering_run(
            session,
            scope_type="api-event-state-test",
            item_ids=[item.id],
            rule_version="api-event-state-test-v1",
        ):
            assign_cluster(session, item)
        session.commit()

    client = TestClient(app)
    items = client.get("/search", params={"q": "Nvidia"}).json()
    assert len(items) == 1
    assert client.get("/search", params={"q": "%"}).json() == []
    assert client.get("/search", params={"q": "_"}).json() == []
    assert client.get("/items", params={"read_later": False}).json()[0]["id"] == items[0]["id"]
    assert client.get("/items", params={"starred": False}).json()[0]["id"] == items[0]["id"]
    cluster = client.get("/clusters").json()[0]
    assert cluster["items"][0]["source_name"] == "AI Feed"
    assert cluster["items"][0]["content_text"] == ""
    assert client.get("/clusters", params={"read_status": "unread"}).json()[0]["id"] == cluster["id"]
    assert client.get("/clusters", params={"starred": True}).json() == []
    cluster_detail = client.get(f"/clusters/{cluster['id']}").json()
    assert cluster_detail["item_count"] == 1
    assert cluster_detail["items"][0]["source_name"] == "AI Feed"
    assert cluster_detail["items"][0]["content_text"] == "Nvidia announces a new AI chip for datacenters."
    state = set_cluster_event_state(
        client,
        cluster,
        operation_id="90300000-0000-4000-8000-000000000000",
        action="read_status_set",
        value="summary_seen",
    )
    set_cluster_event_state(
        client,
        cluster,
        operation_id="90333333-3333-4333-8333-333333333333",
        action="starred_set",
        value=True,
    )
    set_cluster_event_state(
        client,
        cluster,
        operation_id="90444444-4444-4444-8444-444444444444",
        action="read_later_set",
        value=True,
    )
    with Session() as session:
        assert session.query(UserState).filter(
            UserState.object_type == "cluster",
            UserState.object_id == cluster["id"],
        ).count() == 0
    assert state["read_status"] == "summary_seen"
    cluster = client.get("/clusters").json()[0]
    assert cluster["read_status"] == "summary_seen"
    assert cluster["starred"] is True
    assert client.get("/clusters", params={"read_status": "unread"}).json() == []
    assert client.get("/clusters", params={"starred": True}).json()[0]["id"] == cluster["id"]
    assert client.get("/clusters", params={"read_later": True}).json()[0]["id"] == cluster["id"]
    cluster_detail = client.get(f"/clusters/{cluster['id']}").json()
    assert cluster_detail["read_later"] is True
    dismissed = client.patch(
        f"/user-state/cluster/{cluster['id']}",
        json={"read_status": "dismissed"},
    )
    assert dismissed.status_code == 400
    assert dismissed.json()["detail"] == "不支持的状态对象类型"
    assert client.get("/clusters").json()[0]["id"] == cluster["id"]
    assert client.get("/clusters", params={"read_status": "dismissed"}).json() == []
    assert client.get(f"/clusters/{cluster['id']}").json()["read_status"] == "summary_seen"
    assert client.get("/sources").json()[0]["unread_count"] == 0
    state = set_object_user_state(
        client,
        "item",
        items[0]["id"],
        operation_id="94000000-0000-4000-8000-000000000000",
        read_status="summary_seen",
    )
    state = set_object_user_state(
        client,
        "item",
        items[0]["id"],
        operation_id="94010000-0000-4000-8000-000000000000",
        starred=True,
    )
    assert state["read_status"] == "summary_seen"
    assert state["starred"] is True
    source = client.get("/sources").json()[0]
    assert source["unread_count"] == 0
    assert source["read_count"] == 2
    assert source["starred_count"] == 2
    assert source["feed_trust_score"] == 100.0
    assert client.get("/items", params={"starred": True}).json()[0]["id"] == items[0]["id"]
    state = set_object_user_state(
        client,
        "item",
        items[0]["id"],
        operation_id="94020000-0000-4000-8000-000000000000",
        starred=False,
    )
    assert state["starred"] is False
    assert client.get("/sources").json()[0]["starred_count"] == 1
    assert client.get("/items", params={"starred": True}).json() == []
    assert client.get("/items", params={"read_later": True}).json() == []
    state = set_object_user_state(
        client,
        "item",
        items[0]["id"],
        operation_id="94030000-0000-4000-8000-000000000000",
        read_later=True,
    )
    assert state["read_later"] is True
    assert client.get("/sources").json()[0]["read_later_count"] == 2
    assert client.get("/items", params={"read_later": True}).json()[0]["id"] == items[0]["id"]
    state = set_object_user_state(
        client,
        "item",
        items[0]["id"],
        operation_id="94040000-0000-4000-8000-000000000000",
        read_later=False,
    )
    assert state["read_later"] is False
    assert client.get("/sources").json()[0]["read_later_count"] == 1
    assert client.get("/items", params={"read_later": True}).json() == []

    state = set_object_user_state(
        client,
        "item",
        items[0]["id"],
        operation_id="94050000-0000-4000-8000-000000000000",
        read_status="original_opened",
    )
    assert state["read_status"] == "original_opened"
    state = set_object_user_state(
        client,
        "item",
        items[0]["id"],
        operation_id="94060000-0000-4000-8000-000000000000",
        read_status="summary_seen",
    )
    assert state["read_status"] == "original_opened"
    source = client.get("/sources").json()[0]
    assert source["read_count"] == 2
    assert source["opened_count"] == 1
    assert source["starred_count"] == 1
    state = set_object_user_state(
        client,
        "item",
        items[0]["id"],
        operation_id="94070000-0000-4000-8000-000000000000",
        read_status="dismissed",
    )
    assert state["read_status"] == "dismissed"
    source = client.get("/sources").json()[0]
    assert source["read_count"] == 1
    assert source["opened_count"] == 0
    assert client.get("/items").json() == []
    assert client.get("/items", params={"read_status": "dismissed"}).json()[0]["id"] == items[0]["id"]
    assert client.get(f"/items/{items[0]['id']}").json()["id"] == items[0]["id"]
    state = set_object_user_state(
        client,
        "item",
        items[0]["id"],
        operation_id="94080000-0000-4000-8000-000000000000",
        read_status="summary_seen",
    )
    assert state["read_status"] == "summary_seen"
    assert client.get("/sources").json()[0]["read_count"] == 2
    state = set_object_user_state(
        client,
        "item",
        items[0]["id"],
        operation_id="94090000-0000-4000-8000-000000000000",
        read_status="original_opened",
    )
    assert state["read_status"] == "original_opened"
    state = set_object_user_state(
        client,
        "item",
        items[0]["id"],
        operation_id="94100000-0000-4000-8000-000000000000",
        read_status="summary_seen",
    )
    assert state["read_status"] == "original_opened"
    state = set_object_user_state(
        client,
        "item",
        items[0]["id"],
        operation_id="94110000-0000-4000-8000-000000000000",
        read_status="unread",
    )
    assert state["read_status"] == "unread"
    assert client.get("/items", params={"read_status": "unread"}).json()[0]["id"] == items[0]["id"]
    set_cluster_event_state(
        client,
        cluster,
        operation_id="90555555-5555-4555-8555-555555555555",
        action="read_status_set",
        value="unread",
    )
    source = client.get("/sources").json()[0]
    assert source["unread_count"] == 1
    assert source["read_count"] == 0
    assert source["opened_count"] == 0


def test_cluster_list_returns_all_sources() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        cluster = Cluster(
            cluster_key="all-sources",
            title="Shared event",
            first_seen_at=datetime(2026, 6, 30, tzinfo=timezone.utc),
            last_seen_at=datetime(2026, 6, 30, tzinfo=timezone.utc),
        )
        session.add(cluster)
        session.flush()
        for index in range(5):
            source = Source(name=f"Feed {index}", url=f"https://example.com/{index}.xml")
            session.add(source)
            session.flush()
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title="Shared event", content_hash=content_hash("raw", str(index)))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title="Shared event", content_text=f"Shared event report {index}")
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=f"Shared event {index}",
                summary=f"Shared event summary {index}",
                content_text=f"Shared event report {index}",
                content_hash=content_hash("item", str(index)),
                normalized_title=normalize_title("Shared event"),
                published_at=datetime(2026, 6, 30, index, tzinfo=timezone.utc),
            )
            session.add(item)
            session.flush()
            session.add(ClusterItem(cluster_id=cluster.id, content_item_id=item.id))
        session.commit()

    cluster = TestClient(app).get("/clusters").json()[0]

    assert [item["source_name"] for item in cluster["items"]] == [f"Feed {index}" for index in range(5)]


def test_postgres_search_uses_fts_with_fuzzy_fallback() -> None:
    session = SimpleNamespace(bind=SimpleNamespace(dialect=SimpleNamespace(name="postgresql")))
    sql = str(search_clause(session, "Nvidia"))

    assert "to_tsvector" in sql
    assert sql.count("to_tsvector") >= 2
    assert "plainto_tsquery" in sql
    assert "content_items.title" in sql
    assert "content_items.content_text" in sql
    assert "sources.name" in sql
    assert "clusters.generated_title" in str(search_clause(session, "Nvidia", include_cluster=True))

    fuzzy_sql = str(search_clause(session, "芯片", include_cluster=True))
    assert "to_tsvector" not in fuzzy_sql
    assert "lower(content_items.title)" in fuzzy_sql
    assert "clusters.generated_title" in fuzzy_sql


def test_postgres_cluster_search_splits_fts_queries() -> None:
    session = SimpleNamespace(
        get_bind=lambda: SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    )
    sql = str(
        indexed_cluster_search_query(session, "Amazon", None, None).compile(
            dialect=postgresql.dialect()
        )
    )

    assert "UNION" in sql
    assert "content_items.content_text" in sql
    assert "sources.name" in sql
    assert "clusters.generated_content" in sql


def test_postgres_item_search_splits_fts_queries() -> None:
    sql = str(indexed_item_search_query("Amazon").compile(dialect=postgresql.dialect()))

    assert "UNION" in sql
    assert "content_items.content_text" in sql
    assert "sources.name" in sql


def test_item_summary_generation_persists(monkeypatch) -> None:
    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            assert model
            assert "摘要助手" in system_prompt
            assert "Nvidia announces" in input_text
            return {"summary": "Nvidia 发布新 AI 芯片。"}

    monkeypatch.setattr("reader_api.main.LocalChatProvider", FakeProvider)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        enable_generation(session)
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="1", title="Nvidia chip", content_hash=content_hash("Nvidia chip"))
        session.add(raw)
        session.flush()
        doc = Document(raw_entry_id=raw.id, title="Nvidia chip", content_text="Nvidia announces a new AI chip")
        session.add(doc)
        session.flush()
        item = ContentItem(
            document_id=doc.id,
            source_id=source.id,
            title="Nvidia announces a new AI chip",
            summary="New chip",
            content_text="Nvidia announces a new AI chip for datacenters.",
            content_hash=content_hash("Nvidia chip item"),
            normalized_title=normalize_title("Nvidia announces a new AI chip"),
        )
        session.add(item)
        session.flush()
        item_id = item.id
        assign_cluster(session, item)
        fixed_updated_at = datetime(2026, 6, 1, tzinfo=timezone.utc)
        report_start, _report_end = report_bounds("day", "2026-06-29")
        session.add_all(
            [
                UserState(
                    object_type="item",
                    object_id=item_id,
                    read_status="unread",
                    read_later=True,
                    starred=True,
                    updated_at=fixed_updated_at,
                ),
                UserState(
                    object_type="report",
                    object_id=report_state_key("day", report_start),
                    read_status="summary_seen",
                    read_later=True,
                    updated_at=fixed_updated_at,
                ),
            ]
        )
        session.commit()

    client = TestClient(app)
    assert client.get(f"/items/{item_id}/summary").json()["status"] == "empty"
    state_before_generation = user_state_snapshot()
    generated = client.post(f"/items/{item_id}/summarize").json()
    saved = client.get(f"/items/{item_id}/summary").json()

    assert generated["summary"] == "Nvidia 发布新 AI 芯片。"
    assert saved["status"] == "ready"
    assert saved["summary"] == "Nvidia 发布新 AI 芯片。"
    item = client.get(f"/items/{item_id}").json()
    assert item["read_status"] == "unread"
    assert item["read_later"] is True
    assert item["starred"] is True
    assert user_state_snapshot() == state_before_generation
    with Session() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 0
        assert session.query(GenerationRequest).count() == 1
        assert session.query(GenerationAttempt).count() == 1
        assert session.query(GenerationResult).count() == 1
        assert session.query(GenerationApplication).count() == 1
        assert (
            session.query(LLMTask)
            .filter(LLMTask.task_type != "translation:text")
            .count()
            == 0
        )
    assert set_object_user_state(
        client,
        "item",
        item_id,
        operation_id="95000000-0000-4000-8000-000000000000",
        read_status="summary_seen",
    )["read_status"] == "summary_seen"



def test_assistant_answers_with_citations(monkeypatch) -> None:
    prompts: list[str] = []

    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            assert model
            assert "个人信息阅读器助手" in system_prompt
            prompts.append(input_text)
            assert "Nvidia announces" in input_text
            return {"text": '{"answer":"Nvidia 发布了新的 AI 芯片。 [1]"}'}

    monkeypatch.setattr("reader_api.main.LocalChatProvider", FakeProvider)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        sources = [
            Source(name="AI Feed", url="https://example.com/rss.xml"),
            Source(name="The Verge", url="https://example.com/verge.xml"),
        ]
        session.add_all(sources)
        session.flush()
        item_ids = []
        cluster_id = 0
        for index, (title, body) in enumerate(
            [
                ("Nvidia announces a new AI chip", "Nvidia announces a new AI chip for datacenters."),
                ("Nvidia announces a new AI chip", "The Verge follows Nvidia announces coverage with launch details."),
            ],
            1,
        ):
            source = sources[index - 1]
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title=title, content_hash=content_hash(title, str(index)))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=body)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary="New chip",
                content_text=body,
                content_hash=content_hash("Nvidia chip item", str(index)),
                normalized_title=normalize_title("Nvidia announces a new AI chip"),
                lsh_signature=lsh_signature("Nvidia announces a new AI chip", "Nvidia announces a new AI chip for datacenters."),
                embedding_vector=json.dumps([1, 0]),
            )
            session.add(item)
            session.flush()
            item_ids.append(item.id)
            cluster_id = assign_cluster(session, item).id
        session.commit()

    client = TestClient(app)
    body = client.get(
        "/assistant",
        params={
            "q": "这篇讲什么",
            "item_id": item_ids[0],
            "operation_id": "assistant-item-summary-seen",
        },
    ).json()
    cluster_body = client.get("/assistant", params={"q": "这个事件有哪些来源", "cluster_id": cluster_id}).json()

    assert body["answer"] == "Nvidia 发布了新的 AI 芯片。 [1]"
    assert body["citations"][0]["title"] == "Nvidia announces a new AI chip"
    assert len(body["citations"]) == 1
    assert len(cluster_body["citations"]) == 2
    assert "The Verge follows" not in prompts[0]
    assert "The Verge follows" in prompts[1]
    assert client.get(f"/items/{item_ids[0]}").json()["read_status"] == "summary_seen"
    assert client.get(f"/clusters/{cluster_id}").json()["read_status"] == "unread"
    with Session() as session:
        assert session.scalar(select(func.count(InteractionEvent.id))) == 1


def test_assistant_rejects_missing_cluster_context() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    response = TestClient(app).get("/assistant", params={"q": "这个事件讲什么", "cluster_id": 999})

    assert response.status_code == 404


def test_cluster_synthesis_updates_generated_fields(monkeypatch) -> None:
    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            assert model
            assert "只输出 JSON" in system_prompt
            assert input_text.index("[1] 来源：Older Feed") < input_text.index("[2] 来源：Newer Feed")
            return {"text": '{"title":"合成标题","summary":"合成摘要 [1][2]","content":"合成全文第一段 [1]\\n\\n合成全文第二段 [2]"}'}

    monkeypatch.setattr("reader_api.main.LocalChatProvider", FakeProvider)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        enable_generation(session)
        older_source = Source(name="Older Feed", url="https://example.com/older.xml")
        newer_source = Source(name="Newer Feed", url="https://example.com/newer.xml")
        session.add_all([older_source, newer_source])
        session.flush()
        cluster = None
        item_ids: list[int] = []
        for source, published_at in [
            (older_source, datetime(2026, 6, 28, tzinfo=timezone.utc)),
            (newer_source, datetime(2026, 6, 29, tzinfo=timezone.utc)),
        ]:
            raw = make_raw_entry(source_id=source.id, external_id=source.name, title="Nvidia chip", published_at=published_at, content_hash=content_hash(source.name))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title="Nvidia chip", content_text="Nvidia announces a new AI chip")
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title="Nvidia announces a new AI chip",
                summary="New chip",
                content_text="Nvidia announces a new AI chip for datacenters.",
                published_at=published_at,
                content_hash=content_hash(source.name, "Nvidia chip item"),
                normalized_title=normalize_title("Nvidia announces a new AI chip"),
                lsh_signature=lsh_signature("Nvidia announces a new AI chip", "Nvidia announces a new AI chip for datacenters."),
                embedding_vector=json.dumps([1, 0]),
            )
            session.add(item)
            session.flush()
            item_ids.append(item.id)
            cluster = assign_cluster(session, item)
        assert cluster is not None
        cluster_id = cluster.id
        session.add_all(
            [
                UserState(
                    object_type="item",
                    object_id=item_id,
                    read_status="original_opened" if index == 0 else "summary_seen",
                    read_later=index == 0,
                    starred=index == 1,
                    updated_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
                )
                for index, item_id in enumerate(item_ids)
            ]
        )
        session.commit()

    state_before_generation = user_state_snapshot()
    response = TestClient(app).post(f"/clusters/{cluster_id}/synthesize")

    assert response.status_code == 200
    body = response.json()
    assert body["generated_title"] == "合成标题"
    assert body["generated_summary"] == "合成摘要 [1][2]"
    assert body["generated_content"] == "合成全文第一段 [1]\n\n合成全文第二段 [2]"
    assert body["model_version"]
    assert body["prompt_version"] == "cluster-synthesis-v4"
    assert [item["source_name"] for item in body["items"]] == ["Older Feed", "Newer Feed"]
    assert [citation["source_name"] for citation in json.loads(body["citations"])] == ["Older Feed", "Newer Feed"]
    with Session() as session:
        result = session.query(GenerationResult).one()
        assert result.payload_json["summary"] == "合成摘要 [1][2]"
        assert result.payload_json["content"] == "合成全文第一段 [1]\n\n合成全文第二段 [2]"
        assert session.query(GenerationApplication).filter_by(status="applied").count() == 1
        assert session.query(LLMTask).count() == 0
    assert user_state_snapshot() == state_before_generation



def test_cluster_synthesis_freezes_short_rss_body_without_mutating_document(monkeypatch) -> None:
    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def chat(self, _model: str, _system: str, input_text: str) -> dict[str, object]:
            assert "One sentence summary." in input_text
            assert "FULL_ARTICLE_TAIL" not in input_text
            return {
                "title": "新标题",
                "summary": "新摘要 [1]",
                "content": "新正文 [1]",
            }

    monkeypatch.setattr("reader_api.main.LocalChatProvider", FakeProvider)
    monkeypatch.setattr(
        "reader_api.rss.fetch_article_text",
        lambda _url: (_ for _ in ()).throw(
            AssertionError("generation must not fetch or mutate source content")
        ),
    )
    monkeypatch.setattr("reader_api.main.embed_items_by_ids", reject_inline_reclustering)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    cluster_id, item_id, document_id = create_short_cluster_fixture(
        item_status="summary_seen",
        generated=True,
    )
    Session = sessionmaker(bind=engine)
    client = TestClient(app)
    state_before_generation = user_state_snapshot()
    generated = client.post(f"/clusters/{cluster_id}/synthesize").json()
    with Session() as session:
        task = session.query(GenerationRequestPayload).one().payload_json

    assert generated["generated_summary"] == "旧摘要"
    assert generated["generated_content"] == "旧短合成"
    assert "One sentence summary." in task["input"]
    assert "FULL_ARTICLE_TAIL" not in task["input"]
    assert "来源原文" in task["input"]
    assert user_state_snapshot() == state_before_generation
    with Session() as session:
        item = session.get(ContentItem, item_id)
        assert item is not None
        assert item.content_text == "One sentence summary."
        assert item.embedding_vector == "[1.0,0.0]"
        assert item.embedding_model == "old-model"
        assert session.get(Document, document_id).content_text == "One sentence summary."


def test_cluster_detail_does_not_refresh_short_content(monkeypatch) -> None:
    monkeypatch.setattr("reader_api.rss.fetch_article_text", lambda _url: "Full article body " + ("detail " * 220) + "FULL_ARTICLE_TAIL")
    monkeypatch.setattr("reader_api.main.embed_items_by_ids", reject_inline_reclustering)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    cluster_id, item_id, document_id = create_short_cluster_fixture(item_status="original_opened")
    Session = sessionmaker(bind=engine)

    state_before_read = user_state_snapshot()
    body = TestClient(app).get(f"/clusters/{cluster_id}").json()

    assert "FULL_ARTICLE_TAIL" not in body["items"][0]["content_text"]
    assert user_state_snapshot() == state_before_read
    with Session() as session:
        assert session.get(ContentItem, item_id).content_text == "One sentence summary."
        assert session.get(Document, document_id).content_text == "One sentence summary."


def test_cluster_detail_read_only_does_not_turn_shared_digest_into_state_deleting_repair(monkeypatch) -> None:
    monkeypatch.setattr("reader_api.rss.fetch_article_text", lambda _url: "Full standalone article " + ("detail " * 220) + "FULL_ARTICLE_TAIL")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    digest_title = "AI 简报：OpenAI / Nvidia / Anthropic / Microsoft / Google / Apple"
    digest_body = """
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
        source = Source(name="AI Digest", url="https://example.com/rss.xml", fetch_full_content=True)
        raw = make_raw_entry(
            source=source,
            external_id="shared-digest",
            title=digest_title,
            url="https://example.com/digest",
            raw_content=raw_html,
            content_hash=content_hash(digest_title, digest_body),
        )
        document = Document(raw_entry=raw, document_type="digest", title=digest_title, content_text=digest_body, digest_score=0.90)
        session.add(document)
        session.flush()
        item_specs = [
            (digest_title, "Short digest summary", "https://example.com/main"),
            ("Nvidia 推出新芯片", "Short Nvidia summary", "https://example.com/nvidia"),
        ]
        item_ids: list[int] = []
        cluster_ids: list[int] = []
        for index, (title, text_value, url) in enumerate(item_specs):
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=title,
                summary=text_value,
                content_text=text_value,
                url=url,
                published_at=datetime(2026, 7, 10, index, tzinfo=timezone.utc),
                content_hash=content_hash(title, text_value, url),
                normalized_title=normalize_title(title),
                lsh_signature=lsh_signature(title, text_value),
            )
            session.add(item)
            session.flush()
            cluster = assign_cluster(session, item)
            item_ids.append(item.id)
            cluster_ids.append(cluster.id)
            session.add(UserState(object_type="item", object_id=item.id, read_status="summary_seen", starred=index == 1))
        session.commit()

    def visible_states(api_client: TestClient) -> tuple[dict[int, tuple[str, bool, bool]], dict[int, tuple[str, bool, bool]]]:
        item_states = {
            row["id"]: (row["read_status"], row["read_later"], row["starred"])
            for row in api_client.get("/items", params={"include_content": "false"}).json()
            if row["id"] in item_ids
        }
        cluster_states = {
            row["id"]: (row["read_status"], row["read_later"], row["starred"])
            for row in api_client.get("/clusters").json()
            if row["id"] in cluster_ids
        }
        return item_states, cluster_states

    client = TestClient(app)
    before_items, before_clusters = visible_states(client)

    refreshed = client.get(f"/clusters/{cluster_ids[0]}")

    assert refreshed.status_code == 200
    assert "FULL_ARTICLE_TAIL" not in refreshed.json()["items"][0]["content_text"]
    with TestClient(app) as restarted:
        after_items, after_clusters = visible_states(restarted)

    assert after_items == before_items
    assert after_clusters == before_clusters


def test_short_article_extraction_accepts_meaningful_text_under_800_chars() -> None:
    from reader_api.rss import extract_article_text

    article_text = " ".join(f"paragraph{i}" for i in range(45))
    html = f"<html><body><article class='article-content'><p>{article_text}</p></article></body></html>"

    extracted = extract_article_text(html, "https://example.com/article")

    assert "paragraph0" in extracted
    assert "paragraph44" in extracted
    assert 320 <= len(" ".join(extracted.split())) < 800


def test_cluster_synthesis_requires_full_content(monkeypatch) -> None:
    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            return {"text": '{"title":"合成标题","summary":"只有摘要"}'}

    monkeypatch.setattr("reader_api.main.LocalChatProvider", FakeProvider)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        enable_generation(session)
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        cluster_id = 0
        for index in range(2):
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title="Nvidia chip", content_hash=content_hash("Nvidia chip", str(index)))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title="Nvidia chip", content_text="Nvidia announces a new AI chip")
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title="Nvidia announces a new AI chip",
                summary="New chip",
                content_text="Nvidia announces a new AI chip for datacenters.",
                content_hash=content_hash("Nvidia chip item", str(index)),
                normalized_title=normalize_title("Nvidia announces a new AI chip"),
            )
            session.add(item)
            session.flush()
            cluster_id = assign_cluster(session, item).id
        session.commit()

    response = TestClient(app).post(f"/clusters/{cluster_id}/synthesize")

    assert response.status_code == 502
    assert response.json()["detail"] == "模型返回的生成内容无法使用，请重试"


def test_topic_groups_track_matching_clusters() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="1", title="OpenAI buys data company", content_hash=content_hash("OpenAI buys data company"))
        session.add(raw)
        session.flush()
        doc = Document(raw_entry_id=raw.id, title="OpenAI buys data company", content_text="OpenAI buys a data company")
        session.add(doc)
        session.flush()
        item = ContentItem(
            document_id=doc.id,
            source_id=source.id,
            title="OpenAI buys a data company",
            summary="OpenAI deal",
            content_text="OpenAI buys a data company for model training.",
            published_at=datetime(2026, 6, 29, 6, tzinfo=timezone.utc),
            content_hash=content_hash("OpenAI buys data company item"),
            normalized_title=normalize_title("OpenAI buys a data company"),
        )
        session.add(item)
        session.flush()
        hidden_raw = make_raw_entry(source_id=source.id, external_id="1-hidden", title="OpenAI background", content_hash=content_hash("OpenAI background"))
        session.add(hidden_raw)
        session.flush()
        hidden_doc = Document(raw_entry_id=hidden_raw.id, title="OpenAI background", content_text="OpenAI background")
        session.add(hidden_doc)
        session.flush()
        hidden_item = ContentItem(
            document_id=hidden_doc.id,
            source_id=source.id,
            title="OpenAI background",
            summary="OpenAI background",
            content_text="OpenAI background",
            published_at=datetime(2026, 6, 29, 5, 30, tzinfo=timezone.utc),
            content_hash=content_hash("OpenAI background item"),
            normalized_title=normalize_title("OpenAI background"),
        )
        session.add(hidden_item)
        session.flush()
        cluster = Cluster(cluster_key="topic-openai-shared", title="OpenAI buys a data company")
        session.add(cluster)
        session.flush()
        with clustering_run(
            session,
            scope_type="topic-event-state-test-one",
            item_ids=[item.id, hidden_item.id],
            rule_version="topic-event-state-test-v1",
        ):
            session.add_all([
                ClusterItem(cluster_id=cluster.id, content_item_id=item.id),
                ClusterItem(cluster_id=cluster.id, content_item_id=hidden_item.id),
            ])
            session.flush()
        session.add(UserState(
            object_type="item",
            object_id=hidden_item.id,
            uninterested=True,
            uninterested_at=datetime(2026, 6, 29, 7, tzinfo=timezone.utc),
        ))
        raw2 = make_raw_entry(
            source_id=source.id,
            external_id="2",
            title="Model training agreement",
            content_hash=content_hash("Model training agreement"),
        )
        session.add(raw2)
        session.flush()
        doc2 = Document(
            raw_entry_id=raw2.id,
            title="Model training agreement",
            content_text="A model training supplier signs a deal with OpenAI",
        )
        session.add(doc2)
        session.flush()
        item2 = ContentItem(
            document_id=doc2.id,
            source_id=source.id,
            title="Model training agreement",
            summary="New supplier deal",
            content_text="A model training supplier signs a deal with OpenAI.",
            published_at=datetime(2026, 6, 29, 5, tzinfo=timezone.utc),
            content_hash=content_hash("Model training agreement item"),
            normalized_title=normalize_title("Model training agreement"),
        )
        session.add(item2)
        session.flush()
        with clustering_run(
            session,
            scope_type="topic-event-state-test-two",
            item_ids=[item2.id],
            rule_version="topic-event-state-test-v1",
        ):
            assign_cluster(session, item2)
        session.commit()

    client = TestClient(app)
    topic = client.post("/topics", json={"name": "OpenAI", "query": "OpenAI", "description": "OpenAI 长期议题"}).json()
    topics = client.get("/topics").json()
    detail = client.get(f"/topics/{topic['id']}").json()

    assert topics[0]["name"] == "OpenAI"
    assert topics[0]["cluster_count"] == 2
    assert [cluster["title"] for cluster in detail["clusters"]] == [
        "Model training agreement",
        "OpenAI buys a data company",
    ]
    assert detail["clusters"][1]["item_count"] == 1
    state = set_object_user_state(
        client,
        "topic",
        topic["id"],
        operation_id="96000000-0000-4000-8000-000000000000",
        read_status="summary_seen",
    )
    set_object_user_state(
        client,
        "topic",
        topic["id"],
        operation_id="96010000-0000-4000-8000-000000000000",
        read_later=True,
    )
    set_object_user_state(
        client,
        "topic",
        topic["id"],
        operation_id="96020000-0000-4000-8000-000000000000",
        starred=True,
    )
    assert state["read_status"] == "summary_seen"
    topic_with_state = client.get(f"/topics/{topic['id']}").json()
    assert topic_with_state["read_later"] is True
    assert topic_with_state["starred"] is True
    updated = client.patch(f"/topics/{topic['id']}", json={"name": "供应商", "query": "supplier, 不存在", "description": "供应商追踪"}).json()
    assert updated["name"] == "供应商"
    assert updated["read_status"] == "summary_seen"
    assert updated["cluster_count"] == 1
    assert updated["clusters"][0]["title"] == "Model training agreement"
    symbol_topic = client.post("/topics", json={"name": "符号", "query": "%, _"}).json()
    assert symbol_topic["cluster_count"] == 0
    assert symbol_topic["clusters"] == []
    assert client.delete(f"/topics/{topic['id']}").status_code == 204
    assert client.get(f"/topics/{topic['id']}").status_code == 404


def test_topic_queries_reject_database_expression_fanout() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    client = TestClient(app)
    topic = client.post(
        "/topics", json={"name": "Bounded", "query": "one"}
    ).json()
    too_many_terms = ",".join(f"term-{index}" for index in range(21))

    assert client.post(
        "/topics", json={"name": "Too many", "query": too_many_terms}
    ).status_code == 422
    assert client.patch(
        f"/topics/{topic['id']}", json={"query": too_many_terms}
    ).status_code == 422
    assert client.post(
        "/topics", json={"name": "Too long", "query": "q" * 501}
    ).status_code == 422


def test_embedding_similarity_assigns_same_cluster() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    now = datetime(2026, 6, 29, tzinfo=timezone.utc)
    with Session() as session:
        sources = [
            Source(name="AI Feed 1", url="https://example.com/1.xml"),
            Source(name="AI Feed 2", url="https://example.com/2.xml"),
            Source(name="AI Feed 3", url="https://example.com/3.xml"),
            Source(name="AI Feed 4", url="https://example.com/4.xml"),
        ]
        session.add_all(sources)
        session.flush()

        clusters = []
        for index, (title, vector, offset) in enumerate(
            [
                ("OpenAI buys a data company", [1, 0], timedelta(hours=1)),
                ("Data startup acquired by OpenAI", [0.99, 0.01], timedelta(hours=2)),
                ("Football scores update", [0, 1], timedelta(hours=3)),
                ("OpenAI closes another data deal", [1, 0], timedelta(days=40)),
            ],
            1,
        ):
            source = sources[index - 1]
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                published_at=now + offset,
                content_hash=content_hash(title),
                normalized_title=normalize_title(title),
                lsh_signature=lsh_signature(title, title),
                embedding_vector=json.dumps(vector),
            )
            session.add(item)
            session.flush()
            clusters.append(assign_cluster(session, item).id)

        assert clusters[0] == clusters[1]
        assert clusters[2] != clusters[0]
        assert clusters[3] != clusters[0]


def test_exact_cluster_window_uses_unique_later_keys() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/window.xml")
        session.add(source)
        session.flush()

        cluster_ids = []
        for index, offset in enumerate([0, 40, 80], 1):
            title = "Nvidia announces a new AI chip"
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title=title, content_hash=content_hash(title, str(index)))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                published_at=start + timedelta(days=offset),
                content_hash=content_hash(title),
                normalized_title=normalize_title(title),
            )
            session.add(item)
            session.flush()
            cluster_ids.append(assign_cluster(session, item).id)

        assert len(set(cluster_ids)) == 3


def test_exact_cluster_window_rejects_late_arriving_old_item() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    newer = datetime(2026, 3, 15, tzinfo=timezone.utc)
    older = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/reverse-window.xml")
        session.add(source)
        session.flush()

        cluster_ids = []
        for index, published_at in enumerate([newer, older], 1):
            title = "Nvidia announces a new AI chip"
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title=title, content_hash=content_hash(title, str(index)))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                published_at=published_at,
                content_hash=content_hash(title),
                normalized_title=normalize_title(title),
            )
            session.add(item)
            session.flush()
            cluster_ids.append(assign_cluster(session, item).id)

        assert cluster_ids[0] != cluster_ids[1]


def test_exact_cluster_finds_older_in_window_candidate() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/out-of-order.xml")
        session.add(source)
        session.flush()

        cluster_ids = []
        for index, offset in enumerate([70, 0, 20], 1):
            title = "Nvidia announces a new AI chip"
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title=title, content_hash=content_hash(title, str(index)))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                published_at=start + timedelta(days=offset),
                content_hash=content_hash(title),
                normalized_title=normalize_title(title),
            )
            session.add(item)
            session.flush()
            cluster_ids.append(assign_cluster(session, item).id)

        assert cluster_ids[0] != cluster_ids[1]
        assert cluster_ids[2] == cluster_ids[1]


def test_duplicate_relation_does_not_change_current_list_visibility() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    client = TestClient(app)
    story_url = "https://example.com/relation-story"
    with Session() as session:
        source = Source(
            name="Relation Visibility",
            url="https://example.com/relation-visibility.xml",
        )
        cluster = Cluster(cluster_key="relation-visibility", title="Relation story")
        session.add_all([source, cluster])
        session.flush()
        for external_id, title in (
            ("relation-guid", "Original relation story"),
            ("relation-guid:abcdef123456", "Updated relation story"),
        ):
            raw = make_raw_entry(
                source=source,
                external_id=external_id,
                title=title,
                url=story_url,
                raw_content=title,
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                document_type="normal_article",
                title=title,
                content_text=title,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                url=story_url,
                published_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
                content_hash=content_hash(title, title, story_url),
                canonical_url=canonical_url(story_url),
                normalized_title=normalize_title(title),
            )
            session.add(item)
            session.flush()
            session.add(
                ClusterItem(
                    cluster_id=cluster.id,
                    content_item_id=item.id,
                    duplicate_score=1.0,
                )
            )
        session.commit()

    before = {
        "items": client.get("/items").json(),
        "clusters": client.get("/clusters").json(),
        "sources": client.get("/sources").json(),
    }
    with Session() as session:
        assert record_duplicate_feed_relations(session) == 1
        session.commit()
        assert session.scalar(select(func.count()).select_from(SourceEntryRelation)) == 1
    after = {
        "items": client.get("/items").json(),
        "clusters": client.get("/clusters").json(),
        "sources": client.get("/sources").json(),
    }

    assert after == before
