import json
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import pytest
import reader_api.worker as worker  # noqa: E402
from reader_api.cluster import (  # noqa: E402
    EMBEDDING_THRESHOLD,
    POSTGRES_NEIGHBOR_SQL,
    POSTGRES_STORE_EMBEDDING_SQL,
    assign_cluster,
    cluster_key_for,
    embed_items_by_ids,
    embed_missing_items,
    embed_pending_items,
    embedding_threshold_for,
    ensure_zh_canonical_embedding,
    repair_embedding_clusters,
    repair_title_only_clusters,
    repair_windowed_clusters,
    item_needs_translation,
    vector_literal,
)
from reader_api.db import Base, engine  # noqa: E402
from reader_api.digest import content_hash, lsh_signature, normalize_title  # noqa: E402
from reader_api.models import Cluster, ClusterEventProjection, ClusteringRun, ClusteringRunMembership, ClusterItem, ContentEmbedding, ContentItem, Document, Event, EventRevision, LLMTask, Source  # noqa: E402
from reader_api.source_ingest import IngestEntry, ingest_source_entries  # noqa: E402
from reader_api.translations import TRANSLATION_PROMPT_VERSION, cached_translation_texts, ensure_translation, needs_reading_translation, needs_translation, translation_lock_key, translation_object_id  # noqa: E402
from tests.factories import make_raw_entry  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402


class FlakyProvider:
    def embed(self, model: str, input_text: str) -> dict[str, object]:
        if "Fail item" in input_text:
            raise RuntimeError("embedding failed")
        return {"data": [{"embedding": [1, 0]}]}


class ModelProvider:
    def embed(self, model: str, input_text: str) -> dict[str, object]:
        return {"data": [{"embedding": [2, 0] if model == "new-model" else [1, 0]}]}


class SimilarStoryProvider:
    def embed(self, model: str, input_text: str) -> dict[str, object]:
        if "OpenAI" in input_text or "data startup" in input_text:
            return {"data": [{"embedding": [1, 0]}]}
        return {"data": [{"embedding": [0, 1]}]}


class CrossLanguageProvider:
    def embed(self, model: str, input_text: str) -> dict[str, object]:
        if "苹果" in input_text or "Epic" in input_text:
            return {"data": [{"embedding": [1, 0]}]}
        return {"data": [{"embedding": [0, 1]}]}


class TranslationProvider:
    def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
        return {"text": "美国最高法院将审理苹果和 Epic Games 关于 App Store 费用的上诉。"}


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2026 世界电容大会・全球演讲嘉宾招募中", False),
        ("三星半导体负责人 구자흠 表示，新产品将于下月发布。", False),
        ("Apple、Google 与 OpenAI 发布更新，详情见 https://example.com/news", False),
        ("example.com/news", False),
        ("Apple filed an appeal after the App Store ruling.", True),
        ("これは日本語の記事です。新製品を紹介します。", True),
        ("이것은 한국어 기사이며 새 제품을 소개합니다.", True),
        ("Это русская статья о новом продукте.", True),
        ("2026 · 07 · 27", False),
    ],
)
def test_reading_translation_uses_dominant_script(text: str, expected: bool) -> None:
    assert needs_reading_translation(text) is expected


def test_reading_policy_does_not_change_clustering_language_policy() -> None:
    text = "2026 世界电容大会・全球演讲嘉宾招募中"

    assert needs_reading_translation(text) is False
    assert needs_translation(text) is True


def reject_automatic_repair(*_: object, **__: object) -> int:
    raise AssertionError("普通 worker 任务不得执行历史业务修复")


def test_translation_cache_reuses_task_and_lock_key_is_signed() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    calls: list[str] = []

    class Provider:
        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            calls.append(input_text)
            return {"text": "苹果与 Epic 的案件将由最高法院审理。"}

    with Session() as session:
        text = "Apple takes Epic fight over app store fees to the Supreme Court"
        assert ensure_translation(session, Provider(), "hy-mt2-1.8b", text) == "苹果与 Epic 的案件将由最高法院审理。"
        assert ensure_translation(session, Provider(), "hy-mt2-1.8b", text) == "苹果与 Epic 的案件将由最高法院审理。"
        task_count = len(session.scalars(select(LLMTask).where(LLMTask.task_type == "translation:text")).all())

    assert calls == [f"Translate to Simplified Chinese (output translation only):\n\n{text}"]
    assert task_count == 1
    assert translation_lock_key("0" * 64) == 0
    assert -(2**63) <= translation_lock_key("f" * 64) < 2**63


def test_local_translation_reuses_legacy_local_chat_cache() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    text = "Apple takes Epic fight over app store fees to the Supreme Court"
    source_hash = content_hash(text)

    class Provider:
        provider_name = "local"

        @staticmethod
        def chat(*_: object) -> dict[str, object]:
            raise AssertionError("legacy local cache should prevent a model call")

    with Session() as session:
        session.add(
            LLMTask(
                task_type="translation:text",
                provider="local-chat",
                object_type="text",
                object_id=translation_object_id(source_hash),
                status="complete",
                prompt_version=TRANSLATION_PROMPT_VERSION,
                model_version="hy-mt2-1.8b",
                result_json=json.dumps({"source_hash": source_hash, "translation": "旧缓存译文"}, ensure_ascii=False),
            )
        )
        session.flush()

        assert ensure_translation(session, Provider(), "hy-mt2-1.8b", text) == "旧缓存译文"


def test_batch_translation_cache_reads_multiple_titles_together() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    texts = ["First untranslated title", "Second untranslated title"]

    with Session() as session:
        for index, text in enumerate(texts):
            source_hash = content_hash(text)
            session.add(
                LLMTask(
                    task_type="translation:text",
                    provider="local",
                    object_type="text",
                    object_id=translation_object_id(source_hash),
                    status="complete",
                    prompt_version=TRANSLATION_PROMPT_VERSION,
                    model_version="hy-mt2-1.8b",
                    result_json=json.dumps(
                        {"source_hash": source_hash, "translation": f"译文 {index + 1}"},
                        ensure_ascii=False,
                    ),
                )
            )
        session.flush()

        assert cached_translation_texts(session, "hy-mt2-1.8b", texts) == {
            texts[0]: "译文 1",
            texts[1]: "译文 2",
        }


def test_translation_rejects_garbled_model_output() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    class Provider:
        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            return {"text": "あいうえおかきくけこさしすせそたちつてと"}

    with Session() as session:
        text = "Apple takes Epic fight over App Store fees to the Supreme Court"
        assert ensure_translation(session, Provider(), "hy-mt2-1.8b", text) == ""
        task_count = len(session.scalars(select(LLMTask).where(LLMTask.task_type == "translation:text")).all())

    assert task_count == 0


class SingletonProvider:
    def embed_many(self, model: str, input_texts: list[str]) -> dict[str, object]:
        return {"data": [{"embedding": [0, 1]} for _ in input_texts]}

    def embed(self, model: str, input_text: str) -> dict[str, object]:
        return {"data": [{"embedding": [0, 1]}]}


class BatchProvider:
    def __init__(self) -> None:
        self.batch_calls = 0
        self.batch_sizes: list[int] = []
        self.single_calls = 0

    def embed_many(self, model: str, input_texts: list[str]) -> dict[str, object]:
        self.batch_calls += 1
        self.batch_sizes.append(len(input_texts))
        return {"data": [{"embedding": [index + 1, 0]} for index, _ in enumerate(input_texts)]}

    def embed(self, model: str, input_text: str) -> dict[str, object]:
        self.single_calls += 1
        return {"data": [{"embedding": [1, 0]}]}


class CanonicalBatchProvider(BatchProvider):
    def embed_many(self, model: str, input_texts: list[str]) -> dict[str, object]:
        self.batch_calls += 1
        self.batch_sizes.append(len(input_texts))
        return {"data": [{"embedding": [0, 1]} for _ in input_texts]}

    def embed(self, model: str, input_text: str) -> dict[str, object]:
        self.single_calls += 1
        return {"data": [{"embedding": [1, 0] if "译文" in input_text else [0, 1]}]}


class CountingTranslationProvider:
    def __init__(self) -> None:
        self.inputs: list[str] = []

    def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
        self.inputs.append(input_text)
        return {"text": "译文：" + input_text.partition("\n\n")[2]}


class DummySession:
    def __enter__(self) -> "DummySession":
        return self

    def __exit__(self, *_: object) -> None:
        return None


def test_new_articles_publish_one_stable_event_only_after_embedding() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        sources = [
            Source(name="First Feed", url="https://example.com/first.xml"),
            Source(name="Second Feed", url="https://example.com/second.xml"),
        ]
        session.add_all(sources)
        session.flush()
        for index, source in enumerate(sources, 1):
            assert ingest_source_entries(
                session,
                source,
                [
                    IngestEntry(
                        source_guid=f"story-{index}",
                        external_id=f"story-{index}",
                        title=(
                            "OpenAI buys data startup"
                            if index == 1
                            else "Data startup acquired by OpenAI"
                        ),
                        url=f"https://example.com/story/{index}",
                        content_text="OpenAI data startup acquisition for model training.",
                    )
                ],
            ) == 1
        session.commit()

        assert session.scalars(select(ClusterItem)).all() == []
        assert session.scalars(select(Event)).all() == []
        assert session.scalars(select(ClusterEventProjection)).all() == []

        assert (
            embed_pending_items(
                session,
                SimilarStoryProvider(),
                "embedding-model",
                batch_limit=8,
                max_batches=1,
            )
            == 2
        )
        cluster_ids = {
            cluster_id
            for (cluster_id,) in session.execute(select(ClusterItem.cluster_id))
        }

        assert len(cluster_ids) == 1
        events = session.scalars(select(Event)).all()
        assert len(events) == 1
        assert events[0].status == "active"
        assert len(session.scalars(select(EventRevision)).all()) == 1
        assert len(session.scalars(select(ClusterEventProjection)).all()) == 1
        assert session.scalar(select(ClusteringRun)).status == "completed"


def test_pending_item_cannot_be_assigned_to_a_cluster() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        source = Source(name="Pending Feed", url="https://example.com/pending.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="pending-story",
            title="Pending story",
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            title=raw.title,
            content_text="Pending body",
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            content_text=document.content_text,
            content_hash=raw.content_hash,
        )
        session.add(item)
        session.flush()

        with pytest.raises(ValueError, match="完成 Embedding"):
            assign_cluster(session, item)

        assert session.scalars(select(ClusterItem)).all() == []


@pytest.mark.parametrize("entrypoint", ["missing", "ids"])
def test_embedding_batch_skips_failed_item_and_keeps_processing(
    entrypoint: str,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        video_source = Source(name="Video Feed", url="https://example.com/video.xml", media_type="video")
        session.add_all([source, video_source])
        session.flush()
        for item_source, title in [(source, "Good item"), (source, "Fail item"), (video_source, "Video item")]:
            raw = make_raw_entry(source_id=item_source.id, external_id=title, title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            session.add(
                ContentItem(
                    document_id=doc.id,
                    source_id=item_source.id,
                    title=title,
                    summary=title,
                    content_text=title,
                    content_hash=content_hash(title, "item"),
                    normalized_title=normalize_title(title),
                )
            )
        session.commit()

        article_item_ids = list(
            session.scalars(
                select(ContentItem.id).where(ContentItem.source_id == source.id)
            )
        )
        if entrypoint == "missing":
            processed = embed_missing_items(
                session, FlakyProvider(), "embedding-model"
            )
        else:
            processed = embed_items_by_ids(
                session,
                FlakyProvider(),
                "embedding-model",
                article_item_ids,
                batch_limit=1,
            )
        assert processed == 1
        run = session.scalars(select(ClusteringRun)).one()
        assert run.status == "completed"
        assert len(session.scalars(
            select(ClusteringRunMembership).where(
                ClusteringRunMembership.run_id == run.id,
                ClusteringRunMembership.snapshot_phase == "after",
            )
        ).all()) == 1
        rows = {
            title: (vector, model, signature)
            for title, vector, model, signature in session.execute(select(ContentItem.title, ContentItem.embedding_vector, ContentItem.embedding_model, ContentItem.lsh_signature)).all()
        }
        item_ids = dict(
            session.execute(select(ContentItem.title, ContentItem.id)).all()
        )
        good_cluster_id = session.scalar(
            select(ClusterItem.cluster_id).where(
                ClusterItem.content_item_id == item_ids["Good item"]
            )
        )
        assert good_cluster_id is not None
        assert session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.cluster_id == good_cluster_id
            )
        ) is not None
        assert len(session.scalars(select(Event)).all()) == 1
        assert session.scalar(
            select(ClusterItem).where(
                ClusterItem.content_item_id == item_ids["Fail item"]
            )
        ) is None

    assert rows["Fail item"] == (None, "", "")
    assert rows["Good item"][0:2] == ("[1.0,0.0]", "embedding-model")
    assert rows["Good item"][2]
    assert rows["Video item"] == (None, "", "")


def test_embedding_projection_failure_rolls_back_vector_and_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="Atomic Feed", url="https://example.com/atomic.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(
            source_id=source.id,
            external_id="atomic-item",
            title="Atomic item",
            content_hash=content_hash("Atomic item"),
        )
        session.add(raw)
        session.flush()
        document = Document(
            raw_entry_id=raw.id,
            title=raw.title,
            content_text=raw.title,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=raw.title,
            summary=raw.title,
            content_text=raw.title,
            content_hash=content_hash(raw.title, "item"),
            normalized_title=normalize_title(raw.title),
        )
        session.add(item)
        session.commit()

        def reject_projection(*_: object, **__: object) -> None:
            raise RuntimeError("projection failed")

        monkeypatch.setattr(
            "reader_api.event_projection.project_completed_clustering_run",
            reject_projection,
        )
        with pytest.raises(RuntimeError, match="projection failed"):
            embed_missing_items(session, ModelProvider(), "embedding-model")

        session.expire_all()
        persisted = session.get(ContentItem, item.id)
        assert persisted is not None
        assert persisted.embedding_vector is None
        assert persisted.embedding_model == ""
        assert session.scalars(select(ClusterItem)).all() == []
        assert session.scalars(select(Event)).all() == []
        run = session.scalars(select(ClusteringRun)).one()
        assert run.status == "failed"


def test_pending_batches_only_process_the_frozen_run_scope() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="Frozen Feed", url="https://example.com/frozen.xml")
        session.add(source)
        session.flush()
        original_items: list[ContentItem] = []
        for index in range(3):
            title = f"Frozen item {index}"
            raw = make_raw_entry(
                source_id=source.id,
                external_id=title,
                title=title,
                published_at=datetime(2026, 7, 13, index, tzinfo=timezone.utc),
                content_hash=content_hash(title),
            )
            session.add(raw)
            session.flush()
            document = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                content_hash=content_hash(title, "item"),
                normalized_title=normalize_title(title),
                published_at=raw.published_at,
            )
            session.add(item)
            session.flush()
            original_items.append(item)
        session.commit()

        class InsertDuringFirstBatch:
            inserted_item_id: int | None = None

            def embed(self, model: str, input_text: str) -> dict[str, object]:
                if self.inserted_item_id is None:
                    raw = make_raw_entry(
                        source_id=source.id,
                        external_id="late-item",
                        title="Late item",
                        published_at=datetime(2026, 7, 14, tzinfo=timezone.utc),
                        content_hash=content_hash("Late item"),
                    )
                    session.add(raw)
                    session.flush()
                    document = Document(
                        raw_entry_id=raw.id,
                        title=raw.title,
                        content_text=raw.title,
                    )
                    session.add(document)
                    session.flush()
                    item = ContentItem(
                        document_id=document.id,
                        source_id=source.id,
                        title=raw.title,
                        summary=raw.title,
                        content_text=raw.title,
                        content_hash=content_hash(raw.title, "item"),
                        normalized_title=normalize_title(raw.title),
                        published_at=raw.published_at,
                    )
                    session.add(item)
                    session.flush()
                    self.inserted_item_id = item.id
                return {"data": [{"embedding": [1, 0]}]}

        provider = InsertDuringFirstBatch()
        assert embed_pending_items(
            session,
            provider,
            "embedding-model",
            batch_limit=1,
            max_batches=2,
        ) == 2

        assert provider.inserted_item_id is not None
        assert session.get(ContentItem, provider.inserted_item_id).embedding_vector is None
        processed_ids = set(
            session.scalars(
                select(ContentItem.id).where(
                    ContentItem.embedding_model == "embedding-model"
                )
            )
        )
        assert processed_ids == {original_items[1].id, original_items[2].id}


def test_pending_batches_continue_after_a_failed_batch() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    class FailNewestProvider:
        def __init__(self) -> None:
            self.inputs: list[str] = []

        def embed(self, model: str, input_text: str) -> dict[str, object]:
            self.inputs.append(input_text)
            if "Fail first" in input_text:
                raise RuntimeError("embedding failed")
            return {"data": [{"embedding": [1, 0]}]}

    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        for title, published_at in (
            ("Later succeeds", datetime(2026, 7, 13, 10, tzinfo=timezone.utc)),
            ("Fail first", datetime(2026, 7, 13, 11, tzinfo=timezone.utc)),
        ):
            raw = make_raw_entry(
                source_id=source.id,
                external_id=title,
                title=title,
                published_at=published_at,
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
            session.add(
                ContentItem(
                    document_id=document.id,
                    source_id=source.id,
                    title=title,
                    summary=title,
                    content_text=title,
                    content_hash=content_hash(title, "item"),
                    normalized_title=normalize_title(title),
                    published_at=published_at,
                )
            )
        session.commit()

        provider = FailNewestProvider()
        assert (
            embed_pending_items(
                session,
                provider,
                "embedding-model",
                batch_limit=1,
                max_batches=2,
            )
            == 1
        )
        assert len(provider.inputs) == 2
        assert "Fail first" in provider.inputs[0]
        assert "Later succeeds" in provider.inputs[1]
        items = {
            item.title: item for item in session.scalars(select(ContentItem)).all()
        }
        assert items["Fail first"].embedding_vector is None
        assert items["Later succeeds"].embedding_vector == "[1.0,0.0]"
        success_cluster_id = session.scalar(
            select(ClusterItem.cluster_id).where(
                ClusterItem.content_item_id == items["Later succeeds"].id
            )
        )
        assert success_cluster_id is not None
        assert session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.cluster_id == success_cluster_id
            )
        ) is not None
        run = session.scalars(select(ClusteringRun)).one()
        assert run.status == "completed"


def test_pending_batches_fail_when_every_batch_fails() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    class AlwaysFailProvider:
        def __init__(self) -> None:
            self.inputs: list[str] = []

        def embed(self, model: str, input_text: str) -> dict[str, object]:
            self.inputs.append(input_text)
            raise RuntimeError("embedding failed")

    with Session() as session:
        source = Source(name="Failed Feed", url="https://example.com/failed.xml")
        session.add(source)
        session.flush()
        for index in range(2):
            title = f"Failed item {index}"
            raw = make_raw_entry(
                source_id=source.id,
                external_id=title,
                title=title,
                published_at=datetime(2026, 7, 13, 10 + index, tzinfo=timezone.utc),
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
            session.add(
                ContentItem(
                    document_id=document.id,
                    source_id=source.id,
                    title=title,
                    summary=title,
                    content_text=title,
                    content_hash=content_hash(title, "item"),
                    normalized_title=normalize_title(title),
                    published_at=raw.published_at,
                )
            )
        session.commit()

        provider = AlwaysFailProvider()
        assert (
            embed_pending_items(
                session,
                provider,
                "embedding-model",
                batch_limit=1,
                max_batches=2,
            )
            == 0
        )
        assert len(provider.inputs) == 2
        assert session.scalars(select(ClusterItem)).all() == []
        assert session.scalars(select(Event)).all() == []
        run = session.scalars(select(ClusteringRun)).one()
        assert run.status == "failed"


def test_pending_batches_publish_success_before_later_failed_batch() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="Partial Feed", url="https://example.com/partial.xml")
        session.add(source)
        session.flush()
        for title, published_at in (
            ("Good item", datetime(2026, 7, 13, 11, tzinfo=timezone.utc)),
            ("Fail item", datetime(2026, 7, 13, 10, tzinfo=timezone.utc)),
        ):
            raw = make_raw_entry(
                source_id=source.id,
                external_id=title,
                title=title,
                published_at=published_at,
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
            session.add(
                ContentItem(
                    document_id=document.id,
                    source_id=source.id,
                    title=title,
                    summary=title,
                    content_text=title,
                    content_hash=content_hash(title, "item"),
                    normalized_title=normalize_title(title),
                    published_at=published_at,
                )
            )
        session.commit()

        assert (
            embed_pending_items(
                session,
                FlakyProvider(),
                "embedding-model",
                batch_limit=1,
                max_batches=2,
            )
            == 1
        )
        run = session.scalars(select(ClusteringRun)).one()
        assert run.status == "completed"
        items = {
            item.title: item for item in session.scalars(select(ContentItem)).all()
        }
        good_cluster_id = session.scalar(
            select(ClusterItem.cluster_id).where(
                ClusterItem.content_item_id == items["Good item"].id
            )
        )
        assert good_cluster_id is not None
        assert session.scalar(
            select(ClusterEventProjection).where(
                ClusterEventProjection.cluster_id == good_cluster_id
            )
        ) is not None
        assert len(session.scalars(select(Event)).all()) == 1
        assert items["Fail item"].embedding_vector is None
        assert session.scalar(
            select(ClusterItem).where(
                ClusterItem.content_item_id == items["Fail item"].id
            )
        ) is None


def test_embedding_batch_prefers_provider_batch_call() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    provider = BatchProvider()
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        for index in range(2):
            title = f"Item {index}"
            raw = make_raw_entry(source_id=source.id, external_id=title, title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            session.add(
                ContentItem(
                    document_id=doc.id,
                    source_id=source.id,
                    title=title,
                    summary=title,
                    content_text=title,
                    content_hash=content_hash(title, "item"),
                    normalized_title=normalize_title(title),
                )
            )
        session.commit()

        assert embed_missing_items(session, provider, "embedding-model", limit=2) == 2
        vectors = session.scalars(select(ContentItem.embedding_vector).order_by(ContentItem.id)).all()

    assert provider.batch_calls == 1
    assert provider.single_calls == 0
    assert sorted(vectors) == ["[1.0,0.0]", "[2.0,0.0]"]


def test_embedding_by_ids_uses_eight_item_batches() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        item_ids = []
        for index in range(17):
            title = f"Item {index}"
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
                content_hash=content_hash(title, "item"),
                normalized_title=normalize_title(title),
            )
            session.add(item)
            session.flush()
            item_ids.append(item.id)
        session.commit()

        provider = BatchProvider()
        assert embed_items_by_ids(session, provider, "embedding-model", item_ids, batch_limit=8) == 17

    assert provider.batch_sizes == [8, 8, 1]


def test_embedding_by_ids_translates_foreign_items_and_embeds_zh_canonical() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="English Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        title = "Apple takes Epic fight over app store fees to the Supreme Court"
        body = "Apple is asking the Supreme Court to reverse an order related to App Store fees and developer links."
        raw = make_raw_entry(source_id=source.id, external_id="english-1", title=title, content_hash=content_hash(title, body))
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title=title, summary=body, content_text=body)
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=title,
            summary=body,
            content_text=body,
            content_hash=content_hash(title, body),
            normalized_title=normalize_title(title),
        )
        session.add(item)
        session.commit()
        item_id = item.id

        embedding_provider = CanonicalBatchProvider()
        translation_provider = CountingTranslationProvider()
        assert embed_items_by_ids(
            session,
            embedding_provider,
            "embedding-model",
            [item_id],
            batch_limit=8,
            translation_provider=translation_provider,
            translation_model="hy-mt2-1.8b",
        ) == 1

        item = session.get(ContentItem, item_id)
        canonical = session.scalar(select(ContentEmbedding).where(ContentEmbedding.content_item_id == item_id, ContentEmbedding.representation == "zh_canonical"))
        translation_count = len(session.scalars(select(LLMTask).where(LLMTask.task_type == "translation:text")).all())

    assert item is not None
    assert item.embedding_vector == "[0.0,1.0]"
    assert canonical is not None
    assert canonical.vector == "[1.0,0.0]"
    assert embedding_provider.batch_sizes == [1]
    assert embedding_provider.single_calls == 1
    assert translation_provider.inputs == [
        f"Translate to Simplified Chinese (output translation only):\n\n{title}",
        f"Translate to Simplified Chinese (output translation only):\n\n{body}",
    ]
    assert translation_count == 2


def test_embedding_by_ids_translates_foreign_title_even_when_body_is_chinese() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    events: list[str] = []

    class OrderedProvider:
        def embed_many(self, model: str, input_texts: list[str]) -> dict[str, object]:
            events.append(f"original:{len(input_texts)}")
            return {"data": [{"embedding": [0, 1]} for _ in input_texts]}

        def embed(self, model: str, input_text: str) -> dict[str, object]:
            events.append(f"canonical:{input_text}")
            return {"data": [{"embedding": [1, 0]}]}

    class TitleTranslationProvider:
        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            events.append(f"translate:{input_text}")
            return {"text": "译文：" + input_text.partition("\n\n")[2]}

    with Session() as session:
        source = Source(name="Mixed Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        title = "Apple takes Epic fight to the Supreme Court"
        body = "苹果与 Epic 的 App Store 费用争议进入美国最高法院审理阶段，开发者正在关注后续规则变化。"
        raw = make_raw_entry(source_id=source.id, external_id="mixed-1", title=title, content_hash=content_hash(title, body))
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title=title, summary=body, content_text=body)
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=title,
            summary=body,
            content_text=body,
            content_hash=content_hash(title, body),
            normalized_title=normalize_title(title),
        )
        session.add(item)
        session.commit()
        item_id = item.id

        assert item_needs_translation(item)
        assert embed_items_by_ids(
            session,
            OrderedProvider(),
            "embedding-model",
            [item_id],
            translation_provider=TitleTranslationProvider(),
            translation_model="hy-mt2-1.8b",
        ) == 1

        canonical = session.scalar(select(ContentEmbedding).where(ContentEmbedding.content_item_id == item_id, ContentEmbedding.representation == "zh_canonical"))
        cached_title = session.scalar(select(LLMTask).where(LLMTask.task_type == "translation:text"))

    assert events[0] == "original:1"
    assert events[1] == f"translate:Translate to Simplified Chinese (output translation only):\n\n{title}"
    assert events[2].startswith(f"canonical:译文：{title}")
    assert canonical is not None
    assert canonical.vector == "[1.0,0.0]"
    assert cached_title is not None


def test_zh_canonical_embedding_translates_long_body_in_safe_chunks() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    calls: list[int] = []

    class Provider:
        def embed_many(self, model: str, input_texts: list[str]) -> dict[str, object]:
            return {"data": [{"embedding": [0, 1]} for _ in input_texts]}

        def embed(self, model: str, input_text: str) -> dict[str, object]:
            return {"data": [{"embedding": [1, 0]}]}

    class TranslationProvider:
        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            calls.append(len(input_text))
            if len(input_text) > 4000:
                raise RuntimeError("too long")
            return {"text": f"译文：{input_text[:20]}"}

    with Session() as session:
        source = Source(name="English Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        title = "not much happened today"
        body = "Anthropic launched Claude Fable 5. " * 200
        raw = make_raw_entry(source_id=source.id, external_id="long-english", title=title, content_hash=content_hash(title, body))
        session.add(raw)
        session.flush()
        document = Document(raw_entry_id=raw.id, title=title, summary=body[:200], content_text=body)
        session.add(document)
        session.flush()
        item = ContentItem(document_id=document.id, source_id=source.id, title=title, summary=body[:200], content_text=body, content_hash=content_hash(title, body), normalized_title=normalize_title(title))
        session.add(item)
        session.commit()
        item_id = item.id

        assert embed_items_by_ids(session, Provider(), "embedding-model", [item_id], translation_provider=TranslationProvider(), translation_model="hy-mt2-1.8b") == 1
        canonical = session.scalar(select(ContentEmbedding).where(ContentEmbedding.content_item_id == item_id, ContentEmbedding.representation == "zh_canonical"))

    assert canonical is not None
    assert canonical.vector == "[1.0,0.0]"
    assert calls
    assert max(calls) <= 4000


def test_worker_fetch_all_queues_embedding_without_historical_repairs(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        worker,
        "prepare_runtime_database",
        lambda: calls.append("init"),
    )
    monkeypatch.setattr(worker, "SessionLocal", lambda: DummySession())
    monkeypatch.setattr(worker, "repair_duplicate_feed_entries", reject_automatic_repair, raising=False)
    monkeypatch.setattr(worker, "repair_over_split_documents", reject_automatic_repair, raising=False)
    monkeypatch.setattr(worker, "repair_title_only_clusters", reject_automatic_repair, raising=False)
    monkeypatch.setattr(worker, "repair_windowed_clusters", reject_automatic_repair, raising=False)
    def fake_fetch(
        _session: object,
        imported_item_ids: list[int] | None = None,
        run_result: dict[str, int] | None = None,
    ) -> int:
        calls.append("fetch")
        if imported_item_ids is not None:
            imported_item_ids.extend([10, 11, 12])
        if run_result is not None:
            run_result.update(attempted_sources=1, successful_sources=1)
        return 3

    monkeypatch.setattr(worker, "fetch_enabled_sources", fake_fetch)
    monkeypatch.setattr(worker, "enqueue_embedding_backfill", lambda: calls.append("enqueue-backfill"))

    assert worker.fetch_all() == {
        "imported": 3,
        "embedded": 0,
        "item_ids": [10, 11, 12],
        "embedding_model": worker.settings.embedding_model,
        "attempted_sources": 1,
        "successful_sources": 1,
    }
    assert calls == [
        "init",
        "fetch",
        "enqueue-backfill",
    ]


def test_worker_fetch_one_loads_and_fetches_only_the_requested_source(monkeypatch) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        requested = Source(name="Requested", url="https://example.com/requested.xml")
        other = Source(name="Other", url="https://example.com/other.xml")
        session.add_all([requested, other])
        session.commit()
        requested_id = requested.id
        other_id = other.id

    fetched: list[int] = []

    def fake_fetch(_session, source, imported_item_ids=None):
        fetched.append(source.id)
        imported_item_ids.append(101)
        return 1

    monkeypatch.setattr(worker, "prepare_runtime_database", lambda: None)
    monkeypatch.setattr(worker, "SessionLocal", Session)
    monkeypatch.setattr(worker, "fetch_source", fake_fetch)
    monkeypatch.setattr(worker, "enqueue_embedding_backfill", lambda: None)

    assert worker.fetch_one(requested_id) == {
        "source_id": requested_id,
        "imported": 1,
        "item_ids": [101],
    }
    assert fetched == [requested_id]
    assert other_id not in fetched


def test_worker_fetch_all_queues_committed_items_before_reraising_batch_error(
    monkeypatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(worker, "prepare_runtime_database", lambda: calls.append("init"))
    monkeypatch.setattr(worker, "SessionLocal", lambda: DummySession())

    def fake_fetch(
        _session: object,
        imported_item_ids: list[int] | None = None,
        run_result: dict[str, int] | None = None,
    ) -> int:
        calls.append("fetch")
        assert imported_item_ids is not None
        imported_item_ids.extend([10, 11])
        if run_result is not None:
            run_result.update(attempted_sources=1, successful_sources=0)
        raise RuntimeError("later source failed")

    monkeypatch.setattr(worker, "fetch_enabled_sources", fake_fetch)
    monkeypatch.setattr(
        worker,
        "enqueue_embedding_backfill",
        lambda: calls.append("enqueue-backfill"),
    )

    with pytest.raises(RuntimeError, match="later source failed"):
        worker.fetch_all()

    assert calls == ["init", "fetch", "enqueue-backfill"]


def test_pull_refresh_waits_for_new_article_embedding_and_accepts_partial_success(
    monkeypatch,
) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    class FetchJob:
        func_name = worker.FETCH_JOB_NAME
        meta: dict[str, object] = {}
        worker_name = ""
        last_heartbeat: datetime | None = None

        def __init__(self, status: str, result: dict[str, object] | None) -> None:
            self.status = status
            self.result = result

        def get_status(self, refresh: bool = True) -> str:
            return self.status

    class JobLookup:
        current = FetchJob("finished", None)

        @classmethod
        def fetch(cls, job_id: str, connection: object) -> FetchJob:
            assert job_id == "fetch-job"
            return cls.current

    class FakeQueue:
        def __init__(self, name: str, connection: object) -> None:
            self.name = name

    with Session() as session:
        sources = [
            Source(
                name="Article",
                url="https://example.com/article.xml",
                media_type="article",
            ),
            Source(
                name="Social",
                url="https://example.com/social.xml",
                media_type="social",
            ),
        ]
        session.add_all(sources)
        session.flush()
        items: list[ContentItem] = []
        for index, source in enumerate(sources):
            title = f"Refresh item {index}"
            raw = make_raw_entry(
                source_id=source.id,
                external_id=f"refresh-{index}",
                title=title,
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
                content_text=title,
                content_hash=content_hash(title),
            )
            session.add(item)
            items.append(item)
        session.flush()
        article, social = items

        JobLookup.current = FetchJob(
            "finished",
            {
                "item_ids": [article.id, social.id],
                "embedding_model": "embedding-model",
                "imported": 2,
            },
        )
        monkeypatch.setattr(worker, "Job", JobLookup)
        monkeypatch.setattr(worker, "Queue", FakeQueue)
        monkeypatch.setattr(worker, "embed_job_exists", lambda *_: True)

        assert worker.refresh_item_counts(
            session,
            [article.id, social.id],
            "embedding-model",
        ) == (1, 1)
        assert worker.fetch_refresh_status(
            session,
            "fetch-job",
            connection=object(),
        ) == "running"

        article.embedding_vector = "[1.0,0.0]"
        article.embedding_model = "embedding-model"
        cluster = Cluster(cluster_key="refresh-cluster", title=article.title)
        session.add(cluster)
        session.flush()
        session.add(
            ClusterItem(
                cluster_id=cluster.id,
                content_item_id=article.id,
            )
        )
        session.flush()

        assert worker.fetch_refresh_status(
            session,
            "fetch-job",
            connection=object(),
        ) == "complete"

        JobLookup.current = FetchJob(
            "finished",
            {
                "attempted_sources": 2,
                "successful_sources": 0,
                "imported": 0,
            },
        )
        assert worker.fetch_refresh_status(
            session,
            "fetch-job",
            connection=object(),
        ) == "failed"

        stale_job = FetchJob("started", None)
        stale_job.last_heartbeat = datetime.now(timezone.utc) - timedelta(
            seconds=worker.WORKER_HEARTBEAT_STALE_SECONDS + 1
        )
        JobLookup.current = stale_job
        assert worker.fetch_refresh_status(
            session,
            "fetch-job",
            connection=object(),
        ) == "failed"


def test_worker_fetch_all_queues_large_embedding_backfill_without_repairs(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        worker,
        "prepare_runtime_database",
        lambda: calls.append("init"),
    )
    monkeypatch.setattr(worker, "SessionLocal", lambda: DummySession())
    monkeypatch.setattr(worker, "repair_duplicate_feed_entries", reject_automatic_repair, raising=False)
    monkeypatch.setattr(worker, "repair_over_split_documents", reject_automatic_repair, raising=False)
    monkeypatch.setattr(worker, "repair_title_only_clusters", reject_automatic_repair, raising=False)
    monkeypatch.setattr(worker, "repair_windowed_clusters", reject_automatic_repair, raising=False)

    def fake_fetch(
        _session: object,
        imported_item_ids: list[int] | None = None,
        run_result: dict[str, int] | None = None,
    ) -> int:
        calls.append("fetch")
        if imported_item_ids is not None:
            imported_item_ids.extend(range(1, 21))
        if run_result is not None:
            run_result.update(attempted_sources=2, successful_sources=2)
        return 20

    monkeypatch.setattr(worker, "fetch_enabled_sources", fake_fetch)
    monkeypatch.setattr(worker, "enqueue_embedding_backfill", lambda: calls.append("enqueue-backfill"))

    assert worker.fetch_all() == {
        "imported": 20,
        "embedded": 0,
        "item_ids": list(range(1, 21)),
        "embedding_model": worker.settings.embedding_model,
        "attempted_sources": 2,
        "successful_sources": 2,
    }
    assert calls == [
        "init",
        "fetch",
        "enqueue-backfill",
    ]


def test_worker_fetch_all_requeues_embedding_backfill_when_no_new_items_but_pending_remains(monkeypatch) -> None:
    calls: list[str] = []

    class FakeRedis:
        @staticmethod
        def from_url(url: str) -> str:
            calls.append(f"redis:{url}")
            return url

    class FakeQueue:
        def __init__(self, name: str, connection: str) -> None:
            self.name = name
            self.jobs: list[object] = []
            calls.append(f"queue:{name}:{connection}")

        def enqueue(self, job_name: str, **kwargs: object) -> None:
            calls.append(f"enqueue:{job_name}:{kwargs['job_timeout']}")

    class FakeStartedJobRegistry:
        def __init__(self, name: str, connection: str) -> None:
            calls.append(f"started:{name}:{connection}")

        def get_job_ids(self) -> list[str]:
            return []

    monkeypatch.setattr(
        worker,
        "prepare_runtime_database",
        lambda: calls.append("init"),
    )
    monkeypatch.setattr(worker, "SessionLocal", lambda: DummySession())
    monkeypatch.setattr(worker, "repair_duplicate_feed_entries", reject_automatic_repair, raising=False)
    monkeypatch.setattr(worker, "repair_over_split_documents", reject_automatic_repair, raising=False)
    monkeypatch.setattr(worker, "repair_title_only_clusters", reject_automatic_repair, raising=False)
    monkeypatch.setattr(worker, "repair_windowed_clusters", reject_automatic_repair, raising=False)
    def fake_fetch(
        _session: object,
        imported_item_ids: list[int] | None = None,
        run_result: dict[str, int] | None = None,
    ) -> int:
        calls.append("fetch")
        if run_result is not None:
            run_result.update(attempted_sources=1, successful_sources=1)
        return 0

    monkeypatch.setattr(worker, "fetch_enabled_sources", fake_fetch)
    monkeypatch.setattr(worker, "has_pending_embeddings", lambda session, model: calls.append(f"pending:{model}") or True)
    monkeypatch.setattr(worker, "Redis", FakeRedis)
    monkeypatch.setattr(worker, "Queue", FakeQueue)
    monkeypatch.setattr(worker, "StartedJobRegistry", FakeStartedJobRegistry)

    assert worker.fetch_all() == {
        "imported": 0,
        "embedded": 0,
        "item_ids": [],
        "embedding_model": worker.settings.embedding_model,
        "attempted_sources": 1,
        "successful_sources": 1,
    }
    assert calls == [
        "init",
        "fetch",
        f"pending:{worker.settings.embedding_model}",
        f"redis:{worker.settings.redis_url}",
        f"queue:reader-llm:{worker.settings.redis_url}",
        f"started:reader-llm:{worker.settings.redis_url}",
        f"enqueue:{worker.EMBED_JOB_NAME}:{worker.settings.rq_job_timeout_seconds}",
    ]


def test_worker_embed_all_requeues_without_historical_repairs(monkeypatch) -> None:
    calls: list[str] = []

    class FakeProvider:
        def __init__(self, base_url: str, timeout_seconds: float, api_key: str = "") -> None:
            calls.append(f"provider:{base_url}:{timeout_seconds}:{api_key}")

    class FakeRedis:
        @staticmethod
        def from_url(url: str) -> str:
            calls.append(f"redis:{url}")
            return url

    class FakeQueue:
        def __init__(self, name: str, connection: str) -> None:
            self.name = name
            self.jobs: list[object] = []
            calls.append(f"queue:{name}:{connection}")

        def enqueue(self, job_name: str, **kwargs: object) -> None:
            calls.append(f"enqueue:{job_name}:{kwargs['job_timeout']}")

    class FakeStartedJobRegistry:
        def __init__(self, name: str, connection: str) -> None:
            calls.append(f"started:{name}:{connection}")

        def get_job_ids(self) -> list[str]:
            return []

    monkeypatch.setattr(
        worker,
        "prepare_runtime_database",
        lambda: calls.append("init"),
    )
    monkeypatch.setattr(worker, "SessionLocal", lambda: DummySession())
    monkeypatch.setattr(worker, "repair_title_only_clusters", reject_automatic_repair, raising=False)
    monkeypatch.setattr(worker, "repair_windowed_clusters", reject_automatic_repair, raising=False)
    monkeypatch.setattr(worker, "repair_embedding_clusters", reject_automatic_repair, raising=False)
    monkeypatch.setattr(worker, "LocalEmbeddingProvider", FakeProvider)
    monkeypatch.setattr(worker, "embed_pending_items", lambda session, provider, model, **kwargs: calls.append(f"embed:{model}:{kwargs['max_batches']}") or 4)
    monkeypatch.setattr(worker, "has_pending_embeddings", lambda session, model: calls.append(f"pending:{model}") or True)
    monkeypatch.setattr(worker, "Redis", FakeRedis)
    monkeypatch.setattr(worker, "Queue", FakeQueue)
    monkeypatch.setattr(worker, "StartedJobRegistry", FakeStartedJobRegistry)

    assert worker.embed_all() == 4
    assert calls == [
        "init",
        f"provider:{worker.settings.embedding_base_url}:{worker.settings.llm_timeout_seconds}:{worker.settings.embedding_api_key}",
        f"embed:{worker.settings.embedding_model}:{worker.EMBEDDING_AUTO_MAX_BATCHES}",
        f"pending:{worker.settings.embedding_model}",
        f"redis:{worker.settings.redis_url}",
        f"queue:reader-llm:{worker.settings.redis_url}",
        f"started:reader-llm:{worker.settings.redis_url}",
        f"enqueue:reader_api.worker.embed_all:{worker.settings.rq_job_timeout_seconds}",
    ]


def test_worker_schedules_fetch_jobs(monkeypatch) -> None:
    calls: list[str] = []

    class StopSchedule(Exception):
        pass

    class FakeRedis:
        @staticmethod
        def from_url(url: str) -> str:
            calls.append(f"redis:{url}")
            return url

    class FakeQueue:
        def __init__(self, name: str, connection: str) -> None:
            self.name = name
            self.jobs: list[object] = []
            calls.append(f"queue:{name}:{connection}")

        def enqueue(self, job_name: str, **kwargs: object) -> None:
            calls.append(f"enqueue:{job_name}:{kwargs['job_timeout']}")

    class FakeStartedJobRegistry:
        def __init__(self, name: str, connection: str) -> None:
            calls.append(f"started:{name}:{connection}")

        def get_job_ids(self) -> list[str]:
            return []

    def fake_sleep(seconds: int) -> None:
        calls.append(f"sleep:{seconds}")
        raise StopSchedule

    monkeypatch.setattr(worker, "Redis", FakeRedis)
    monkeypatch.setattr(worker, "Queue", FakeQueue)
    monkeypatch.setattr(worker, "StartedJobRegistry", FakeStartedJobRegistry)
    monkeypatch.setattr(
        worker,
        "run_scheduled_generation_retention",
        lambda: calls.append("retention"),
        raising=False,
    )
    monkeypatch.setattr(worker.time, "sleep", fake_sleep)
    monkeypatch.setattr(worker.settings, "rss_fetch_interval_seconds", 300)

    try:
        worker.schedule_fetch_jobs()
    except StopSchedule:
        pass

    assert calls == [
        "retention",
        f"redis:{worker.settings.redis_url}",
        f"queue:reader-fetch:{worker.settings.redis_url}",
        f"started:reader-fetch:{worker.settings.redis_url}",
        f"enqueue:reader_api.worker.fetch_all:{worker.settings.rq_job_timeout_seconds}",
        "sleep:300",
    ]


def test_worker_retries_fetch_schedule_soon_when_enqueue_skips(monkeypatch) -> None:
    calls: list[str] = []

    class StopSchedule(Exception):
        pass

    def fake_sleep(seconds: int) -> None:
        calls.append(f"sleep:{seconds}")
        raise StopSchedule

    monkeypatch.setattr(worker, "enqueue_fetch_job", lambda: calls.append("enqueue-skip") or False)
    monkeypatch.setattr(
        worker,
        "run_scheduled_generation_retention",
        lambda: calls.append("retention"),
        raising=False,
    )
    monkeypatch.setattr(worker.time, "sleep", fake_sleep)
    monkeypatch.setattr(worker.settings, "rss_fetch_interval_seconds", 300)

    try:
        worker.schedule_fetch_jobs()
    except StopSchedule:
        pass

    assert calls == ["retention", "enqueue-skip", "sleep:30"]


def test_worker_role_queues_are_separated() -> None:
    assert worker.worker_queues_for_role("fetch") == ["reader-fetch"]
    assert worker.worker_queues_for_role("llm") == ["reader-llm"]
    assert worker.worker_queues_for_role("embedding") == ["reader-llm"]
    assert worker.worker_queues_for_role("all") == ["reader-fetch", "reader-llm"]


def test_worker_skips_fetch_enqueue_when_fetch_is_already_queued(monkeypatch) -> None:
    calls: list[str] = []

    class FakeRedis:
        @staticmethod
        def from_url(url: str) -> str:
            calls.append(f"redis:{url}")
            return url

    class FakeJob:
        id = "queued-job"
        func_name = worker.FETCH_JOB_NAME

    class FakeQueue:
        def __init__(self, name: str, connection: str) -> None:
            self.name = name
            self.jobs = [FakeJob()]
            calls.append(f"queue:{name}:{connection}")

        def enqueue(self, job_name: str, **kwargs: object) -> None:
            calls.append(f"enqueue:{job_name}:{kwargs['job_timeout']}")

    monkeypatch.setattr(worker, "Redis", FakeRedis)
    monkeypatch.setattr(worker, "Queue", FakeQueue)

    assert worker.enqueue_fetch_job() is False
    assert calls == [
        f"redis:{worker.settings.redis_url}",
        f"queue:reader-fetch:{worker.settings.redis_url}",
    ]


def test_fetch_enqueue_check_and_enqueue_are_serialized(monkeypatch) -> None:
    enqueued: list[str] = []
    mutex = Lock()
    lock_entries = 0

    class FakeConnection:
        def lock(self, *_args: object, **_kwargs: object) -> Lock:
            nonlocal lock_entries
            lock_entries += 1
            return mutex

    connection = FakeConnection()

    class FakeRedis:
        @staticmethod
        def from_url(_url: str) -> FakeConnection:
            return connection

    class FakeQueue:
        def __init__(self, _name: str, connection: FakeConnection) -> None:
            self.connection = connection

        def enqueue(self, job_name: str, **_kwargs: object) -> None:
            enqueued.append(job_name)

    monkeypatch.setattr(worker, "Redis", FakeRedis)
    monkeypatch.setattr(worker, "Queue", FakeQueue)
    monkeypatch.setattr(
        worker,
        "matching_job_id",
        lambda _queue, _connection, _job_name: (
            "existing-job" if enqueued and lock_entries else None
        ),
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: worker.enqueue_fetch_job(), range(2)))

    assert sorted(results) == [False, True]
    assert enqueued == [worker.FETCH_JOB_NAME]


def test_worker_skips_fetch_enqueue_when_fetch_is_running_in_wip(monkeypatch) -> None:
    calls: list[str] = []

    class FakeConnection:
        def zrange(self, key: str, start: int, end: int) -> list[bytes]:
            calls.append(f"zrange:{key}:{start}:{end}")
            return [b"running-job:execution"]

    class FakeRedis:
        @staticmethod
        def from_url(url: str) -> FakeConnection:
            calls.append(f"redis:{url}")
            return FakeConnection()

    class FakeQueue:
        def __init__(self, name: str, connection: FakeConnection) -> None:
            self.name = name
            self.jobs: list[object] = []
            calls.append(f"queue:{name}:{connection.__class__.__name__}")

        def enqueue(self, job_name: str, **kwargs: object) -> None:
            calls.append(f"enqueue:{job_name}:{kwargs['job_timeout']}")

    class FakeFetchedJob:
        func_name = worker.FETCH_JOB_NAME

    class FakeJob:
        @staticmethod
        def fetch(job_id: str, connection: FakeConnection) -> FakeFetchedJob:
            calls.append(f"fetch:{job_id}:{connection.__class__.__name__}")
            return FakeFetchedJob()

    monkeypatch.setattr(worker, "Redis", FakeRedis)
    monkeypatch.setattr(worker, "Queue", FakeQueue)
    monkeypatch.setattr(worker, "Job", FakeJob)

    assert worker.enqueue_fetch_job() is False
    assert calls == [
        f"redis:{worker.settings.redis_url}",
        "queue:reader-fetch:FakeConnection",
        "zrange:rq:wip:reader-fetch:0:-1",
        "fetch:running-job:FakeConnection",
    ]


def test_embedding_backfill_requeue_ignores_current_running_job(monkeypatch) -> None:
    calls: list[str] = []

    class FakeConnection:
        def zrange(self, key: str, start: int, end: int) -> list[bytes]:
            calls.append(f"zrange:{key}:{start}:{end}")
            return [b"current-job:execution"]

    class FakeRedis:
        @staticmethod
        def from_url(url: str) -> FakeConnection:
            calls.append(f"redis:{url}")
            return FakeConnection()

    class FakeQueue:
        def __init__(self, name: str, connection: FakeConnection) -> None:
            self.name = name
            self.jobs: list[object] = []
            calls.append(f"queue:{name}:{connection.__class__.__name__}")

        def enqueue(self, job_name: str, **kwargs: object) -> None:
            calls.append(f"enqueue:{job_name}:{kwargs['job_timeout']}")

    class FakeStartedJobRegistry:
        def __init__(self, name: str, connection: FakeConnection) -> None:
            calls.append(f"started:{name}:{connection.__class__.__name__}")

        def get_job_ids(self) -> list[str]:
            return []

    class FakeCurrentJob:
        id = "current-job"

    monkeypatch.setattr(worker, "Redis", FakeRedis)
    monkeypatch.setattr(worker, "Queue", FakeQueue)
    monkeypatch.setattr(worker, "StartedJobRegistry", FakeStartedJobRegistry)
    monkeypatch.setattr(worker, "get_current_job", lambda connection: FakeCurrentJob())

    assert worker.enqueue_embedding_backfill(ignore_current_job=True) is True
    assert calls == [
        f"redis:{worker.settings.redis_url}",
        "queue:reader-llm:FakeConnection",
        "zrange:rq:wip:reader-llm:0:-1",
        "started:reader-llm:FakeConnection",
        f"enqueue:{worker.EMBED_JOB_NAME}:{worker.settings.rq_job_timeout_seconds}",
    ]


def test_embedding_backfill_ignores_stale_wip_worker(monkeypatch) -> None:
    calls: list[str] = []

    class FakeConnection:
        def smembers(self, key: str) -> list[bytes]:
            calls.append(f"smembers:{key}")
            return [b"rq:worker:stale-worker", b"rq:worker:active-worker"]

        def exists(self, key: str) -> int:
            calls.append(f"exists:{key}")
            return 1 if key.endswith("active-worker") else 0

        def zrange(self, key: str, start: int, end: int) -> list[bytes]:
            calls.append(f"zrange:{key}:{start}:{end}")
            return [b"stale-job:execution"]

    class FakeRedis:
        @staticmethod
        def from_url(url: str) -> FakeConnection:
            calls.append(f"redis:{url}")
            return FakeConnection()

    class FakeQueue:
        def __init__(self, name: str, connection: FakeConnection) -> None:
            self.name = name
            self.jobs: list[object] = []
            calls.append(f"queue:{name}:{connection.__class__.__name__}")

        def enqueue(self, job_name: str, **kwargs: object) -> None:
            calls.append(f"enqueue:{job_name}:{kwargs['job_timeout']}")

    class FakeStartedJobRegistry:
        def __init__(self, name: str, connection: FakeConnection) -> None:
            calls.append(f"started:{name}:{connection.__class__.__name__}")

        def get_job_ids(self) -> list[str]:
            return []

    class FakeFetchedJob:
        func_name = worker.EMBED_JOB_NAME
        worker_name = "stale-worker"

    class FakeJob:
        @staticmethod
        def fetch(job_id: str, connection: FakeConnection) -> FakeFetchedJob:
            calls.append(f"fetch:{job_id}:{connection.__class__.__name__}")
            return FakeFetchedJob()

    monkeypatch.setattr(worker, "Redis", FakeRedis)
    monkeypatch.setattr(worker, "Queue", FakeQueue)
    monkeypatch.setattr(worker, "StartedJobRegistry", FakeStartedJobRegistry)
    monkeypatch.setattr(worker, "Job", FakeJob)

    assert worker.enqueue_embedding_backfill() is True
    assert calls == [
        f"redis:{worker.settings.redis_url}",
        "queue:reader-llm:FakeConnection",
        "smembers:rq:workers:reader-llm",
        "exists:rq:worker:stale-worker",
        "exists:rq:worker:active-worker",
        "zrange:rq:wip:reader-llm:0:-1",
        "fetch:stale-job:FakeConnection",
        "started:reader-llm:FakeConnection",
        f"enqueue:{worker.EMBED_JOB_NAME}:{worker.settings.rq_job_timeout_seconds}",
    ]


def test_fetch_enqueue_ignores_shutdown_worker_wip(monkeypatch) -> None:
    calls: list[str] = []

    class FakeConnection:
        def smembers(self, key: str) -> list[bytes]:
            calls.append(f"smembers:{key}")
            return [b"rq:worker:old-worker", b"rq:worker:new-worker"]

        def exists(self, key: str) -> int:
            calls.append(f"exists:{key}")
            return 1

        def hgetall(self, key: str) -> dict[bytes, bytes]:
            calls.append(f"hgetall:{key}")
            if key.endswith("old-worker"):
                return {b"shutdown_requested_date": b"2026-07-02T06:33:57.258631Z", b"last_heartbeat": b"2026-07-02T06:34:04.528794Z"}
            return {b"last_heartbeat": b"2099-01-01T00:00:00Z"}

        def zrange(self, key: str, start: int, end: int) -> list[bytes]:
            calls.append(f"zrange:{key}:{start}:{end}")
            return [b"old-job:execution"]

    class FakeRedis:
        @staticmethod
        def from_url(url: str) -> FakeConnection:
            calls.append(f"redis:{url}")
            return FakeConnection()

    class FakeQueue:
        def __init__(self, name: str, connection: FakeConnection) -> None:
            self.name = name
            self.jobs: list[object] = []
            calls.append(f"queue:{name}:{connection.__class__.__name__}")

        def enqueue(self, job_name: str, **kwargs: object) -> None:
            calls.append(f"enqueue:{job_name}:{kwargs['job_timeout']}")

    class FakeStartedJobRegistry:
        def __init__(self, name: str, connection: FakeConnection) -> None:
            calls.append(f"started:{name}:{connection.__class__.__name__}")

        def get_job_ids(self) -> list[str]:
            return []

    class FakeFetchedJob:
        func_name = worker.FETCH_JOB_NAME
        worker_name = "old-worker"

    class FakeJob:
        @staticmethod
        def fetch(job_id: str, connection: FakeConnection) -> FakeFetchedJob:
            calls.append(f"fetch:{job_id}:{connection.__class__.__name__}")
            return FakeFetchedJob()

    monkeypatch.setattr(worker, "Redis", FakeRedis)
    monkeypatch.setattr(worker, "Queue", FakeQueue)
    monkeypatch.setattr(worker, "StartedJobRegistry", FakeStartedJobRegistry)
    monkeypatch.setattr(worker, "Job", FakeJob)

    assert worker.enqueue_fetch_job() is True
    assert calls == [
        f"redis:{worker.settings.redis_url}",
        "queue:reader-fetch:FakeConnection",
        "smembers:rq:workers:reader-fetch",
        "exists:rq:worker:old-worker",
        "hgetall:rq:worker:old-worker",
        "exists:rq:worker:new-worker",
        "hgetall:rq:worker:new-worker",
        "zrange:rq:wip:reader-fetch:0:-1",
        "fetch:old-job:FakeConnection",
        "started:reader-fetch:FakeConnection",
        f"enqueue:{worker.FETCH_JOB_NAME}:{worker.settings.rq_job_timeout_seconds}",
    ]


def test_embedding_batch_recomputes_when_model_changes() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="1", title="One item", content_hash=content_hash("One item"))
        session.add(raw)
        session.flush()
        doc = Document(raw_entry_id=raw.id, title="One item", content_text="One item")
        session.add(doc)
        session.flush()
        session.add(
            ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title="One item",
                summary="One item",
                content_text="One item",
                content_hash=content_hash("One item", "item"),
                normalized_title=normalize_title("One item"),
                embedding_vector="[1.0,0.0]",
                embedding_model="old-model",
            )
        )
        session.commit()

        assert embed_missing_items(session, ModelProvider(), "new-model") == 1
        item = session.scalars(select(ContentItem)).one()

    assert item.embedding_vector == "[2.0,0.0]"
    assert item.embedding_model == "new-model"


def test_embedding_batch_does_not_recompute_for_missing_lsh_signature() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="AI Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        raw = make_raw_entry(source_id=source.id, external_id="1", title="One item", content_hash=content_hash("One item"))
        session.add(raw)
        session.flush()
        doc = Document(raw_entry_id=raw.id, title="One item", content_text="One item")
        session.add(doc)
        session.flush()
        session.add(
            ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title="One item",
                summary="One item",
                content_text="One item",
                content_hash=content_hash("One item", "item"),
                normalized_title=normalize_title("One item"),
                embedding_vector="[1.0,0.0]",
                embedding_model="embedding-model",
                lsh_signature="",
            )
        )
        session.commit()

        assert embed_missing_items(session, FlakyProvider(), "embedding-model") == 0
        item = session.scalars(select(ContentItem)).one()

    assert item.embedding_vector == "[1.0,0.0]"
    assert item.embedding_model == "embedding-model"
    assert item.lsh_signature == ""


def test_postgres_neighbor_query_uses_pgvector_ordering() -> None:
    assert "<=>" in POSTGRES_NEIGHBOR_SQL
    assert "ORDER BY ci.embedding_vector <=> CAST(:target_vector AS halfvec(2560))" in POSTGRES_NEIGHBOR_SQL
    assert "CAST(:start_at AS TIMESTAMP WITH TIME ZONE) IS NULL" in POSTGRES_NEIGHBOR_SQL
    assert "CAST(:end_at AS TIMESTAMP WITH TIME ZONE) IS NULL" in POSTGRES_NEIGHBOR_SQL
    assert "LIMIT :limit" in POSTGRES_NEIGHBOR_SQL
    assert "CAST(:embedding_vector AS halfvec(2560))" in POSTGRES_STORE_EMBEDDING_SQL
    assert vector_literal([1, 0.5]) == "[1.0,0.5]"


def test_repair_embedding_clusters_merges_stale_cross_language_singleton() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    now = datetime.now(timezone.utc) - timedelta(days=1)
    with Session() as session:
        chinese_source = Source(name="IT之家", url="https://example.com/cn.xml")
        wired_source = Source(name="Wired", url="https://example.com/wired.xml")
        engadget_source = Source(name="Engadget", url="https://example.com/engadget.xml")
        noise_source = Source(name="Noise", url="https://example.com/noise.xml")
        session.add_all([chinese_source, wired_source, engadget_source, noise_source])
        session.flush()
        item_ids = []
        for index, (source, title, summary, vector) in enumerate(
            [
                (
                    chinese_source,
                    "Anthropic 现可出口 Fable 及 Mythos AI 模型",
                    "美国商务部撤销 Anthropic 的 Fable 和 Mythos 出口限制。",
                    "[1.0,0.0,0.0]",
                ),
                (
                    wired_source,
                    "Trump drops restrictions on Anthropic Mythos and Fable models",
                    "Anthropic said access to Fable and Mythos would resume.",
                    "[0.82,0.572,0.0]",
                ),
                (
                    engadget_source,
                    "US government allows Anthropic to redeploy Mythos and Fable AI models",
                    "Anthropic will restart access to Mythos and Fable tomorrow.",
                    "[0.8,-0.6,0.0]",
                ),
            ],
            1,
        ):
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=summary)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=summary,
                content_text=summary,
                published_at=now + timedelta(minutes=index),
                content_hash=content_hash(title, str(index)),
                normalized_title=normalize_title(title),
                embedding_vector=vector,
                embedding_model="embedding-model",
            )
            session.add(item)
            session.flush()
            assign_cluster(session, item)
            session.add(
                ContentEmbedding(
                    content_item_id=item.id,
                    representation="zh_canonical",
                    model="embedding-model",
                    vector="[1.0,0.0,0.0]",
                )
            )
            item_ids.append(item.id)
        for index in range(170):
            title = f"Unrelated newer story {index}"
            raw = make_raw_entry(source_id=noise_source.id, external_id=f"noise-{index}", title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=noise_source.id,
                title=title,
                summary=title,
                content_text=title,
                published_at=now + timedelta(hours=1, minutes=index),
                content_hash=content_hash(title, str(index)),
                normalized_title=normalize_title(title),
                embedding_vector="[0.0,1.0,0.0]",
                embedding_model="embedding-model",
            )
            session.add(item)
            session.flush()
            assign_cluster(session, item)
        before = {row[0] for row in session.execute(select(ClusterItem.cluster_id).where(ClusterItem.content_item_id.in_(item_ids))).all()}
        assert len(before) == 3

        assert repair_embedding_clusters(session, "embedding-model") == 2
        after = {row[0] for row in session.execute(select(ClusterItem.cluster_id).where(ClusterItem.content_item_id.in_(item_ids))).all()}
        item_count = session.query(ClusterItem).filter(ClusterItem.content_item_id.in_(item_ids)).count()

    assert len(after) == 1
    assert item_count == 3


def test_repair_title_only_clusters_splits_legacy_title_merge() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="Blog", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        title = "Not much happened today"
        cluster = Cluster(cluster_key=content_hash(normalize_title(title))[:40], title=title)
        session.add(cluster)
        session.flush()
        for index in range(2):
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
                content_hash=content_hash(title, str(index)),
                normalized_title=normalize_title(title),
                embedding_vector="[1.0,0.0]",
                embedding_model="embedding-model",
            )
            session.add(item)
            session.flush()
            session.add(ClusterItem(cluster_id=cluster.id, content_item_id=item.id, duplicate_score=1.0))
        session.commit()

        assert repair_title_only_clusters(session) == 1
        cluster_ids = {row[0] for row in session.execute(select(ClusterItem.cluster_id)).all()}

    assert len(cluster_ids) == 2


def test_repair_windowed_clusters_splits_legacy_chained_embedding_cluster() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        sources = [
            Source(name="Daily Feed 1", url="https://example.com/1.xml"),
            Source(name="Daily Feed 2", url="https://example.com/2.xml"),
            Source(name="Daily Feed 3", url="https://example.com/3.xml"),
        ]
        session.add_all(sources)
        session.flush()
        dates = [
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 30, tzinfo=timezone.utc),
            datetime(2026, 2, 28, tzinfo=timezone.utc),
        ]
        cluster = Cluster(cluster_key="legacy-daily-chain", title="分享创造日报", first_seen_at=dates[0], last_seen_at=dates[-1])
        session.add(cluster)
        session.flush()
        for index, published_at in enumerate(dates):
            source = sources[index]
            title = f"分享创造日报 - {published_at.date()}"
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=f"Daily format item {index}")
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=f"Daily format item {index}",
                url=f"https://example.com/daily/{index}",
                published_at=published_at,
                canonical_url=f"https://example.com/daily/{index}",
                content_hash=content_hash(title, str(index)),
                normalized_title=normalize_title(title),
                lsh_signature=lsh_signature("daily recurring format", "daily recurring format"),
                embedding_vector="[1.0,0.0]",
                embedding_model="embedding-model",
            )
            session.add(item)
            session.flush()
            session.add(ClusterItem(cluster_id=cluster.id, content_item_id=item.id, duplicate_score=0.96))
        session.commit()

        assert repair_windowed_clusters(session) == 1
        rows = session.execute(select(Cluster.first_seen_at, Cluster.last_seen_at).order_by(Cluster.first_seen_at)).all()

    assert len(rows) == 2
    assert all(last - first <= timedelta(days=31) for first, last in rows)


def test_repair_windowed_clusters_splits_single_source_soft_cluster() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="One Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        dates = [
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        ]
        cluster = Cluster(cluster_key="legacy-single-source", title="Daily template", first_seen_at=dates[0], last_seen_at=dates[-1])
        session.add(cluster)
        session.flush()
        for index, published_at in enumerate(dates):
            title = f"Daily template {index}"
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
                url=f"https://example.com/story/{index}",
                published_at=published_at,
                canonical_url=f"https://example.com/story/{index}",
                content_hash=content_hash(title, str(index)),
                normalized_title=normalize_title(title),
                lsh_signature=lsh_signature("daily template", "daily template"),
                embedding_vector="[1.0,0.0]",
                embedding_model="embedding-model",
            )
            session.add(item)
            session.flush()
            session.add(ClusterItem(cluster_id=cluster.id, content_item_id=item.id, duplicate_score=0.96))
        session.commit()

        assert repair_windowed_clusters(session) == 1
        cluster_ids = {row[0] for row in session.execute(select(ClusterItem.cluster_id)).all()}

    assert len(cluster_ids) == 2


def test_embedding_similarity_does_not_require_lsh_signature() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        sources = [
            Source(name="AI Feed 1", url="https://example.com/1.xml"),
            Source(name="AI Feed 2", url="https://example.com/2.xml"),
        ]
        session.add_all(sources)
        session.flush()
        clusters = []
        for index, signature in enumerate([lsh_signature("OpenAI data startup", "OpenAI data startup"), ""], 1):
            source = sources[index - 1]
            title = f"OpenAI data startup {index}"
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
                url=f"https://example.com/story/{index}",
                canonical_url=f"https://example.com/story/{index}",
                content_hash=content_hash(title, str(index)),
                normalized_title=normalize_title(title),
                lsh_signature=signature,
                embedding_vector="[1.0,0.0]",
                embedding_model="embedding-model",
            )
            session.add(item)
            session.flush()
            clusters.append(assign_cluster(session, item).id)

    assert len(set(clusters)) == 1


def test_replace_existing_does_not_reuse_legacy_multi_item_cluster() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="One Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        items = []
        for index in range(2):
            title = f"Different story {index}"
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
                url=f"https://example.com/story/{index}",
                canonical_url=f"https://example.com/story/{index}",
                content_hash=content_hash(title, str(index)),
                normalized_title=normalize_title(title),
                embedding_vector="[1.0,0.0]",
                embedding_model="embedding-model",
            )
            session.add(item)
            session.flush()
            items.append(item)
        cluster = Cluster(cluster_key=cluster_key_for(items[0]), title="Legacy soft")
        session.add(cluster)
        session.flush()
        for item in items:
            session.add(ClusterItem(cluster_id=cluster.id, content_item_id=item.id, duplicate_score=0.96))
        session.commit()

        assign_cluster(session, items[0], replace_existing=True)
        cluster_ids = {row[0] for row in session.execute(select(ClusterItem.cluster_id)).all()}

    assert len(cluster_ids) == 2


def test_repair_windowed_clusters_keeps_single_source_exact_duplicates() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="One Feed", url="https://example.com/rss.xml")
        session.add(source)
        session.flush()
        cluster = Cluster(cluster_key="exact", title="Exact duplicate")
        session.add(cluster)
        session.flush()
        for index in range(2):
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title="Same story", content_hash=content_hash("raw", str(index)))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title="Same story", content_text="Same story")
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title="Same story",
                summary="Same story",
                content_text="Same story",
                url="https://example.com/same",
                canonical_url="https://example.com/same",
                content_hash=content_hash("item", str(index)),
                normalized_title=normalize_title("Same story"),
            )
            session.add(item)
            session.flush()
            session.add(ClusterItem(cluster_id=cluster.id, content_item_id=item.id, duplicate_score=1.0))
        session.commit()

        assert repair_windowed_clusters(session) == 0
        cluster_ids = {row[0] for row in session.execute(select(ClusterItem.cluster_id)).all()}

    assert len(cluster_ids) == 1


def test_cross_language_cluster_uses_zh_canonical_embedding_instead_of_lowering_threshold() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    with Session() as session:
        chinese_source = Source(name="IT之家", url="https://example.com/cn.xml")
        english_source = Source(name="Ars", url="https://example.com/en.xml")
        session.add_all([chinese_source, english_source])
        session.flush()
        items: list[ContentItem] = []
        for index, (source, title, body, vector) in enumerate(
            [
                (
                    chinese_source,
                    "巅峰对决：美国最高法院受理苹果和 Epic 纠纷上诉",
                    "App Store 外链抽佣争议涉及 Epic Games。",
                    "[1.0,0.0]",
                ),
                (
                    english_source,
                    "Supreme Court agrees to hear Apple appeal over Epic Games ruling",
                    "The App Store fee dispute returns to court.",
                    "[0.82,0.572]",
                ),
            ],
            1,
        ):
            raw = make_raw_entry(source_id=source.id, external_id=str(index), title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=body)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=body,
                content_text=body,
                published_at=now + timedelta(minutes=index),
                content_hash=content_hash(title, body),
                normalized_title=normalize_title(title),
                embedding_vector=vector,
                embedding_model="embedding-model",
            )
            session.add(item)
            session.flush()
            assign_cluster(session, item)
            items.append(item)
        assert embedding_threshold_for(items[0], items[1]) == EMBEDDING_THRESHOLD
        before = {row[0] for row in session.execute(select(ClusterItem.cluster_id)).all()}
        assert len(before) == 2

        assert ensure_zh_canonical_embedding(session, items[0], CrossLanguageProvider(), "embedding-model", TranslationProvider(), "hy-mt2-1.8b") == [1.0, 0.0]
        assert ensure_zh_canonical_embedding(session, items[1], CrossLanguageProvider(), "embedding-model", TranslationProvider(), "hy-mt2-1.8b") == [1.0, 0.0]
        assign_cluster(
            session,
            items[1],
            replace_existing=True,
            target_vector=[0.82, 0.572],
            embedding_provider=CrossLanguageProvider(),
            translation_provider=TranslationProvider(),
            embedding_model="embedding-model",
            translation_model="hy-mt2-1.8b",
        )
        after = {row[0] for row in session.execute(select(ClusterItem.cluster_id)).all()}

    assert after != before
    assert len(after) == 1


def test_gray_zone_cluster_uses_zh_title_embedding() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    now = datetime(2026, 7, 3, tzinfo=timezone.utc)

    class TitleProvider:
        def embed(self, model: str, input_text: str) -> dict[str, object]:
            compact = input_text.replace(" ", "")
            if "\n\n" not in input_text and "iOS27.4" in compact:
                return {"data": [{"embedding": [1, 0]}]}
            if "IT之家" in input_text:
                return {"data": [{"embedding": [1, 0]}]}
            return {"data": [{"embedding": [0.82, 0.572]}]}

    class TitleTranslationProvider:
        def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
            if "Apple Already Testing iOS 27.4" in input_text:
                return {"text": "苹果正在测试 iOS 27.4"}
            return {"text": "苹果软件工程师正在测试 iOS 27.4。"}

    with Session() as session:
        cn_source = Source(name="IT之家", url="https://example.com/cn.xml")
        en_source = Source(name="MacRumors", url="https://example.com/en.xml")
        session.add_all([cn_source, en_source])
        session.flush()
        items = []
        for source, title, body, vector in [
            (
                cn_source,
                "苹果内部正测试 iOS 27.4，预计明年春季发布",
                "IT之家消息，苹果软件工程师正在内部测试 iOS 27.4，并补充了 iOS 27.0、Siri 和 Liquid Glass 等背景。",
                "[1.0,0.0]",
            ),
            (
                en_source,
                "Apple Already Testing iOS 27.4",
                "Apple's software engineers recently began testing iOS 27.4, according to MacRumors visitor logs.",
                "[0.82,0.572]",
            ),
        ]:
            raw = make_raw_entry(source_id=source.id, external_id=title, title=title, content_hash=content_hash(title))
            session.add(raw)
            session.flush()
            doc = Document(raw_entry_id=raw.id, title=title, content_text=body)
            session.add(doc)
            session.flush()
            item = ContentItem(
                document_id=doc.id,
                source_id=source.id,
                title=title,
                summary=body,
                content_text=body,
                published_at=now,
                content_hash=content_hash(title, body),
                normalized_title=normalize_title(title),
                embedding_vector=vector,
                embedding_model="embedding-model",
            )
            session.add(item)
            session.flush()
            items.append(item)

        first_cluster = assign_cluster(session, items[0])
        second_cluster = assign_cluster(
            session,
            items[1],
            embedding_provider=TitleProvider(),
            translation_provider=TitleTranslationProvider(),
            embedding_model="embedding-model",
            translation_model="hy-mt2-1.8b",
        )
        title_embeddings = session.scalars(select(ContentEmbedding).where(ContentEmbedding.representation == "zh_title")).all()

    assert second_cluster.id == first_cluster.id
    assert len(title_embeddings) == 2
