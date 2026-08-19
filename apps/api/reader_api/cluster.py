import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, func, or_, select, text, tuple_
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from .digest import content_hash, lsh_signature
from .clustering_run import (
    ClusteringRule,
    clustering_run,
    clustering_run_execution_lock,
    defer_clustering_run_lock_until_transaction_end,
    mark_clustering_run_incomplete,
    register_clustering_target,
)
from .llm import EmbeddingProvider, LLMProvider, embedding_from_response, embeddings_from_response
from .models import Cluster, ClusterItem, ContentEmbedding, ContentItem, Source
from . import translations as translation_utils

WINDOW_DAYS = 31
EMBEDDING_THRESHOLD = 0.85
CROSS_LANGUAGE_ENTITY_THRESHOLD = 0.78
MIN_SHARED_LATIN_TERMS = 3
EMBEDDING_BATCH_LIMIT = 8
EMBEDDING_AUTO_MAX_BATCHES = 2
EMBEDDING_RECLUSTER_LIMIT = 1000
EMBEDDING_NEIGHBOR_LIMIT = 50
ZH_CANONICAL_REPRESENTATION = "zh_canonical"
ZH_TITLE_REPRESENTATION = "zh_title"
ZH_CANONICAL_CANDIDATE_THRESHOLD = 0.75
TITLE_CANDIDATE_THRESHOLD = 0.80
TITLE_EMBEDDING_THRESHOLD = 0.88
CANONICAL_TEXT_LIMIT = 4000
logger = logging.getLogger(__name__)
SOURCE_DECLUSTER_RULE_VERSION = ClusteringRule("source-decluster-v1").version
TITLE_REPAIR_RULE_VERSION = ClusteringRule("repair-title-only-v1").version
WINDOW_REPAIR_RULE_VERSION = ClusteringRule("repair-windowed-v1").version
EXACT_CONTENT_REPAIR_RULE_VERSION = ClusteringRule(
    "repair-exact-content-duplicates-v1"
).version
POSTGRES_NEIGHBOR_SQL = """
SELECT
  ci.id AS item_id,
  ci.source_id AS source_id,
  cl.id AS cluster_id,
  1 - (ci.embedding_vector <=> CAST(:target_vector AS halfvec(2560))) AS score
FROM content_items ci
JOIN cluster_items cli ON cli.content_item_id = ci.id
JOIN clusters cl ON cl.id = cli.cluster_id
JOIN sources s ON s.id = ci.source_id
WHERE ci.id <> :item_id
  AND ci.source_id <> :source_id
  AND ci.embedding_vector IS NOT NULL
  AND ci.embedding_model = :embedding_model
  AND s.status = 'active'
  AND s.media_type = 'article'
  AND (CAST(:start_at AS TIMESTAMP WITH TIME ZONE) IS NULL OR ci.published_at >= CAST(:start_at AS TIMESTAMP WITH TIME ZONE))
  AND (CAST(:end_at AS TIMESTAMP WITH TIME ZONE) IS NULL OR ci.published_at <= CAST(:end_at AS TIMESTAMP WITH TIME ZONE))
ORDER BY ci.embedding_vector <=> CAST(:target_vector AS halfvec(2560))
LIMIT :limit
"""
POSTGRES_STORE_EMBEDDING_SQL = """
UPDATE content_items
SET embedding_vector = CAST(:embedding_vector AS halfvec(2560)),
    embedding_model = :embedding_model,
    lsh_signature = :lsh_signature
WHERE id = :item_id
"""
POSTGRES_STORE_CONTENT_EMBEDDING_SQL = """
INSERT INTO content_embeddings (content_item_id, representation, model, vector, created_at)
VALUES (:content_item_id, :representation, :model, CAST(:vector AS halfvec(2560)), CURRENT_TIMESTAMP)
ON CONFLICT (content_item_id, representation, model)
DO UPDATE SET vector = EXCLUDED.vector, created_at = EXCLUDED.created_at
"""
POSTGRES_CONTENT_EMBEDDING_SQL = """
SELECT vector::text
FROM content_embeddings
WHERE content_item_id = :content_item_id
  AND representation = :representation
  AND model = :model
LIMIT 1
"""


@dataclass(frozen=True)
class EmbeddingRepairCandidate:
    item_id: int
    cluster_id: int


LATIN_TERM_STOPWORDS = {
    "about",
    "after",
    "and",
    "com",
    "for",
    "from",
    "http",
    "https",
    "image",
    "img",
    "jpg",
    "png",
    "that",
    "the",
    "this",
    "with",
}


def cluster_key_for(item: ContentItem) -> str:
    base = item.canonical_url or item.content_hash
    return content_hash(base)[:40]


def assign_cluster(
    session: Session,
    item: ContentItem,
    replace_existing: bool = False,
    target_vector: list[float] | None = None,
    embedding_provider: EmbeddingProvider | None = None,
    translation_provider: LLMProvider | None = None,
    embedding_model: str = "",
    translation_model: str = "",
) -> Cluster:
    target = target_vector or parse_vector(item.embedding_vector)
    if not target or not ((embedding_model or item.embedding_model) or "").strip():
        raise ValueError("ContentItem 完成 Embedding 后才能分配 Cluster 并发布 Event")
    old_cluster_ids = detach_item(session, item) if replace_existing else set()
    cluster = matching_exact_cluster(session, item)
    score = 1.0
    if cluster is None:
        cluster, score = embedding_cluster(session, item, target, embedding_provider, translation_provider, embedding_model, translation_model)
    if cluster is None:
        cluster, score = exact_cluster(session, item, reusable_old_cluster(session, old_cluster_ids, item)), 1.0

    register_clustering_target(session, cluster.id)

    exists = session.scalar(
        select(ClusterItem).where(
            ClusterItem.cluster_id == cluster.id,
            ClusterItem.content_item_id == item.id,
        )
    )
    if exists is None:
        session.add(ClusterItem(cluster_id=cluster.id, content_item_id=item.id, duplicate_score=score))
    item.cluster_score = score
    refresh_cluster_dates(cluster, item)
    removed_cluster_ids = old_cluster_ids - {cluster.id}
    prune_clusters(session, removed_cluster_ids)
    return cluster


def exact_cluster(session: Session, item: ContentItem, fallback_cluster: Cluster | None = None) -> Cluster:
    cluster = matching_exact_cluster(session, item)
    if cluster is not None and cluster_within_window(cluster, item):
        return cluster
    if fallback_cluster is not None and cluster_within_window(fallback_cluster, item):
        return fallback_cluster

    key = cluster_key_for(item)
    cluster = active_cluster_by_key(session, key, item)
    if cluster is None and session.scalar(select(Cluster.id).where(Cluster.cluster_key == key)) is not None:
        key = windowed_cluster_key(key, item)
        cluster = active_cluster_by_key(session, key, item)
    if cluster is not None and not cluster_within_window(cluster, item):
        key = windowed_cluster_key(key, item)
        cluster = active_cluster_by_key(session, key, item)
    if cluster is None:
        key = unused_cluster_key(session, key, item)
    if cluster is None:
        cluster = Cluster(
            cluster_key=key,
            title=item.title,
            first_seen_at=item.published_at,
            last_seen_at=item.published_at,
        )
        session.add(cluster)
        session.flush()
    return cluster


def reusable_old_cluster(session: Session, cluster_ids: set[int], item: ContentItem) -> Cluster | None:
    if not cluster_ids:
        return None
    clusters = [cluster for cluster in (session.get(Cluster, cluster_id) for cluster_id in cluster_ids) if cluster is not None]
    if len(clusters) != 1:
        return None
    cluster = clusters[0]
    existing_items = session.scalar(select(func.count(ClusterItem.id)).where(ClusterItem.cluster_id == cluster.id)) or 0
    return cluster if existing_items == 0 and cluster_within_window(cluster, item) else None


def cluster_within_window(cluster: Cluster, item: ContentItem) -> bool:
    if item.published_at is None:
        return True
    item_at = utc_key(item.published_at)
    dates = [date for date in (cluster.first_seen_at, cluster.last_seen_at) if date is not None]
    return all(abs((utc_key(date) - item_at).total_seconds()) <= timedelta(days=WINDOW_DAYS).total_seconds() for date in dates)


def matching_exact_cluster(session: Session, item: ContentItem) -> Cluster | None:
    clauses = []
    if item.content_hash:
        clauses.append(ContentItem.content_hash == item.content_hash)
    if item.canonical_url:
        clauses.append(ContentItem.canonical_url == item.canonical_url)
    if item.source_id and item.title and item.content_text and item.published_at:
        clauses.append(
            and_(
                ContentItem.source_id == item.source_id,
                ContentItem.title == item.title,
                ContentItem.content_text == item.content_text,
                ContentItem.published_at == item.published_at,
            )
        )
    if not clauses:
        return None
    stmt = (
        select(Cluster)
        .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(ContentItem.id != item.id)
        .where(Source.status == "active", Source.media_type == "article")
        .where(or_(*clauses))
        .order_by(Cluster.first_seen_at.desc().nullslast(), Cluster.id.desc())
    )
    for cluster in session.scalars(stmt).all():
        if cluster_within_window(cluster, item):
            return cluster
    return None


def repair_exact_content_duplicates(session: Session) -> int:
    with clustering_run_execution_lock(session):
        groups = session.execute(
            select(
                ContentItem.source_id,
                ContentItem.title,
                ContentItem.content_text,
                ContentItem.published_at,
            )
            .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
            .join(Source, Source.id == ContentItem.source_id)
            .where(
                Source.status == "active",
                Source.media_type == "article",
                ContentItem.title != "",
                ContentItem.content_text != "",
                ContentItem.published_at.is_not(None),
            )
            .group_by(
                ContentItem.source_id,
                ContentItem.title,
                ContentItem.content_text,
                ContentItem.published_at,
            )
            .having(func.count(func.distinct(ClusterItem.cluster_id)) > 1)
        ).all()
        if not groups:
            return 0
        grouped_item_ids: list[list[int]] = []
        # ponytail: exact duplicate groups are sparse; batch this lookup only if cardinality grows.
        for source_id, title, body, published_at in groups:
            grouped_item_ids.append(list(session.scalars(
                select(ContentItem.id)
                .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
                .where(
                    ContentItem.source_id == source_id,
                    ContentItem.title == title,
                    ContentItem.content_text == body,
                    ContentItem.published_at == published_at,
                )
                .order_by(ContentItem.id)
            )))
        item_ids = [item_id for group_ids in grouped_item_ids for item_id in group_ids]
        with clustering_run(
            session,
            scope_type="repair-exact-content-duplicates",
            item_ids=item_ids,
            rule_version=EXACT_CONTENT_REPAIR_RULE_VERSION,
        ):
            for group_ids in grouped_item_ids:
                for item_id in group_ids:
                    item = session.get(ContentItem, item_id)
                    if item is not None:
                        assign_cluster(session, item, replace_existing=True)
        return len(groups)


def active_cluster_by_key(session: Session, key: str, item: ContentItem) -> Cluster | None:
    clauses = []
    if item.content_hash:
        clauses.append(ContentItem.content_hash == item.content_hash)
    if item.canonical_url:
        clauses.append(ContentItem.canonical_url == item.canonical_url)
    if not clauses:
        return None
    return session.scalar(
        select(Cluster)
        .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(Cluster.cluster_key == key, Source.status == "active", Source.media_type == "article")
        .where(or_(*clauses))
        .limit(1)
    )


def unused_cluster_key(session: Session, key: str, item: ContentItem) -> str:
    candidate = key
    index = 0
    while session.scalar(select(Cluster.id).where(Cluster.cluster_key == candidate)) is not None:
        index += 1
        candidate = content_hash(key, str(item.id or item.content_hash), str(index))[:40]
    return candidate


def windowed_cluster_key(key: str, item: ContentItem) -> str:
    if item.published_at is None:
        return key
    bucket = utc_key(item.published_at).date().toordinal() // WINDOW_DAYS
    return content_hash(key, str(bucket))[:40]


def embedding_cluster(
    session: Session,
    item: ContentItem,
    target_vector: list[float] | None = None,
    embedding_provider: EmbeddingProvider | None = None,
    translation_provider: LLMProvider | None = None,
    embedding_model: str = "",
    translation_model: str = "",
) -> tuple[Cluster | None, float]:
    target = target_vector or parse_vector(item.embedding_vector)
    if not target:
        return None, 0.0
    if is_postgres(session):
        return postgres_embedding_cluster(session, item, target, embedding_provider, translation_provider, embedding_model, translation_model)
    return python_embedding_cluster(session, item, target, embedding_provider, translation_provider, embedding_model, translation_model)


def python_embedding_cluster(
    session: Session,
    item: ContentItem,
    target: list[float],
    embedding_provider: EmbeddingProvider | None = None,
    translation_provider: LLMProvider | None = None,
    embedding_model: str = "",
    translation_model: str = "",
) -> tuple[Cluster | None, float]:
    stmt = (
        select(ContentItem, Cluster)
        .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
        .join(Cluster, Cluster.id == ClusterItem.cluster_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(ContentItem.id != item.id, embedding_vector_present_clause(session), ContentItem.embedding_model == item.embedding_model)
        .where(Source.status == "active", Source.media_type == "article")
    )
    if item.published_at:
        stmt = stmt.where(ContentItem.published_at >= item.published_at - timedelta(days=WINDOW_DAYS))
        stmt = stmt.where(ContentItem.published_at <= item.published_at + timedelta(days=WINDOW_DAYS))

    best_cluster = None
    best_score = 0.0
    best_seen_score = 0.0
    for other, cluster in session.execute(stmt).all():
        if other.source_id == item.source_id:
            continue
        if not cluster_within_window(cluster, item):
            continue
        score = cosine_similarity(target, parse_vector(other.embedding_vector))
        if score > best_seen_score:
            best_seen_score = score
        score = best_similarity_score(session, item, other, score, embedding_provider, translation_provider, embedding_model, translation_model)
        if score >= embedding_threshold_for(item, other) and score > best_score:
            best_cluster, best_score = cluster, score
    if best_cluster is not None:
        return best_cluster, round(best_score, 4)
    return None, best_seen_score


def postgres_embedding_cluster(
    session: Session,
    item: ContentItem,
    target: list[float],
    embedding_provider: EmbeddingProvider | None = None,
    translation_provider: LLMProvider | None = None,
    embedding_model: str = "",
    translation_model: str = "",
) -> tuple[Cluster | None, float]:
    start_at = item.published_at - timedelta(days=WINDOW_DAYS) if item.published_at else None
    end_at = item.published_at + timedelta(days=WINDOW_DAYS) if item.published_at else None
    rows = session.execute(
        text(POSTGRES_NEIGHBOR_SQL),
        {
            "target_vector": vector_literal(target),
            "item_id": item.id,
            "source_id": item.source_id,
            "embedding_model": item.embedding_model,
            "start_at": start_at,
            "end_at": end_at,
            "limit": EMBEDDING_NEIGHBOR_LIMIT,
        },
    ).mappings()
    best_cluster = None
    best_score = 0.0
    best_seen_score = 0.0
    for row in rows:
        other = session.get(ContentItem, row["item_id"])
        cluster = session.get(Cluster, row["cluster_id"])
        if other is None or cluster is None or not cluster_within_window(cluster, item):
            continue
        score = float(row["score"] or 0.0)
        if score > best_seen_score:
            best_seen_score = score
        score = best_similarity_score(session, item, other, score, embedding_provider, translation_provider, embedding_model, translation_model)
        if score >= embedding_threshold_for(item, other) and score > best_score:
            best_cluster, best_score = cluster, score
    if best_cluster is not None:
        return best_cluster, round(best_score, 4)
    return None, best_seen_score


def embed_pending_items(
    session: Session,
    provider: EmbeddingProvider,
    model: str,
    *,
    batch_limit: int = EMBEDDING_BATCH_LIMIT,
    max_batches: int = 1,
    translation_provider: LLMProvider | None = None,
    translation_model: str = "",
) -> int:
    with clustering_run_execution_lock(session):
        candidate_ids = list(
            session.scalars(
                select(ContentItem.id)
                .join(Source, Source.id == ContentItem.source_id)
                .where(embedding_pending_clause(session, model))
                .where(Source.status == "active", Source.media_type == "article")
                .order_by(
                    ContentItem.published_at.desc().nullslast(),
                    ContentItem.id.desc(),
                )
                .limit(max(batch_limit, 1) * max(max_batches, 1))
            ).all()
        )
        if not candidate_ids:
            return 0
        with clustering_run(
            session,
            scope_type="embedding-pending",
            item_ids=candidate_ids,
            rule_version=ClusteringRule(
                "embedding-assign-v1",
                embedding_model=model,
                translation_model=translation_model,
            ).version,
        ):
            processed = 0
            chunk_size = max(batch_limit, 1)
            for start in range(0, len(candidate_ids), chunk_size):
                chunk_processed = _embed_items_by_ids_locked(
                    session,
                    provider,
                    model,
                    candidate_ids[start : start + chunk_size],
                    batch_limit=chunk_size,
                    translation_provider=translation_provider,
                    translation_model=translation_model,
                    mark_empty_run_incomplete=False,
                )
                processed += chunk_processed
            if processed == 0:
                mark_clustering_run_incomplete(
                    session,
                    "Embedding scope 没有成功生成向量的条目",
                )
            return processed


def embed_items_by_ids(
    session: Session,
    provider: EmbeddingProvider,
    model: str,
    item_ids: list[int],
    *,
    batch_limit: int = EMBEDDING_BATCH_LIMIT,
    translation_provider: LLMProvider | None = None,
    translation_model: str = "",
) -> int:
    with clustering_run_execution_lock(session):
        return _embed_items_by_ids_locked(
            session,
            provider,
            model,
            item_ids,
            batch_limit=batch_limit,
            translation_provider=translation_provider,
            translation_model=translation_model,
            mark_empty_run_incomplete=True,
        )


def _embed_items_by_ids_locked(
    session: Session,
    provider: EmbeddingProvider,
    model: str,
    item_ids: list[int],
    *,
    batch_limit: int,
    translation_provider: LLMProvider | None,
    translation_model: str,
    mark_empty_run_incomplete: bool,
) -> int:
    if not item_ids:
        return 0
    processed = 0
    seen_ids = list(dict.fromkeys(item_ids))
    active_ids = list(
        session.scalars(
            select(ContentItem.id)
            .join(Source, Source.id == ContentItem.source_id)
            .where(ContentItem.id.in_(seen_ids))
            .where(Source.status == "active", Source.media_type == "article")
        ).all()
    )
    if not active_ids:
        return 0
    attempted_embeddings = 0
    with clustering_run(
        session,
        scope_type="embedding-items",
        item_ids=active_ids,
        rule_version=ClusteringRule(
            "embedding-assign-v1",
            embedding_model=model,
            translation_model=translation_model,
        ).version,
    ):
        for start in range(0, len(seen_ids), max(batch_limit, 1)):
            chunk_ids = seen_ids[start : start + max(batch_limit, 1)]
            all_items = session.scalars(
                select(ContentItem)
                .join(Source, Source.id == ContentItem.source_id)
                .where(ContentItem.id.in_(chunk_ids))
                .where(Source.status == "active", Source.media_type == "article")
            ).all()
            all_items_by_id = {item.id: item for item in all_items}
            ordered_all_items = [all_items_by_id[item_id] for item_id in chunk_ids if item_id in all_items_by_id]
            pending_items = session.scalars(
                select(ContentItem)
                .join(Source, Source.id == ContentItem.source_id)
                .where(ContentItem.id.in_(chunk_ids))
                .where(embedding_pending_clause(session, model))
                .where(Source.status == "active", Source.media_type == "article")
            ).all()
            pending_by_id = {item.id: item for item in pending_items}
            ordered_pending_items = [pending_by_id[item_id] for item_id in chunk_ids if item_id in pending_by_id]
            attempted_embeddings += len(ordered_pending_items)
            vectors = embed_item_vectors(provider, model, ordered_pending_items)
            processed_ids: set[int] = set()
            for item, vector in zip(ordered_pending_items, vectors):
                if not vector:
                    continue
                store_item_embedding(session, item, vector, model)
                prepare_item_translation_embeddings(session, item, provider, model, translation_provider, translation_model)
                with session.no_autoflush:
                    assign_cluster(session, item, replace_existing=True, target_vector=vector, embedding_provider=provider, translation_provider=translation_provider, embedding_model=model, translation_model=translation_model)
                processed += 1
                if item.id is not None:
                    processed_ids.add(item.id)
            for item in ordered_all_items:
                if item.id in processed_ids:
                    continue
                changed = prepare_item_translation_embeddings(session, item, provider, model, translation_provider, translation_model)
                vector = parse_vector(item.embedding_vector)
                if changed and vector:
                    with session.no_autoflush:
                        assign_cluster(session, item, replace_existing=True, target_vector=vector, embedding_provider=provider, translation_provider=translation_provider, embedding_model=model, translation_model=translation_model)
                    processed += 1
        if mark_empty_run_incomplete and attempted_embeddings and processed == 0:
            mark_clustering_run_incomplete(
                session,
                "Embedding scope 没有成功生成向量的条目",
            )
    return processed


def embed_missing_items(
    session: Session,
    provider: EmbeddingProvider,
    model: str,
    limit: int = EMBEDDING_BATCH_LIMIT,
    translation_provider: LLMProvider | None = None,
    translation_model: str = "",
) -> int:
    with clustering_run_execution_lock(session):
        items = session.scalars(
            select(ContentItem)
            .join(Source, Source.id == ContentItem.source_id)
            .where(embedding_pending_clause(session, model))
            .where(Source.status == "active", Source.media_type == "article")
            .order_by(
                ContentItem.published_at.desc().nullslast(), ContentItem.id.desc()
            )
            .limit(limit)
        ).all()
        if not items:
            return 0
        processed = 0
        with clustering_run(
            session,
            scope_type="embedding-missing",
            item_ids=[item.id for item in items],
            rule_version=ClusteringRule(
                "embedding-assign-v1",
                embedding_model=model,
                translation_model=translation_model,
            ).version,
        ):
            vectors = embed_item_vectors(provider, model, items)
            if all(not vector for vector in vectors):
                mark_clustering_run_incomplete(
                    session,
                    "Embedding scope 没有成功生成向量的条目",
                )
            for item, vector in zip(items, vectors):
                if not vector:
                    continue
                store_item_embedding(session, item, vector, model)
                prepare_item_translation_embeddings(
                    session,
                    item,
                    provider,
                    model,
                    translation_provider,
                    translation_model,
                )
                with session.no_autoflush:
                    assign_cluster(
                        session,
                        item,
                        replace_existing=True,
                        target_vector=vector,
                        embedding_provider=provider,
                        translation_provider=translation_provider,
                        embedding_model=model,
                        translation_model=translation_model,
                    )
                processed += 1
        return processed


def store_item_embedding(session: Session, item: ContentItem, vector: list[float], model: str) -> None:
    signature = lsh_signature(item.title, item.content_text or item.summary)
    literal = vector_literal(vector)
    item.lsh_signature = signature
    item.embedding_model = model
    if is_postgres(session):
        session.execute(
            text(POSTGRES_STORE_EMBEDDING_SQL),
            {
                "embedding_vector": literal,
                "embedding_model": model,
                "lsh_signature": signature,
                "item_id": item.id,
            },
        )
        session.expire(item, ["embedding_vector"])
        return
    item.embedding_vector = literal


def best_similarity_score(
    session: Session,
    item: ContentItem,
    other: ContentItem,
    original_score: float,
    embedding_provider: EmbeddingProvider | None,
    translation_provider: LLMProvider | None,
    embedding_model: str,
    translation_model: str,
) -> float:
    if original_score >= EMBEDDING_THRESHOLD or original_score < ZH_CANONICAL_CANDIDATE_THRESHOLD:
        return original_score
    score = original_score
    if translation_utils.language_bucket(canonical_embedding_input(item)) != translation_utils.language_bucket(canonical_embedding_input(other)):
        left = ensure_zh_canonical_embedding(session, item, embedding_provider, embedding_model, translation_provider, translation_model)
        right = ensure_zh_canonical_embedding(session, other, embedding_provider, embedding_model, translation_provider, translation_model)
        score = max(score, cosine_similarity(left, right))
    if score >= EMBEDDING_THRESHOLD or score < TITLE_CANDIDATE_THRESHOLD:
        return score
    left_title = ensure_zh_title_embedding(session, item, embedding_provider, embedding_model, translation_provider, translation_model)
    right_title = ensure_zh_title_embedding(session, other, embedding_provider, embedding_model, translation_provider, translation_model)
    title_score = cosine_similarity(left_title, right_title)
    return max(score, title_score) if title_score >= TITLE_EMBEDDING_THRESHOLD else score


def ensure_zh_canonical_embedding(
    session: Session,
    item: ContentItem,
    embedding_provider: EmbeddingProvider | None,
    embedding_model: str,
    translation_provider: LLMProvider | None,
    translation_model: str,
) -> list[float]:
    stored = load_content_embedding(session, item.id, ZH_CANONICAL_REPRESENTATION, embedding_model)
    if stored:
        return stored
    if not embedding_provider or not embedding_model:
        return []
    source_text = canonical_embedding_input(item)
    if not source_text:
        return []
    zh_text = source_text
    if item_needs_translation(item):
        zh_text = translated_embedding_input(session, item, translation_provider, translation_model)
    if not zh_text:
        return []
    try:
        vector = embedding_from_response(embedding_provider.embed(embedding_model, zh_text[:CANONICAL_TEXT_LIMIT]))
    except RuntimeError:
        return []
    if vector:
        store_content_embedding(session, item.id, ZH_CANONICAL_REPRESENTATION, embedding_model, vector)
    return vector


def ensure_zh_title_embedding(
    session: Session,
    item: ContentItem,
    embedding_provider: EmbeddingProvider | None,
    embedding_model: str,
    translation_provider: LLMProvider | None,
    translation_model: str,
) -> list[float]:
    stored = load_content_embedding(session, item.id, ZH_TITLE_REPRESENTATION, embedding_model)
    if stored:
        return stored
    if not embedding_provider or not embedding_model:
        return []
    text = zh_title_embedding_input(session, item, translation_provider, translation_model)
    if not text:
        return []
    try:
        vector = embedding_from_response(embedding_provider.embed(embedding_model, text[:CANONICAL_TEXT_LIMIT]))
    except RuntimeError:
        return []
    if vector:
        store_content_embedding(session, item.id, ZH_TITLE_REPRESENTATION, embedding_model, vector)
    return vector


def load_content_embedding(session: Session, item_id: int | None, representation: str, model: str) -> list[float]:
    if item_id is None:
        return []
    if is_postgres(session):
        value = session.scalar(
            text(POSTGRES_CONTENT_EMBEDDING_SQL),
            {"content_item_id": item_id, "representation": representation, "model": model},
        )
        return parse_vector(value)
    row = session.scalar(
        select(ContentEmbedding).where(
            ContentEmbedding.content_item_id == item_id,
            ContentEmbedding.representation == representation,
            ContentEmbedding.model == model,
        )
    )
    return parse_vector(row.vector if row else None)


def store_content_embedding(session: Session, item_id: int | None, representation: str, model: str, vector: list[float]) -> None:
    if item_id is None:
        return
    literal = vector_literal(vector)
    if is_postgres(session):
        session.execute(
            text(POSTGRES_STORE_CONTENT_EMBEDDING_SQL),
            {
                "content_item_id": item_id,
                "representation": representation,
                "model": model,
                "vector": literal,
            },
        )
        return
    row = session.scalar(
        select(ContentEmbedding).where(
            ContentEmbedding.content_item_id == item_id,
            ContentEmbedding.representation == representation,
            ContentEmbedding.model == model,
        )
    )
    if row is None:
        row = ContentEmbedding(content_item_id=item_id, representation=representation, model=model)
        session.add(row)
    row.vector = literal
    row.created_at = datetime.now(timezone.utc)


def canonical_embedding_input(item: ContentItem) -> str:
    body = normalize_text_for_embedding(item.content_text)
    if len(body) < 160:
        body = normalize_text_for_embedding(item.summary)
    return f"{item.title or ''}\n\n{body}"[:CANONICAL_TEXT_LIMIT].strip()


def item_needs_translation(item: ContentItem) -> bool:
    return any(
        translation_utils.needs_translation(value)
        for value in (
            normalize_text_for_embedding(item.title),
            normalize_text_for_embedding(item.summary),
            normalize_text_for_embedding(item.content_text),
        )
        if value
    )


def normalize_text_for_embedding(value: str | None) -> str:
    return re.sub(r"\s+", " ", re.sub(r"!\[[^\]]*]\([^)]*(?:\)|$)", " ", value or "")).strip()


def translated_embedding_input(session: Session, item: ContentItem, provider: LLMProvider | None, model: str) -> str:
    title = (item.title or "").strip()
    body = normalize_text_for_embedding(item.content_text or item.summary or "")[:CANONICAL_TEXT_LIMIT]
    translated_title = translation_utils.ensure_translation(session, provider, model, title) if title else ""
    translated_body = translation_utils.ensure_translation(session, provider, model, body) if body else ""
    if title and translation_utils.needs_translation(title) and not translated_title:
        return ""
    if body and translation_utils.needs_translation(body) and not translated_body:
        return ""
    return f"{normalize_text_for_embedding(translated_title or title)}\n\n{normalize_text_for_embedding(translated_body or body)}".strip()


def zh_title_embedding_input(session: Session, item: ContentItem, provider: LLMProvider | None, model: str) -> str:
    title = normalize_text_for_embedding(item.title)
    if title and translation_utils.needs_translation(title):
        return normalize_text_for_embedding(translation_utils.ensure_translation(session, provider, model, title))
    return title


def prepare_item_translation_embeddings(
    session: Session,
    item: ContentItem,
    embedding_provider: EmbeddingProvider | None,
    embedding_model: str,
    translation_provider: LLMProvider | None,
    translation_model: str,
) -> bool:
    changed = ensure_item_translation_cache(session, item, translation_provider, translation_model)
    if item_needs_translation(item):
        had_canonical = bool(load_content_embedding(session, item.id, ZH_CANONICAL_REPRESENTATION, embedding_model))
        canonical = ensure_zh_canonical_embedding(session, item, embedding_provider, embedding_model, translation_provider, translation_model)
        changed = changed or (bool(canonical) and not had_canonical)
    return changed


def ensure_item_translation_cache(session: Session, item: ContentItem, provider: LLMProvider | None, model: str) -> bool:
    if not provider or not model:
        return False
    changed = False
    for value in (item.title, item.summary, item.content_text):
        text = (value or "").strip()
        if text and translation_utils.needs_translation(text):
            had_cached = bool(
                translation_utils.cached_translation_text(
                    session,
                    model,
                    text,
                    translation_utils.translation_provider_name(provider),
                )
            )
            translated = translation_utils.ensure_translation(session, provider, model, text)
            changed = changed or (bool(translated) and not had_cached)
    return changed


def has_pending_embeddings(session: Session, model: str) -> bool:
    return (
        session.scalar(
            select(ContentItem.id)
            .join(Source, Source.id == ContentItem.source_id)
            .where(embedding_pending_clause(session, model))
            .where(Source.status == "active", Source.media_type == "article")
            .limit(1)
        )
        is not None
    )


def repair_embedding_clusters(
    session: Session,
    model: str,
    limit: int = EMBEDDING_RECLUSTER_LIMIT,
    embedding_provider: EmbeddingProvider | None = None,
    translation_provider: LLMProvider | None = None,
    translation_model: str = "",
) -> int:
    with clustering_run_execution_lock(session):
        cutoff = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
        candidates = [
            EmbeddingRepairCandidate(item_id=item_id, cluster_id=cluster_id)
            for item_id, cluster_id in session.execute(
                select(ContentItem.id, Cluster.id)
                .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
                .join(Cluster, Cluster.id == ClusterItem.cluster_id)
                .join(Source, Source.id == ContentItem.source_id)
                .where(
                    embedding_vector_present_clause(session),
                    ContentItem.embedding_model == model,
                )
                .where(Source.status == "active", Source.media_type == "article")
                .where(
                    or_(
                        ContentItem.published_at.is_(None),
                        ContentItem.published_at >= cutoff,
                    )
                )
                .order_by(
                    ContentItem.published_at.desc().nullslast(),
                    ContentItem.id.desc(),
                    Cluster.id.asc(),
                )
                .limit(limit)
            ).all()
        ]
        if not candidates:
            return 0
        item_ids = sorted({candidate.item_id for candidate in candidates})
        with clustering_run(
            session,
            scope_type="repair-embedding-clusters",
            item_ids=item_ids,
            rule_version=ClusteringRule(
                "repair-embedding-v1",
                embedding_model=model,
                translation_model=translation_model,
            ).version,
        ):
            return _repair_embedding_clusters(
                session,
                model,
                candidates,
                embedding_provider,
                translation_provider,
                translation_model,
            )


def _repair_embedding_clusters(
    session: Session,
    model: str,
    candidates: list[EmbeddingRepairCandidate],
    embedding_provider: EmbeddingProvider | None,
    translation_provider: LLMProvider | None,
    translation_model: str,
) -> int:
    if is_postgres(session):
        return repair_embedding_clusters_indexed(
            session,
            model,
            candidates,
            embedding_provider,
            translation_provider,
            translation_model,
        )
    return repair_embedding_clusters_python(
        session,
        model,
        candidates,
        embedding_provider,
        translation_provider,
        translation_model,
    )


def load_frozen_embedding_repair_rows(
    session: Session,
    model: str,
    candidates: list[EmbeddingRepairCandidate],
) -> list[tuple[ContentItem, Cluster]]:
    return session.execute(
        select(ContentItem, Cluster)
        .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
        .join(Cluster, Cluster.id == ClusterItem.cluster_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(
            tuple_(ContentItem.id, Cluster.id).in_(
                [(row.item_id, row.cluster_id) for row in candidates]
            )
        )
        .where(
            embedding_vector_present_clause(session),
            ContentItem.embedding_model == model,
        )
        .where(Source.status == "active", Source.media_type == "article")
        .order_by(
            ContentItem.published_at.desc().nullslast(),
            ContentItem.id.desc(),
            Cluster.id.asc(),
        )
    ).all()


def repair_embedding_clusters_indexed(
    session: Session,
    model: str,
    candidates: list[EmbeddingRepairCandidate],
    embedding_provider: EmbeddingProvider | None,
    translation_provider: LLMProvider | None,
    translation_model: str,
) -> int:
    rows = load_frozen_embedding_repair_rows(session, model, candidates)
    repaired = 0
    for item, current_cluster in rows:
        item_id = item.id
        current_cluster_id = current_cluster.id
        target_cluster_id = None
        try:
            vector = parse_vector(item.embedding_vector)
            if not vector:
                continue
            target_cluster, score = embedding_cluster(session, item, vector, embedding_provider, translation_provider, model, translation_model)
            if target_cluster is None:
                continue
            target_cluster_id = target_cluster.id
            if target_cluster_id == current_cluster_id:
                continue
            before = {row[0] for row in session.execute(select(ClusterItem.cluster_id).where(ClusterItem.content_item_id == item_id))}
            if before == {target_cluster_id}:
                continue
            move_item_to_cluster(session, item, target_cluster, score)
            session.commit()
            repaired += 1
        except OperationalError as exc:
            session.rollback()
            if is_deadlock_error(exc):
                mark_clustering_run_incomplete(
                    session,
                    "Embedding repair scope 存在因数据库死锁跳过的条目",
                )
                logger.warning("Embedding repair skipped item after database deadlock: item_id=%s target_cluster_id=%s", item_id, target_cluster_id)
                continue
            raise
    session.commit()
    return repaired


def is_deadlock_error(exc: OperationalError) -> bool:
    original = getattr(exc, "orig", None)
    code = getattr(original, "sqlstate", None) or getattr(original, "pgcode", None)
    if code == "40P01":
        return True
    return "deadlock detected" in str(exc).lower()


def repair_embedding_clusters_python(
    session: Session,
    model: str,
    candidates: list[EmbeddingRepairCandidate],
    embedding_provider: EmbeddingProvider | None,
    translation_provider: LLMProvider | None,
    translation_model: str,
) -> int:
    rows = load_frozen_embedding_repair_rows(session, model, candidates)
    ordered = sorted(rows, key=lambda row: (utc_key(row[0].published_at) if row[0].published_at else datetime.min.replace(tzinfo=timezone.utc), row[0].id or 0))
    vectors = {item.id: parse_vector(item.embedding_vector) for item, _cluster in ordered}
    parents = {item.id: item.id for item, _cluster in ordered}
    best_scores: dict[int, float] = {item.id: 0.0 for item, _cluster in ordered}

    def find(item_id: int) -> int:
        parent = parents[item_id]
        if parent != item_id:
            parents[item_id] = find(parent)
        return parents[item_id]

    def union(left_id: int, right_id: int) -> None:
        left_root = find(left_id)
        right_root = find(right_id)
        if left_root != right_root:
            parents[right_root] = left_root

    for index, (item, _current_cluster) in enumerate(ordered):
        target = vectors.get(item.id) or []
        if not target:
            continue
        for other, other_cluster in ordered[index + 1 :]:
            if other.source_id == item.source_id:
                continue
            if item.published_at and other.published_at and abs((utc_key(other.published_at) - utc_key(item.published_at)).total_seconds()) > timedelta(days=WINDOW_DAYS).total_seconds():
                continue
            if not cluster_within_window(other_cluster, item):
                continue
            score = cosine_similarity(target, vectors.get(other.id) or [])
            score = best_similarity_score(session, item, other, score, embedding_provider, translation_provider, model, translation_model)
            if score >= embedding_threshold_for(item, other):
                union(item.id, other.id)
                best_scores[item.id] = max(best_scores[item.id], score)
                best_scores[other.id] = max(best_scores[other.id], score)

    components: dict[int, list[tuple[ContentItem, Cluster]]] = {}
    for item, cluster in ordered:
        components.setdefault(find(item.id), []).append((item, cluster))

    repaired = 0
    for component in components.values():
        cluster_ids = {cluster.id for _item, cluster in component}
        if len(component) < 2 or len(cluster_ids) < 2:
            continue
        _target_item, target_cluster = min(component, key=lambda row: (utc_key(row[0].published_at) if row[0].published_at else datetime.min.replace(tzinfo=timezone.utc), row[0].id or 0))
        target_cluster = session.get(Cluster, target_cluster.id)
        if target_cluster is None:
            continue
        for item, _current_cluster in component:
            before = {row[0] for row in session.execute(select(ClusterItem.cluster_id).where(ClusterItem.content_item_id == item.id))}
            if before == {target_cluster.id}:
                continue
            move_item_to_cluster(session, item, target_cluster, best_scores.get(item.id, 0.0))
            repaired += 1
    session.commit()
    return repaired


def move_item_to_cluster(session: Session, item: ContentItem, cluster: Cluster, score: float) -> None:
    register_clustering_target(session, cluster.id)
    old_cluster_ids = detach_item(session, item)
    exists = session.scalar(
        select(ClusterItem).where(
            ClusterItem.cluster_id == cluster.id,
            ClusterItem.content_item_id == item.id,
        )
    )
    if exists is None:
        session.add(ClusterItem(cluster_id=cluster.id, content_item_id=item.id, duplicate_score=round(score, 4)))
    item.cluster_score = round(score, 4)
    refresh_cluster_dates(cluster, item)
    removed_cluster_ids = old_cluster_ids - {cluster.id}
    prune_clusters(session, removed_cluster_ids)


def embedding_input(item: ContentItem) -> str:
    return f"{item.title}\n\n{item.content_text or item.summary}"[:4000]


def embed_item_vectors(provider: EmbeddingProvider, model: str, items: list[ContentItem]) -> list[list[float]]:
    inputs = [embedding_input(item) for item in items]
    embed_many = getattr(provider, "embed_many", None)
    if callable(embed_many) and inputs:
        try:
            vectors = embeddings_from_response(embed_many(model, inputs))
            if len(vectors) == len(inputs):
                return vectors
            logger.warning("Embedding 批量返回数量不匹配：expected=%s actual=%s", len(inputs), len(vectors))
        except RuntimeError as exc:
            logger.warning("Embedding 批量请求失败，降级为逐条请求：items=%s error=%s", len(inputs), exc)
    vectors: list[list[float]] = []
    for input_text in inputs:
        try:
            result = provider.embed(model, input_text)
        except RuntimeError as exc:
            logger.warning("Embedding 单条请求失败：text_len=%s error=%s", len(input_text), exc)
            vectors.append([])
            continue
        vectors.append(embedding_from_response(result))
    return vectors


def refresh_source_clustering_eligibility(
    session: Session, source_id: int
) -> bool:
    session.flush()
    source = session.get(Source, source_id)
    if source is None:
        return False
    session.refresh(source)
    return (
        source.status == "active"
        and source.media_type == "article"
    )


def source_cluster_rule_version(items: list[ContentItem]) -> str:
    models = sorted(
        {
            (item.embedding_model or "").strip()
            for item in items
            if parse_vector(item.embedding_vector)
            and (item.embedding_model or "").strip()
        }
    )
    model_version = ""
    if len(models) == 1:
        model_version = models[0]
    elif models:
        model_version = json.dumps(
            models,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    return ClusteringRule(
        "source-cluster-v2",
        embedding_model=model_version,
    ).version


def cluster_source_items(session: Session, source_id: int) -> None:
    with clustering_run_execution_lock(session):
        defer_clustering_run_lock_until_transaction_end(session)
        if not refresh_source_clustering_eligibility(session, source_id):
            return
        items = session.scalars(
            select(ContentItem)
            .join(
                ClusterItem,
                ClusterItem.content_item_id == ContentItem.id,
                isouter=True,
            )
            .where(
                ContentItem.source_id == source_id,
                ClusterItem.id.is_(None),
                embedding_vector_present_clause(session),
                func.trim(ContentItem.embedding_model) != "",
            )
            .order_by(
                ContentItem.published_at.asc().nullslast(), ContentItem.id.asc()
            )
        ).all()
        if not items:
            return
        with clustering_run(
            session,
            scope_type="source-cluster",
            source_id=source_id,
            rule_version=source_cluster_rule_version(items),
            commit_on_success=False,
        ):
            for item in items:
                assign_cluster(session, item)


def decluster_source_items(
    session: Session,
    source_id: int,
    *,
    force: bool = False,
    rollback_on_failure: bool = False,
) -> None:
    with clustering_run_execution_lock(session):
        defer_clustering_run_lock_until_transaction_end(session)
        if not force and refresh_source_clustering_eligibility(session, source_id):
            return
        cluster_ids = {
            row[0]
            for row in session.execute(
                select(ClusterItem.cluster_id)
                .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
                .where(ContentItem.source_id == source_id)
            )
        }
        if not cluster_ids:
            return
        with clustering_run(
            session,
            scope_type="source-decluster",
            source_id=source_id,
            rule_version=SOURCE_DECLUSTER_RULE_VERSION,
            commit_on_success=False,
            rollback_on_failure=rollback_on_failure,
        ):
            session.query(ClusterItem).filter(
                ClusterItem.cluster_id.in_(cluster_ids),
                ClusterItem.content_item_id.in_(
                    select(ContentItem.id).where(ContentItem.source_id == source_id)
                ),
            ).delete(synchronize_session=False)
            prune_clusters(session, cluster_ids)


def repair_title_only_clusters(session: Session) -> int:
    with clustering_run_execution_lock(session):
        item_ids = multi_item_cluster_item_ids(session)
        if not item_ids:
            return 0
        with clustering_run(
            session,
            scope_type="repair-title-only-clusters",
            item_ids=item_ids,
            rule_version=TITLE_REPAIR_RULE_VERSION,
        ):
            return _repair_title_only_clusters(session, item_ids)


def _repair_title_only_clusters(session: Session, item_ids: list[int]) -> int:
    clusters = session.scalars(
        select(Cluster)
        .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
        .where(ClusterItem.content_item_id.in_(item_ids))
        .group_by(Cluster.id)
        .having(func.count(ClusterItem.id) > 1)
    ).all()
    repaired = 0
    for cluster in clusters:
        items = session.scalars(
            select(ContentItem)
            .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
            .where(ClusterItem.cluster_id == cluster.id)
            .order_by(ContentItem.published_at.asc().nullslast(), ContentItem.id.asc())
        ).all()
        if not title_only_cluster(cluster, items):
            continue
        session.query(ClusterItem).filter(ClusterItem.cluster_id == cluster.id).delete(synchronize_session=False)
        session.delete(cluster)
        session.flush()

        for item in items:
            assign_cluster(session, item)
        repaired += 1
    session.commit()
    return repaired


def repair_windowed_clusters(session: Session) -> int:
    with clustering_run_execution_lock(session):
        item_ids = multi_item_cluster_item_ids(session)
        if not item_ids:
            return 0
        with clustering_run(
            session,
            scope_type="repair-windowed-clusters",
            item_ids=item_ids,
            rule_version=WINDOW_REPAIR_RULE_VERSION,
        ):
            return _repair_windowed_clusters(session, item_ids)


def _repair_windowed_clusters(session: Session, item_ids: list[int]) -> int:
    clusters = session.scalars(
        select(Cluster)
        .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
        .where(ClusterItem.content_item_id.in_(item_ids))
        .group_by(Cluster.id)
        .having(func.count(ClusterItem.id) > 1)
    ).all()
    repaired = 0
    for cluster in clusters:
        items = session.scalars(
            select(ContentItem)
            .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
            .where(ClusterItem.cluster_id == cluster.id)
            .order_by(ContentItem.published_at.asc().nullslast(), ContentItem.id.asc())
        ).all()
        if not cluster_span_exceeds_window(cluster) and not single_source_soft_cluster(items):
            continue
        session.query(ClusterItem).filter(ClusterItem.cluster_id == cluster.id).delete(synchronize_session=False)
        session.delete(cluster)
        session.flush()

        for item in items:
            assign_cluster(session, item)
        repaired += 1
    session.commit()
    return repaired


def multi_item_cluster_item_ids(session: Session) -> list[int]:
    multi_cluster_ids = select(ClusterItem.cluster_id).group_by(
        ClusterItem.cluster_id
    ).having(func.count(ClusterItem.id) > 1)
    return list(
        session.scalars(
            select(ClusterItem.content_item_id)
            .where(ClusterItem.cluster_id.in_(multi_cluster_ids))
            .order_by(ClusterItem.content_item_id)
        ).all()
    )


def cluster_span_exceeds_window(cluster: Cluster) -> bool:
    if cluster.first_seen_at is None or cluster.last_seen_at is None:
        return False
    return abs((utc_key(cluster.last_seen_at) - utc_key(cluster.first_seen_at)).total_seconds()) > timedelta(days=WINDOW_DAYS).total_seconds()


def single_source_soft_cluster(items: list[ContentItem]) -> bool:
    if len(items) < 2 or len({item.source_id for item in items}) != 1:
        return False
    return not exact_identity_covers_cluster(items)


def title_only_cluster(cluster: Cluster, items: list[ContentItem]) -> bool:
    if len(items) < 2:
        return False
    if exact_identity_covers_cluster(items):
        return False
    return any(item.normalized_title and content_hash(item.normalized_title)[:40] == cluster.cluster_key for item in items)


def exact_identity_covers_cluster(items: list[ContentItem]) -> bool:
    urls = [item.canonical_url for item in items if item.canonical_url]
    if len(urls) == len(items) and len(set(urls)) == 1:
        return True
    hashes = [item.content_hash for item in items if item.content_hash]
    return len(hashes) == len(items) and len(set(hashes)) == 1


def embedding_pending_clause(session: Session, model: str):
    vector_missing = ContentItem.embedding_vector.is_(None) if is_postgres(session) else or_(ContentItem.embedding_vector.is_(None), ContentItem.embedding_vector == "")
    return or_(vector_missing, ContentItem.embedding_model != model)


def embedding_vector_present_clause(session: Session):
    return ContentItem.embedding_vector.is_not(None) if is_postgres(session) else and_(ContentItem.embedding_vector.is_not(None), ContentItem.embedding_vector != "")


def is_postgres(session: Session) -> bool:
    return bool(session.bind is not None and session.bind.dialect.name == "postgresql")


def vector_literal(vector: list[float]) -> str:
    return json.dumps([float(value) for value in vector], separators=(",", ":"))


def parse_vector(value: object) -> list[float]:
    if not value:
        return []
    if not isinstance(value, str):
        value = str(value)
    try:
        data = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(data, list):
        return []
    try:
        return [float(item) for item in data]
    except (TypeError, ValueError):
        return []


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or len(left) != len(right):
        return 0.0
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = sum(a * a for a in left) ** 0.5
    right_norm = sum(b * b for b in right) ** 0.5
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return dot / (left_norm * right_norm)


def embedding_threshold_for(left: ContentItem, right: ContentItem) -> float:
    return EMBEDDING_THRESHOLD


def cross_language_entity_match(left: ContentItem, right: ContentItem) -> bool:
    left_text = title_summary_text(left)
    right_text = title_summary_text(right)
    if has_cjk(left_text) == has_cjk(right_text):
        return False
    return len(latin_terms(left_text) & latin_terms(right_text)) >= MIN_SHARED_LATIN_TERMS


def title_summary_text(item: ContentItem) -> str:
    return f"{item.title or ''}\n{item.summary or ''}"


def has_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text or ""))


def latin_terms(text: str) -> set[str]:
    return {
        token
        for token in (match.group(0).lower() for match in re.finditer(r"\b[A-Za-z][A-Za-z0-9]{2,}\b", text or ""))
        if token not in LATIN_TERM_STOPWORDS
    }


def detach_item(session: Session, item: ContentItem) -> set[int]:
    old_ids = {row[0] for row in session.execute(select(ClusterItem.cluster_id).where(ClusterItem.content_item_id == item.id))}
    session.query(ClusterItem).filter(ClusterItem.content_item_id == item.id).delete()
    return old_ids


def prune_clusters(session: Session, cluster_ids: set[int]) -> None:
    session.flush()
    for cluster_id in cluster_ids:
        count = session.scalar(select(func.count(ClusterItem.id)).where(ClusterItem.cluster_id == cluster_id))
        cluster = session.get(Cluster, cluster_id)
        if cluster is None:
            continue
        if not count:
            session.delete(cluster)
            continue
        first_seen_at, last_seen_at = session.execute(
            select(func.min(ContentItem.published_at), func.max(ContentItem.published_at))
            .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
            .where(ClusterItem.cluster_id == cluster_id)
        ).one()
        cluster.first_seen_at = first_seen_at
        cluster.last_seen_at = last_seen_at

def refresh_cluster_dates(cluster: Cluster, item: ContentItem) -> None:
    if item.published_at:
        if cluster.first_seen_at is None or utc_key(item.published_at) < utc_key(cluster.first_seen_at):
            cluster.first_seen_at = item.published_at
        if cluster.last_seen_at is None or utc_key(item.published_at) > utc_key(cluster.last_seen_at):
            cluster.last_seen_at = item.published_at


def utc_key(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
