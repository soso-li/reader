import logging
import os
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from threading import Thread

from redis import Redis
from rq import Queue, Worker, get_current_job
from rq.exceptions import NoSuchJobError
from rq.job import Job
from rq.registry import StartedJobRegistry
from sqlalchemy import select
from sqlalchemy.orm import Session

from .cluster import EMBEDDING_AUTO_MAX_BATCHES, EMBEDDING_BATCH_LIMIT, embed_pending_items, has_pending_embeddings
from .ai_runtime import runtime_ai_settings, translation_chat_provider, translation_settings_for_source
from .config import settings
from .db import SessionLocal, prepare_runtime_database
from .llm import LocalEmbeddingProvider
from .maintenance import run_scheduled_generation_retention
from .models import ClusterItem, ContentItem, Source
from .queues import FETCH_QUEUE_NAME, LLM_QUEUE_NAME
from .rss import fetch_enabled_sources, fetch_source, source_is_fetch_eligible

logger = logging.getLogger(__name__)
FETCH_JOB_NAME = "reader_api.worker.fetch_all"
SOURCE_FETCH_JOB_NAME = "reader_api.worker.fetch_one"
FETCH_ENQUEUE_LOCK_NAME = "reader:enqueue:fetch"
ACTIVE_REFRESH_JOB_KEY = "reader:refresh:active-fetch-job"
FETCH_RESULT_TTL_SECONDS = 24 * 60 * 60
EMBED_JOB_NAME = "reader_api.worker.embed_all"
WORKER_HEARTBEAT_STALE_SECONDS = 600


def fetch_all() -> dict[str, object]:
    prepare_runtime_database()
    with SessionLocal() as session:
        logger.info("RSS job started")
        ai_settings = runtime_ai_settings(session)
        item_ids: list[int] = []
        run_result: dict[str, int] = {}
        record_fetch_job_progress(
            item_ids,
            ai_settings.embedding_model,
            0,
            run_result,
        )
        try:
            imported = fetch_enabled_sources(
                session,
                imported_item_ids=item_ids,
                run_result=run_result,
            )
        except Exception:
            record_fetch_job_progress(
                item_ids,
                ai_settings.embedding_model,
                len(item_ids),
                run_result,
                fetch_failed=True,
            )
            if item_ids:
                try:
                    enqueue_embedding_backfill()
                except Exception:
                    logger.exception("RSS job 失败后无法补排已提交条目的 Embedding")
            raise
        record_fetch_job_progress(
            item_ids,
            ai_settings.embedding_model,
            imported,
            run_result,
        )
        logger.info("RSS job fetched: imported=%s item_ids=%s", imported, len(item_ids))
        if item_ids or has_pending_embeddings(session, ai_settings.embedding_model):
            enqueue_embedding_backfill()
        logger.info("RSS job completed: imported=%s embedded=0", imported)
        return {
            "imported": imported,
            "embedded": 0,
            "item_ids": list(dict.fromkeys(item_ids)),
            "embedding_model": ai_settings.embedding_model,
            **run_result,
        }


def fetch_one(source_id: int) -> dict[str, object]:
    prepare_runtime_database()
    with SessionLocal() as session:
        source = session.get(Source, source_id)
        if source is None or not source_is_fetch_eligible(source):
            return {"source_id": source_id, "imported": 0, "item_ids": []}
        item_ids: list[int] = []
        imported = fetch_source(session, source, imported_item_ids=item_ids)
        if item_ids:
            enqueue_embedding_backfill()
        return {
            "source_id": source_id,
            "imported": imported,
            "item_ids": list(dict.fromkeys(item_ids)),
        }


def record_fetch_job_progress(
    item_ids: list[int],
    embedding_model: str,
    imported: int,
    run_result: dict[str, int],
    *,
    fetch_failed: bool = False,
) -> None:
    try:
        job = get_current_job()
    except TypeError:
        job = get_current_job(connection=Redis.from_url(settings.redis_url))
    if job is None:
        return
    job.meta.update(
        {
            "item_ids": list(dict.fromkeys(item_ids)),
            "embedding_model": embedding_model,
            "imported": imported,
            "attempted_sources": run_result.get("attempted_sources", 0),
            "successful_sources": run_result.get("successful_sources", 0),
            "fetch_failed": fetch_failed,
        }
    )
    job.save_meta()


def embed_all() -> int:
    prepare_runtime_database()
    with SessionLocal() as session:
        logger.info("Embedding job started")
        ai_settings = translation_settings_for_source(runtime_ai_settings(session), None)
        embedding_provider = LocalEmbeddingProvider(ai_settings.embedding_base_url, ai_settings.timeout_seconds, settings.embedding_api_key)
        translation_provider = translation_chat_provider(ai_settings)
        embedded = embed_pending_items(
            session,
            embedding_provider,
            ai_settings.embedding_model,
            batch_limit=EMBEDDING_BATCH_LIMIT,
            max_batches=EMBEDDING_AUTO_MAX_BATCHES,
            translation_provider=translation_provider,
            translation_model=ai_settings.translation_model,
        )
        requeue_embedding_backfill(session, embedded)
        logger.info("Embedding job completed: embedded=%s", embedded)
        return embedded


def requeue_embedding_backfill(session: Session, embedded: int, *, force: bool = False) -> None:
    ai_settings = runtime_ai_settings(session)
    if (force or embedded) and has_pending_embeddings(session, ai_settings.embedding_model):
        enqueue_embedding_backfill(ignore_current_job=True)


def fetch_job_exists(queue: Queue, connection: Redis) -> bool:
    return matching_job_id(queue, connection, FETCH_JOB_NAME) is not None


def embed_job_exists(queue: Queue, connection: Redis, ignore_job_ids: set[str] | None = None) -> bool:
    return matching_job_id(
        queue,
        connection,
        EMBED_JOB_NAME,
        ignore_job_ids=ignore_job_ids,
    ) is not None


def job_exists(queue: Queue, connection: Redis, job_name: str, ignore_job_ids: set[str] | None = None) -> bool:
    return (
        matching_job_id(
            queue,
            connection,
            job_name,
            ignore_job_ids=ignore_job_ids,
        )
        is not None
    )


def matching_job_id(
    queue: Queue,
    connection: Redis,
    job_name: str,
    ignore_job_ids: set[str] | None = None,
) -> str | None:
    ignored = ignore_job_ids or set()
    for job in queue.jobs:
        if getattr(job, "id", "") in ignored:
            continue
        if job.func_name == job_name:
            return str(job.id)

    active_workers = active_worker_names(queue, connection)
    for job_id in wip_job_ids(queue, connection):
        if job_id in ignored:
            continue
        try:
            job = Job.fetch(job_id, connection=connection)
        except NoSuchJobError:
            continue
        if job_worker_is_stale(job, active_workers):
            continue
        if job.func_name == job_name:
            return job_id

    registry = StartedJobRegistry(queue.name, connection=connection)
    for registry_id in registry.get_job_ids():
        job_id = registry_id.split(":", 1)[0]
        if job_id in ignored:
            continue
        try:
            job = Job.fetch(job_id, connection=connection)
        except NoSuchJobError:
            continue
        if job_worker_is_stale(job, active_workers):
            continue
        if job.func_name == job_name:
            return job_id
    return None


def active_worker_names(queue: Queue, connection: Redis) -> set[str]:
    smembers = getattr(connection, "smembers", None)
    exists = getattr(connection, "exists", None)
    hgetall = getattr(connection, "hgetall", None)
    if not callable(smembers) or not callable(exists):
        return set()
    try:
        worker_keys = smembers(f"rq:workers:{queue.name}")
    except Exception:
        logger.exception("读取 RQ worker 集合失败")
        return set()
    names: set[str] = set()
    for raw_key in worker_keys:
        key = raw_key.decode("utf-8", "ignore") if isinstance(raw_key, bytes) else str(raw_key)
        try:
            if not exists(key):
                continue
        except Exception:
            continue
        if callable(hgetall):
            try:
                if worker_metadata_is_stale(hgetall(key)):
                    continue
            except Exception:
                logger.exception("读取 RQ worker 状态失败")
                continue
        names.add(key.rsplit(":", 1)[-1])
    return names


def worker_metadata_is_stale(raw_metadata: dict[object, object]) -> bool:
    metadata = {decode_redis_value(key): decode_redis_value(value) for key, value in raw_metadata.items()}
    if metadata.get("shutdown_requested_date"):
        return True
    last_heartbeat = parse_rq_datetime(metadata.get("last_heartbeat", ""))
    if last_heartbeat is None:
        return False
    return (datetime.now(timezone.utc) - last_heartbeat).total_seconds() > WORKER_HEARTBEAT_STALE_SECONDS


def decode_redis_value(value: object) -> str:
    return value.decode("utf-8", "ignore") if isinstance(value, bytes) else str(value)


def parse_rq_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def job_worker_is_stale(job: Job, active_workers: set[str]) -> bool:
    worker_name = getattr(job, "worker_name", "") or ""
    return bool(worker_name and active_workers and worker_name not in active_workers)


def wip_job_ids(queue: Queue, connection: Redis) -> list[str]:
    zrange = getattr(connection, "zrange", None)
    if not callable(zrange):
        return []
    try:
        raw_ids = zrange(f"rq:wip:{queue.name}", 0, -1)
    except Exception:
        logger.exception("读取 RQ wip 任务失败")
        return []
    ids: list[str] = []
    for raw_id in raw_ids:
        value = raw_id.decode("utf-8", "ignore") if isinstance(raw_id, bytes) else str(raw_id)
        ids.append(value.split(":", 1)[0])
    return ids


def enqueue_fetch_job_details(
    connection: Redis | None = None,
) -> tuple[bool, str]:
    connection = connection or Redis.from_url(settings.redis_url)
    queue = Queue(FETCH_QUEUE_NAME, connection=connection)
    lock_factory = getattr(connection, "lock", None)
    enqueue_lock = (
        lock_factory(FETCH_ENQUEUE_LOCK_NAME, timeout=10, blocking_timeout=5)
        if callable(lock_factory)
        else nullcontext()
    )
    with enqueue_lock:
        existing_job_id = matching_job_id(queue, connection, FETCH_JOB_NAME)
        if existing_job_id is not None:
            logger.info("已有 RSS 抓取任务排队或运行，跳过本轮定时入队")
            remember_active_refresh_job(connection, existing_job_id)
            return False, existing_job_id
        job = queue.enqueue(
            FETCH_JOB_NAME,
            job_timeout=settings.rq_job_timeout_seconds,
            result_ttl=FETCH_RESULT_TTL_SECONDS,
        )
        job_id = str(getattr(job, "id", "") or "")
        remember_active_refresh_job(connection, job_id)
        return True, job_id


def enqueue_fetch_job() -> bool:
    queued, _job_id = enqueue_fetch_job_details()
    return queued


def enqueue_source_fetch_job(source_id: int) -> str:
    job = Queue(
        FETCH_QUEUE_NAME,
        connection=Redis.from_url(settings.redis_url),
    ).enqueue(
        SOURCE_FETCH_JOB_NAME,
        source_id,
        job_timeout=settings.rq_job_timeout_seconds,
        result_ttl=FETCH_RESULT_TTL_SECONDS,
    )
    return str(job.id)


def begin_fetch_refresh(session: Session) -> tuple[bool, str]:
    connection = Redis.from_url(settings.redis_url)
    active_job_id = active_refresh_job_id(connection)
    if active_job_id:
        try:
            status = fetch_refresh_status(
                session,
                active_job_id,
                connection=connection,
            )
        except (NoSuchJobError, ValueError):
            status = "failed"
        if status == "running":
            return False, active_job_id
        forget_active_refresh_job(connection, active_job_id)

    queued, job_id = enqueue_fetch_job_details(connection)
    if not job_id:
        raise RuntimeError("RSS 抓取任务缺少可跟踪 ID")
    return queued, job_id


def active_refresh_job_id(connection: Redis) -> str:
    getter = getattr(connection, "get", None)
    if not callable(getter):
        return ""
    raw_job_id = getter(ACTIVE_REFRESH_JOB_KEY)
    return decode_redis_value(raw_job_id) if raw_job_id else ""


def remember_active_refresh_job(connection: Redis, job_id: str) -> None:
    setter = getattr(connection, "set", None)
    if not callable(setter) or not job_id:
        return
    setter(
        ACTIVE_REFRESH_JOB_KEY,
        job_id,
        ex=FETCH_RESULT_TTL_SECONDS,
        nx=True,
    )


def forget_active_refresh_job(connection: Redis, job_id: str) -> None:
    getter = getattr(connection, "get", None)
    deleter = getattr(connection, "delete", None)
    if not callable(getter) or not callable(deleter):
        return
    current = getter(ACTIVE_REFRESH_JOB_KEY)
    if current and decode_redis_value(current) == job_id:
        deleter(ACTIVE_REFRESH_JOB_KEY)


def fetch_refresh_status(
    session: Session,
    job_id: str,
    *,
    connection: Redis | None = None,
) -> str:
    connection = connection or Redis.from_url(settings.redis_url)
    job = Job.fetch(job_id, connection=connection)
    if job.func_name != FETCH_JOB_NAME:
        raise ValueError("不是 RSS 抓取任务")

    raw_status = job.get_status(refresh=True)
    status = str(getattr(raw_status, "value", raw_status))
    if status in {"queued", "deferred", "scheduled"}:
        return "running"
    if status == "started":
        queue = Queue(FETCH_QUEUE_NAME, connection=connection)
        last_heartbeat = getattr(job, "last_heartbeat", None)
        heartbeat_is_stale = (
            isinstance(last_heartbeat, datetime)
            and (
                datetime.now(timezone.utc)
                - (
                    last_heartbeat
                    if last_heartbeat.tzinfo is not None
                    else last_heartbeat.replace(tzinfo=timezone.utc)
                )
            ).total_seconds()
            > WORKER_HEARTBEAT_STALE_SECONDS
        )
        if not heartbeat_is_stale and not job_worker_is_stale(
            job,
            active_worker_names(queue, connection),
        ):
            return "running"
        status = "failed"

    (
        item_ids,
        embedding_model,
        imported,
        attempted_sources,
        successful_sources,
        fetch_failed,
    ) = fetch_job_result(job)
    completed_items, pending_articles = refresh_item_counts(
        session,
        item_ids,
        embedding_model,
    )
    has_success = imported > 0 or completed_items > 0 or successful_sources > 0
    if pending_articles:
        embedding_queue = Queue(LLM_QUEUE_NAME, connection=connection)
        if embed_job_exists(embedding_queue, connection):
            return "running"
        return "complete" if has_success else "failed"
    if status in {"failed", "stopped", "canceled"} and not has_success:
        return "failed"
    if (fetch_failed or attempted_sources > 0) and not has_success:
        return "failed"
    return "complete"


def fetch_job_result(job: Job) -> tuple[list[int], str, int, int, int, bool]:
    raw_result = getattr(job, "result", None)
    raw_meta = getattr(job, "meta", None)
    result = raw_result if isinstance(raw_result, dict) else {}
    meta = raw_meta if isinstance(raw_meta, dict) else {}
    raw_ids = result.get("item_ids", meta.get("item_ids", []))
    item_ids = [
        int(item_id)
        for item_id in raw_ids
        if isinstance(item_id, int) and item_id > 0
    ]
    embedding_model = str(
        result.get("embedding_model", meta.get("embedding_model", ""))
    ).strip()
    raw_imported = result.get("imported", meta.get("imported", 0))
    imported = int(raw_imported) if isinstance(raw_imported, int) else 0
    raw_attempted = result.get(
        "attempted_sources",
        meta.get("attempted_sources", 0),
    )
    attempted_sources = raw_attempted if isinstance(raw_attempted, int) else 0
    raw_successful = result.get(
        "successful_sources",
        meta.get("successful_sources", 0),
    )
    successful_sources = raw_successful if isinstance(raw_successful, int) else 0
    fetch_failed = bool(
        result.get("fetch_failed", meta.get("fetch_failed", False))
    )
    return (
        list(dict.fromkeys(item_ids)),
        embedding_model,
        imported,
        attempted_sources,
        successful_sources,
        fetch_failed,
    )


def refresh_item_counts(
    session: Session,
    item_ids: list[int],
    embedding_model: str,
) -> tuple[int, int]:
    if not item_ids:
        return 0, 0
    rows = session.execute(
        select(
            ContentItem.id,
            Source.status,
            Source.media_type,
            ContentItem.embedding_vector,
            ContentItem.embedding_model,
        )
        .join(Source, Source.id == ContentItem.source_id)
        .where(ContentItem.id.in_(item_ids))
    ).all()
    clustered_ids = set(
        session.scalars(
            select(ClusterItem.content_item_id).where(
                ClusterItem.content_item_id.in_(item_ids)
            )
        ).all()
    )
    completed = 0
    pending = 0
    for item_id, source_status, media_type, vector, item_model in rows:
        if media_type != "article" or source_status != "active":
            completed += 1
            continue
        has_vector = vector is not None and str(vector) != ""
        model_matches = (
            item_model == embedding_model if embedding_model else bool(item_model)
        )
        if has_vector and model_matches and item_id in clustered_ids:
            completed += 1
        else:
            pending += 1
    return completed, pending


def enqueue_embedding_backfill(*, ignore_current_job: bool = False) -> bool:
    connection = Redis.from_url(settings.redis_url)
    queue = Queue(LLM_QUEUE_NAME, connection=connection)
    ignored: set[str] = set()
    if ignore_current_job:
        try:
            current = get_current_job()
        except TypeError:
            current = get_current_job(connection=connection)
        if current is not None and current.id:
            ignored.add(current.id)
    if embed_job_exists(queue, connection, ignore_job_ids=ignored):
        logger.info("已有 embedding 回填任务排队或运行，跳过本轮入队")
        return False
    queue.enqueue(
        EMBED_JOB_NAME,
        job_timeout=settings.rq_job_timeout_seconds,
    )
    return True


def schedule_fetch_jobs() -> None:
    while True:
        queued = False
        try:
            run_scheduled_generation_retention()
        except Exception:
            logger.exception("Generation 保留清理失败")
        try:
            queued = enqueue_fetch_job()
        except Exception:
            logger.exception("定时 RSS 抓取入队失败")
        delay = settings.rss_fetch_interval_seconds if queued else min(settings.rss_fetch_interval_seconds, 30)
        time.sleep(delay)


def start_fetch_scheduler() -> None:
    Thread(target=schedule_fetch_jobs, daemon=True).start()


def worker_queues_for_role(role: str) -> list[str]:
    if role == "fetch":
        return [FETCH_QUEUE_NAME]
    if role in {"llm", "embedding"}:
        return [LLM_QUEUE_NAME]
    return [FETCH_QUEUE_NAME, LLM_QUEUE_NAME]


def run_worker(role: str | None = None) -> None:
    prepare_runtime_database()
    selected_role = (
        role
        if role is not None
        else os.getenv("READER_WORKER_ROLE", "all").strip().lower() or "all"
    )
    if selected_role in {"all", "fetch"}:
        start_fetch_scheduler()
    worker = Worker(
        worker_queues_for_role(selected_role),
        connection=Redis.from_url(settings.redis_url),
    )
    worker.work()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run_worker()
